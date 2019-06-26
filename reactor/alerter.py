import os
import json
import socket
import smtplib
import requests
import subprocess
import sys
from texttable import Texttable
from email.mime.text import MIMEText
from email.utils import formatdate
from reactor.exceptions import ReactorException
from reactor.loader import Rule
from reactor.util import load_yaml, dots_get, resolve_string, reactor_logger


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if hasattr(o, 'isoformat'):
            return o.isoformat()
        else:
            return super(DateTimeEncoder, self).default(o)


class BasicMatchString(object):
    """ Creates a string containing fields in match for the given rule. """

    def __init__(self, rule: Rule, match):
        self.rule = rule
        self.match = match

    def _ensure_new_line(self):
        while self.text[-2:] != '\n\n':
            self.text += '\n'

    def _add_custom_alert_text(self):
        missing = self.rule.conf('alert_missing_value', '<MISSING VALUE>')
        alert_text = str(self.rule.conf('alert_text', ''))
        if self.rule.conf('alert_text_args'):
            alert_text_args = self.rule.conf('alert_text_args')
            alert_text_values = [dots_get(self.match, arg) for arg in alert_text_args]

            # Support referencing other top-level rule properties
            # This technically may not work if there is a top-level rule property with the same name
            # as an es result key, since it would have been matched in the lookup_es_key call above
            for i, text_value in enumerate(alert_text_values):
                if text_value is None:
                    alert_value = self.rule.conf(alert_text_args[i])
                    if alert_value:
                        alert_text_values[i] = alert_value

            alert_text_values = [missing if val is None else val for val in alert_text_values]
            alert_text = alert_text.format(*alert_text_values)
        elif self.rule.conf('alert_text_kw'):
            kw = {}
            for name, kw_name in self.rule.conf('alert_text_kw').items():
                val = dots_get(self.match, name)

                # Support referencing other top-level rule properties
                # This technically may not work if there is a top-level rule property with the same name
                # as an es result key, since it would have been matched in the lookup_es_key call above
                if val is None:
                    val = self.rule.conf(name)

                kw[kw_name] = missing if val is None else val
            alert_text = alert_text.format(**kw)

        self.text += alert_text

    def _add_rule_text(self):
        self.text += self.rule.type.get_match_str(self.match)

    def _add_top_counts(self):
        for key, counts in self.match.items():
            if key.startswith('top_events_'):
                self.text += '%s:\n' % (key[11:])
                top_events = counts.items()

                if not top_events:
                    self.text += 'No events found.\n'
                else:
                    top_events.sort(key=lambda x: x[1], reverse=True)
                    for term, count in top_events:
                        self.text += '%s: %s\n' % (term, count)

                self.text += '\n'

    def _add_match_items(self):
        for key, value in sorted(self.match.items(), key=lambda x: x[0]):
            if key.startswith('top_events_'):
                continue
            value_str = str(value)
            value_str.replace('\\n', '\n')
            if type(value) in [list, dict]:
                try:
                    value_str = self._pretty_print_as_json(value)
                except TypeError:
                    # Non serializable object, fallback to str
                    pass
            self.text += '%s: %s\n' % (key, value_str)

    @staticmethod
    def _pretty_print_as_json(blob):
        try:
            return json.dumps(blob, cls=DateTimeEncoder, sort_keys=True,
                              indent=4, ensure_ascii=False)
        except UnicodeDecodeError:
            # This blob contains non-unicode, so lets pretend it's Latin-1 to show something
            return json.dumps(blob, cls=DateTimeEncoder, sort_keys=True,
                              indent=4, encoding='Latin-1', ensure_ascii=False)

    def __str__(self):
        self.text = ''
        if not self.rule.conf('alert_text'):
            self.text += self.rule.name + '\n\n'

        self._add_custom_alert_text()
        self._ensure_new_line()
        if self.rule.conf('alert_text_type') != 'alert_text_only':
            self._add_rule_text()
            self._ensure_new_line()
            if self.rule.conf('top_count_keys'):
                self._add_top_counts()
            if self.rule.conf('alert_text_type') != 'exclude_fields':
                self._add_match_items()
        return self.text


class Alerter(object):

    def __init__(self, rule: Rule, conf: dict):
        self.rule = rule
        self.conf = conf
        # pipeline object is created by Client.send_alert()
        # and attached to each reactor used by a rule before calling alert()
        self.pipeline = None
        self.aggregation_summary_text_maximum_width = 80

    def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
        """
        Send an alert. alert_body a copy of the alert_body that will be stored in ElasticSearch.
        :param alerts: A list of alerts to be sent
        :param silenced: True if the alerts are silenced
        :param publish: True if alerter should perform this in debug mode
        :type alerts: list[dict]
        """
        raise NotImplementedError()

    def get_info(self):
        """ Returns a dictionary of data related to this alert. At minimum, this should contain
        a field type corresponding to the type of Alerter. """
        return {'type': 'Unknown'}

    def create_alert_title(self, alerts):
        """
        Creates custom alert title to be used, e.g. as an e-mail subject.
        :param alerts: A list of dictionaries of relevant information to the alert.
        """
        if not self.rule.conf('alert_subject'):
            return self.rule.name

        alert_subject = str(self.rule.conf('alert_subject'))

        if self.rule.conf('alert_subject_args'):
            alert_subject_args = self.rule.conf('alert_subject_args')
            alert_subject_values = [dots_get(alerts[0]['match_body'], arg) for arg in alert_subject_args]

            # Support referencing other top-level rule properties
            # This technically may not work if there is a top-level rule property with the same name
            # as an es result key, since it would have been matched in the lookup_es_key call above
            for i, subject_value in enumerate(alert_subject_values):
                if subject_value is None:
                    alert_value = self.rule.conf(alert_subject_args[i])
                    if alert_value:
                        alert_subject_values[i] = alert_value

            missing = self.rule.conf('alert_missing_value', '<MISSING VALUE>')
            alert_subject_values = [missing if val is None else val for val in alert_subject_values]
            return alert_subject.format(*alert_subject_values)

        return alert_subject

    def create_alert_body(self, alerts):
        body = self.get_aggregation_summary_text(alerts)
        if self.rule.conf('alert_text_type') != 'aggregation_summary_only':
            for alert in alerts:
                body += str(BasicMatchString(self.rule, alert['match_body']))
                # Separate text of aggregated alerts with dashes
                if len(alerts) > 1:
                    body += '\n' + ('-'*20) + '\n'
        return body

    def get_aggregation_summary_text(self, alerts):
        text = ''
        if self.rule.conf('aggregation') and self.rule.conf('summary_table_fields'):
            text = self.rule.conf('summary_prefix', '')
            summary_table_fields = self.rule.conf('summary_table_fields')
            if not isinstance(summary_table_fields, list):
                summary_table_fields = [summary_table_fields]
            # Include count aggregation so that we can see at a glance how many of each aggregation_key were encountered
            summary_table_fields_with_count = summary_table_fields + ['count']
            text += "Aggregation resulted in the following data for summary_table_fields ==> {0}:\n\n".format(
                summary_table_fields_with_count
            )
            text_table = Texttable(max_width=self.aggregation_summary_text_maximum_width)
            text_table.header(summary_table_fields_with_count)
            # Format all fields as 'text' to avoid long numbers being shown as scientific notation
            text_table.set_cols_dtype(['t' for _ in summary_table_fields_with_count])
            match_aggregation = {}

            # Maintain an aggregate count for each unique key encountered in the aggregation period
            for alert in alerts:
                key_tuple = tuple([str(dots_get(alert['match_body'], key)) for key in summary_table_fields])
                if key_tuple not in match_aggregation:
                    match_aggregation[key_tuple] = 1
                else:
                    match_aggregation[key_tuple] = match_aggregation[key_tuple] + 1
            for keys, count in match_aggregation.items():
                text_table.add_row([key for key in keys] + [count])
            text += text_table.draw() + '\n\n'
            text += self.rule.conf('summary_prefix', '')
        return str(text)

    def get_account(self, account_file: str) -> (str, str):
        """ Gets the username and password from an account file.

        :param account_file: Path to the file which contains user and password information.
        It can be either an absolute file path or one that is relative to the given rule.
        """
        if os.path.isabs(account_file):
            account_file_path = account_file
        else:
            account_file_path = os.path.join(os.path.dirname(self.rule.conf('rule_file')), account_file)
        account_conf = load_yaml(account_file_path)
        if 'user' not in account_conf or 'password' not in account_conf:
            raise ReactorException('Account file must have user and password fields')

        return account_conf['user'], account_conf['password']


class DebugAlerter(Alerter):
    """ The debug alerter uses a Python logger (by default, alerting to terminal). """

    def __init__(self, *args):
        super(DebugAlerter, self).__init__(*args)

    def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
        if silenced:
            return

        qk = self.rule.conf('query_key', None)
        ts_field = self.rule.conf('timestamp_field')
        for alert in alerts:
            match_body = alert['match_body']
            if qk and qk in match_body:
                reactor_logger.log(self.conf['level'], 'Alert for %s, %s at %s:',
                                   self.rule.name, match_body[qk], dots_get(match_body, ts_field))
            else:
                reactor_logger.log(self.conf['level'], 'Alert for %s at %s:',
                                   self.rule.name, dots_get(match_body, ts_field))

            reactor_logger.log(self.conf['level'], str(BasicMatchString(self.rule, match_body)))

    def get_info(self):
        return {'type': 'debug',
                'level': self.conf['level']}


class TestAlerter(Alerter):
    """ The test alerter uses a Python logger (by default, alerting to terminal). """
    mode = 'w'

    def __init__(self, *args):
        super(TestAlerter, self).__init__(*args)

    def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
        formatted_str = []
        for alert in alerts:
            alert['silenced'] = silenced
            if self.conf['format'] == 'json':
                formatted_str.append(json.dumps(alert, cls=DateTimeEncoder, sort_keys=False, indent=None))
            else:
                formatted_str.append(str(BasicMatchString(self.rule, alert['match_body'])) + ('-' * 80))

        # Print the formatted alerts
        if self.conf['output'] == 'stdout':
            print(*formatted_str, sep='\n', end='\n', file=sys.stdout)
        elif self.conf['output'] == 'stderr':
            print(*formatted_str, sep='\n', end='\n', file=sys.stderr)
        else:
            with open(self.conf['output'], self.mode) as f:
                f.writelines([line + '\n' for line in formatted_str])
            # Now that we have written at least one set of alerts, switch to append mode
            self.mode = 'a'

    def get_info(self):
        return {'type': 'test',
                'format': self.conf['format'],
                'output': self.conf['output']}


class EmailAlerter(Alerter):

    def __init__(self, *args):
        super(EmailAlerter, self).__init__(*args)

        self.smtp_host = self.conf.get('smtp_host', 'localhost')
        self.smtp_port = self.conf.get('smtp_port', 0)
        self.smtp_ssl = self.conf.get('smtp_ssl', False)
        self.from_addr = self.conf.get('from_addr', 'Alerter')
        if self.conf.get('smtp_auth_file'):
            self.user, self.password = self.get_account(self.conf.get('smtp_auth_file'))
        self.smtp_key_file = self.conf.get('smtp_key_file')
        self.smtp_cert_file = self.conf.get('smtp_cert_file')

        # Convert email to a list if it isn't already
        if isinstance(self.conf.get('to'), str):
            self.conf['to'] = [self.conf.get('to')]
        # If there is a cc/bcc then also convert it to a list
        if self.conf.get('cc') and isinstance(self.conf.get('cc'), str):
            self.conf['cc'] = [self.conf.get('cc')]
        if self.conf.get('bcc') and isinstance(self.conf.get('bcc'), str):
            self.conf['bcc'] = [self.conf.get('bcc')]

        # Ensure email add domain starts with an @
        if self.conf.get('add_domain') and not self.conf.get('add_domain').startswith('@'):
            self.conf['add_domain'] = '@' + self.conf.get('add_domain')

    def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
        if silenced or not publish:
            return

        body = self.create_alert_body(alerts)

        to_addr = self.conf['to']
        if 'from_field' in self.conf:
            recipient = dots_get(alerts[0]['match_body'], self.conf['from_field'])
            if isinstance(recipient, str):
                if '@' in recipient:
                    to_addr = [recipient]
                elif 'add_domain' in self.conf:
                    to_addr = [recipient + self.conf['add_domain']]
            elif isinstance(recipient, list):
                to_addr = recipient
                if 'add_domain' in self.conf:
                    to_addr = [name + self.conf['add_domain'] for name in to_addr]
        if self.conf.get('email_format') == 'html':
            email_msg = MIMEText(body.encode('UTF-8'), 'html', _charset='UTF-8')
        else:
            email_msg = MIMEText(body.encode('UTF-8'), _charset='UTF-8')
        email_msg['Subject'] = self.create_alert_title(alerts)
        email_msg['To'] = ', '.join(to_addr)
        email_msg['From'] = self.from_addr
        email_msg['Reply-To'] = self.conf.get('reply_to', email_msg['To'])
        email_msg['Date'] = formatdate()
        if self.conf.get('cc'):
            email_msg['CC'] = ','.join(self.conf['cc'])
            to_addr = to_addr + self.conf['cc']
        if self.conf.get('bcc'):
            to_addr = to_addr + self.conf['bcc']

        smtp = None
        try:
            if self.smtp_ssl:
                smtp = smtplib.SMTP_SSL(self.smtp_host, port=self.smtp_port,
                                        keyfile=self.smtp_key_file, certfile=self.smtp_cert_file)
            else:
                smtp = smtplib.SMTP(self.smtp_host, port=self.smtp_port)
                smtp.ehlo()
                if smtp.has_extn('STARTTLS'):
                    smtp.starttls(keyfile=self.smtp_key_file, certfile=self.smtp_cert_file)
            if 'smtp_auth_file' in self.conf:
                smtp.login(self.user, self.password)

            smtp.sendmail(self.from_addr, to_addr, email_msg.as_string())
            smtp.close()

        except (smtplib.SMTPException, socket.error) as e:
            raise ReactorException("Error connecting to SMTP host: %s" % e)
        except smtplib.SMTPAuthenticationError as e:
            raise ReactorException("SMTP username/password rejected: %s" % e)
        finally:
            if smtp:
                smtp.close()

        reactor_logger.info("Sent email to %s" % to_addr)

    def get_info(self):
        return {'type': 'email',
                'recipients': self.conf['to']}


class WebhookAlerter(Alerter):

    def __init__(self, *args):
        super(WebhookAlerter, self).__init__(*args)
        urls = self.conf.get('url')
        if isinstance(urls, str):
            urls = [urls]
        self.urls = urls
        self.verb = self.conf.get('verb', 'POST').upper()
        self.proxy = self.conf.get('proxy')
        self.dynamic_payload = self.conf.get('dynamic_payload', None)
        self.static_payload = self.conf.get('static_payload', {})
        self.headers = self.conf.get('headers')
        self.timeout = self.conf.get('timeout', 10)

    def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
        if silenced or not publish:
            return

        for alert in alerts:
            payload = alert['match_body'] if self.dynamic_payload is None else {}
            payload.update(self.static_payload)
            for payload_key, match_body_key in self.dynamic_payload or {}:
                payload[payload_key] = dots_get(alert['match_body'], match_body_key)
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json;charset=utf-8',
            }
            headers.update(self.headers)
            proxies = {'https': self.proxy} if self.proxy else None
            for url in self.urls:
                try:
                    response = requests.request(self.verb, url, data=json.dumps(payload), cls=DateTimeEncoder,
                                                headers=headers, proxies=proxies, timeout=self.timeout)
                    response.raise_for_status()
                except requests.RequestException as e:
                    raise ReactorException('Error %sing Webhook alert: %s' % (self.verb, e))
            reactor_logger.info('Webhook Alert sent')

    def get_info(self):
        return {'type': 'webhook',
                'urls': self.urls}


class CommandAlerter(Alerter):

    def __init__(self, *args):
        super(CommandAlerter, self).__init__(*args)

        self.last_command = []
        self.shell = False
        if isinstance(self.conf.get('command'), str):
            self.shell = True
            if '%' in self.conf.get('command'):
                reactor_logger.warning('Warning! You could be vulnerable to shell injection!')
            self.conf['command'] = [self.conf['command']]

    def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
        if silenced or not publish:
            return

        # Format the command and arguments
        try:
            command = [resolve_string(command_arg, alerts[0]['match_body']) for command_arg in self.conf['command']]
            self.last_command = command
        except KeyError as e:
            raise ReactorException('Error formatting command: %s' % e)

        # Run command and pipe data
        try:
            subp = subprocess.Popen(command, stdin=subprocess.PIPE, shell=self.shell)

            if self.conf.get('pipe_format') == 'json':
                match_json = json.dumps(alerts, cls=DateTimeEncoder) + '\n'
                _, _ = subp.communicate(input=match_json)
            elif self.conf.get('pipe_format') == 'plain':
                alert_text = self.create_alert_body(alerts)
                _, _ = subp.communicate(input=alert_text)
            if self.conf.get('fail_on_non_zero_exit', False) and subp.wait():
                raise ReactorException('Non-zero exit code while running command %s' % (' '.join(command)))
        except OSError as e:
            raise ReactorException('Error while running command %s: %s' % (' '.join(command), e))

    def get_info(self):
        return {'type': 'command',
                'command': ' '.join(self.last_command)}
