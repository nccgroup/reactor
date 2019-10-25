import os
import smtplib
import socket
from email.mime.text import MIMEText
from email.utils import formatdate

from .exceptions import ReactorException
from .util import load_yaml, reactor_logger


def get_account(account_file: str, relative_file: str = None) -> (str, str):
    """
    Gets the username and password from an account file.
    It can be either an absolute file path or one that is relative to ``relative_file``.

    :param account_file: Path to the file which contains user and password information.
    :param relative_file: Path to a file which `account_file` is relative
    """
    if os.path.isabs(account_file):
        account_file_path = account_file
    elif relative_file:
        account_file_path = os.path.join(os.path.dirname(relative_file), account_file)
    else:
        account_file_path = account_file
    account_conf = load_yaml(account_file_path)
    if 'user' not in account_conf or 'password' not in account_conf:
        raise ReactorException('Account file must have user and password fields')

    return account_conf['user'], account_conf['password']


class BaseNotifier(object):
    """
    The base class for all notifiers used by Reactor.
    Notifier is used to inform Reactor administrator of important events such as uncaught exceptions
    raised during the execution of a rule.

    :param conf: Configuration for the notifier
    """

    def __init__(self, conf: dict):
        self.conf = conf

    def notify(self, subject: str, body: str) -> None:
        """
        Trigger a notification to be fired with ``subject`` and ``body``.

        :param subject: Subject of the notification
        :param body: Body of the notification
        """
        raise NotImplementedError()


class EmailNotifier(BaseNotifier):
    def __init__(self, conf: dict):
        super().__init__(conf)

        self.smtp_host = self.conf.get('smtp_host', 'localhost')
        self.smtp_port = self.conf.get('smtp_port', 0)
        self.smtp_ssl = self.conf.get('smtp_ssl', False)
        self.from_addr = self.conf.get('from_addr', 'Alerter')
        if self.conf.get('smtp_auth_file'):
            self.user, self.password = get_account(self.conf.get('smtp_auth_file'))
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

    def notify(self, subject: str, body: str) -> None:
        to_addr = self.conf['to']
        if self.conf.get('format') == 'html':
            email_msg = MIMEText(body.encode('UTF-8'), 'html', _charset='UTF-8')
        else:
            email_msg = MIMEText(body.encode('UTF-8'), _charset='UTF-8')
        email_msg['Subject'] = subject
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
