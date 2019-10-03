import datetime
import itertools
import logging
import multiprocessing
import multiprocessing.managers
import pytz
import secrets
import sys
import threading
import time
import traceback
from typing import Optional

import apscheduler.events
import apscheduler.schedulers.background
import apscheduler.triggers.interval
import apscheduler.executors.pool
import croniter
import elasticsearch.helpers
import reactor.kibana
import reactor.raft
import reactor.rule
from elasticsearch import Elasticsearch
from reactor.exceptions import ReactorException, QueryException
from reactor.loader import Rule, RuleLoader
from reactor.util import (
    reactor_logger,
    dt_now, dt_to_ts, ts_to_dt, unix_to_dt, dt_to_unix, pretty_ts,
    dots_get,
    elasticsearch_client,
)

_authkey = secrets.token_urlsafe(32)


class Reactor(object):
    MAX_TERMINATE_CALLED = 3

    def __init__(self, conf: dict, args: dict):
        self.mode = args.get('mode', 'default')

        if self.mode == 'debug':
            reactor_logger.setLevel(min(reactor_logger.level, logging.INFO))
            reactor_logger.info('Note: In debug mode, alerts will be logged to console but NOT actually sent.')

        if args.get('es_debug', False):
            logging.getLogger('elasticsearch').setLevel(logging.WARNING)
        if args.get('es_debug_trace', False):
            tracer = logging.getLogger('elasticsearch.trace')
            tracer.setLevel(logging.WARNING)
            tracer.addHandler(logging.FileHandler(args['es_debug_trace']))

        self.conf = conf
        self.args = args
        self.loader = conf['loader']  # type: RuleLoader

        self.es_client = elasticsearch_client(conf['elasticsearch'])
        self.writeback_index = conf['index']
        self.alert_alias = conf['alert_alias']

        self.start_time = args.get('start', dt_now())
        self.scheduler = apscheduler.schedulers.background.BackgroundScheduler()
        self.terminate_called = 0
        self.core_pid = multiprocessing.current_process().pid
        self.max_processpool = max(1, min(multiprocessing.cpu_count(), self.conf['max_processpool'] or float('inf')))

        self.core = Core(conf, args)
        self.raft = None

        # Establish a default setting for the cluster
        self.conf.setdefault('cluster', {'host': 'localhost:7000', 'neighbours': []})
        self.raft = reactor.raft.RaftNode(tuple(self.conf['cluster']['host'].split(":")),
                                          [tuple(n.split(":")) for n in self.conf['cluster']['neighbours']])
        self.raft.meta['cpu_count'] = self.max_processpool
        self.raft.meta['executing'] = []
        self.cluster = {'leader': None,
                        'rules': []}

        self._configure_schedule()

    @property
    def running(self):
        """ Returns whether Reactor core has been started and not shutdown. """
        return self.scheduler.running

    def _configure_schedule(self):
        jobstores = {
            'internal': {'type': 'memory'},
            'default': {'type': 'memory'},
        }
        executors = {
            'default': {'type': 'threadpool', 'max_workers': 3},
            'processpool': apscheduler.executors.pool.ProcessPoolExecutor(max_workers=self.max_processpool),
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
        }
        self.scheduler.configure(jobstores=jobstores,
                                 executors=executors,
                                 job_defaults=job_defaults)
        self.scheduler.add_listener(self.listen_rule_execution,
                                    apscheduler.events.EVENT_JOB_SUBMITTED |
                                    apscheduler.events.EVENT_JOB_ERROR |
                                    apscheduler.events.EVENT_JOB_EXECUTED |
                                    apscheduler.events.EVENT_JOB_MAX_INSTANCES)

    def reset_rule_schedule(self, rule: Rule):
        # We hit the end of an execution schedule, pause ourselves until next run
        if rule.conf('limit_execution') and rule.data.next_start_time:
            self.scheduler.modify_job(job_id=rule.locator, next_run_time=rule.data.next_start_time)
            # If we are preventing covering non-scheduled time periods, reset min_start_time and previous_end_time
            if rule.data.next_min_start_time:
                rule.data.minimum_start_time = rule.data.next_min_start_time
                rule.data.previous_end_time = rule.data.next_min_start_time
            reactor_logger.info('Pausing %s until next run at %s', rule.name, pretty_ts(rule.data.next_start_time))

    def test_rule(self, rule: Rule, end_time, start_time=None):
        try:
            rule.data = self.core.run_rule(rule, end_time, start_time)
        except ReactorException as e:
            reactor_logger.error('Error running rule "%s": %s', rule.name, str(e))
        except Exception as e:
            self.handle_uncaught_exception(e, rule)
        else:
            reactor_logger.info('Ran from %s to %s "%s": %s query hits (%s already seen), %s matches, '
                                '%s alerts sent (%s silenced)',
                                pretty_ts(start_time, rule.conf('use_local_time')),
                                pretty_ts(end_time, rule.conf('use_local_time')),
                                rule.name,
                                rule.data.cumulative_hits,
                                rule.data.num_duplicates,
                                rule.data.num_matches,
                                rule.data.alerts_sent,
                                rule.data.alerts_silenced)

    def start(self):
        """ Periodically update rules and schedule to run. """
        if self.running:
            raise ReactorException('Reactor already running')

        # Ensure ElasticSearch is responsive
        if not self.wait_until_responsive(timeout=self.args['timeout']):
            return 1

        reactor_logger.info('ElasticSearch version: %s', self.es_client.es_version)
        reactor_logger.info('Starting up (max_processpool=%s cluster_size=%s)',
                            self.max_processpool,  1 + len(self.raft.neighbours))

        # Start the RAFT cluster
        self.raft.start()

        # Add internal jobs to the scheduler
        self.scheduler.add_job(self.handle_pending_alerts, 'interval',
                               seconds=self.conf['resend'].total_seconds(),
                               id='_internal_handle_pending_alerts',
                               jobstore='internal',
                               executor='default')
        self.scheduler.add_job(self.handle_config_changes, 'interval',
                               seconds=(self.args['reload'] or self.conf['reload']).total_seconds(),
                               id='_internal_handle_config_changes',
                               next_run_time=datetime.datetime.now(),
                               jobstore='internal',
                               executor='default')
        self.scheduler.start()

        while self.running:
            # If an end time was specified and it has elapsed
            if self.args['end'] and self.args['end'] < dt_now():
                # If the rule have been loaded and every run has been run once
                if self.loader.loaded and all([r.data.has_run_once for r in self.loader]):
                    reactor_logger.info('Reached end time, shutting down reactor')
                    self.stop()

            # Briefly sleep
            time.sleep(0.1)

        reactor_logger.info('Goodbye')
        return 0

    def terminate(self, signal_num, frame):
        """ Try safe ``stop`` for up to ``self.MAX_TERMINATE_CALLED`` times. """
        self.terminate_called += 1

        if self.terminate_called >= self.MAX_TERMINATE_CALLED:
            reactor_logger.critical('Terminating reactor')
            sys.exit(signal_num)

        elif self.core_pid == multiprocessing.current_process().pid:
            reactor_logger.info('Attempting normal shutdown')
            self.stop()

    def stop(self):
        """ Stop a running Reactor. """
        if self.running:
            reactor_logger.info('Waiting for running jobs to complete')

            self.scheduler.remove_all_jobs('internal')
            self.scheduler.remove_all_jobs('default')
            self.scheduler.shutdown()

            self.raft.shutdown()

    def wait_until_responsive(self, timeout: datetime.timedelta):
        """ Wait until ElasticSearch becomes responsive (or too much time passes). """
        timeout = timeout.total_seconds()

        # Don't poll unless we're asked to
        if timeout <= 0.0:
            return True

        # Periodically poll ElasticSearch. Keep going until ElasticSearch is responds
        ref = time.time()
        while (time.time() - ref) < timeout:
            try:
                if self.es_client.indices.exists(self.alert_alias):
                    return True
            except elasticsearch.ConnectionError:
                pass
            time.sleep(1.0)

        if self.es_client.ping():
            reactor_logger.error('Alert alias "%s" does not exist, did you run `reactor init`?', self.alert_alias)
        else:
            reactor_logger.error('Could not reach ElasticSearch at %s:%s',
                                 self.conf['elasticsearch']['host'], self.conf['elasticsearch']['port'])
        return False

    def handle_pending_alerts(self):
        if not self.running:
            return

        alerts_sent = self.core.send_pending_alerts()
        if alerts_sent > 0:
            reactor_logger.info('Sent %s pending alerts at %s', alerts_sent, pretty_ts(dt_now()))

    def handle_config_changes(self):
        if not self.running:
            return

        try:
            # If not already loaded or not pinned
            if not (self.loader.loaded and self.args['pin_rules']):
                # Load in/detect changes in the rules
                reactor_logger.log(logging.DEBUG if self.loader.loaded else logging.INFO, 'Loading rules')
                self.loader.load(self.args)
        except ReactorException as e:
            reactor_logger.error(str(e))
        else:
            # If the leadership of the cluster has changed
            if self.raft.leader != self.cluster['leader']:
                self.cluster['leader'] = self.raft.leader
                if not self.raft.has_leader():
                    reactor_logger.critical('No cluster leader!')
                else:
                    reactor_logger.info('Cluster leader elected: %s', self.raft.leader)

            # If we are the leader
            if self.raft.is_leader():
                self._distribute_workload()

            # Get our list of rules
            distributed_rules = []
            if self.raft.has_leader():
                meta = self.raft.neighbourhood_meta()[self.raft.leader]
                distributed_rules = meta.get('rules', {}).get(self.raft.address, [])
            if self.cluster['rules'] != distributed_rules:
                reactor_logger.info('Rule set updated: %s', distributed_rules)
            self.cluster['rules'] = distributed_rules

            # Remove removed rules from the scheduler
            for job in self.scheduler.get_jobs('default'):
                if job.id not in self.loader or job.id not in distributed_rules:
                    job.remove()

            # Add/modify rules in the scheduler
            for rule_locator in distributed_rules:
                if not self.running or rule_locator not in self.loader:
                    # TODO: rule_locator should always be in self.loader
                    continue

                rule = self.loader[rule_locator]
                rule.data.initial_start_time = self.start_time if not self.running else rule.data.initial_start_time

                # Determine the trigger for this rule
                trigger = apscheduler.triggers.interval.IntervalTrigger(seconds=rule.run_every.total_seconds(),
                                                                        jitter=1,
                                                                        start_date=datetime.datetime.now(),
                                                                        timezone=pytz.utc)
                if not self.scheduler.get_job(rule.locator):
                    # Add the rule to the scheduler
                    self.scheduler.add_job(self.core.handle_rule_execution,
                                           args=[rule],
                                           id=rule.locator,
                                           jobstore='default',
                                           executor='processpool',
                                           name=rule.name,
                                           next_run_time=datetime.datetime.now(),
                                           trigger=trigger)
                else:
                    # Add the rule to the scheduler
                    self.scheduler.modify_job(job_id=rule.locator,
                                              jobstore='default',
                                              args=[rule],
                                              name=rule.name,
                                              trigger=trigger)

    def _distribute_workload(self):
        """
        Devise how the rules should be distributed across the cluster. The distribution should be a dictionary
        mapping of cluster node addresses to a list of rule locators. The distribution should then be stored in
        ``self.raft.meta['rules']``, e.g.:


            self.raft.meta['rules'] = {('node1', 7000): ['rule_locator1', 'rule_locator2'],
                                       ('node2', 7000): ['rule_locator3'],
                                       ('node3', 7000): ['rule_locator4']}
        """
        # If all nodes in the cluster have reported in their cpu_count (they do this every message)
        if all(['cpu_count' in n.meta for n in self.raft.neighbours.values()]):
            # Determine worker pool
            workers = [self.raft.address] * self.raft.meta['cpu_count']
            for neighbour in self.raft.neighbours.values():
                # TODO: handle the case where not all neighbours have reported their cpu_count
                workers.extend([neighbour.address] * neighbour.meta['cpu_count'])
            worker_pool = itertools.cycle(sorted(workers))
            # Distribute the rules across the cluster
            distribution = {}
            for rule in sorted(self.loader, key=lambda r: r.locator):
                node = next(worker_pool)
                distribution.setdefault(node, [])
                distribution[node].append(rule.locator)
            # Round robin rules that are assigned to disconnected workers
            unavailable = [n.address for n in self.raft.neighbours.values() if n.failed_count > 0]
            for neighbour in unavailable:
                while len(distribution[neighbour]):
                    rule = distribution[neighbour].pop()
                    node = next(worker_pool)
                    while node in unavailable:
                        node = next(worker_pool)
                    distribution[node].append(rule)
            available = [n.address for n in self.raft.neighbours.values() if n.failed_count == 0]
            # Remove any that are being run by another node other than they are assigned
            for neighbour in available:
                for rule_locator in self.raft.neighbours[neighbour].meta['executing']:
                    for node in distribution:
                        if node != neighbour and rule_locator in distribution[node]:
                            distribution[node].remove(rule_locator)

            self.raft.meta['rules'] = distribution

    def silence(self, rule: Rule, duration: datetime.timedelta, revoke: bool = False):
        """ Silence an alert for a period of time. --silence and --rule must be passed as args. """

        # TODO: implement revoking silences (making sure to inform all running reactors of the change)
        reactor_logger.info('ElasticSearch version: %s', self.es_client.es_version)
        if self.core.set_realert(rule, '_silence', dt_now() + duration, 0):
            reactor_logger.warning('Silenced rule %s for %s', rule.name, duration)

    def listen_rule_execution(self, event):
        """ Listener of events from handler rule execution. """
        # Ignore non rule events
        if event.jobstore != 'default':
            return

        # Get the rule
        rule = self.loader[event.job_id]

        # If there are too many instances running
        if event.code == apscheduler.events.EVENT_JOB_MAX_INSTANCES:
            reactor_logger.warning('Execution time for "%s" longer than run_every (%s)', rule.name, rule.run_every)
            return

        # If the rule was submitted to the executor to be run, add the rule to the list of executing rules
        if event.code == apscheduler.events.EVENT_JOB_SUBMITTED:
            self.raft.meta['executing'].append(event.job_id)
            return

        # If there was an uncaught exception raised
        if event.code == apscheduler.events.EVENT_JOB_ERROR:
            self.handle_uncaught_exception(event.exception, rule)

        # If the rule successfully executed
        elif event.code == apscheduler.events.EVENT_JOB_EXECUTED:
            rule.data = event.retval

        # Remove the rule from the list of executing rules
        if event.job_id in self.raft.meta['executing']:
            self.raft.meta['executing'].remove(event.job_id)

        # Apply rules based on execution time limits
        self.reset_rule_schedule(rule)
        rule.data.next_start_time = None
        rule.data.next_min_start_time = None

    def handle_uncaught_exception(self, exception, rule):
        """ Disables a rule and sends a notification. """
        # reactor_logger.error(traceback.format_exc())
        self.core.handle_error('Uncaught exception running rule %s: %s' % (rule.name, exception), {'rule': rule.name})

        if rule.conf('disable_rule_on_error'):
            self.loader.disable(rule.locator)
            if self.running and self.scheduler.get_job(job_id=rule.locator):
                self.scheduler.remove_job(job_id=rule.locator)
            reactor_logger.info('Rule "%s" disabled', rule.name)
        # TODO: add notification
        # if self.conf['notify_email']:
        #     self.send_notification_email(self.conf['notify_email'], exception=exception, rule=rule)


class Core(object):
    thread_data = threading.local()

    def __init__(self, conf: dict, args: dict):
        self.mode = args.get('mode', 'default')
        self.conf = conf
        self.args = args
        self.loader = conf['loader']  # type: reactor.loader.RuleLoader

        self.es_client = elasticsearch_client(conf['elasticsearch'])
        self.writeback_index = conf['index']
        self.alert_alias = conf['alert_alias']
        self.alert_time_limit = conf['alert_time_limit']
        self.old_query_limit = conf['old_query_limit']
        self.max_aggregation = conf['max_aggregation']

    def get_writeback_index(self, doc_type: str, rule=None, match_body=None):
        """ In ElasticSearch >= 6.x, multiple doc types in a single index. """
        writeback_index = self.writeback_index
        if self.es_client.es_version_at_least(6):
            writeback_index += '_' + doc_type

        if rule is not None and rule.conf('writeback_suffix'):
            try:
                suffix = rule.conf('writeback_suffix').format(match_body or {})
                suffix = datetime.datetime.utcnow().strftime(suffix)
                writeback_index += '_' + suffix
            except KeyError as e:
                reactor_logger.critical('Failed to add suffix. Unknown key %s' % str(e))

        return writeback_index

    def writeback(self, doc_type, writeback_body, rule=None, doc_id=None, update=False) -> Optional[dict]:
        for key in writeback_body.keys():
            # Convert any datetime objects to timestamps
            if isinstance(writeback_body[key], datetime.datetime):
                writeback_body[key] = dt_to_ts(writeback_body[key])

        if '@timestamp' not in writeback_body:
            writeback_body['@timestamp'] = dt_to_ts(dt_now())

        index = self.get_writeback_index(doc_type, rule=rule, match_body=writeback_body.get('match_body'))
        if self.mode in ['debug', 'test']:
            reactor_logger.debug('Skipping writing to ElasticSearch "%s": %s' % (index, writeback_body))
            return None

        # Add the alert to the cache
        if rule and doc_type == 'alert':
            rule.data.alerts_cache[doc_id] = writeback_body

        try:
            doc_type = '_doc' if self.es_client.es_version_at_least(6) else doc_type
            # If there is a writeback cache available, cache the action for a later bulk request
            if hasattr(self.thread_data, 'writeback_cache'):
                action = 'update' if update else 'index'
                self.thread_data.writeback_cache.extend([
                    {action: {'_id': doc_id, '_index': index, '_type': doc_type}},
                    {'doc': writeback_body} if update else writeback_body
                ])
                # Automatically flush the writeback cache if limit is reached (2 items in list per action)
                if len(self.thread_data.writeback_cache) >= 2 * self.conf['writeback_flush']:
                    self.flush_writeback()
            elif update:
                return self.es_client.update(id=doc_id, index=index, doc_type=doc_type, body={'doc': writeback_body})
            else:
                return self.es_client.index(id=doc_id, index=index, doc_type=doc_type, body=writeback_body)
        except elasticsearch.ElasticsearchException as e:
            reactor_logger.exception('Error writing alert info to ElasticSearch: %s' % e)

    def flush_writeback(self):
        """ Flush the thread local `writeback_cache` and clear. """
        if hasattr(self.thread_data, 'writeback_cache') and self.thread_data.writeback_cache:
            self.es_client.bulk(self.thread_data.writeback_cache)
            self.thread_data.writeback_cache.clear()

    def get_top_counts(self, rule: Rule, start_time, end_time, keys, number=None, qk=None):
        """
        Counts the number of events for each unique value for each key field.
        Returns a diction with top_events_<key> mapped to the top 5 counts for each key.
        """
        all_counts = {}
        if not number:
            number = rule.conf('top_count_number', 5)
        for key in keys:
            index = rule.get_index(start_time, end_time)

            try:
                hits_terms = rule.get_hits_terms(start_time, end_time, index, key, qk, number)
            except QueryException as e:
                self.handle_error('Error running query: %s' % str(e), {'rule': rule.name, 'query': e.query})
                hits_terms = None

            if hits_terms is None:
                top_events_count = {}
            else:
                buckets = list(hits_terms.values())[0]
                # get_hits_terms adds to num_hits, bu we don't want to count these
                rule.data.num_hits -= len(buckets)
                terms = {}
                for bucket in buckets:
                    terms[bucket['key']] = bucket['doc_count']
                counts = sorted(terms.values(), key=lambda x: x[1], reverse=True)
                top_events_count = dict(counts[:number])

            # Save a dict with the top 5 events by key
            all_counts['top_events_%s' % key] = top_events_count

        return all_counts

    def get_alert(self, rule: Rule, uuid: str):
        """ Attempt to retrieve an alert from ElasticSearch by UUID. """
        if uuid in rule.data.alerts_cache:
            return rule.data.alerts_cache[uuid]

        try:
            query = {'query': {'term': {'_id': uuid}}}
            # TODO: look into using `version`
            res = self.es_client.search(index=self.conf['alert_alias'], body=query, size=1)

            if res['hits']['hits']:
                rule.data.alerts_cache[uuid] = res['hits']['hits'][0]['_source']
                return res['hits']['hits'][0]['_source']

        except:
            pass

        return None

    def send_alert(self, alerts, rule: Rule, alert_time=None, retried=False, silenced=False) -> int:
        """ Send out an alert. """
        if not alerts:
            return 0

        alert_time = alert_time or dt_now()

        # Compute top count keys
        if rule.conf('top_count_keys'):
            for alert in alerts:
                if rule.conf('query_key') and rule.conf('query_key') in alert['match_body']:
                    qk = alert['match_body'][rule.conf('query_key')]
                else:
                    qk = None

                if isinstance(rule, reactor.rule.FlatlineRule):
                    # Flatline rule triggers when there have been no events from now()-timeframe to now(),
                    # so using now()-timeframe will return no results. For now we can just multiple the timeframe
                    # by 2, but this could probably be timeframe+run_every to prevent too large of a lookup?
                    timeframe = datetime.timedelta(seconds=2 * rule.conf('timeframe').total_seconds())
                else:
                    timeframe = rule.conf('timeframe', datetime.timedelta(minutes=10))

                match_time = ts_to_dt(dots_get(alert['match_body'], rule.conf('timestamp_field')))
                start = match_time - timeframe
                end = match_time + datetime.timedelta(minutes=10)
                keys = rule.conf('top_count_keys')
                counts = self.get_top_counts(rule, start, end, keys, qk=qk)
                alert['match_body'].update(counts)

        # Generate a kibana3 dashboard for the first alert match body
        if rule.conf('generate_kibana_link') or rule.conf('use_kibana_dashboard'):
            try:
                if rule.conf('generate_kibana_link'):
                    kb_link = reactor.kibana.generate_kibana_db(rule, alerts[0]['match_body'], rule.get_index())
                else:
                    kb_link = reactor.kibana.use_kibana_link(rule, alerts[0]['match_body'])
            except ReactorException as e:
                self.handle_error('Could not generate Kibana dashboard for %s match: %s' % (rule.name, e))
            else:
                alerts[0]['kibana_link'] = kb_link

        if rule.conf('use_kibana4_dashboard'):
            kb_link = reactor.kibana.generate_kibana4_db(rule, alerts[0]['match_body'])
            if kb_link:
                alerts[0]['kibana_link'] = kb_link

        # Enhancements were already run at match time if `run_enhancements_first` is set or retried=True,
        # which means this is a retry of a failed alert
        if not rule.conf('run_enhancements_first') and not retried:
            valid_alerts = []
            for alert in alerts:
                try:
                    for enhancement in rule.match_enhancements:
                        try:
                            enhancement.process(alert['match_body'])
                        except ReactorException as e:
                            self.handle_error('Error running match enhancement: %s' % str(e), {'rule': rule.name})

                    for enhancement in rule.alert_enhancements:
                        try:
                            enhancement.process(alert)
                        except ReactorException as e:
                            self.handle_error('Error running alert enhancement: %s' % str(e), {'rule': rule.name})

                    valid_alerts.append(alert)

                except reactor.DropException:
                    pass
            alerts = valid_alerts
            if not alerts:
                return 0

        # Run the alerts
        alert_sent = False
        alert_exception = None
        # Alert pipeline is a single object shared between every alerter
        # This allows alerters to pass objects and data between themselves
        alert_pipeline = {'alert_time': alert_time}
        for alerter in rule.alerters:  # type: reactor.alerter.Alerter
            alerter.pipeline = alert_pipeline
            try:
                alerter.alert(alerts, silenced=silenced, publish=self.mode in ['default'])
            except ReactorException as e:
                self.handle_error('Error while running alert %s: %s' % (alerter.get_info()['type'], e),
                                  {'rule': rule.name})
                alert_exception = str(e)
            else:
                alert_sent += 1
                alert_sent = True

        # Write the alerts to ElasticSearch
        if not silenced:
            agg_id = None
            for alert in alerts:
                alert['alert_sent'] = alert_sent > 0
                if not alert_sent:
                    alert['alert_exception'] = alert_exception

                # Set all matches to aggregate together
                if agg_id:
                    alert['aggregate_id'] = agg_id
                res = self.writeback('alert', alert, rule, doc_id=alert['uuid'], update=retried or '_id' in alert)
                if res and not agg_id:
                    agg_id = res['_id']

                # Add the alert to the alerts cache (will be cleared up in garbage collection)
                if res:
                    rule.data.alerts_cache[alert['uuid']] = alert
        else:
            for alert in alerts:
                # Lookup the existing alert
                alert_uuid = self.get_silenced(rule, rule.get_query_key_value(alert['match_body']) or '_silence')[2]
                if alert_uuid:
                    og_alert = self.get_alert(rule, alert_uuid)
                    if og_alert:
                        rule.merge_alert_body(og_alert, alert)
                        self.writeback('alert', og_alert, rule, doc_id=alert_uuid, update=True)

        return len(alerts)

    def alert(self, alerts, rule, alert_time=None, retried=False, silenced=False) -> int:
        """ Wraps alerting, Kibana linking and enhancements in an exception handler. """
        try:
            return self.send_alert(alerts, rule, alert_time, retried=retried, silenced=silenced)
        except Exception as e:
            self.handle_uncaught_exception(e, rule)
        return 0

    def find_pending_aggregate_alert(self, rule: Rule, aggregation_key_value=None):
        query = {'filter': {'bool': {'must': [{'term': {'rule_uuid': rule.locator}},
                                              {'range': {'alert_time': {'gt': dt_now()}}},
                                              {'term': {'alert_sent': 'false'}}],
                                     'must_not': [{'exists': {'field': 'aggregate_id'}}]}}}
        if aggregation_key_value:
            query['filter']['bool']['must'].append({'term': {'aggregation_key': aggregation_key_value}})
        if self.es_client.es_version_at_least(5):
            query = {'query': {'bool': query}}
        query['sort'] = {'alert_time': {'order': 'desc'}}
        try:
            if self.es_client.es_version_at_least(6):
                res = self.es_client.search(index=self.writeback_index, body=query, size=1)
            else:
                res = self.es_client.search(index=self.writeback_index, doc_type='alert', body=query, size=1)

            if len(res['hits']['hits']) == 0:
                return None
        except (KeyError, elasticsearch.ElasticsearchException) as e:
            self.handle_error("Error searching for pending aggregated matches: %s" % e, {'rule_uuid': rule.locator})
            return None

        return res['hits']['hits'][0]

    def add_aggregated_alert(self, alert: dict, rule: Rule):
        """ Save a match as pending aggregate alert to ElasticSearch. """

        match_body = alert['match_body']
        # Optionally include the 'aggregation_key' as a dimension for aggregations
        aggregation_key_value = rule.get_aggregation_key_value(match_body)
        match_time = ts_to_dt(dots_get(match_body, rule.conf('timestamp_field')))

        if (not rule.data.current_aggregate_id.get(aggregation_key_value) or
                (rule.data.aggregate_alert_time.get(aggregation_key_value) < match_time)):
            # Reactor may have restarted while pending alerts exist
            pending_alert = self.find_pending_aggregate_alert(rule, aggregation_key_value)
            if pending_alert:
                alert_time = ts_to_dt(pending_alert['_source']['alert_time'])
                rule.data.aggregate_alert_time[aggregation_key_value] = alert_time
                agg_id = pending_alert['_id']
                rule.data.current_aggregate_id = {aggregation_key_value: agg_id}
                reactor_logger.info('Adding alert for %s to aggregation(id: %s, aggregation_key: %s), next alert at %s',
                                    rule.name, agg_id, aggregation_key_value, alert_time)

            else:
                # First match, set alert_time
                alert_time = ''
                if isinstance(rule.conf('aggregation'), dict) and rule.conf('aggregation').get('schedule'):
                    try:
                        iterator = croniter.croniter(rule.conf('aggregation.schedule'), dt_now())
                        alert_time = unix_to_dt(iterator.get_next())
                    except Exception as e:
                        self.handle_error('Error parsing aggregate send time Cron format %s' % e,
                                          rule.conf('aggregation.schedule'))
                else:
                    if rule.conf('aggregate_by_match_time', False):
                        alert_time = match_time + rule.conf('aggregation')
                    else:
                        alert_time = dt_now() + rule.conf('aggregation')

                rule.data.aggregate_alert_time[aggregation_key_value] = alert_time
                agg_id = None
                reactor_logger.info('New aggregation for %s, aggregation_key: %s, next alert at %s.',
                                    rule.name, aggregation_key_value, alert_time)
        else:
            # Already pending aggregation, use existing alert_time
            alert_time = rule.data.aggregate_alert_time.get(aggregation_key_value)
            agg_id = rule.data.current_aggregate_id.get(aggregation_key_value)
            reactor_logger.info('Adding alert for %s to aggregation(id: %s, aggregation_key: %s), next alert at %s',
                                rule.name, agg_id, aggregation_key_value, alert_time)

        if agg_id:
            alert['aggregate_id'] = agg_id
        if aggregation_key_value:
            alert['aggregation_key'] = aggregation_key_value
        res = self.writeback('alert', alert, rule)

        # If new aggregation, save id
        if res and not agg_id:
            rule.data.current_aggregate_id[aggregation_key_value] = res['_id']

        # Couldn't write to match to ElasticSearch, save it in memory for new
        if not res:
            rule.data.agg_alerts.append(alert)

        return res

    def get_start_time(self, rule) -> Optional[datetime.datetime]:
        """ Query ElasticSearch for the last time we ran this rule. """
        sort = {'sort': {'@timestamp': {'order': 'desc'}}}
        query = {'filter': {'term': {'rule_uuid': rule.locator}}}
        if self.es_client.es_version_at_least(5):
            query = {'query': {'bool': query}}
        query.update(sort)

        try:
            doc_type = 'status'
            index = self.get_writeback_index(doc_type)
            if self.es_client.es_version_at_least(6, 6):
                res = self.es_client.search(index=index, size=1, body=query,
                                            _source_includes=['end_time', 'rule_id'])
            elif self.es_client.es_version_at_least(6):
                res = self.es_client.search(index=index, size=1, body=query,
                                            _source_include=['end_time', 'rule_id'])
            else:
                res = self.es_client.search(index=index, doc_type=doc_type,
                                            size=1, body=query, _source_include=['end_time', 'rule_id'])

            if res['hits']['hits']:
                end_time = ts_to_dt(res['hits']['hits'][0]['_source']['end_time'])

                if dt_now() - end_time < self.old_query_limit:
                    return end_time
                else:
                    reactor_logger.info('Found expired previous run for %s at %s', rule.name, end_time)
                    return None
        except (elasticsearch.ElasticsearchException, KeyError) as e:
            self.handle_error('Error querying for last run: %s' % e, {'rule': rule.name})
            return None

    def set_start_time(self, rule: Rule, end_time) -> None:
        """ Given a rule and an end time, sets the appropriate start_time for it. """
        # This means we are starting fresh
        if rule.data.start_time is None:
            if not rule.conf('scan_entire_timeframe'):
                # Try to get the last run
                last_run_end = self.get_start_time(rule)
                if last_run_end:
                    rule.data.start_time = last_run_end
                    rule.data.start_time = rule.adjust_start_time_for_overlapping_agg_query(rule.data.start_time)
                    rule.data.start_time = rule.adjust_start_time_for_interval_sync(rule.data.start_time)
                    rule.data.minimum_start_time = rule.data.start_time
                    return None

        # Use buffer_time for normal queries, or run_every increments otherwise
        # or, if scan_entire_timeframe
        if not rule.conf('use_count_query') and not rule.conf('use_terms_query'):
            if not rule.conf('scan_entire_timeframe'):
                buffer_delta = end_time - rule.conf('buffer_time')
            else:
                buffer_delta = end_time - rule.conf('timeframe')
            # If we started using a previous run, don't go past that
            if rule.data.minimum_start_time and rule.data.minimum_start_time > buffer_delta:
                rule.data.start_time = rule.data.minimum_start_time
            # If buffer_time doesn't bring us past the previous end time, use that instead
            elif rule.data.previous_end_time and rule.data.previous_end_time < buffer_delta:
                rule.data.start_time = rule.data.previous_end_time
                rule.data.start_time = rule.adjust_start_time_for_overlapping_agg_query(rule.data.start_time)
            else:
                rule.data.start_time = buffer_delta

        else:
            if not rule.conf('scan_entire_timeframe'):
                # Query from the end of the last run, if it exists, otherwise a run_every sized window
                rule.data.start_time = rule.data.previous_end_time or (end_time - rule.conf('run_every'))
            else:
                rule.data.start_time = rule.data.previous_end_time or (end_time - rule.conf('timeframe'))

        return None

    def get_index_start(self, es_client: Elasticsearch, index: str, timestamp_field: str = '@timestamp') -> str:
        """
        Query for one result sorted by timestamp to find the beginning of the index.
        :param es_client: The rule's elasticsearch client
        :param index: The index of which to find the earliest event
        :param timestamp_field: The field where the timestamp is stored
        :return: Timestamp of the earliest event
        """
        query = {'sort': {timestamp_field: {'ord': 'asc'}}}
        try:
            if self.es_client.es_version_at_least(6):
                res = es_client.search(index=index, size=1, body=query,
                                       _source_includes=[timestamp_field], ignore_unavailable=True)
            else:
                res = es_client.search(index=index, size=1, body=query,
                                       _source_include=[timestamp_field], ignore_unavailable=True)
        except elasticsearch.ElasticsearchException as e:
            # An exception was raised, return a date before the epoch
            self.handle_error("Elasticsearch query error: %s" % str(e), {'index': index, 'query': query})
            return '1969-12-30T00:00:00Z'
        if len(res['hits']['hits']) == 0:
            # Index is completely empty, return a date before the epoch
            return '1969-12-30T00:00:00Z'
        return res['hits']['hits'][0][timestamp_field]

    def next_alert_time(self, rule: Rule, name: str, timestamp: datetime.datetime):
        """ Calculate an 'until' time and exponent based on how much past the last 'until' we are. """
        if name in rule.data.silence_cache:
            last_until, exponent, alert_uuid = rule.data.silence_cache[name]
        else:
            # If this isn't cached, this is the first alert or writeback_es is down, normal realert
            return timestamp + rule.conf('realert'), 0

        if not rule.conf('exponential_realert'):
            return timestamp + rule.conf('realert'), 0
        diff = (timestamp - last_until).total_seconds()
        # Increase exponent if we've alerted recently
        if diff < rule.conf('realert').total_seconds() * 2 ** exponent:
            exponent += 1
        else:
            # Continue decreasing exponent the longer it's been since the last alert
            while diff > rule.conf('realert').total_seconds() * 2 ** exponent and exponent > 0:
                diff -= rule.conf('realert').total_seconds() * 2 ** exponent
                exponent -= 1

        wait = datetime.timedelta(seconds=rule.conf('realert').total_seconds() * 2 ** exponent)
        if wait >= rule.conf('exponential_realert'):
            return timestamp + rule.conf('exponential_realert'), exponent - 1
        return timestamp + wait, exponent

    def is_silenced(self, rule: Rule, silence_key=None, timestamp=None):
        """ Checks if a rule is silenced. Return false on exception. """
        silenced = self.get_silenced(rule, silence_key)
        return silenced and (timestamp or dt_now()) < silenced[0]

    def get_silenced(self, rule: Rule, silence_key=None) -> Optional[tuple]:
        """ Look up whether the rule and silence key exists. """
        cache_key = silence_key or '_silence'
        if cache_key in rule.data.silence_cache:
            return rule.data.silence_cache[cache_key]

        # In debug/test mode we don't populate from Reactor status index
        if self.mode in ['debug', 'test']:
            return None

        query = {'term': {'silence_key': rule.locator + '.' + cache_key}}
        sort = {'sort': {'until': {'order': 'desc'}}}
        if self.es_client.es_version_at_least(5):
            query = {'query': query}
        else:
            query = {'filter': query}
        query.update(sort)

        try:
            index = self.get_writeback_index('silence')
            if self.es_client.es_version_at_least(6, 2):
                res = self.es_client.search(index=index, size=1, body=query, _source_includes=['until', 'exponent'])
            elif self.es_client.es_version_at_least(6):
                res = self.es_client.search(index=index, size=1, body=query, _source_include=['until', 'exponent'])
            else:
                res = self.es_client.search(index=index, doc_type='silence',
                                            size=1, body=query, _source_include=['until', 'exponent'])
        except elasticsearch.ElasticsearchException as e:
            self.handle_error('Error while querying for alert silence status: %s' % e, {'rule': rule.name})
            return None
        else:
            if res['hits']['hits']:
                until_ts = res['hits']['hits'][0]['_source']['until']
                exponent = res['hits']['hits'][0]['_source'].get('exponent', 0)
                alert_uuid = res['hits']['hits'][0]['_source'].get('alert_uuid')
                rule.data.silence_cache[cache_key] = (ts_to_dt(until_ts), exponent, alert_uuid)
                return rule.data.silence_cache[cache_key]

            return None

    def set_realert(self, rule: Rule, silence_cache_key: str, until: datetime.datetime, exponent: int, alert_uuid=None):
        """ Write a silence to ElasticSearch for silence_cache_key until timestamp. """
        # Round up the silence until
        until = until.replace(microsecond=0) + datetime.timedelta(seconds=1)

        body = {'exponent': exponent,
                'rule_uuid': rule.locator,
                'silence_key': rule.locator + '.' + silence_cache_key,
                'alert_uuid': alert_uuid,
                '@timestamp': dt_now(),
                'until': until}

        rule.data.silence_cache[silence_cache_key] = (until, exponent, alert_uuid)
        return self.writeback('silence', body)

    def run_query(self, rule: Rule, start_time=None, end_time=None) -> Optional[list]:
        """ Query for the rule and pass all of the results to the Rule instance. """
        start_time = start_time or self.get_index_start(rule.es_client, rule.conf('index'))
        end_time = end_time or dt_to_ts(dt_now)

        index = rule.get_index(start_time, end_time)
        complete = False
        data = None

        # Get the hits in the timeframe, scroll if necessary
        while not complete:
            try:
                if rule.conf('use_count_query'):
                    data = rule.get_hits_count(start_time, end_time, index)
                elif rule.conf('use_terms_query'):
                    data = rule.get_hits_terms(start_time, end_time, index, rule.conf('query_key'))
                elif rule.conf('aggregation_query_element'):
                    data = rule.get_hits_aggregation(start_time, end_time, index, rule.conf('query_key'))
                else:
                    data = rule.get_hits(start_time, end_time, index)
                    if data:
                        old_len = len(data)
                        data = rule.remove_duplicate_events(data)
                        rule.data.num_duplicates += old_len - len(data)
            except QueryException as e:
                self.handle_error('Error running query: %s' % str(e), {'rule': rule.name, 'query': e.query})
                return None

            # There was an exception while querying
            if data is None:
                return []
            elif data:
                if rule.conf('use_count_query'):
                    yield from rule.add_count_data(data)
                elif rule.conf('use_terms_query'):
                    yield from rule.add_terms_data(data)
                elif rule.conf('aggregation_query_element'):
                    yield from rule.add_aggregation_data(data)
                else:
                    yield from rule.add_hits_data(data)

            # We are complete if we don't have a scroll id or num of hits is equal to total hits
            complete = not (rule.data.scroll_id and rule.data.num_hits < rule.data.total_hits)

        # Tidy up scroll_id (after scrolling is finished)
        rule.data.scroll_id = None
        return data

    def garbage_collect(self, rule: Rule):
        """ Collect the garbage after running a rule. """
        now = dt_now()
        buffer_time = rule.conf('buffer_time') + rule.conf('query_delay')

        # Clear up the silence cache (and possible cached alerts)
        if rule.data.silence_cache:
            # TODO: alter silence cache to support multiple silenced alerts with the same key (or add alert time??)
            stale_silences = []
            stale_alerts = []
            for _id, (timestamp, _, alert_uuid) in rule.data.silence_cache.items():
                if now - timestamp > buffer_time:
                    stale_silences.append(_id)
                    stale_alerts.append(alert_uuid)
            list(map(rule.data.silence_cache.pop, stale_silences))
            list(map(rule.data.alerts_cache.pop, stale_alerts, [None] * len(stale_alerts)))

        # Clear up the silence index in the writeback elasticsearch
        if self.es_client.es_version_at_least(6):
            res = self.es_client.search(index=self.get_writeback_index('silence'), body={
                'query': {'bool': {'must': [
                    {'term': {'rule_uuid': rule.locator}},
                    {'range': {'until': {'lt': dt_to_ts(now - buffer_time)}}}
                ]}}
            }, _source=['alert_uuid'], size=1000)
        else:
            res = self.es_client.search(index=self.get_writeback_index('silence'), doc_type='_doc', body={
                'query': {'bool': {'must': [
                    {'term': {'rule_uuid': rule.locator}},
                    {'range': {'until': {'lt': dt_to_ts(now - buffer_time)}}}
                ]}}
            }, _source=['alert_uuid'], size=1000)
        elasticsearch.helpers.bulk(self.es_client, [{
            '_op_type': 'delete',
            '_index': self.get_writeback_index('silence'),
            '_type': '_doc',
            '_id': hit['_id'],
        } for hit in res['hits']['hits']])
        list(map(rule.data.alerts_cache.pop,
                 [s['_source']['alert_uuid'] for s in res['hits']['hits']],
                 [None] * len(res['hits']['hits'])))

        # Remove events from rules that are outside of the buffer timeframe
        stale_hits = []
        for _id, timestamp in rule.data.processed_hits.items():
            if now - timestamp > buffer_time:
                stale_hits.append(_id)
        list(map(rule.data.processed_hits.pop, stale_hits))

    def process_alert(self, rule: Rule, extra: dict, match: dict):
        alert = rule.get_alert_body(extra, match, dt_now())

        # If realert is set, silence the rule for that duration
        # Silence is cached by query_key, if it exists
        # Default realert time is 0 seconds
        match_time = ts_to_dt(dots_get(match, rule.conf('timestamp_field')))
        silence_key = rule.get_query_key_value(match) or '_silence'
        silenced = self.is_silenced(rule, silence_key, match_time)

        # If not silenced and there is a realert, silence the rule
        if not silenced and rule.conf('realert'):
            next_alert, exponent = self.next_alert_time(rule, silence_key, match_time or dt_now())
            self.set_realert(rule, silence_key, next_alert, exponent, alert['uuid'])

        if rule.conf('run_enhancements_first'):
            try:
                for enhancement in rule.match_enhancements:
                    try:
                        enhancement.process(match)
                    except ReactorException as e:
                        self.handle_error('Error running match enhancement: %s' % str(e), {'rule': rule.name})

                for enhancement in rule.alert_enhancements:
                    try:
                        enhancement.process(alert)
                    except ReactorException as e:
                        self.handle_error('Error running alert enhancement: %s' % str(e), {'rule': rule.name})

            except reactor.enhancement.DropException:
                # Drop this match
                return

        # If no aggregation, alert immediately
        if not rule.conf('aggregation'):
            num_sent = self.alert([alert], rule, silenced=silenced)
            rule.data.alerts_sent += num_sent
            rule.data.alerts_silenced += num_sent if silenced else 0
            return

        # Add it as an aggregated alert
        self.add_aggregated_alert(alert, rule)

    def run_rule(self, rule: Rule, end_time, start_time=None):
        # Start the clock
        rule.data.start_run_time()

        self.thread_data.writeback_cache = []

        # If there are pending aggregate matches, try processing them
        while rule.data.agg_alerts:
            alert = rule.data.agg_alerts.pop()
            self.add_aggregated_alert(alert, rule)

        # Start from provided time if it's given
        if start_time:
            rule.data.start_time = start_time
        else:
            self.set_start_time(rule, end_time)
        rule.data.original_start_time = rule.data.start_time

        # Don't run if start_time was set to the future
        if dt_now() <= rule.data.start_time:
            reactor_logger.warning('Attempted to use query start time in the future (%s), sleeping instead', start_time)
            return rule.data

        # Reset working data
        rule.data.reset()

        # Prepare the self before running it
        try:
            rule.prepare(self.es_client, rule.data.start_time)
        except Exception as e:
            raise ReactorException('Error preparing rule %s: %s' % (rule.name, repr(e)))

        # Run the rule. If querying over a large time period, split it up into segments
        segment_size = rule.get_segment_size()
        tmp_end_time = rule.data.start_time
        while (end_time - rule.data.start_time) > segment_size:
            tmp_end_time = tmp_end_time + segment_size
            for extra, match in self.run_query(rule, rule.data.start_time, tmp_end_time):
                self.process_alert(rule, extra, match)
            rule.data.cumulative_hits += rule.data.num_hits
            rule.data.num_hits = 0
            rule.data.start_time = tmp_end_time
            for extra, match in rule.garbage_collect(tmp_end_time):
                self.process_alert(rule, extra, match)

        # Guarantee that at least one search search occurs
        if rule.conf('aggregation_query_element'):
            if end_time - tmp_end_time == segment_size:
                for extra, match in self.run_query(rule, tmp_end_time, end_time):
                    self.process_alert(rule, extra, match)
                rule.data.cumulative_hits += rule.data.num_hits
            elif (rule.data.original_start_time - tmp_end_time).total_seconds() == 0:
                return {}
            else:
                end_time = tmp_end_time
        else:
            for extra, match in self.run_query(rule, rule.data.start_time, end_time):
                self.process_alert(rule, extra, match)
            rule.data.cumulative_hits += rule.data.num_hits
            for extra, match in rule.garbage_collect(end_time):
                self.process_alert(rule, extra, match)

        # Mark this end time for next run's start
        rule.data.previous_end_time = end_time

        rule.data.end_run_time()
        # Write to ElasticSearch that we've run this rule against this time period
        body = {'rule_uuid': rule.locator,
                'rule_name': rule.name,
                'end_time': end_time,
                'start_time': rule.data.original_start_time,
                'matches': rule.data.num_matches,
                'hits': max(rule.data.num_hits, rule.data.cumulative_hits),
                '@timestamp': dt_now(),
                'time_taken': rule.data.time_taken}
        self.writeback('status', body)

        self.flush_writeback()

        return rule.data

    def handle_error(self, message, data=None):
        """ Logs messages at error level and writes message, data and traceback to ElasticSearch. """
        reactor_logger.error(message)
        body = {'message': message,
                'traceback': traceback.format_exc().strip().split('\n'),
                'data': data}
        self.writeback('error', body)

    def handle_rule_execution(self, rule: Rule):
        next_run = datetime.datetime.utcnow() + rule.run_every

        # Set end time based on the rule's delay
        if self.args['end']:
            end_time = self.args['end']
        elif rule.conf('query_delay'):
            end_time = dt_now() - rule.conf('query_delay')
        else:
            end_time = dt_now()

        # Disable the rule if it has run at least once, an end time was specified, and the end time has elapsed
        if rule.data.has_run_once and self.args['end'] and self.args['end'] < dt_now():
            return rule.data

        # Apply rules based on execution time limits
        if rule.conf('limit_execution'):
            rule.data.next_start_time = None
            rule.data.next_min_start_time = None
            exec_next = croniter.croniter(rule.conf('limit_execution')).next()
            # If the estimated next end time (end + run_every) isn't at least a minute past the next exec time
            # That means that we need to pause execution after this run
            if dt_to_unix(end_time) + rule.run_every.total_seconds() < exec_next - 59:
                next_start_time = unix_to_dt(exec_next)
                if rule.conf('limit_execution_coverage'):
                    rule.data.next_min_start_time = next_start_time
                rule.data.next_start_time = next_start_time
                if not rule.data.has_run_once:
                    return rule.data

        # Run the rule
        try:
            rule.data = self.run_rule(rule, end_time, rule.data.initial_start_time)
        except ReactorException as e:
            self.handle_error('Error running rule %s: %s' % (rule.name, e), {'rule': rule.name})
        # except Exception as e:
        #     self.handle_uncaught_exception(e, rule)
        else:
            # old_start_time = pretty_ts(rule.data.original_start_time, rule.conf('use_local_time'))
            old_start_time = pretty_ts(rule.data.start_time, rule.conf('use_local_time'))
            reactor_logger.log(logging.INFO, # if rule.data.alerts_sent else logging.DEBUG,
                               'Ran from %s to %s "%s": %s query hits (%s already seen), %s matches, '
                               '%s alerts sent (%s silenced)',
                               old_start_time, pretty_ts(end_time, rule.conf('use_local_time')), rule.name,
                               rule.data.cumulative_hits,
                               rule.data.num_duplicates,
                               rule.data.num_matches,
                               rule.data.alerts_sent,
                               rule.data.alerts_silenced)

            if next_run < datetime.datetime.utcnow():
                # We were processing for longer than our refresh interval
                # This can happen if --start was specified with a large time period
                # or if we running too slowly to process events in real time
                reactor_logger.warning('Querying from %s to %s "%s" took longer than %s (%s)!',
                                       old_start_time,
                                       pretty_ts(end_time, rule.conf('use_local_time')),
                                       rule.name,
                                       rule.run_every,
                                       datetime.timedelta(seconds=rule.data.time_taken))

        rule.data.initial_start_time = None
        self.garbage_collect(rule)

        # Mark the rule has having been run at least once
        rule.data.has_run_once = True
        rule.data.end_time = end_time
        return rule.data

    def find_recent_pending_alerts(self, time_limit):
        """ Queries writeback ElasticSearch to find alerts that did not send and are newer than the time limit. """
        # XXX only fetches 1000 results. If limit is reached, next loop will catch them
        # unless there is constantly more than 1000 alerts to send

        # Fetch recent, unsent alerts that aren't part of an aggregate, earlier alerts first.
        inner_query = {'query_string': {'query': '!_exists_:aggregate_id AND alert_sent:false'}}
        time_filter = {'range': {'alert_time': {'from': dt_to_ts(dt_now() - time_limit),
                                                'to': dt_to_ts(dt_now())}}}
        sort = {'sort': {'alert_time': {'order': 'asc'}}}
        if self.es_client.es_version_at_least(5):
            query = {'query': {'bool': {'must': inner_query, 'filter': time_filter}}}
        else:
            query = {'query': inner_query, 'filter': time_filter}
        query.update(sort)

        try:
            if self.es_client.es_version_at_least(6):
                res = self.es_client.search(index=self.alert_alias, body=query, size=1000)
            else:
                res = self.es_client.search(index=self.alert_alias, doc_type='alert', body=query, size=1000)
            if res['hits']['hits']:
                return res['hits']['hits']
        except elasticsearch.ElasticsearchException as e:
            reactor_logger.exception('Error finding recent pending alerts: %s %s' % (e, query))
        return []

    def get_aggregated_alerts(self, _id):
        """ Removes and returns all alerts from writeback es that have aggregate_id == _id """

        # XXX if there are more than self.max_aggregation matches, you have big alerts and we will leave
        # entries in ElasticSearch
        query = {'query': {'query_string': {'query': 'aggregate_id:' + _id}}, 'sort': {'@timestamp': 'asc'}}
        matches = []
        try:
            if self.es_client.es_version_at_least(6):
                res = self.es_client.search(index=self.alert_alias, body=query, size=self.max_aggregation)
            else:
                res = self.es_client.search(index=self.alert_alias, doc_type='alert',
                                            body=query, size=self.max_aggregation)

            for match in res['hits']['hits']:
                matches.append(match['_source'])
        except (KeyError, elasticsearch.ElasticsearchException) as e:
            self.handle_error('Error fetching aggregated matches: %s' % e, {'id': '_id'})
        return matches

    def send_pending_alerts(self) -> int:
        alerts_sent = 0
        for alert in self.find_recent_pending_alerts(self.alert_time_limit):
            _id = alert['_id']
            alert = alert['_source']
            try:
                rule_uuid = alert.pop('rule_uuid')
                alert_time = alert.pop('alert_time')
            except KeyError:
                # Malformed alert, drop it
                continue

            # If the original rule is missing, keep alert for later if rule reappears
            rule = self.loader.rules.get(rule_uuid)
            if not rule:
                continue

            # Send the alert unless it's a future alert
            if dt_now() > ts_to_dt(alert_time):
                aggregated_alerts = self.get_aggregated_alerts(_id)
                if aggregated_alerts:
                    alerts = [alert] + aggregated_alerts
                    alerts_sent += self.alert(alerts, rule, alert_time=alert_time)
                else:
                    # If this rule isn't using aggregation, this must be a retry of a failed alert
                    retried = not rule.conf('aggregation')
                    alerts_sent += self.alert([alert], rule, alert_time=alert_time, retried=retried)

                if rule.conf('current_aggregate_id'):
                    for qk, agg_id in rule.data.current_aggregate_id.items():
                        if agg_id == _id:
                            rule.data.current_aggregate_id.pop(qk)
                            break

        for rule in self.loader:
            if rule.data.agg_alerts:
                for aggregation_key_value, aggregation_alert_time in rule.data.aggregate_alert_time.items():
                    if dt_now() > aggregation_alert_time:
                        alertable_alerts = [
                            agg_alert for agg_alert in rule.data.agg_alerts
                            if rule.get_aggregation_key_value(agg_alert['match_body']) == aggregation_key_value
                        ]
                        alerts_sent += self.alert(alertable_alerts, rule)
                        rule.data.agg_alerts = [
                            agg_alert for agg_alert in rule.data.agg_alerts
                            if rule.get_aggregation_key_value(agg_alert['match_body']) != aggregation_key_value
                        ]

        return alerts_sent
