import apscheduler.schedulers.background
import croniter
import datetime
import dateutil.tz
import elasticsearch
import elasticsearch.helpers
import logging
import random
import time
import traceback

from functools import lru_cache

import reactor.ruletype
from reactor.loader import Rule, RuleLoader
from typing import Optional
from reactor.exceptions import ReactorException, QueryException
from reactor.util import (
    reactor_logger,
    dt_now, dt_to_ts, ts_to_dt, unix_to_dt, pretty_ts,
    dots_get,
    elasticsearch_client,
    get_index,
)
import reactor.kibana


class Client(object):

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
        self.alert_time_limit = conf['alert_time_limit']
        self.old_query_limit = conf['old_query_limit']
        self.max_aggregation = conf['max_aggregation']
        self.string_multi_field_name = conf['string_multi_field_name']

        self.silence_cache = {}
        self.alerts_cache = {}

        self.start_time = args.get('start', dt_now())
        self._es_version = None
        self.running = False
        self.scheduler = apscheduler.schedulers.background.BackgroundScheduler()

    @property
    def es_version(self):
        return self.es_client.es_version

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

    def get_start_time(self, rule) -> Optional[datetime.datetime]:
        """ Query ElasticSearch for the last time we ran this rule. """
        sort = {'sort': {'@timestamp': {'order': 'desc'}}}
        query = {'filter': {'term': {'rule_uuid': rule.uuid}}}
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
                # TODO: decide whether to support older versions of ES (probably not)
                res = self.es_client.deprecated_search(index=index, doc_type=doc_type,
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
        if rule.start_time is None:
            if not rule.conf('scan_entire_timeframe'):
                # Try to get the last run
                last_run_end = self.get_start_time(rule)
                if last_run_end:
                    rule.start_time = last_run_end
                    rule.start_time = rule.adjust_start_time_for_overlapping_agg_query(rule.start_time)
                    rule.start_time = rule.adjust_start_time_for_interval_sync(rule.start_time)
                    rule.minimum_start_time = rule.start_time
                    return None

        # Use buffer_time for normal queries, or run_every increments otherwise
        # or, if scan_entire_timeframe
        if not rule.conf('use_count_query') and not rule.conf('use_terms_query'):
            if not rule.conf('scan_entire_timeframe'):
                buffer_delta = end_time - rule.conf('buffer_time')
            else:
                buffer_delta = end_time - rule.conf('timeframe')
            # If we started using a previous run, don't go past that
            if rule.minimum_start_time and rule.minimum_start_time > buffer_delta:
                rule.start_time = rule.minimum_start_time
            # If buffer_time doesn't bring us past the previous end time, use that instead
            elif rule.previous_end_time and rule.previous_end_time < buffer_delta:
                rule.start_time = rule.previous_end_time
                rule.start_time = rule.adjust_start_time_for_overlapping_agg_query(rule.start_time)
            else:
                rule.start_time = buffer_delta

        else:
            if not rule.conf('scan_entire_timeframe'):
                # Query from the end of the last run, if it exists, otherwise a run_every sized window
                rule.start_time = rule.previous_end_time or (end_time - rule.conf('run_every'))
            else:
                rule.start_time = rule.previous_end_time or (end_time - rule.conf('timeframe'))

        return None

    def run_rule(self, rule: Rule, end_time, start_time=None) -> (int, int, int):
        # Start the clock
        run_start = time.time()

        # If there are pending aggregate matches, try processing them
        while rule.agg_matches:
            match = rule.agg_matches.pop()
            self.add_aggregated_alert(match, rule)

        # Start from provided time if it's given
        if start_time:
            rule.start_time = start_time
        else:
            self.set_start_time(rule, end_time)
        rule.original_start_time = rule.start_time

        # Don't run if start_time was set to the future
        if dt_now() <= rule.start_time:
            reactor_logger.warning('Attempted to use query start time in the future (%s), sleeping instead', start_time)
            return 0, 0, 0

        # Reset counters
        rule.num_hits = 0
        rule.num_duplicates = 0
        rule.total_hits = 0
        rule.cumulative_hits = 0

        # Prepare the self before running it
        try:
            rule.type.prepare(self.es_client, rule.start_time)
        except Exception as e:
            raise ReactorException('Error preparing rule %s: %s' % (rule.name, repr(e)))

        # Run the rule. If querying over a large time period, split it up into segments
        segment_size = rule.get_segment_size()
        tmp_end_time = rule.start_time
        while (end_time - rule.start_time) > segment_size:
            tmp_end_time = tmp_end_time + segment_size
            if not self.run_query(rule, rule.start_time, tmp_end_time):
                return 0, 0, 0
            rule.cumulative_hits += rule.num_hits
            rule.num_hits = 0
            rule.start_time = tmp_end_time
            rule.type.garbage_collect(tmp_end_time)

        # Guarantee that at least one search search occurs
        if rule.conf('aggregation_query_element'):
            if end_time - tmp_end_time == segment_size:
                self.run_query(rule, tmp_end_time, end_time)
                rule.cumulative_hits += rule.num_hits
            elif (rule.original_start_time - tmp_end_time).total_seconds() == 0:
                return 0, 0, 0
            else:
                end_time = tmp_end_time
        else:
            if not self.run_query(rule, rule.start_time, end_time):
                return 0, 0, 0
            rule.cumulative_hits += rule.num_hits
            rule.type.garbage_collect(end_time)

        # Process any new matches
        num_matches = len(rule.type.matches)
        alerts_sent = 0
        num_silenced = 0
        while rule.type.matches:
            extra, match = rule.type.matches.pop(0)
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
                    continue

            # If no aggregation, alert immediately
            if not rule.conf('aggregation'):
                num_sent = self.alert([alert], rule, silenced=silenced)
                alerts_sent += num_sent
                num_silenced += num_sent if silenced else 0
                continue

            # Add it as an aggregated alert
            self.add_aggregated_alert(alert, rule)

        # Mark this end time for next run's start
        rule.previous_end_time = end_time

        time_taken = time.time() - run_start
        # Write to ElasticSearch that we've run this rule against this time period
        body = {'rule_uuid': rule.uuid,
                'rule_name': rule.name,
                'end_time': end_time,
                'start_time': rule.original_start_time,
                'matches': num_matches,
                'hits': max(rule.num_hits, rule.cumulative_hits),
                '@timestamp': dt_now(),
                'time_taken': time_taken}
        self.writeback('status', body)

        return num_matches, alerts_sent, num_silenced

    def run_query(self, rule: Rule, start_time=None, end_time=None) -> Optional[list]:
        """ Query for the rule and pass all of the results to the RuleType instance. """
        start_time = start_time or self.get_index_start(rule.es_client, rule.conf('index'))
        end_time = end_time or dt_to_ts(dt_now)

        index = get_index(rule, start_time, end_time)
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
                        rule.num_duplicates += old_len - len(data)
            except QueryException as e:
                self.handle_error('Error running query: %s' % str(e), {'rule': rule.name, 'query': e.query})
                return None

            # There was an exception while querying
            if data is None:
                return []
            elif data:
                if rule.conf('use_count_query'):
                    rule.type.add_count_data(data)
                elif rule.conf('use_terms_query'):
                    rule.type.add_terms_data(data)
                elif rule.conf('aggregation_query_element'):
                    rule.type.add_aggregation_data(data)
                else:
                    rule.type.add_hits_data(data)

            # We are complete if we don't have a scroll id or num of hits is equal to total hits
            complete = not (rule.scroll_id and rule.num_hits < rule.total_hits)

        # Tidy up scroll_id (after scrolling is finished)
        rule.scroll_id = None
        return data

    def test_rule(self, rule: Rule, end_time, start_time=None):
        try:
            num_matches, alerts_sent, num_silenced = self.run_rule(rule, end_time, start_time)
        except ReactorException as e:
            self.handle_error('Error running rule %s: %s' % (rule.name, e), {'rule': rule.name})
        except Exception as e:
            self.handle_uncaught_exception(e, rule)
        else:
            reactor_logger.info('Ran from %s to %s "%s": %s query hits (%s already seen), %s matches, '
                                '%s alerts sent (%s silenced)',
                                pretty_ts(start_time, rule.conf('use_local_time')),
                                pretty_ts(end_time, rule.conf('use_local_time')),
                                rule.name,
                                rule.cumulative_hits, rule.num_duplicates, num_matches, alerts_sent, num_silenced)

    def start(self):
        """ Periodically update rules and schedule to run. """
        # Ensure ElasticSearch is responsive
        if not self.wait_until_responsive(timeout=self.args['timeout']):
            return 1

        reactor_logger.info('Starting up')
        self.running = True
        self.scheduler.add_job(self.handle_pending_alerts, 'interval',
                               seconds=self.conf['resend'].total_seconds(),
                               id='_internal_handle_pending_alerts')
        self.scheduler.add_job(self.handle_config_changes, 'interval',
                               seconds=(self.args['reload'] or self.conf['reload']).total_seconds(),
                               id='_internal_handle_config_changes')
        self.scheduler.start()
        while self.running:
            # If an end time was specified and it has elapsed
            if 'end' in self.args and self.args['end'] < dt_now():
                # If the rule have been loaded and every run has been run once
                if self.loader.loaded and all([r.has_run_once for r in self.loader]):
                    reactor_logger.info('Reached end time, shutting down reactor')
                    self.running = False
        return 0

    def stop(self):
        """ Stop a running Reactor. """
        self.running = False

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
        alerts_sent = self.send_pending_alerts()
        if alerts_sent > 0:
            reactor_logger.info('Sent %s pending alerts at %s', alerts_sent, pretty_ts(dt_now()))

    def handle_config_changes(self):
        # If already loaded and pinned
        if self.loader.loaded and self.args['pin_rules']:
            return

        if not self.loader.loaded:
            reactor_logger.info('Loading rules')
        else:
            reactor_logger.debug('Detecting rule changes')

        # Remove rule jobs that have been modified or deleted
        for rule_hash, rule_locator in self.loader.load(self.args):
            self.scheduler.remove_job(job_id=rule_hash)
            # Clear up the silence cache
            if rule_locator not in self.loader:
                self.silence_cache.pop(rule_locator)

        # Add the rule to the scheduler
        for rule in self.loader:
            rule.initial_start_time = self.start_time if not self.running else rule.initial_start_time

            # If this rule already is in the scheduler nothing left to do
            if self.scheduler.get_job(rule.hash):
                continue

            # Add a default value for the silence cache and alerts cache
            self.silence_cache.setdefault(rule.uuid, {})
            self.alerts_cache.setdefault(rule.uuid, {})

            # Add the rule to the scheduler
            next_run_time = datetime.datetime.now() + datetime.timedelta(seconds=random.randint(0, len(self.loader)))
            self.scheduler.add_job(self.handle_rule_execution, 'interval',
                                   args=[rule],
                                   seconds=rule.run_every.total_seconds(),
                                   id=rule.hash,
                                   max_instances=1,
                                   jitter=5,
                                   next_run_time=next_run_time)

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
        if rule.has_run_once and self.args['end'] and self.args['end'] < dt_now():
            self.loader.disable(rule.uuid)
            return

        # Apply rules based on execution time limits
        if rule.conf('limit_execution'):
            rule.next_start_time = None
            rule.next_min_start_time = None
            exec_next = croniter.croniter(rule.conf('limit_execution')).next()
            # If the estimated next end time (end + run_every) isn't at least a minute past the next exec time
            # That means that we need to pause execution after this run
            if (end_time + rule.run_every).total_seconds() < exec_next - 59:
                rule.next_start_time = datetime.datetime.utcfromtimestamp(exec_next).replace(tzinfo=dateutil.tz.tzutc())
                if rule.conf('limit_execution_coverage'):
                    rule.next_min_start_time = rule.next_start_time
                if not rule.has_run_once:
                    self.reset_rule_schedule(rule)

        # Run the rule
        try:
            num_matches, alerts_sent, num_silenced = self.run_rule(rule, end_time, rule.initial_start_time)
        except ReactorException as e:
            self.handle_error('Error running rule %s: %s' % (rule.name, e), {'rule': rule.name})
        except Exception as e:
            self.handle_uncaught_exception(e, rule)
        else:
            old_start_time = pretty_ts(rule.original_start_time, rule.conf('use_local_time'))
            reactor_logger.log(logging.INFO if alerts_sent else logging.DEBUG,
                               'Ran from %s to %s "%s": %s query hits (%s already seen), %s matches, '
                               '%s alerts sent (%s silenced)',
                               old_start_time, pretty_ts(end_time, rule.conf('use_local_time')), rule.name,
                               rule.cumulative_hits, rule.num_duplicates, num_matches, alerts_sent, num_silenced)

            if next_run < datetime.datetime.utcnow():
                # We were processing for longer than our refresh interval
                # This can happen if --start was specified with a large time period
                # or if we running too slowly to process events in real time
                reactor_logger.warning('Querying from %s to %s took longer than %s!',
                                       old_start_time, pretty_ts(end_time, rule.conf('use_local_time')), rule.run_every)

        rule.initial_start_time = None
        self.garbage_collect(rule)
        self.reset_rule_schedule(rule)
        # Mark the rule has having been run at least once
        rule.has_run_once = True
        reactor_logger.info('Rule %s run once', rule.name)

    def reset_rule_schedule(self, rule: Rule):
        # We hit the end of an execution schedule, pause ourselves until next run
        # TODO: potentially add next_start_time/next_run_time/next_min_start_time in method parameters
        if rule.conf('limit_execution') and rule.next_start_time:
            self.scheduler.modify_job(job_id=rule.hash, next_run_time=rule.next_run_time)
            # If we are preventing covering non-scheduled time periods, reset min_start_time and previous_end_time
            if rule.next_min_start_time:
                rule.minimum_start_time = rule.next_min_start_time
                rule.previous_end_time = rule.next_min_start_time
            reactor_logger.info('Pausing %s until next run at %s', rule.name, pretty_ts(rule.next_start_time))

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
                    for qk, agg_id in rule.current_aggregate_id.items():
                        if agg_id == _id:
                            rule.current_aggregate_id.pop(qk)
                            break

                # TODO: No need to delete as we will be overriding existing id

        for rule in self.loader:
            if rule.agg_matches:
                for aggregation_key_value, aggregation_alert_time in rule.aggregate_alert_time.items():
                    if dt_now() > aggregation_alert_time:
                        alertable_matches = [
                            agg_match for agg_match in rule.agg_matches
                            if rule.get_aggregation_key_value(agg_match) == aggregation_key_value
                        ]
                        alerts_sent += self.alert(alertable_matches, rule)
                        rule.agg_matches = [
                            agg_match for agg_match in rule.agg_matches
                            if rule.get_aggregation_key_value(agg_match) != aggregation_key_value
                        ]

        return alerts_sent

    def garbage_collect(self, rule: Rule):
        """ Collect the garbage after running a rule. """
        now = dt_now()
        buffer_time = rule.conf('buffer_time') + rule.conf('query_delay')

        # Clear up the alerts cache
        self.alerts_cache.setdefault(rule.uuid, {})
        self.alerts_cache[rule.uuid] = {}

        # Clear up the silence cache
        if rule.uuid in self.silence_cache:
            stale_silences = []
            for _id, (timestamp, _, _) in self.silence_cache[rule.uuid].items():
                if now - timestamp > buffer_time:
                    stale_silences.append(_id)
                    # TODO: Run a final update on the silenced alert
            list(map(self.silence_cache[rule.uuid].pop, stale_silences))

        # Clear up the silence index in the writeback elasticsearch
        res = self.es_client.search(index=self.get_writeback_index('silence'), doc_type='_doc', body={
            'query': {'bool': {'must': [
                {'term': {'rule_uuid': rule.uuid}},
                {'range': {'until': {'lt': dt_to_ts(now - buffer_time)}}}
            ]}}
        }, _source=False, size=1000)
        elasticsearch.helpers.bulk(self.es_client, [{
            '_op_type': 'delete',
            '_index': self.get_writeback_index('silence'),
            '_type': '_doc',
            '_id': hit['_id'],
        } for hit in res['hits']['hits']])

        # Remove events from rules that are outside of the buffer timeframe
        stale_hits = []
        for _id, timestamp in rule.processed_hits.items():
            if now - timestamp > buffer_time:
                stale_hits.append(_id)
        list(map(rule.processed_hits.pop, stale_hits))

    def alert(self, alerts, rule, alert_time=None, retried=False, silenced=False) -> int:
        """ Wraps alerting, Kibana linking and enhancements in an exception handler. """
        try:
            return self.send_alert(alerts, rule, alert_time, retried=retried, silenced=silenced)
        except Exception as e:
            self.handle_uncaught_exception(e, rule)
        return 0

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

                if isinstance(rule.type, reactor.ruletype.FlatlineRuleType):
                    # Flatline rule triggers when there have been no events from now()-timeframe to now(),
                    # so using now()-timeframe will return no results. For now we can just multiple the timeframe
                    # by 2, but this could probably be timeframe+run_every to prevent too large of a lookup?
                    timeframe = datetime.timedelta(seconds=2 * rule.conf('timeframe').total_seconds())
                else:
                    timeframe = rule.conf('timeframe', datetime.timedelta(minutes=10))

                start = ts_to_dt(dots_get(alert['match_body'], rule.conf('timestamp_field'))) - timeframe
                end = ts_to_dt(dots_get(alert['match_body'], rule.conf('timestamp_field'))) + datetime.timedelta(minutes=10)
                keys = rule.conf('top_count_keys')
                counts = self.get_top_counts(rule, start, end, keys, qk=qk)
                alert['match_body'].update(counts)

        # Generate a kibana3 dashboard for the first alert match body
        if rule.conf('generate_kibana_link') or rule.conf('use_kibana_dashboard'):
            try:
                if rule.conf('generate_kibana_link'):
                    kb_link = reactor.kibana.generate_kibana_db(rule, alerts[0]['match_body'], get_index(rule))
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
        for alerter in rule.type.alerters:  # type: reactor.alerter.Alerter
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
                res = self.writeback('alert', alert, rule, doc_id=alert['uuid'], update=retried)
                if res and not agg_id:
                    agg_id = res['_id']

                # Add the alert to the alerts cache (will be cleared up in garbage collection)
                if res:
                    self.alerts_cache[rule.uuid][alert['uuid']] = alert
        else:
            for alert in alerts:
                # Lookup the existing alert
                alert_uuid = self.get_silenced(rule, rule.get_query_key_value(alert['match_body']) or '_silence')[2]
                if alert_uuid:
                    og_alert = self.get_alert(rule.uuid, alert_uuid)
                    if og_alert:
                        rule.merge_alert_body(og_alert, alert)
                        self.writeback('alert', og_alert, rule, doc_id=alert_uuid, update=True)

        return len(alerts)

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

        try:
            doc_type = '_doc' if self.es_client.es_version_at_least(6) else doc_type
            if update:
                return self.es_client.update(id=doc_id, index=index, doc_type=doc_type, body={'doc': writeback_body})
            else:
                return self.es_client.index(id=doc_id, index=index, doc_type=doc_type, body=writeback_body)
        except elasticsearch.ElasticsearchException as e:
            reactor_logger.exception('Error writing alert info to ElasticSearch: %s' % e)

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
                # TODO: no need to delete as we will update existing
        except (KeyError, elasticsearch.ElasticsearchException) as e:
            self.handle_error('Error fetching aggregated matches: %s' % e, {'id': '_id'})
        return matches

    def find_pending_aggregate_alert(self, rule: Rule, aggregation_key_value=None):
        query = {'filter': {'bool': {'must': [{'term': {'rule_uuid': rule.uuid}},
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
            self.handle_error("Error searching for pending aggregated matches: %s" % e, {'rule_uuid': rule.uuid})
            return None

        return res['hits']['hits'][0]

    def add_aggregated_alert(self, match: dict, rule: Rule):
        """ Save a match as pending aggregate alert to ElasticSearch. """

        # Optionally include the 'aggregation_key' as a dimension for aggregations
        aggregation_key_value = rule.get_aggregation_key_value(match)
        match_time = ts_to_dt(dots_get(match, rule.conf('timestamp_field')))

        if (not rule.current_aggregate_id.get(aggregation_key_value) or
                (rule.aggregate_alert_time.get(aggregation_key_value) < match_time)):
            # Reactor may have restarted while pending alerts exist
            pending_alert = self.find_pending_aggregate_alert(rule, aggregation_key_value)
            if pending_alert:
                alert_time = ts_to_dt(pending_alert['_source']['alert_time'])
                rule.aggregate_alert_time[aggregation_key_value] = alert_time
                agg_id = pending_alert['_id']
                rule.current_aggregate_id = {aggregation_key_value: agg_id}
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

                rule.aggregate_alert_time[aggregation_key_value] = alert_time
                agg_id = None
                reactor_logger.info('New aggregation for %s, aggregation_key: %s, next alert at %s.',
                                    rule.name, aggregation_key_value, alert_time)
        else:
            # Already pending aggregation, use existing alert_time
            alert_time = rule.aggregate_alert_time.get(aggregation_key_value)
            agg_id = rule.current_aggregate_id.get(aggregation_key_value)
            reactor_logger.info('Adding alert for %s to aggregation(id: %s, aggregation_key: %s), next alert at %s',
                                rule.name, agg_id, aggregation_key_value, alert_time)

        # TODO: update this
        alert_body = self.get_alert_body(match, rule, False, alert_time)
        if agg_id:
            alert_body['aggregate_id'] = agg_id
        if aggregation_key_value:
            alert_body['aggregation_key'] = aggregation_key_value
        res = self.writeback('alert', alert_body, rule)

        # If new aggregation, save id
        if res and not agg_id:
            rule.current_aggregate_id[aggregation_key_value] = res['_id']

        # Couldn't write to match to ElasticSearch, save it in memory for new
        if not res:
            rule.agg_matches.append(match)

        return res

    def silence(self, rule: Rule, duration: datetime.timedelta, revoke: bool = False):
        """ Silence an alert for a period of time. --silence and --rule must be passed as args. """

        # TODO: implement revoking silences (making sure to inform all running reactors of the change)
        reactor_logger.info('ElasticSearch version: %s', self.es_client.es_version)
        self.silence_cache.setdefault(rule.uuid, {})
        if self.set_realert(rule, '_silence', dt_now() + duration, 0):
            reactor_logger.warning('Silenced rule %s for %s', rule.name, duration)

    def set_realert(self, rule: Rule, silence_cache_key: str, until: datetime.datetime, exponent: int, alert_uuid=None):
        """ Write a silence to ElasticSearch for silence_cache_key until timestamp. """
        body = {'exponent': exponent,
                'rule_uuid': rule.uuid,
                'silence_key': rule.uuid + '.' + silence_cache_key,
                'alert_uuid': alert_uuid,
                '@timestamp': dt_now(),
                'until': until}

        self.silence_cache[rule.uuid][silence_cache_key] = (until, exponent, alert_uuid)
        # TODO: Inform the rule type of the silence. `Client.get_silenced` as well.
        return self.writeback('silence', body)

    def is_silenced(self, rule: Rule, silence_key=None, timestamp=None):
        """ Checks if a rule is silenced. Return false on exception. """
        silenced = self.get_silenced(rule, silence_key)
        return silenced and (timestamp or dt_now()) < silenced[0]

    def get_silenced(self, rule: Rule, silence_key=None) -> Optional[tuple]:
        """ Look up whether the rule and silence key exists. """
        cache_key = silence_key or '_silence'
        self.silence_cache.setdefault(rule.uuid, {})
        if cache_key in self.silence_cache[rule.uuid]:
            return self.silence_cache[rule.uuid][cache_key]

        # In debug/test mode we don't populate from Reactor status index
        if self.mode in ['debug', 'test']:
            return None

        query = {'term': {'silence_key': rule.uuid + '.' + cache_key}}
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
                self.silence_cache[rule.uuid][cache_key] = (ts_to_dt(until_ts), exponent, alert_uuid)
                return self.silence_cache[rule.uuid][cache_key]

            return None

    def handle_error(self, message, data=None):
        """ Logs messages at error level and writes message, data and traceback to ElasticSearch. """
        reactor_logger.error(message)
        body = {'message': message,
                'traceback': traceback.format_exc().strip().split('\n'),
                'data': data}
        self.writeback('error', body)

    def handle_uncaught_exception(self, exception, rule):
        """ Disables a rule and sends a notification. """
        reactor_logger.error(traceback.format_exc())
        self.handle_error('Uncaught exception running rule %s: %s' % (rule.name, exception), {'rule': rule.name})

        if rule.conf('disable_rule_on_error'):
            # TODO: implement rule disabling
            self.loader.disable(rule.uuid)
            self.scheduler.remove_job(job_id=rule.hash)
            reactor_logger.info('Rule %s disabled', rule.name)
        # TODO: add notification
        # if self.notify_email:
        #     self.send_notification()

    def get_top_counts(self, rule: Rule, start_time, end_time, keys, number=None, qk=None):
        """
        Counts the number of events for each unique value for each key field.
        Returns a diction with top_events_<key> mapped to the top 5 counts for each key.
        """
        all_counts = {}
        if not number:
            number = rule.conf('top_count_number', 5)
        for key in keys:
            index = get_index(rule, start_time, end_time)

            try:
                hits_terms = rule.get_hits_terms(start_time, end_time, index, key, qk, number)
            except QueryException as e:
                self.handle_error('Error running query: %s' % str(e), {'rule': rule.name, 'query': e.query})
                hits_terms = None

            if hits_terms is None:
                top_events_count = {}
            else:
                # TODO: convert this from py2 to py3
                buckets = hits_terms.values()[0]
                # get_hits_terms adds to num_hits, bu we don't want to count these
                rule.num_hits -= len(buckets)
                terms = {}
                for bucket in buckets:
                    terms[bucket['key']] = bucket['doc_count']
                counts = sorted(terms.values(), key=lambda x: x[1], reverse=True)
                top_events_count = dict(counts[:number])

            # Save a dict with the top 5 events by key
            all_counts['top_events_%s' % key] = top_events_count

        return all_counts

    def next_alert_time(self, rule: Rule, name: str, timestamp: datetime.datetime):
        """ Calculate an 'until' time and exponent based on how much past the last 'until' we are. """
        if name in self.silence_cache[rule.uuid]:
            last_until, exponent, alert_uuid = self.silence_cache[rule.uuid][name]
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

    def get_index_start(self, index: str, timestamp_field: str = '@timestamp') -> str:
        """
        Query for one result sorted by timestamp to find the beginning of the index.
        :param index: The index of which to find the earliest event
        :param timestamp_field: The field where the timestamp is stored
        :return: Timestamp of the earliest event
        """
        query = {'sort': {timestamp_field: {'ord': 'asc'}}}
        try:
            if self.es_client.es_version_at_least(6):
                res = self.es_client.search(index=index, size=1, body=query,
                                            _source_includes=[timestamp_field], ignore_unavailable=True)
            else:
                res = self.es_client.search(index=index, size=1, body=query,
                                            _source_include=[timestamp_field], ignore_unavailable=True)
        except elasticsearch.ElasticsearchException as e:
            # An exception was raised, return a date before the epoch
            self.handle_error("Elasticsearch query error: %s" % str(e), {'index': index, 'query': query})
            return '1969-12-30T00:00:00Z'
        if len(res['hits']['hits']) == 0:
            # Index is completely empty, return a date before the epoch
            return '1969-12-30T00:00:00Z'
        return res['hits']['hits'][0][timestamp_field]

    @lru_cache(maxsize=100)
    def get_alert(self, rule_uuid: str, uuid: str):
        """ Attempt to retrieve an alert from ElasticSearch by UUID. """
        if uuid in self.alerts_cache[rule_uuid]:
            return self.alerts_cache[rule_uuid][uuid]

        try:
            query = {'query': {'term': {'_id': uuid}}}
            # TODO: look into using `version`
            res = self.es_client.search(index=self.conf['alert_alias'], body=query, size=1)

            if res['hits']['hits']:
                return res['hits']['hits'][0]['_source']

        except:
            pass

        return None

