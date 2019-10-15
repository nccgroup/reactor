import copy
import datetime as dt
import os
import time
from typing import Generator, List, Optional

import elasticsearch
from sortedcontainers import SortedKeyList

import reactor.enhancement
from .exceptions import ConfigException, ReactorException, QueryException
from .kibana import filters_from_kibana
from .util import (
    dots_get, dots_set,
    generate_id, hashable,
    ts_to_dt, unix_to_dt, unixms_to_dt, ts_to_dt_with_format,
    dt_to_ts, dt_to_unix, dt_to_unixms, dt_to_ts_with_format,
    pretty_ts,
    dt_now,
    total_seconds,
    reactor_logger,
    ElasticSearchClient, elasticsearch_client,
    add_raw_postfix, format_index,
)


class WorkingData(object):
    def __init__(self, ts_field: str):
        # Counters
        self.hits = []
        self.num_hits = 0
        self.num_duplicates = 0
        self.total_hits = 0
        self.cumulative_hits = 0
        self.num_matches = 0
        self.alerts_sent = 0
        self.alerts_silenced = 0

        # Timers
        self.start_time = None
        self.end_time = None
        self.initial_start_time = None
        self.minimum_start_time = None
        self.original_start_time = None
        self.previous_end_time = None
        self.next_start_time = None
        self.next_min_start_time = None

        self._run_time = None

        # Misc.
        self.agg_alerts = []
        self.aggregate_alert_time = {}
        self.current_aggregate_id = {}
        self.processed_hits = {}
        self.has_run_once = False
        self.scroll_id = None

        # Rule Type
        self.ts_field = ts_field
        self.occurrences = {}
        self.silence_cache = {}
        self.alerts_cache = {}

    def reset(self):
        """ Reset working data ready for the rule to be run. """
        # Reset the rule counters.
        self.hits = []
        self.num_hits = 0
        self.num_duplicates = 0
        self.total_hits = 0
        self.cumulative_hits = 0
        self.num_matches = 0
        self.alerts_sent = 0
        self.alerts_silenced = 0

    def start_run_time(self):
        self._run_time = time.time()

    def end_run_time(self):
        self._run_time = time.time() - self._run_time

    @property
    def time_taken(self):
        return self._run_time


class EventWindow(object):
    """ A container to hold event counts for rules which need a chronological ordered event window. """

    def __init__(self, timeframe, get_timestamp=None, on_remove=None):
        self.timeframe = timeframe
        self.on_remove = on_remove
        self.get_ts = get_timestamp or (lambda pair: dots_get(pair[0], '@timestamp'))
        self.data = SortedKeyList(key=self.get_ts)
        self.running_count = 0

    def __iter__(self):
        return iter(self.data)

    def clear(self):
        self.data = SortedKeyList(key=self.get_ts)
        self.running_count = 0

    def append(self, event):
        """
        Add an event to the window. Event should be of the form (dict, count).
        This will also pop the oldest events and call on_remove on them until the
        window size is less than timeframe.
        """
        self.data.add(event)
        self.running_count += event[1]

        while self.duration() >= self.timeframe:
            oldest = self.data.pop(0)
            self.running_count -= oldest[1]
            self.on_remove and self.on_remove(oldest)

    def duration(self):
        """ Get the size in timedelta of the window. """
        if not self.data:
            return dt.timedelta(0)
        else:
            return self.get_ts(self.data[-1]) - self.get_ts(self.data[0])

    def count(self):
        """ Count the number of events in the window. """
        return self.running_count

    def mean(self):
        """ Compute the mean of the value_field in the window. """
        if len(self.data) > 0:
            data_sum = 0
            data_len = 0
            for event, count in self.data:
                if 'placeholder' not in event:
                    data_sum += count
                    data_len += 1
            if data_len > 0:
                return float(data_sum) / float(data_len)
            return None
        else:
            return None


class AcceptsHitsDataMixin(object):
    """ A mixin class to denote that the Rule accepts hits data. """
    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AcceptsCountDataMixin(object):
    """ A mixin class to denote that the Rule accepts count data. """
    def add_count_data(self, counts) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AcceptsTermsDataMixin(object):
    """ A mixin class to denote that the Rule accepts terms data. """
    def add_terms_data(self, counts) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AcceptsAggregationDataMixin(object):
    """ A mixin class to denote that the Rule accepts aggregation data. """
    def add_aggregation_data(self, payload) -> Generator[dict, None, None]:
        raise NotImplementedError()


class Rule(object):
    _schema_file = None
    _schema_relative = __file__

    def __init__(self, locator: str, hash: str, conf: dict):
        """
        :param locator: Locator of the rule
        :param hash: Uniquely identifying hash of the rule and its configuration
        :param conf: Configuration dictionary
        """
        self.locator = locator
        self._conf = conf
        self._hash = hash
        self._data = self._working_data()
        self._max_hits = float('inf')

        self._es_client = None
        self.alerters = []  # type: List[reactor.alerter.Alerter]
        self.enhancements = []  # type: List[reactor.enhancement.BaseEnhancement]

    @classmethod
    def schema_file(cls):
        """ Return the absolute path to the schema file. """
        return os.path.join(os.path.dirname(cls._schema_relative), cls._schema_file)

    def _working_data(self):
        return WorkingData(self.conf('timestamp_field', '@timestamp'))

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._hash == other

    def __repr__(self):
        return '%s.%s%s' % (self.__module__, type(self).__name__, self._conf)

    @property
    def enabled(self):
        return self._conf.get('is_enabled', True)

    @property
    def name(self):
        return self._conf['name']

    @property
    def run_every(self) -> dt.timedelta:
        return self._conf['run_every']

    @property
    def max_hits(self) -> float:
        return self._max_hits

    @max_hits.setter
    def max_hits(self, value: int):
        if value is None or 1 <= value <= 10000:
            self._max_hits = value or float('inf')
        else:
            raise ValueError('max_hits must either be None or between 1 and 10000 inclusive')

    def conf(self, key: str, default: any = None) -> any:
        """
        :param key: Key, in dots notation, to be found
        :param default: (optional) Default value if key not found
        """
        return dots_get(self._conf, key, default)

    def set_conf(self, key: str, value: any) -> bool:
        """
        :param key: Key, in dots notation, to be set
        :param value: Value
        """
        return dots_set(self._conf, key, value)

    @property
    def data(self) -> WorkingData:
        return self._data

    @data.setter
    def data(self, value: WorkingData) -> None:
        self._data = value

    @property
    def es_client(self) -> ElasticSearchClient:
        if self._es_client is None:
            self.es_client = elasticsearch_client(self.conf('elasticsearch'))
        return self._es_client

    @es_client.setter
    def es_client(self, value: reactor.util.ElasticSearchClient) -> None:
        if self._es_client is not None:
            raise Exception('Not allowed to be overridden')
        self._es_client = value

        # Get ElasticSearch version per rule
        if not self._es_client.es_version_at_least(5):
            return

        # In ElasticSearch >= v5.x, filters starting with query should have the top wrapper removed
        new_filters = []
        for es_filter in self.conf('filter', []):
            if es_filter.get('query'):
                new_filters.append(es_filter['query'])
            else:
                new_filters.append(es_filter)
        self.set_conf('filters', new_filters)

        # Change the top_count_keys to .raw
        if self.conf('top_count_keys') and self.conf('raw_count_keys', True):
            if self.conf('string_multi_field_name'):
                string_multi_field_name = self.conf('string_multi_field_name')
            else:
                string_multi_field_name = '.keyword'

            for i, key in enumerate(self.conf('top_count_keys')):
                if not key.endswith(string_multi_field_name):
                    self.conf('top_count_keys')[i] += string_multi_field_name

        # If download dashboard
        if self.conf('filter.download_dashboard'):
            # Download filters from Kibana and set the rules filters to them
            db_filters = filters_from_kibana(self, self.conf('filter.download_dashboard'))
            if db_filters is not None:
                self.set_conf('filter', db_filters)
            else:
                raise ReactorException(
                    'Could not download filters from %s' % self.conf('filter.download_dashboard'))

    def get_segment_size(self):
        """
        The segment size is either buffer_size for queries which can overlap or run_every for queries
        which must be strictly separate. This mimics the query size for when Reactor is running continuously.
        """
        if all(not self.conf(k) for k in ['use_count_query', 'use_terms_query', 'aggregation_query_element']):
            if self.conf('segment_size'):
                return self.conf('segment_size')
            else:
                return self.conf('buffer_time')
        elif self.conf('aggregation_query_element'):
            if self.conf('use_run_every_query_size'):
                return self.conf('run_every')
            else:
                return self.conf('buffer_time')
        else:
            return self.conf('run_every')

    def ts_to_dt(self, ts):
        """ Use the configured timestamp to datetime function. """
        if self._conf['timestamp_type'] == 'iso':
            return ts_to_dt(ts)
        elif self._conf['timestamp_type'] == 'unix':
            return unix_to_dt(ts)
        elif self._conf['timestamp_type'] == 'unix_ms':
            return unixms_to_dt(ts)
        elif self._conf['timestamp_type'] == 'custom':
            return ts_to_dt_with_format(ts, self._conf['timestamp_format'])
        else:
            raise ReactorException('Unknown timestamp type %s' % self._conf['timestamp_type'])

    def dt_to_ts(self, ts):
        """ Use the configured datetime to timestamp function. """
        if self._conf['timestamp_type'] == 'iso':
            return dt_to_ts(ts)
        elif self._conf['timestamp_type'] == 'unix':
            return dt_to_unix(ts)
        elif self._conf['timestamp_type'] == 'unix_ms':
            return dt_to_unixms(ts)
        elif self._conf['timestamp_type'] == 'custom':
            return dt_to_ts_with_format(ts, self._conf['timestamp_format'])
        else:
            raise ReactorException('Unknown timestamp type %s' % self._conf['timestamp_type'])

    def get_alert_body(self, data: dict, match: dict, alert_time) -> dict:
        doc_uuid = generate_id()
        body = {'uuid': doc_uuid,
                'rule_uuid': self.locator,
                'rule_name': self.name,
                'match_data': data,
                'match_body': match,
                'alert_info': [alerter.get_info() for alerter in self.alerters],
                'alert_time': alert_time,
                'modify_time': alert_time,
                'num_matches': 1}

        match_time = dots_get(match, self.conf('timestamp_field'))
        if match_time is not None:
            body['match_time'] = match_time

        return body

    def merge_alert_body(self, orig_alert: dict, new_alert: dict):
        """ Merge `new_alert` into `orig_alert`. """
        # Merge number of matches and update modify time
        orig_alert['num_matches'] += new_alert['num_matches']
        orig_alert['modify_time'] = new_alert['modify_time']

        # Merge match_data properties
        orig_alert['match_data']['num_events'] += new_alert['match_data']['num_events']
        orig_alert['match_data']['began_at'] = min(ts_to_dt(orig_alert['match_data']['began_at']),
                                                   ts_to_dt(new_alert['match_data']['began_at']))
        orig_alert['match_data']['ended_at'] = max(ts_to_dt(orig_alert['match_data']['ended_at']),
                                                   ts_to_dt(new_alert['match_data']['ended_at']))

    def get_query(self, filters: list, start_time=None, end_time=None, sort: Optional[str] = 'asc',
                  timestamp_field: str = '@timestamp') -> dict:
        """
        :param filters:
        :param start_time:
        :param end_time:
        :param sort: asc|desc|None
        :param timestamp_field:
        """
        start_time = self.dt_to_ts(start_time) if start_time else None
        end_time = self.dt_to_ts(end_time) if end_time else None
        filters = copy.copy(filters)
        if start_time and end_time:
            filters.insert(0, {'range': {timestamp_field: {'gt': start_time, 'lte': end_time}}})
        elif start_time:
            filters.insert(0, {'range': {timestamp_field: {'gt': start_time}}})
        elif end_time:
            filters.insert(0, {'range': {timestamp_field: {'lte': end_time}}})
        es_filters = {'filter': {'bool': {'must': filters}}}

        if self.es_client.es_version_at_least(5):
            query = {'query': {'bool': es_filters}}
        else:
            query = {'query': {'filtered': es_filters}}

        if sort:
            query['sort'] = [{timestamp_field: {'order': sort}}]

        return query

    def get_terms_query(self, query, size, field):
        """ Takes a query generated by `get_query` and outputs an aggregation query. """
        min_doc_count = self.conf('min_doc_count', 1)
        if self.es_client.es_version_at_least(5):
            aggs_query = query
            aggs_query['aggs'] = {'counts': {'terms': {'field': field, 'size': size, 'min_doc_count': min_doc_count}}}
        else:
            query_element = query['query']
            query_element.pop('sort', None)
            query_element['filtered'].update({'aggs': {'counts': {'terms': {'fields': field,
                                                                            'size': size,
                                                                            'min_doc_count': min_doc_count}}}})
            aggs_query = {'aggs': query_element}

        return aggs_query

    def get_aggregation_query(self, query, query_key, terms_size, timestamp_field='@timestamp'):
        """ Takes a query generated by `get_query` and outputs an aggregation query. """
        metric_agg_element = self.conf('aggregation_query_element')
        bucket_interval_period = self.conf('bucket_interval_period')
        if bucket_interval_period is not None:
            aggs_element = {
                'interval_aggs': {
                    'date_histogram': {
                        'field': timestamp_field,
                        'interval': bucket_interval_period},
                    'aggs': metric_agg_element
                }
            }
            if self.conf('bucket_offset_delta'):
                aggs_element['interval_aggs']['date_histogram']['offset'] = '+%ss' % self.conf('bucket_offset_delta')
        else:
            aggs_element = metric_agg_element

        if query_key is not None:
            for idx, key in reversed(list(enumerate(query_key.split(',')))):
                aggs_element = {'bucket_aggs': {'terms': {'field': key, 'size': terms_size,
                                                          'min_doc_count': self.conf('min_doc_count', 1)}}}

        aggs_query = query
        aggs_query['aggs'] = aggs_element
        return aggs_query

    def remove_duplicate_events(self, data):
        """ Removes the  """
        new_events = []
        for event in data:
            if event['_id'] in self._data.processed_hits:
                continue

            # Remember the new data's IDs
            self._data.processed_hits[event['_id']] = dots_get(event, self.conf('timestamp_field'))
            new_events.append(event)

        return new_events

    def adjust_start_time_for_overlapping_agg_query(self, start_time: dt.datetime) -> dt.datetime:
        if self.conf('aggregation_query_element'):
            if self.conf('allow_buffer_time_overlap') and not self.conf('use_run_every_query_size') \
                    and self.conf('buffer_time') > self.conf('run_every'):
                start_time = start_time - (self.conf('buffer_time') - self.conf('run_every'))

        return start_time

    def adjust_start_time_for_interval_sync(self, start_time: dt.datetime) -> dt.datetime:
        # If aggregation query adjust bucket offset
        if self.conf('aggregation_query_element'):
            if self.conf('bucket_interval'):
                es_interval_delta = self.conf('bucket_interval')
                unix_start_time = dt_to_unix(start_time)
                es_interval_delta_in_sec = total_seconds(es_interval_delta)
                offset = int(unix_start_time % es_interval_delta_in_sec)

                if self.conf('sync_bucket_interval'):
                    start_time = unix_to_dt(unix_start_time - offset)
                else:
                    self.set_conf('bucket_offset_delta', offset)

        return start_time

    def get_query_key_value(self, match):
        """
        Get the value for the match's query_key (or None) to form the key used for the silence_cache.
        Flatline rule types sets key instead of the actual query_key.
        """
        if self.conf('query_key'):
            return dots_get(match, self.conf('query_key'), '_missing')
        else:
            return None

    def get_aggregation_key_value(self, match):
        """
        Get the value for the match's aggregation_key (or None) to form the key used for grouped aggregates.
        """
        if self.conf('aggregation_key'):
            return dots_get(match, self.conf('aggregation_key'), '_missing')
        else:
            return None

    def get_hits(self, start_time, end_time, index: str) -> Optional[list]:
        """
        Query ElasticSearch for the given rule and return the processed results.
        :raises: reactor.exceptions.QueryException
        """
        query = self.get_query(self.conf('filter'), start_time, end_time,
                               timestamp_field=self.conf('timestamp_field'))

        # Ensure that the rule include fields are returned
        if not self.conf('_source_enabled'):
            if self.es_client.es_version_at_least(5):
                query['stored_fields'] = self.conf('include')
            else:
                query['fields'] = self.conf('include')
            extra_args = {}
        elif self.es_client.es_version_at_least(6, 6):
            extra_args = {'_source_includes': self.conf('include')}
        else:
            extra_args = {'_source_include': self.conf('include')}

        # Only scroll if max_hits is not set or max_query_size is smaller
        scroll_keepalive = self.conf('scroll_keepalive') if self.conf('max_query_size') < self.max_hits else None
        try:
            if self._data.scroll_id:
                res = self.es_client.scroll(scroll_id=self._data.scroll_id, scroll=scroll_keepalive)
            else:
                res = self.es_client.search(scroll=scroll_keepalive, index=index,
                                            size=int(min(self.max_hits-self._data.num_hits, self.conf('max_query_size'))),
                                            body=query, ignore_unavailable=True, **extra_args)
                if self.es_client.es_version_at_least(7):
                    self._data.total_hits = int(res['hits']['total']['value'])
                else:
                    self._data.total_hits = int(res['hits']['total'])

            if len(res.get('shards', {}).get('failures', [])) > 0:
                try:
                    errs = [e['reason']['reason'] for e in res['_shards']['failures']
                            if 'Failed to parse' in e['reason']['reason']]
                    if len(errs):
                        raise elasticsearch.ElasticsearchException(errs)
                except (TypeError, KeyError):
                    # Different versions of ElasticSearch have this formatted in different ways. Fallback to str-ing the
                    # whole thing
                    raise elasticsearch.ElasticsearchException(str(res['_shard']['failures']))

            reactor_logger.debug(str(res))

        except elasticsearch.ElasticsearchException as e:
            # ElasticSearch sometimes gives us GIGANTIC error messages
            # (so big that they will fill the entire terminal buffer)
            if len(str(e)) > 1024:
                e = str(e)[:1024] + '... (%d characters removed)' % (len(str(e)) - 1024)
            raise QueryException(e, query=query)

        hits = res['hits']['hits']

        self._data.num_hits += len(hits)
        lt = self.conf('use_local_time')

        # If the max_hits reached and there are more hits available
        if self._data.num_hits >= self.max_hits and self._data.total_hits > self.max_hits:
            if self._data.num_hits > self.max_hits:
                hits = hits[:(self.max_hits % self.conf('max_query_size'))]
            self._data.num_hits = self.max_hits
            self._data.scroll_id = None
            reactor_logger.debug('Queried rule %s from %s to %s: %s / %s hits (%s total hits)',
                                 self.name, pretty_ts(start_time, lt), pretty_ts(end_time, lt),
                                 self._data.num_hits, len(hits), self._data.total_hits)
            reactor_logger.warning('Maximum hits reached (%s hits of %s total hits), '
                                   'this could trigger false positives alerts', self.max_hits, self._data.total_hits)
        elif self._data.total_hits > self.conf('max_query_size'):
            reactor_logger.debug('Queried rule %s from %s to %s: %s / %s hits (%s total hits) (scrolling...)',
                                 self.name, pretty_ts(start_time, lt), pretty_ts(end_time, lt),
                                 self._data.num_hits, len(hits), self._data.total_hits)
            self._data.scroll_id = res['_scroll_id']
        else:
            reactor_logger.debug('Queried rule %s from %s to %s: %s / %s hits (%s total hits)',
                                 self.name, pretty_ts(start_time, lt), pretty_ts(end_time, lt),
                                 self._data.num_hits, len(hits), self._data.total_hits)

        hits = self.process_hits(hits)

        # Record doc_type for use in get_top_counts
        if not self.conf('doc_type') and len(hits):
            self.set_conf('doc_type', hits[0]['_type'])
        return hits

    def get_hits_count(self, start_time: dt.datetime, end_time: dt.datetime,
                       index: str) -> Optional[dict]:
        """
        Query ElasticSearch for the count of results and returns a list of timestamps
        equal to the end_time. This allows the results to be passed to rules which expect
        an object for each hit.
        """
        query = self.get_query(self.conf('filter'), start_time, end_time, sort=None,
                               timestamp_field=self.conf('timestamp_field'))

        try:
            res = self.es_client.count(index=index, doc_type=self.conf('doc_type'), body=query, ignore_unavailable=True)
        except elasticsearch.ElasticsearchException as e:
            # ElasticSearch sometimes gives us GIGANTIC error messages
            # (so big that they will fill the entire terminal buffer)
            if len(str(e)) > 1024:
                e = str(e)[:1024] + '... (%d characters removed)' % (len(str(e)) - 1024)
            raise QueryException(e, query=query)

        self._data.num_hits += res['count']
        lt = self.conf('use_local_time')
        reactor_logger.debug('Queried rule %s from %s to %s: %s hits', self.name,
                             pretty_ts(start_time, lt), pretty_ts(end_time, lt), res['count'])

        return {end_time: res['count']}

    def get_hits_terms(self, start_time: dt.datetime, end_time: dt.datetime,
                       index, key, qk=None, size=None) -> Optional[dict]:
        rule_filter = copy.copy(self.conf('filter'))
        if qk:
            qk_list = qk.split(',')
            end = '.keyword'

            if len(qk_list) == 1:
                qk = qk_list[0]
                filter_key = self.conf('query_key')
                if self.conf('raw_count_keys', True) and not self.conf('query_key').endswith(end):
                    filter_key = add_raw_postfix(filter_key, self.es_client.es_version)
                rule_filter.extend([{'term': {filter_key: qk}}])
            else:
                filter_keys = self.conf('compound_query_keys')
                for i, filter_key in enumerate(filter_keys):
                    if self.conf('raw_count_keys', True) and not key.endswith(end):
                        filter_key = add_raw_postfix(filter_key, self.es_client.es_version)
                    rule_filter.extend([{'term': {filter_key: qk_list[i]}}])

        base_query = self.get_query(rule_filter, start_time, end_time, sort=None,
                                    timestamp_field=self.conf('timestamp_field'))
        if size is None:
            size = self.conf('terms_size', 50)
        query = self.get_terms_query(base_query, size, key)

        try:
            res = self.es_client.search(index=index, doc_type=self.conf('doc_type'),
                                        body=query, size=0, ignore_unavailable=True)

        except elasticsearch.ElasticsearchException as e:
            # ElasticSearch sometimes gives us GIGANTIC error messages
            # (so big that they will fill the entire terminal buffer)
            if len(str(e)) > 1024:
                e = str(e)[:1024] + '... (%d characters removed)' % (len(str(e)) - 1024)
            raise QueryException(e, query=query)

        if 'aggregations' not in res:
            return {}
        buckets = res['aggregations']['counts']['buckets']

        self._data.num_hits += len(buckets)
        lt = self.conf('use_local_time')
        reactor_logger.debug('Queried rule %s from %s to %s: %s buckets', self.name,
                             pretty_ts(start_time, lt), pretty_ts(end_time, lt), len(buckets))

        return {end_time: buckets}

    def get_hits_aggregation(self, start_time: dt.datetime, end_time: dt.datetime,
                             index: str, query_key, term_size=None) -> Optional[dict]:
        rule_filter = copy.copy(self.conf('filter'))
        base_query = self.get_query(rule_filter, start_time, end_time, sort=None,
                                    timestamp_field=self.conf('timestamp_field'))

        if term_size is None:
            term_size = self.conf('terms_size', 50)
        query = self.get_aggregation_query(base_query, query_key, term_size, self.conf('timestamp_field'))

        try:
            res = self.es_client.search(index=index, doc_type=self.conf('doc_type'),
                                        body=query, size=0, ignore_unavailable=True)

        except elasticsearch.ElasticsearchException as e:
            # ElasticSearch sometimes gives us GIGANTIC error messages
            # (so big that they will fill the entire terminal buffer)
            if len(str(e)) > 1024:
                e = str(e)[:1024] + '... (%d characters removed)' % (len(str(e)) - 1024)
            raise QueryException(e, query=query)

        if 'aggregations' not in res:
            return {}
        payload = res['aggregations']

        if self.es_client.es_version_at_least(7):
            self._data.total_hits += res['hits']['total']['value']
        else:
            self._data.total_hits += res['hits']['total']

        return {end_time: payload}

    def process_hits(self, hits: list) -> list:
        """
        Update the _source field for each hit received from ElasticSearch based on the rule configuration.

        This replaces timestamps with datetime objects,
        folds important fields into _source and creates compound query_keys.
        """
        processed_hits = []
        for hit in hits:
            # Merge fields and _source
            hit.setdefault('_source', {})
            for key, value in hit.get('fields', {}).items():
                # Fields are returned as lists, assume any with length 1 are not arrays in _source
                # Except sometimes they aren't lists. This is dependent on ElasticSearch version
                hit['_source'].setdefault(key, value[0] if type(value) is list and len(value) == 1 else value)

            # Convert the timestamp to a datetime
            ts = dots_get(hit['_source'], self.conf('timestamp_field'))
            if not ts and not self.conf('_source_enabled'):
                raise ReactorException('Error: No timestamp was found for hit. _source_enabled is set to false,'
                                       'Check your mappings for stored fields')
            dots_set(hit['_source'], self.conf('timestamp_field'), self.ts_to_dt(ts))
            dots_set(hit, self.conf('timestamp_field'), dots_get(hit['_source'], self.conf('timestamp_field')))

            # Tack metadata fields into _source
            for field in ['_id', '_index', '_type']:
                hit['_source'][field] = hit.get(field, None)

            # Add the compound keys
            if self.conf('compound_query_key'):
                values = [dots_get(hit['_source'], key) for key in self.conf('compound_query_key')]
                hit['_source'][self.conf('query_key')] = ','.join(values)
            if self.conf('compound_aggregation_key'):
                values = [dots_get(hit['_source'], key) for key in self.conf('compound_aggregation_key')]
                hit['_source'][self.conf('aggregation_key')] = ','.join(values)

            processed_hits.append(hit['_source'])

        return processed_hits

    def get_index(self, start_time=None, end_time=None):
        index = self.conf('index')
        add_extra = self.conf('search_extra_index')
        if self.conf('use_strftime_index'):
            if start_time and end_time:
                return format_index(index, start_time, end_time, add_extra)
            else:
                # Replace the substring containing format characters with a *
                format_start = index.find('%')
                format_end = index.rfind('%') + 2
                return index[:format_start] + '*' + index[format_end:]
        else:
            return index

    #
    # RuleType methods
    #
    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        """ Prepare the Rule for receiving data. Should be called before running a rule. """
        pass

    def add_match(self, extra: dict, event: dict) -> (dict, dict):
        """
        :param extra: Extra data about the triggered alert
        :param event: Event that triggered the rule match
        :return: Tuple of `extra` and a deep copy of `event`
        """
        event = copy.deepcopy(event)
        if self._data.ts_field in event:
            event[self._data.ts_field] = dt_to_ts(event[self._data.ts_field])

        self._data.num_matches += 1
        return extra, event

    def get_match_str(self, extra: dict, match: dict) -> str:
        return ''

    def garbage_collect(self, timestamp: dt.datetime) -> Generator[dict, None, None]:
        yield from ()

    #
    # Replacement to lambda function to allow pickling
    #
    def get_ts(self, pair: tuple):
        """  """
        return dots_get(pair[0], self._data.ts_field)


class AnyRule(AcceptsHitsDataMixin, Rule):
    _schema_file = 'schemas/ruletype-any.yaml'

    """ A rule that will match on any input data. """
    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self._conf.get('query_key', None)
        for event in data:
            extra = {'key': hashable(dots_get(event, qk)) if qk else 'all',
                     'num_events': 1,
                     'began_at': ts_to_dt(dots_get(event, self._data.ts_field)),
                     'ended_at': ts_to_dt(dots_get(event, self._data.ts_field))}
            yield self.add_match(extra, event)


class CompareRule(AcceptsHitsDataMixin, Rule):
    """ A base class for matching a specific term by passing it to a compare function. """

    def expand_entries(self, list_type: str):
        """
        Expand entries specified in files using the '!file' directive, if there are
        any, then add everything to a set.
        """
        entries_set = set()
        for entry in self._conf[list_type]:
            if entry.startswith('!file'):  # - "!file /path/to/list"
                filename = entry.split()[1]
                with open(filename, 'r') as f:
                    for line in f:
                        entries_set.add(line.rstrip())
            else:
                entries_set.add(entry)
        self._conf[list_type] = entries_set

    def compare(self, event: dict) -> bool:
        """ An event is a match if this returns true """
        raise NotImplementedError()

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        for event in data:
            if self.compare(event):
                yield self.add_match(*self.generate_match(event))

    def generate_match(self, event: dict) -> (dict, dict):
        raise NotImplementedError()


class BlacklistRule(CompareRule):
    """ A CompareRule where the compare function checks a given key against a blacklist. """

    _schema_file = 'schemas/ruletype-blacklist.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(BlacklistRule, self).__init__(locator, hash, conf)
        self.expand_entries('blacklist')

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        # Add the blacklist to the filter
        self._conf.setdefault('original_filter', self._conf['filter'])
        self._conf['filter'] = self._conf['original_filter']
        terms_query = {'terms': {self._conf['compare_key']: list(self._conf['blacklist'])}}
        if es_client.es_version_at_least(6):
            self._conf['filter'].append(terms_query)
        else:
            self._conf['filter'].append({'constant_score': {'filter': terms_query}})

    def compare(self, event: dict) -> bool:
        term = dots_get(event, self._conf['compare_key'])
        return term in self._conf['blacklist']

    def generate_match(self, event: dict) -> (dict, dict):
        extra = {'compare_key': self._conf['compare_key'],
                 'num_events': 1,
                 'began_at': dots_get(event, self._data.ts_field),
                 'ended_at': dots_get(event, self._data.ts_field)}
        return extra, event


class WhitelistRule(CompareRule):
    """ A CompareRule where the compare function checks a given key against a whitelist. """

    _schema_file = 'schemas/ruletype-whitelist.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(WhitelistRule, self).__init__(locator, hash, conf)
        self.expand_entries('whitelist')

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        # Add the whitelist to the filter
        self._conf.setdefault('original_filter', self._conf['filter'])
        self._conf['filter'] = self._conf['original_filter']
        terms_query = {'bool': {'must_not': {'terms': {self._conf['compare_key']: list(self._conf['whitelist'])}}}}
        if es_client.es_version_at_least(6):
            self._conf['filter'].append(terms_query)
        else:
            self._conf['filter'].append({'constant_score': {'filter': terms_query}})

    def compare(self, event: dict) -> bool:
        term = dots_get(event, self._conf['compare_key'])
        if term is None:
            return not self._conf['ignore_null']
        else:
            return term not in self._conf['whitelist']

    def generate_match(self, event: dict) -> (dict, dict):
        extra = {'compare_key': self._conf['compare_key'],
                 'num_events': 1,
                 'began_at': dots_get(event, self._data.ts_field),
                 'ended_at': dots_get(event, self._data.ts_field)}
        return extra, event


class ChangeRule(CompareRule):
    """ A rule that will store values for a certain term and match if those values change. """

    _schema_file = 'schemas/ruletype-change.yaml'

    change_map = {}
    occurrence_time = {}

    def compare(self, event: dict) -> bool:
        key = hashable(dots_get(event, self._conf['query_key']))
        values = []
        reactor_logger.debug('Previous values of compare keys: %s', self._data.occurrences)
        # compound_compare_key is generated automatically from compare_key
        for val in self._conf['compound_compare_key']:
            lookup_value = dots_get(event, val)
            values.append(lookup_value)
        reactor_logger.debug('Current values of compare keys: %s', values)

        changed = False
        for val in values:
            if not isinstance(val, bool) and not val and self._conf['ignore_null']:
                return False
        # If we have seen this key before, compare it to the new value
        if key in self._data.occurrences:
            for idx, previous_values in enumerate(self._data.occurrences[key]):
                reactor_logger.debug('%s  %s', previous_values, values[idx])
                changed = previous_values != values[idx]
                if changed:
                    break
            if changed:
                self.change_map[key] = (self._data.occurrences[key], values)
                # If using timeframe, only return if the time delta is < timeframe
                if key in self.occurrence_time:
                    changed = event[self._data.ts_field] - self.occurrence_time[key] <= self._conf['timeframe']

        # Update the current value and time
        reactor_logger.debug('Setting current value of compare keys values: %s', values)
        self._data.occurrences[key] = values
        if 'timeframe' in self._conf:
            self.occurrence_time[key] = event[self._data.ts_field]
        reactor_logger.debug('Final result of comparison between previous and current values: %r', changed)
        return changed

    def generate_match(self, event: dict) -> (dict, dict):
        # TODO: this is not technically correct
        # if the term changes multiple times before an alert is sent
        # this data will be overwritten with the most recent change
        change = self.change_map.get(hashable(dots_get(event, self._conf['query_key'])))
        extra = {}
        if change:
            extra = {'old_value': change[0],
                     'new_value': change[1],
                     'key': hashable(dots_get(event, self._conf['query_key'])),
                     'num_events': 1,
                     'began_at': dots_get(event, self._data.ts_field),
                     'ended_at': dots_get(event, self._data.ts_field)}
            reactor_logger.debug('Description of the changed records (%s): %s', extra, event)
        return extra, event


class FrequencyRule(AcceptsTermsDataMixin, AcceptsCountDataMixin, AcceptsHitsDataMixin, Rule):
    """ A Rule that matches if num_events number of events occur within a timeframe. """

    _schema_file = 'schemas/ruletype-frequency.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(FrequencyRule, self).__init__(locator, hash, conf)
        self.attach_related = self._conf['attach_related']

    def check_for_match(self, key, end=False):
        # Match if, after removing old events, we hit num_events.
        # the 'end' parameter depends on whether this was called from the
        # middle or end of an add_data call and is used in sub classes
        if self._data.occurrences[key].count() >= self._conf['num_events']:
            # Check if the occurrences are split across a silence
            e = self._data.occurrences[key].data[0][0]
            silence_cache_key = dots_get(e, self._conf.get('query_key'), '_missing') if self._conf.get('query_key') else '_silence'
            if silence_cache_key in self._data.silence_cache:
                oldest_ts = ts_to_dt(self.get_ts(self._data.occurrences[key].data[0]))
                newest_ts = ts_to_dt(self.get_ts(self._data.occurrences[key].data[-1]))
                until, _, _ = self._data.silence_cache[silence_cache_key]
                if oldest_ts <= until < newest_ts:
                    while self.get_ts(self._data.occurrences[key].data[0]) <= until:
                        self._data.occurrences[key].data.pop(0)
                    return

            extra = {'key': key, 'num_events': self._data.occurrences[key].count(),
                     'began_at': self.get_ts(self._data.occurrences[key].data[0]),
                     'ended_at': self.get_ts(self._data.occurrences[key].data[-1])}
            event = self._data.occurrences[key].data[-1][0]
            if self.attach_related:
                event['related_events'] = [data[0] for data in self._data.occurrences[key].data[:-1]]
            self._data.occurrences.pop(key)
            yield self.add_match(extra, event)

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self._conf.get('query_key', None)
        keys = set()
        for event in data:
            # If no query_key, we use the key 'all' for all events
            key = hashable(dots_get(event, qk)) if qk else 'all'

            # Store the timestamps of recent occurrences, per key
            self._data.occurrences.setdefault(key, EventWindow(self._conf['timeframe'], self.get_ts)).append((event, 1))
            yield from self.check_for_match(key, end=False)
            keys.add(key)

            # TODO: will potentially need to accept that any solution for now will not be able to easily handle historic
            #  data added after future alerts have been fired, or that it requires data to come in order

        # We call this multiple times with the 'end' parameter because subclasses
        # may or may not want to check while only partial data has been added
        for key in keys:
            if key in self._data.occurrences:
                yield from self.check_for_match(key, end=True)

    def get_match_str(self, extra: dict, match: dict) -> str:
        lt = self._conf['use_local_time']
        match_ts = dots_get(match, self._data.ts_field)
        start_time = pretty_ts(ts_to_dt(match_ts) - self._conf['timeframe'], lt)
        end_time = pretty_ts(match_ts, lt)
        return 'At least %d events occurred between %s and %s\n\n' % (self._conf['num_events'], start_time, end_time)

    def garbage_collect(self, timestamp: dt.datetime) -> Generator[dict, None, None]:
        """ Remove all occurrence data that is beyond the timeframe away. """
        stale_keys = []
        for key, window in self._data.occurrences.items():
            if timestamp - dots_get(window.data[-1][0], self._data.ts_field) > self._conf['timeframe']:
                stale_keys.append(key)
        list(map(self._data.occurrences.pop, stale_keys))
        yield from ()

    def add_count_data(self, counts) -> Generator[dict, None, None]:
        """ Add count data to the rule. Data should be of the form {ts: count}. """
        if len(counts) > 1:
            raise ReactorException('add_count_data can only accept one count at a time')
        (ts, count), = list(counts.items())

        event = ({self._data.ts_field: ts}, count)
        self._data.occurrences.setdefault('all', EventWindow(self._conf['timeframe'], self.get_ts)).append(event)
        yield from self.check_for_match('all')

    def add_terms_data(self, terms) -> Generator[dict, None, None]:
        for timestamp, buckets in terms.items():
            for bucket in buckets:
                event = ({self._data.ts_field: timestamp,
                          self._conf['query_key']: bucket['key']}, bucket['doc_count'])
                self._data.occurrences.setdefault(bucket['key'],
                                                  EventWindow(self._conf['timeframe'], self.get_ts)).append(event)
                yield from self.check_for_match(bucket['key'])


class FlatlineRule(FrequencyRule):
    """ A FrequencyRule that matches when there is low number of events within a timeframe. """

    _schema_file = 'schemas/ruletype-flatline.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(FlatlineRule, self).__init__(locator, hash, conf)
        self.threshold = self._conf['threshold']

        # Dictionary mapping query keys to the first event observed
        self.first_event = {}

    def get_query_key_value(self, match):
        """
        Get the value for the match's query_key (or None) to form the key used for the silence_cache.
        Flatline rule types sets key instead of the actual query_key.
        """
        if 'key' in match:
            return str(match['key'])
        elif self.conf('query_key'):
            return dots_get(match, self.conf('query_key'), '_missing')
        else:
            return None

    def check_for_match(self, key, end=True):
        # This function gets called between every added document with end=True after the last
        # We ignore the calls before the end because it may trigger false positives
        if not end:
            return

        most_recent_ts = self.get_ts(self._data.occurrences[key].data[-1])
        if self.first_event.get(key) is None:
            self.first_event[key] = most_recent_ts

        # Don't check for matches until timeframe has elapsed
        if most_recent_ts - self.first_event[key] < self._conf['timeframe']:
            return

        # Match if, after removing old events, we hit num_events
        count = self._data.occurrences[key].count()
        if count < self._conf['threshold']:
            # Do a deep-copy, otherwise we lose the datetime type in the timestamp field of the last event
            extra = {'key': key,
                     'count': count,
                     'num_events': count,
                     'began_at': self.first_event[key],
                     'ended_at': most_recent_ts}
            event = copy.deepcopy(self._data.occurrences[key].data[-1][0])
            yield self.add_match(extra, event)

            if not self._conf['forget_keys']:
                # After adding this match, leave the occurrences window alone since it will
                # be pruned in the next add_data or garbage_collect, but reset the first_event
                # so that alerts continue to fire until the threshold is passed again.
                least_recent_ts = self.get_ts(self._data.occurrences[key].data[0])
                timeframe_ago = most_recent_ts - self._conf['timeframe']
                self.first_event[key] = min(least_recent_ts, timeframe_ago)
            else:
                # Forget about this key until we see it again
                self.first_event.pop(key)
                self._data.occurrences.pop(key)

    def get_match_str(self, extra: dict, match: dict) -> str:
        ts = match[self._data.ts_field]
        lt = self._conf.get('use_local_time')
        message = 'An abnormally low number of events occurred around %s.\n' % pretty_ts(ts, lt)
        message += 'Between %s and %s, there were less than %s events.\n\n' % (
            pretty_ts(ts_to_dt(ts) - self._conf['timeframe'], lt),
            pretty_ts(ts, lt),
            self._conf['threshold']
        )
        return message

    def garbage_collect(self, timestamp: dt.datetime) -> Generator[dict, None, None]:
        # We add an event with a count of zero to the EventWindow for each key. This will cause the EventWindow
        # to remove events that occurred more than one timeframe ago, and call on_remove on them.
        default = ['all'] if 'query_key' not in self._conf else []
        for key in list(self._data.occurrences.keys()) or default:
            event = ({self._data.ts_field: timestamp}, 0)
            self._data.occurrences.setdefault(key, EventWindow(self._conf['timeframe'], self.get_ts)).append(event)
            self.first_event.setdefault(key, timestamp)
            yield from self.check_for_match(key, end=True)


class SpikeRule(AcceptsTermsDataMixin, AcceptsCountDataMixin, AcceptsHitsDataMixin, Rule):
    """ A Rule that uses two sliding windows to compare relative event frequency. """

    _schema_file = 'schemas/ruletype-spike.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(SpikeRule, self).__init__(locator, hash, conf)
        self.timeframe = self._conf['timeframe']

        self.ref_windows = {}
        self.cur_windows = {}

        self.first_event = {}
        self.skip_checks = {}

        self.field_value = self._conf.get('field_value')

        self.ref_window_filled_once = False

    def clear_windows(self, qk, event):
        """ Reset the state and prevent alerts until windows are filled again """
        self.ref_windows[qk].clear()
        self.first_event.pop(qk)
        self.skip_checks[qk] = dots_get(event, self._data.ts_field) + self._conf['timeframe'] * 2

    def handle_event(self, event: dict, count: int, qk: str = 'all'):
        self.first_event.setdefault(qk, event)

        self.ref_windows.setdefault(qk, EventWindow(self.timeframe, self.get_ts))
        self.cur_windows.setdefault(qk, EventWindow(self.timeframe, self.get_ts, self.ref_windows[qk].append))

        self.cur_windows[qk].append((event, count))

        # Don't alert if ref window has not yet been filled for this key AND
        if dots_get(event, self._data.ts_field) - self.first_event[qk][self._data.ts_field] < self._conf['timeframe'] * 2:
            # Reactor has not been running long enough for any alerts OR
            if not self.ref_window_filled_once:
                return
            # This rule is not using alert_on_new_data (with query_key) OR
            if not (self._conf.get('query_key') and self._conf.get('alert_on_new_data')):
                return
            # An alert for this qk has recently fired
            if qk in self.skip_checks and dots_get(event, self._data.ts_field) < self.skip_checks[qk]:
                return
        else:
            self.ref_window_filled_once = True

        if self.field_value is not None:
            if self.find_matches(self.ref_windows[qk].mean(), self.cur_windows[qk].mean()):
                # Skip over placeholder events
                e = None
                for e, count in self.cur_windows[qk].data:
                    if "placeholder" not in e:
                        break
                if e:
                    yield self.add_match(*self.generate_match(e, qk))
                    self.clear_windows(qk, e)
        else:
            if self.find_matches(self.ref_windows[qk].count(), self.cur_windows[qk].count()):
                # Skip over placeholder events which have count=0
                e = None
                for e, count in self.cur_windows[qk].data:
                    if count:
                        break
                if e:
                    yield self.add_match(*self.generate_match(e, qk))
                    self.clear_windows(qk, e)

    def find_matches(self, ref, cur):
        """ Determines if an event spike or dip is happening. """
        # Apply threshold limits
        if self.field_value is None:
            if cur < self._conf.get('threshold_cur', 0) or ref < self._conf.get('threshold_ref', 0):
                return False
        elif ref is None or ref == 0 or cur is None or cur == 0:
            return False

        spike_dn = cur <= (ref / self._conf['spike_height'])
        spike_up = cur >= (ref * self._conf['spike_height'])

        return (spike_up and self._conf['spike_type'] in ['both', 'up']) or \
               (spike_dn and self._conf['spike_type'] in ['both', 'down'])

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self._conf.get('query_key', None)
        for event in data:
            # If no query_key, we use the key 'all' for all events
            key = hashable(dots_get(event, qk)) if qk else 'all'
            key = key or 'other'

            if self.field_value is not None:
                count = dots_get(event, self.field_value)
                if count is not None:
                    try:
                        count = int(count)
                    except ValueError:
                        reactor_logger.warning('%s is not a number: %s', self.field_value, count)
                    else:
                        yield from self.handle_event(event, count, qk)
            else:
                yield from self.handle_event(event, 1, key)

    def generate_match(self, event: dict, qk: str) -> (dict, dict):
        """ Generate a SpikeRule event. """
        if self.field_value is None:
            spike_count = self.cur_windows[qk].count()
            reference_count = self.ref_windows[qk].count()
        else:
            spike_count = self.cur_windows[qk].mean()
            reference_count = self.ref_windows[qk].mean()
        extra = {'spike_count': spike_count,
                 'reference_count': reference_count,
                 'key': hashable(dots_get(event, qk)) if qk else 'all',
                 'num_events': spike_count,
                 'began_at': self.get_ts(self.cur_windows[qk].data[0]),
                 'ended_at': self.get_ts(self.cur_windows[qk].data[-1])}

        return extra, event

    def get_match_str(self, extra: dict, match: dict) -> str:
        spike_count = extra['spike_count']
        ref_count = extra['reference_count']
        ts_str = pretty_ts(match[self._data.ts_field], self._conf['use_local_time'])
        timeframe = self._conf['timeframe']
        if self.field_value is None:
            message = 'An abnormal number (%d) of events occurred around %s.\n' % (spike_count, ts_str)
            message += 'Preceding that time, there were only %d events within %s\n\n' % (ref_count, timeframe)
        else:
            message = 'An abnormal average value (%.2f) of field \'%s\' occurred around %s.\n' % (spike_count,
                                                                                                  self.field_value,
                                                                                                  ts_str)
            message += 'Preceding that time, the field had an average value of %.2f events within %s\n\n' % (ref_count,
                                                                                                             timeframe)
        return message

    def garbage_collect(self, timestamp: dt.datetime) -> Generator[dict, None, None]:
        # Windows are sized according to their newest event
        # This is a placeholder to accurately size windows in the absence of events
        for qk in self.cur_windows.keys():
            if qk != 'all' and self.ref_windows[qk].count() == 0 and self.cur_windows[qk].count() == 0:
                self.cur_windows.pop(qk)
                self.ref_windows.pop(qk)
                continue
            placeholder = {self._data.ts_field: timestamp, "placeholder": True}
            # The placeholder may trigger an alert, in which case, qk will be expected
            if qk != 'all':
                placeholder.update({self._conf['query_key']: qk})
            yield from self.handle_event(placeholder, 0, qk)

    def add_count_data(self, counts) -> Generator[dict, None, None]:
        """ Add count data to the rule. Data should be of the form {ts: count}. """
        if len(counts) > 1:
            raise ReactorException('SpikeRule.add_count_data can only accept one count at a time')
        for ts, count in counts.items():
            yield from self.handle_event({self._data.ts_field: ts}, count, 'all')

    def add_terms_data(self, terms) -> Generator[dict, None, None]:
        for timestamp, buckets in terms.items():
            for bucket in buckets:
                count = bucket['doc_count']
                event = {self._data.ts_field: timestamp,
                         self._conf['query_key']: bucket['key']}
                key = bucket['key']
                yield from self.handle_event(event, count, key)


class NewTermRule(AcceptsTermsDataMixin, AcceptsHitsDataMixin, Rule):
    _schema_file = 'schemas/ruletype-new_term.yaml'

    """ A Rule that detects a new value in a list of fields. """
    # TODO: alter self.seen_values to be a mapping of value to timestamp of last seen - add option to forget old terms
    #  outside timeframe
    def __init__(self, locator: str, hash: str, conf: dict):
        super(NewTermRule, self).__init__(locator, hash, conf)
        self.seen_values = {}
        # Allow the use of query_key or fields
        if 'fields' not in self._conf and 'query_key' not in self._conf:
            raise ConfigException('NewTermRule fields or query_key must be specified')
        self.fields = self._conf.get('query_key', self._conf.get('fields'))

        if not self.fields:
            raise ConfigException('NewTermRule requires fields or query_key to not be empty')
        if type(self.fields) != list:
            self.fields = [self.fields]
        if self._conf.get('use_terms_query') and (len(self.fields) != 1 or type(self.fields[0]) == list):
            raise ConfigException('use_terms_query can only be used with a single non-composite field')
        if self._conf.get('use_terms_query'):
            if [self._conf['query_key']] != self.fields:
                raise ConfigException('If use_terms_query is specified, you cannot specify different query_key and fields')
            if not self._conf.get('query_key').endswith('.keyword') and not self._conf.get('query_key').endswith('.raw'):
                if self._conf.get('use_keyword_postfix', True):
                    reactor_logger.warning('If query_key is a non-keyword field, you must set '
                                           'use_keyword_postfix to false, or add .keyword/.raw to your query_key')

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        """ Performs a terms aggregation for each field to get every existing term. """
        # Check if already prepared
        if self.seen_values:
            return

        # Get the version of ElasticSearch
        es_version = es_client.info()['version']['number'].split('.')

        window_size = dt.timedelta(**self._conf.get('terms_window_size', {'days': 30}))
        field_name = {'field': '', 'size': 2**31 - 1}  # Maximum 32 bit integer value
        query_template = {'aggs': {'values': {'terms': field_name}}}  # type: dict
        if start_time:
            end = ts_to_dt(start_time)
        elif 'start_date' in self._conf:
            end = ts_to_dt(self._conf['start_date'])
        else:
            end = dt_now()
        start = end - window_size
        step = dt.timedelta(**self._conf.get('window_step_size', {'days': 1}))

        for field in self.fields:
            tmp_start = start
            tmp_end = min(start + step, end)
            time_filter = {self._data.ts_field: {'lt': dt_to_ts(tmp_end),
                                                 'gte': dt_to_ts(tmp_start)}}
            query_template['filter'] = {'bool': {'must': [{'range': time_filter}]}}
            query = {'aggs': {'filtered': query_template}}
            if 'filter' in self._conf:
                for item in self._conf['filter']:
                    query_template['filter']['bool']['must'].append(item)

            # For composite keys, we will need to perform sub-aggregations
            if type(field) == list:
                self.seen_values.setdefault(tuple(field), set())
                level = query_template['aggs']
                # Iterate on each part of the composite key and add a sub-aggs clause to the elasticsearch query
                for i, sub_field in enumerate(field):
                    if self._conf.get('use_keyword_postfix', True):
                        level['values']['terms']['field'] = add_raw_postfix(sub_field, es_version)
                    else:
                        level['values']['terms']['field'] = sub_field
                    if i < len(field) - 1:
                        # If we have more fields after the current one, then set up the next nested structure
                        level['values']['aggs'] = {'values': {'terms': copy.deepcopy(field_name)}}
                        level = level['values']['aggs']
            else:
                self.seen_values.setdefault(field, set())
                # For non-composite keys, only a single agg is needed
                if self._conf.get('use_keyword_postfix', True):
                    field_name['field'] = add_raw_postfix(field, es_version)
                else:
                    field_name['field'] = field

            # Query the entire time range in small chunks
            while tmp_start < end:
                if self._conf.get('use_strftime_index'):
                    index = format_index(self._conf['index'], tmp_start, tmp_end)
                else:
                    index = self._conf['index']
                res = es_client.search(body=query, index=index, ignore_unavailable=True, timeout='50s')
                if 'aggregations' in res:
                    buckets = res['aggregations']['filtered']['values']['buckets']
                    if type(field) == list:
                        # For composite keys, make the lookup based on all fields
                        # Make it a tuple since it can be hashed and used in dictionary lookups
                        for bucket in buckets:
                            # We need to walk down the hierarchy and obtain the value at each level
                            self.seen_values[tuple(field)] |= self.flatten_aggregation_hierarchy(bucket)
                    else:
                        keys = {bucket['key'] for bucket in buckets}
                        self.seen_values[field] |= keys
                else:
                    if type(field) == list:
                        self.seen_values.setdefault(tuple(field), set())
                    else:
                        self.seen_values.setdefault(field, set())
                if tmp_start == tmp_end:
                    break
                tmp_start = tmp_end
                tmp_end = min(tmp_start + step, end)
                time_filter[self._data.ts_field] = {'lt': dt_to_ts(tmp_end),
                                                    'gte': dt_to_ts(tmp_start)}

            for key, values in self.seen_values.items():
                if not values:
                    if type(key) == tuple:
                        # If we don't have any results, it could either be because of the absence of any baseline data
                        # OR it may be because the composite key contained a non-primitive type. Either way, give the
                        # end-users a heads up to help them debug what might be going on/
                        reactor_logger.warning(
                            'No results were found from all sub-aggregations. This can either indicate that there is '
                            'no baseline data OR that a non-primitive field was used in a composite key.'
                        )
                    else:
                        reactor_logger.warning('Found no values for %s', field)
                    continue
                self.seen_values[key] = set(values)
                reactor_logger.info('Found %s unique values for %s', len(self.seen_values[key]), key)

    def flatten_aggregation_hierarchy(self, root, hierarchy_tuple=()):
        """
        For nested aggregations, the results come back in the following format:
        ```yaml
        ---
        aggregations:
          filtered:
            doc_count: 37
            values:
              doc_count_error_upper_bound: 0
              sum_other_doc_count: 0
              buckets:
              - key: 1.1.1.1    # IP address (root)
                doc_count: 13
                values:
                  doc_count_error_upper_bound: 0
                  sum_other_doc_count: 0
                  buckets:
                  - key: '80'    # Port (sub-aggregation)
                    doc_count: 3
                    values:
                      doc_count_error_upper_bound: 0
                      sum_other_doc_count: 0
                      buckets:
                      - key: ack    # Reason (sub-aggregation, leaf-node)
                        doc_count: 3
                      - key: syn    # Reason (sub-aggregation, leaf-node)
                        doc_count: 1
                  - key: '82'
                    doc_count: 3
                    values:
                      doc_count_error_upper_bound: 0
                      sum_other_doc_count: 0
                      buckets:
                      - key: ack    # Reason (sub-aggregation, leaf-node)
                        doc_count: 3
                      - key: syn    # Reason (sub-aggregation, leaf-node)
                        doc_count: 3
              - key: 2.2.2.2    # IP address (root)
                doc_count: 4
                values:
                  doc_count_error_upper_bound: 0
                  sum_other_doc_count: 0
                  buckets:
                  - key: '443'    # Port (sub-aggregation)
                    doc_count: 3
                    values:
                      doc_count_error_upper_bound: 0
                      sum_other_doc_count: 0
                      buckets:
                      - key: ack    # Reason (sub-aggregation, leaf-node)
                        doc_count: 3
                      - key: syn    # Reason (sub-aggregation, leaf-node)
                        doc_count: 3
        ...
        ```

        Each level will either have more values and buckets, or it will be a leaf node.
        We'll ultimately return a flattened list with the hierarchies appended as strings,
        e.g. the above snippet would yield a list with:
        ```python
        [
          ('1.1.1.1', '80', 'ack'),
          ('1.1.1.1', '80', 'syn'),
          ('1.1.1.1', '82', 'ack'),
          ('1.1.1.1', '82', 'syn'),
          ('2.2.2.2', '443', 'ack'),
          ('2.2.2.2', '443', 'syn'),
        ]
        ```

        A similar formatting will be performed in the add_data method and used as the basis for comparison.
        """
        results = set()
        # If there are more aggregations hierarchies left, traverse them
        if 'values' in root:
            results |= self.flatten_aggregation_hierarchy(root['values']['buckets'], hierarchy_tuple + (root['key'],))
        else:
            for node in root:
                if 'values' in node:
                    results |= self.flatten_aggregation_hierarchy(node, hierarchy_tuple)
                else:
                    results.add(hierarchy_tuple + (node['key'],))
        return results

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        for event in data:
            for field in self.fields:
                value = ()
                lookup_field = field
                if type(field) == list:
                    # For composite keys, make the lookup based on all fields
                    # Make it a tuple since it can be hashed and used in dictionary lookups
                    lookup_field = tuple(field)
                    for sub_field in field:
                        lookup_result = dots_get(event, sub_field)
                        if not lookup_result:
                            value = None
                            break
                        value += (lookup_result,)
                else:
                    value = dots_get(event, lookup_field)
                if not value and self._conf.get('alert_on_missing_field'):
                    yield self.add_match(*self.generate_match(lookup_field, None, event=event))
                elif value:
                    if value not in self.seen_values[lookup_field]:
                        yield self.add_match(*self.generate_match(lookup_field, value, event=event))

    def generate_match(self, field, value, event: dict = None, timestamp=None) -> (dict, dict):
        """ Generate a match and, if there is a value, store in `self.seen_values[field]`. """
        event = event or {field: value, self._data.ts_field: timestamp}
        event = copy.deepcopy(event)
        qk = self._conf.get('query_key', None)
        extra = {'key': hashable(dots_get(event, qk)) if qk else 'all',
                 'num_events': 1,
                 'began_at': dots_get(event, self._data.ts_field),
                 'ended_at': dots_get(event, self._data.ts_field)}
        if value is None:
            extra['missing_field'] = field
        else:
            extra['new_field'] = field
            extra['new_value'] = value
            self.seen_values[field].add(value)
        return extra, event

    def add_terms_data(self, terms) -> Generator[dict, None, None]:
        field = self.fields[0]
        for timestamp, buckets in terms.items():
            for bucket in buckets:
                if bucket['doc_count']:
                    if bucket['key'] not in self.seen_values[field]:
                        yield self.add_match(*self.generate_match(field, bucket['key'], timestamp=timestamp))


class CardinalityRule(AcceptsHitsDataMixin, Rule):
    """ A Rule that matches if cardinality of a field is above or below a threshold within a timeframe. """

    _schema_file = 'schemas/ruletype-cardinality.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(CardinalityRule, self).__init__(locator, hash, conf)
        if 'max_cardinality' not in self._conf and 'min_cardinality' not in self._conf:
            raise ConfigException('CardinalityRule must have one of either max_cardinality or min_cardinality')
        self.cardinality_field = self._conf['cardinality_field']
        self.cardinality_cache = {}
        self.first_event = {}
        self.timeframe = self._conf['timeframe']

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self._conf.get('query_key', None)
        for event in data:
            # If no query_key, we use the key 'all' for all events
            key = hashable(dots_get(event, qk)) if qk else 'all'
            self.cardinality_cache.setdefault(key, {})
            self.first_event.setdefault(key, dots_get(event, self._data.ts_field))
            value = hashable(dots_get(event, self.cardinality_field))
            if value is not None:
                # Store this timestamp as most recent occurrence of the term
                self.cardinality_cache[key][value] = dots_get(event, self._data.ts_field)
                yield from self.check_for_match(key, event)

    def check_for_match(self, key, event, garbage_collect=True):
        time_elapsed = dots_get(event, self._data.ts_field) - self.first_event.get(key, dots_get(event, self._data.ts_field))
        timeframe_elapsed = time_elapsed > self.timeframe
        if (len(self.cardinality_cache[key]) > self._conf.get('max_cardinality', float('inf')) or
                (len(self.cardinality_cache[key]) < self._conf.get('min_cardinality', 0.0) and timeframe_elapsed)):
            # If there might be a match, run garbage collect first to remove outdated terms
            # Only run it if there might be a match so it doesn't impact performance
            if garbage_collect:
                yield from self.garbage_collect(dots_get(event, self._data.ts_field))
                yield from self.check_for_match(key, event, False)
            else:
                self.first_event.pop(key, None)
                extra = {'cardinality': self.cardinality_cache[key],
                         'key': key,
                         'num_events': len(self.cardinality_cache[key]),
                         'began_at': self.first_event.get(key, dots_get(event, self._data.ts_field)),
                         'ended_at': dots_get(event, self._data.ts_field)}
                yield self.add_match(extra, event)

    def get_match_str(self, extra: dict, match: dict) -> str:
        lt = self._conf.get('use_local_time')
        began_time = pretty_ts(ts_to_dt(dots_get(match, self._data.ts_field)) - self._conf['timeframe'], lt)
        ended_time = pretty_ts(dots_get(match, self._data.ts_field), lt)
        if 'max_cardinality' in self._conf:
            return 'A maximum of %d unique %s(s) occurred since last alert or between %s and %s\n\n' % (
                self._conf['max_cardinality'],
                self.cardinality_field,
                began_time,
                ended_time)
        else:
            return 'Less than  %d unique %s(s) occurred since last alert or between %s and %s\n\n' % (
                self._conf['max_cardinality'],
                self.cardinality_field,
                began_time,
                ended_time)

    def garbage_collect(self, timestamp: dt.datetime) -> Generator[dict, None, None]:
        """ Remove all occurrence data that is beyond the timeframe away. """
        stale_terms = []
        for qk, terms in self.cardinality_cache.items():
            for term, last_occurrence in terms.items():
                if timestamp - last_occurrence > self._conf['timeframe']:
                    stale_terms.append((qk, term))

        for qk, term in stale_terms:
            self.cardinality_cache[qk].pop(term)

            # Create a placeholder event for min_cardinality match occurred
            if 'min_cardinality' in self._conf:
                event = {self._data.ts_field: timestamp}
                if 'query_key' in self._conf:
                    event.update({self._conf['query_key']: qk})
                yield from self.check_for_match(qk, event, False)


class BaseAggregationRule(AcceptsAggregationDataMixin, Rule):
    allowed_aggregations = frozenset(['min', 'max', 'avg', 'sum', 'cardinality', 'value_count'])

    def __init__(self, locator: str, hash: str, conf: dict):
        super(BaseAggregationRule, self).__init__(locator, hash, conf)
        bucket_interval = self._conf.get('bucket_interval')
        if bucket_interval:
            seconds = total_seconds(bucket_interval)
            if seconds % (60 * 60 * 24 * 7) == 0:
                self._conf['bucket_interval_period'] = str(int(seconds) // (60 * 60 * 24 * 7)) + 'w'
            elif seconds % (60 * 60 * 24) == 0:
                self._conf['bucket_interval_period'] = str(int(seconds) // (60 * 60 * 24)) + 'd'
            elif seconds % (60 * 60) == 0:
                self._conf['bucket_interval_period'] = str(int(seconds) // (60 * 60)) + 'h'
            elif seconds % 60 == 0:
                self._conf['bucket_interval_period'] = str(int(seconds) // 60) + 'm'
            elif seconds % 1 == 0:
                self._conf['bucket_interval_period'] = str(int(seconds)) + 's'
            else:
                raise ConfigException('Unsupported window size')

            if self._conf.get('use_run_every_query_size'):
                if total_seconds(self._conf['run_every']) % total_seconds(self._conf['bucket_interval']) != 0:
                    raise ConfigException('run_every must be evenly divisible by bucket_interval if specified')
            else:
                if total_seconds(self._conf['buffer_time']) % total_seconds(self._conf['bucket_interval']) != 0:
                    raise ConfigException('buffer_time must be evenly divisible by bucket_interval if specified')

    def generate_aggregation_query(self):
        raise NotImplementedError()

    def add_aggregation_data(self, payload) -> Generator[dict, None, None]:
        for timestamp, payload_data in payload.items():
            if 'interval_aggs' in payload_data:
                yield from self.unwrap_interval_buckets(timestamp, None, payload_data['interval_aggs']['buckets'])
            elif 'bucket_aggs' in payload_data:
                yield from self.unwrap_term_buckets(timestamp, payload_data['bucket_aggs']['buckets'])
            else:
                yield from self.check_for_matches(timestamp, None, payload_data)

    def unwrap_interval_buckets(self, timestamp, query_key, interval_buckets):
        for interval_data in interval_buckets:
            # Use bucket key here instead of start_time for more accurate match timestamp
            yield from self.check_for_matches(ts_to_dt(interval_data['key_as_string']), query_key, interval_data)

    def unwrap_term_buckets(self, timestamp, term_buckets):
        for term_data in term_buckets:
            if 'interval_aggs' in term_data:
                yield from self.unwrap_interval_buckets(timestamp, term_data['key'], term_data['interval_aggs']['buckets'])
            else:
                yield from self.check_for_matches(timestamp, term_data['key'], term_data)

    def check_for_matches(self, timestamp, query_key, aggregation_data) -> Generator[dict, None, None]:
        raise NotImplementedError()


class MetricAggregationRule(BaseAggregationRule):
    """ A Rule that matches when there is a low number of events within a timeframe. """

    _schema_file = 'schemas/ruletype-metric_aggregation.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(MetricAggregationRule, self).__init__(locator, hash, conf)
        if 'max_threshold' not in self._conf and 'min_threshold' not in self._conf:
            raise ConfigException('MetricAggregationRule must have one of either max_threshold or min_threshold')
        if self._conf['metric_agg_type'] not in self.allowed_aggregations:
            raise ConfigException('metric_agg_type must be one of %s' % str(self.allowed_aggregations))

        self.metric_key = 'metric_%s_%s' % (self._conf['metric_agg_key'], self._conf['metric_agg_type'])
        self._conf['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, extra: dict, match: dict) -> str:
        return 'Threshold violation, %s:%s %s (min: %s, max: %s)\n\n' % (
            self._conf['metric_agg_type'],
            self._conf['metric_agg_key'],
            match[self.metric_key],
            self._conf.get('min_threshold'),
            self._conf.get('max_threshold'),
        )

    def generate_aggregation_query(self):
        return {self.metric_key: {self._conf['metric_agg_type']: {'field': self._conf['metric_agg_key']}}}

    def check_for_matches(self, timestamp, query_key, aggregation_data):
        if 'compound_query_key' in self._conf:
            yield from self.check_matches_recursive(timestamp, query_key, aggregation_data, self._conf['compound_query_key'], {})
        else:
            metric_val = aggregation_data[self.metric_key]['value']
            if self.crossed_thresholds(metric_val):
                match = {self._data.ts_field: timestamp,
                         self.metric_key: metric_val}
                if query_key is not None:
                    match[self._conf['query_key']] = query_key
                extra = {'num_events': 1,
                         'began_at': timestamp,
                         'ended_at': timestamp}
                yield self.add_match(extra, match)

    def check_matches_recursive(self, timestamp, query_key, aggregation_data, compound_keys, match_data):
        if len(compound_keys) < 1:
            # Shouldn't get to this point, but checking for safety
            return

        match_data[compound_keys[0]] = aggregation_data['key']
        if 'bucket_aggs' in aggregation_data:
            for bucket in aggregation_data['bucket_aggs']['buckets']:
                yield from self.check_matches_recursive(timestamp, query_key, bucket, compound_keys[1:], match_data)
        else:
            metric_val = aggregation_data[self.metric_key]['value']
            if self.crossed_thresholds(metric_val):
                match_data[self._data.ts_field] = timestamp
                match_data[self.metric_key] = metric_val

                # Add compound key to payload to allow alerts to trigger for every unique occurrence
                compound_value = [match_data[key] for key in self._conf['compound_query_key']]
                match_data[self._conf['query_key']] = ','.join(compound_value)

                extra = {'num_events': 1,
                         'began_at': timestamp,
                         'ended_at': timestamp}
                yield self.add_match(extra, match_data)

    def crossed_thresholds(self, metric_value):
        if metric_value is None:
            return False
        if 'max_threshold' in self._conf and metric_value > self._conf['max_threshold']:
            return True
        if 'min_threshold' in self._conf and metric_value < self._conf['min_threshold']:
            return True
        return False


class SpikeMetricAggregationRule(SpikeRule, BaseAggregationRule):
    """ A rule that matches when there is a spike in an aggregated event compared to its reference window, """

    _schema_file = 'schemas/ruletype-spike_metric_aggregation.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(SpikeMetricAggregationRule, self).__init__(locator, hash, conf)

        # Metric aggregation alert things
        self.metric_key = 'metric_%s_%s' % (self._conf['metric_agg_key'], self._conf['metric_agg_type'])
        if self._conf['metric_agg_type'] not in self.allowed_aggregations:
            raise ConfigException('metric_agg_type must be one of %s' % str(self.allowed_aggregations))

        # Disabling bucket intervals (doesn't make sense in context of spike to split up your time period)
        if self._conf.get('bucket_interval'):
            raise ConfigException('bucket_interval not supported for spike metric aggregations')

        self._conf['aggregation_query_element'] = self.generate_aggregation_query()

    def generate_aggregation_query(self):
        """ Lifted from MetricAggregation, added support for scripted fields. """
        if self._conf.get('metric_agg_script'):
            return {self.metric_key: {self._conf['metric_agg_type']: self._conf['metric_agg_script']}}
        else:
            return {self.metric_key: {self._conf['metric_agg_type']: {'field': self._conf['metric_agg_key']}}}

    def add_aggregation_data(self, payload) -> Generator[dict, None, None]:
        """
        BaseAggregationRule.add_aggregation_data unpacks our results and runs checks directly against hardcoded
        cutoffs.
        We, instead, want to use all of our SpikeRule.handle_event inherited logic (current/reference) from
        the aggregation's `value` key to determine spikes from aggregations.
        """
        for timestamp, payload_data in payload.items():
            if 'bucket_aggs' in payload_data:
                yield from self.unwrap_term_buckets(timestamp, payload_data['bucket_aggs'])
            else:
                # no time / term split, just focus on aggregations
                event = {self._data.ts_field: timestamp}
                agg_value = payload_data[self.metric_key]['value']
                yield from self.handle_event(event, agg_value, 'all')

    def unwrap_term_buckets(self, timestamp, term_buckets, qk=None):
        """ Create separate spike event trackers for each term, handle compound query keys. """
        qk = qk or []
        for term_data in term_buckets['buckets']:
            qk.append(term_data['key'])

            # Handle compound query keys (nest aggregations)
            if term_data.get('bucket_aggs'):
                self.unwrap_term_buckets(timestamp, term_data['bucket_aggs'], qk)
                # reset query key to consider the proper depth for N > 2
                del qk[-1]
                continue

            qk_str = ','.join(qk)
            agg_value = term_data[self.metric_key]['value']
            event = {self._data.ts_field: timestamp,
                     self._conf['query_key']: qk_str}
            # Pass to SpikeRule's tracker
            yield from self.handle_event(event, agg_value, qk_str)

            # Handle unpack of lowest level
            del qk[-1]

    def get_match_str(self, extra: dict, match: dict) -> str:
        message = 'An abnormal %s of %s (%s) occurred around %s.\n' % (
            self._conf['metric_agg_type'], self._conf['metric_agg_key'], round(extra['spike_count']),
            pretty_ts(dots_get(match, self._data.ts_field), self._conf['use_local_time']))
        message += 'Preceding that time, there was %s of %s of (%s) within %s\n\n' % (
            self._conf['metric_agg_type'], self._conf['metric_agg_key'],
            round(extra['reference_count']), self._conf['timeframe'])
        return message

    def check_for_matches(self, timestamp, query_key, aggregation_data):
        raise Exception('Method not used by SpikeMetricAggregationRule')


class PercentageMatchRule(BaseAggregationRule):
    """ A Rule that matches when there is percentage violation of match_bucket_filter hits of the total hits. """

    _schema_file = 'schemas/ruletype-percentage_match.yaml'

    def __init__(self, locator: str, hash: str, conf: dict):
        super(PercentageMatchRule, self).__init__(locator, hash, conf)
        if all([f not in self._conf for f in ['max_percentage', 'min_percentage']]):
            raise ConfigException('PercentageMatchRule must have one of either min_percentage or max_percentage')

        self.min_denominator = self._conf.get('min_denominator', 0)
        self.match_bucket_filter = self._conf['match_bucket_filter']
        self._conf['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, extra: dict, match: dict) -> str:
        percentage_format_string = self._conf.get('percentage_format_string', '%s')
        return 'Percentage violation, value: %s (min: %s, max: %s) of %s items.\n\n' % (
            (percentage_format_string % extra['percentage']),
            self._conf.get('min_percentage'),
            self._conf.get('max_percentage'),
            extra['denominator']
        )

    def generate_aggregation_query(self):
        return {
            'percentage_match_aggs': {
                'filters': {
                    'other_bucket': True,
                    'filters': {
                        'match_bucket': {
                            'bool': {
                                'must': self.match_bucket_filter
                            }
                        }
                    }
                }
            }
        }

    def check_for_matches(self, timestamp, query_key, aggregation_data):
        match_bucket_count = aggregation_data['percentage_match_aggs']['buckets']['match_bucket']['doc_count']
        other_bucket_count = aggregation_data['percentage_match_aggs']['buckets']['_other_']['doc_count']

        if match_bucket_count is None or other_bucket_count is None:
            return
        else:
            total_count = other_bucket_count + match_bucket_count
            if total_count == 0 or total_count < self.min_denominator:
                return
            else:
                match_percentage = float(match_bucket_count) / float(total_count) * 100.0
                if self.percentage_violation(match_percentage):
                    extra = {'percentage': match_percentage,
                             'denominator': total_count,
                             'num_events': 1,
                             'began_at': timestamp,
                             'ended_at': timestamp}
                    event = {self._data.ts_field: timestamp}
                    if query_key is not None:
                        event[self._conf['query_key']] = query_key
                    yield self.add_match(extra, event)

    def percentage_violation(self, match_percentage):
        if 'max_percentage' in self._conf and match_percentage > self._conf['max_percentage']:
            return True
        if 'min_percentage' in self._conf and match_percentage < self._conf['min_percentage']:
            return True
        return False
