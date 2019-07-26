import copy
import datetime
import elasticsearch

import reactor.enhancement
from typing import List, Optional
from reactor.exceptions import ReactorException, QueryException
from reactor.util import (
    generate_id,
    dots_get, dots_set,
    elasticsearch_client,
    reactor_logger,
    dt_now, pretty_ts,
    ts_to_dt, dt_to_ts, unix_to_dt, unixms_to_dt, dt_to_unix, dt_to_unixms, ts_to_dt_with_format, dt_to_ts_with_format,
    total_seconds, add_raw_postfix,
)
from reactor.ruletype import RuleType
from reactor.kibana import filters_from_kibana


class Rule(object):
    def __init__(self, rule_uuid: str, conf: dict, rule_type: RuleType):
        self.uuid = rule_uuid
        self.type = rule_type
        self.hash = None
        self._conf = conf

        # Counters
        self.num_hits = 0
        self.num_duplicates = 0
        self.total_hits = 0
        self.cumulative_hits = 0
        self._max_hits = float('inf')

        # Timers
        self.start_time = None
        self.initial_start_time = None
        self.minimum_start_time = None
        self.original_start_time = None
        self.previous_end_time = None

        # Misc.
        self.agg_matches = []
        self.aggregate_alert_time = {}
        self.current_aggregate_id = {}
        self.processed_hits = {}
        self.has_run_once = False
        self.scroll_id = None

        self._es_client = None
        self.alerters = []  # type: List[reactor.alerter.Alerter]
        self.match_enhancements = []  # type: List[reactor.enhancement.MatchEnhancement]
        self.alert_enhancements = []  # type: List[reactor.enhancement.AlertEnhancement]

    def __hash__(self):
        return self.hash

    def __eq__(self, other):
        return self.hash == other

    def __repr__(self):
        return '%s.%s%s' % (self.__module__, type(self.type).__name__, self._conf)

    def conf(self, key, default_value=None):
        return dots_get(self._conf, key, default_value)

    def set_conf(self, key, value):
        return dots_set(self._conf, key, value)

    @property
    def run_every(self) -> datetime.timedelta:
        return self._conf['run_every']

    @property
    def enabled(self):
        return self._conf.get('is_enabled', True)

    @property
    def name(self):
        return self._conf['name']

    @property
    def max_hits(self):
        return self._max_hits

    @max_hits.setter
    def max_hits(self, value: int):
        if value is None or 1 <= value <= 10000:
            self._max_hits = value or float('inf')
        else:
            raise ValueError('max_hits must either be None or between 1 and 10000 inclusive')

    @property
    def es_client(self):
        if self._es_client is None:
            self.es_client = elasticsearch_client(self.conf('elasticsearch'))
        return self._es_client

    @es_client.setter
    def es_client(self, value: reactor.util.ElasticSearchClient):
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
            elif self.es_client.es_version_at_least(5):
                string_multi_field_name = '.keyword'
            else:
                string_multi_field_name = '.raw'

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
        if self.conf('segment_size'):
            return self.conf('segment_size')
        elif all(not self.conf(k) for k in ['use_count_query', 'use_terms_query', 'aggregation_query_element']):
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
        """ Ue the configured datetime to timestamp function. """
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

    def get_alert_body(self, data: dict, match: dict, alert_time):
        doc_uuid = generate_id()
        body = {'uuid': doc_uuid,
                'rule_uuid': self.uuid,
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
        orig_alert['num_matches'] += new_alert['num_matches']
        orig_alert['modify_time'] = new_alert['modify_time']

        self.type.merge_alert_body(orig_alert, new_alert)

    def get_query(self, filters: dict, start_time=None, end_time=None, sort: Optional[str] = 'asc',
                  timestamp_field: str = '@timestamp') -> dict:
        """
        :param filters:
        :param start_time:
        :param end_time:
        :param sort: asc|desc|None
        :param timestamp_field:
        """
        start_time = self.dt_to_ts(start_time)
        end_time = self.dt_to_ts(end_time)
        filters = copy.copy(filters)
        es_filters = {'filter': {'bool': {'must': filters}}}
        if start_time and end_time:
            es_filters['filter']['bool']['must'].insert(0, {'range': {timestamp_field: {'gt': start_time,
                                                                                        'lte': end_time}}})
        elif start_time:
            es_filters['filter']['bool']['must'].insert(0, {'range': {timestamp_field: {'gt': start_time}}})
        elif end_time:
            es_filters['filter']['bool']['must'].insert(0, {'range': {timestamp_field: {'lte': end_time}}})

        if self.es_client.es_version_at_least(5):
            query = {'query': {'bool': es_filters}}
        else:
            query = {'query': {'filtered': es_filters}}

        if sort:
            query['sort'] = [{timestamp_field: {'order': sort}}]

        return query

    def get_terms_query(self, query, rule, size, field):
        """ Takes a query generated by `get_query` and outputs an aggregation query. """
        min_doc_count = rule.conf('min_doc_count', 1)
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

    def get_aggregation_query(self, query, rule, query_key, terms_size, timestamp_field='@timestamp'):
        """ Takes a query generated by `get_query` and outputs an aggregation query. """
        metric_agg_element = rule.conf('aggregation_query_element')
        bucket_interval_period = rule.conf('bucket_interval_period')
        if bucket_interval_period is not None:
            aggs_element = {
                'interval_aggs': {
                    'date_histogram': {
                        'field': timestamp_field,
                        'interval': bucket_interval_period},
                    'aggs': metric_agg_element
                }
            }
            if rule.conf('bucket_offset_delta'):
                aggs_element['interval_aggs']['date_histogram']['offset'] = '+%ss' % rule.conf('bucket_offset_delta')
        else:
            aggs_element = metric_agg_element

        if query_key is not None:
            for idx, key in reversed(list(enumerate(query_key.split(',')))):
                aggs_element = {'bucket_aggs': {'terms': {'field': key, 'size': terms_size,
                                                          'min_doc_count': rule.conf('min_doc_count', 1)}}}

        if self.es_client.es_version_at_least(5):
            aggs_query = query
            aggs_query['aggs'] = aggs_element
        else:
            query_element = query['query']
            query_element.pop('sort', None)
            query_element['filtered'].update({'aggs': aggs_element})
            aggs_query = {'aggs': query_element}
        return aggs_query

    def remove_duplicate_events(self, data):
        """ Removes the  """
        new_events = []
        for event in data:
            if event['_id'] in self.processed_hits:
                continue

            # Remember the new data's IDs
            self.processed_hits[event['_id']] = dots_get(event, self.conf('timestamp_field'))
            new_events.append(event)

        return new_events

    def adjust_start_time_for_overlapping_agg_query(self, start_time: datetime.datetime) -> datetime.datetime:
        if self.conf('aggregation_query_element'):
            if self.conf('allow_buffer_time_overlap') and not self.conf('use_run_every_query_size') \
                    and self.conf('buffer_time') > self.conf('run_every'):
                start_time = start_time - (self.conf('buffer_time') - self.conf('run_every'))

        return start_time

    def adjust_start_time_for_interval_sync(self, start_time: datetime.datetime) -> datetime.datetime:
        # If aggregation query adjust bucket offset
        if self.conf('aggregation_query_element'):
            if self.conf('bucket_interval'):
                es_interval_delta = self.conf('bucket_interval_timedelta')
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
        if isinstance(self.type, reactor.ruletype.FlatlineRuleType) and 'key' in match:
            return str(match['key'])
        elif self.conf('query_key'):
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
        elif self.es_client.es_version_at_least(5):
            extra_args = {'_source_include': self.conf('include')}
        else:
            extra_args = {'_source_includes': self.conf('include')}

        # Only scroll if max_hits is not set or max_query_size is smaller
        scroll_keepalive = self.conf('scroll_keepalive') if self.conf('max_query_size') < self.max_hits else None
        try:
            if self.scroll_id:
                res = self.es_client.scroll(scroll_id=self.scroll_id, scroll=scroll_keepalive)
            else:
                res = self.es_client.search(scroll=scroll_keepalive, index=index,
                                            size=int(min(self.max_hits-self.num_hits, self.conf('max_query_size'))),
                                            body=query, ignore_unavailable=True, **extra_args)
                if self.es_client.es_version_at_least(7):
                    self.total_hits = int(res['hits']['total']['value'])
                else:
                    self.total_hits = int(res['hits']['total'])

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

        self.num_hits += len(hits)
        lt = self.conf('use_local_time')

        # If the max_hits reached and there are more hits available
        if self.num_hits >= self.max_hits and self.total_hits > self.max_hits:
            if self.num_hits > self.max_hits:
                hits = hits[:(self.max_hits % self.conf('max_query_size'))]
            self.num_hits = self.max_hits
            self.scroll_id = None
            reactor_logger.debug('Queried rule %s from %s to %s: %s / %s hits (%s total hits)',
                                 self.name, pretty_ts(start_time, lt), pretty_ts(end_time, lt),
                                 self.num_hits, len(hits), self.total_hits)
            reactor_logger.warning('Maximum hits reached (%s hits of %s total hits), '
                                   'this could trigger false positives alerts', self.max_hits, self.total_hits)
        elif self.total_hits > self.conf('max_query_size'):
            reactor_logger.debug('Queried rule %s from %s to %s: %s / %s hits (%s total hits) (scrolling...)',
                                 self.name, pretty_ts(start_time, lt), pretty_ts(end_time, lt),
                                 self.num_hits, len(hits), self.total_hits)
            self.scroll_id = res['_scroll_id']
        else:
            reactor_logger.debug('Queried rule %s from %s to %s: %s / %s hits (%s total hits)',
                                 self.name, pretty_ts(start_time, lt), pretty_ts(end_time, lt),
                                 self.num_hits, len(hits), self.total_hits)

        hits = self.process_hits(hits)

        # Record doc_type for use in get_top_counts
        if not self.conf('doc_type') and len(hits):
            self.set_conf('doc_type', hits[0]['_type'])
        return hits

    def get_hits_count(self, start_time: datetime.datetime, end_time: datetime.datetime,
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

        self.num_hits += res['count']
        lt = self.conf('use_local_time')
        reactor_logger.debug('Queried rule %s from %s to %s: %s hits', self.name,
                             pretty_ts(start_time, lt), pretty_ts(end_time, lt), res['count'])

        return {end_time: res['count']}

    def get_hits_terms(self, start_time: datetime.datetime, end_time: datetime.datetime,
                       index, key, qk=None, size=None) -> Optional[dict]:
        rule_filter = copy.copy(self.conf('filter'))
        if qk:
            qk_list = qk.split(',')
            if self.es_client.es_version_at_least(6):
                end = '.raw'
            else:
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
        query = self.get_terms_query(base_query, self, size, key)

        try:
            if self.es_client.es_version_at_least(6):
                res = self.es_client.deprecated_search(index=index, doc_type=self.conf('doc_type'),
                                                       body=query, size=0, ignore_unavailable=True)
            else:
                res = self.es_client.deprecated_search(index=index, doc_type=self.conf('doc_type'),
                                                       body=query, search_type='count', ignore_unavailable=True)

        except elasticsearch.ElasticsearchException as e:
            # ElasticSearch sometimes gives us GIGANTIC error messages
            # (so big that they will fill the entire terminal buffer)
            if len(str(e)) > 1024:
                e = str(e)[:1024] + '... (%d characters removed)' % (len(str(e)) - 1024)
            raise QueryException(e, query=query)

        if 'aggregations' not in res:
            return {}
        if self.es_client.es_version_at_least(6):
            buckets = res['aggregations']['filtered']['counts']['buckets']
        else:
            buckets = res['aggregations']['counts']['buckets']

        self.num_hits += len(buckets)
        lt = self.conf('use_local_time')
        reactor_logger.debug('Queried rule %s from %s to %s: %s buckets', self.name,
                             pretty_ts(start_time, lt), pretty_ts(end_time, lt), len(buckets))

        return {end_time: buckets}

    def get_hits_aggregation(self, start_time: datetime.datetime, end_time: datetime.datetime,
                             index: str, query_key, term_size=None) -> Optional[dict]:
        rule_filter = copy.copy(self.conf('filter'))
        base_query = self.get_query(rule_filter, start_time, end_time, sort=None,
                                    timestamp_field=self.conf('timestamp_field'))

        if term_size is None:
            term_size = self.conf('terms_size', 50)
        query = self.get_aggregation_query(base_query, self, query_key, term_size, self.conf('timestamp_field'))

        try:
            if self.es_client.es_version_at_least(6):
                res = self.es_client.deprecated_search(index=index, doc_type=self.conf('doc_type'),
                                                       body=query, search_type='count', ignore_unavailable=True)
            else:
                res = self.es_client.deprecated_search(index=index, doc_type=self.conf('doc_type'),
                                                       body=query, size=0, ignore_unavailable=True)

        except elasticsearch.ElasticsearchException as e:
            # ElasticSearch sometimes gives us GIGANTIC error messages
            # (so big that they will fill the entire terminal buffer)
            if len(str(e)) > 1024:
                e = str(e)[:1024] + '... (%d characters removed)' % (len(str(e)) - 1024)
            raise QueryException(e, query=query)

        if 'aggregations' not in res:
            return {}
        if self.es_client.es_version_at_least(6):
            payload = res['aggregations']['filtered']
        else:
            payload = res['aggregations']

        if self.es_client.es_version_at_least(7):
            self.total_hits += res['hits']['total']['value']
        else:
            self.total_hits += res['hits']['total']

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
