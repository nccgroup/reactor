import datetime
import pytest
import mock

from reactor.exceptions import ReactorException
from reactor.rule import Rule
from reactor.ruletype import RuleType, FlatlineRuleType
from reactor.util import (
    ts_to_dt, unix_to_dt, unixms_to_dt, ts_to_dt_with_format,
    dt_to_ts, dt_to_unix, dt_to_unixms, dt_to_ts_with_format,
)


def gen_rule(rule_type: RuleType = None, **kwargs):
    return Rule('123', kwargs, rule_type)


def gen_hits(size, timestamp_field='@timestamp', fields=None, **kwargs):
    hits = []
    for n in range(size):
        ts = ts_to_dt('2019-07-26T12:%s:%sZ' % (n // 60, n % 60))
        hit = {
            '_id': str(n),
            '_index': 'reactor_logs',
            '_type': '_doc',
            '_source': {timestamp_field: ts},
        }
        hit['_source'].update(kwargs)
        if fields:
            hit['fields'] = fields
        hits.append(hit)
    return hits


def test_get_segment_explicit():
    run_every = datetime.timedelta(minutes=2)
    segment_size = datetime.timedelta(minutes=5)
    buffer_time = datetime.timedelta(minutes=7)

    rule = gen_rule(segment_size=segment_size, run_every=run_every, buffer_time=buffer_time)
    assert rule.get_segment_size() == segment_size


def test_get_segment_size_fallback_to_buffer_time():
    run_every = datetime.timedelta(minutes=2)
    buffer_time = datetime.timedelta(minutes=7)

    rule = gen_rule(run_every=run_every, buffer_time=buffer_time)
    assert rule.get_segment_size() == buffer_time


def test_get_segment_fallback_to_run_every():
    run_every = datetime.timedelta(minutes=2)
    buffer_time = datetime.timedelta(minutes=7)

    rule = gen_rule(run_every=run_every, buffer_time=buffer_time, use_count_query=True)
    assert rule.get_segment_size() == run_every
    rule = gen_rule(run_every=run_every, buffer_time=buffer_time, use_terms_query=True)
    assert rule.get_segment_size() == run_every


def test_get_segment_size_aggregation_query_element():
    run_every = datetime.timedelta(minutes=2)
    buffer_time = datetime.timedelta(minutes=7)

    rule = gen_rule(run_every=run_every, aggregation_query_element=True, use_run_every_query_size=True)
    assert rule.get_segment_size() == run_every
    rule = gen_rule(run_every=run_every, buffer_time=buffer_time, aggregation_query_element=True)
    assert rule.get_segment_size() == buffer_time


def test_ts_to_dt_iso():
    rule = gen_rule(timestamp_type='iso')
    assert ts_to_dt('2019-07-26T09:34:52Z') == rule.ts_to_dt('2019-07-26T09:34:52Z')

    with pytest.raises(ValueError):
        rule.ts_to_dt('Invalid')


def test_ts_to_dt_unix():
    rule = gen_rule(timestamp_type='unix')
    assert unix_to_dt('1564133692') == rule.ts_to_dt('1564133692')

    with pytest.raises(ValueError):
        rule.ts_to_dt('Invalid')

    with pytest.raises(ValueError):
        rule.ts_to_dt('1564133692000')


def test_ts_to_dt_unixms():
    rule = gen_rule(timestamp_type='unix_ms')
    assert unixms_to_dt('1564133692000') == rule.ts_to_dt('1564133692000')

    with pytest.raises(ValueError):
        rule.ts_to_dt('Invalid')


def test_ts_to_dt_timestamp_format():
    rule = gen_rule(timestamp_type='custom', timestamp_format='%Y-%m-%dT%H:%M:%SZ')
    assert ts_to_dt_with_format('2019-07-26T09:34:52Z', '%Y-%m-%dT%H:%M:%SZ') == rule.ts_to_dt('2019-07-26T09:34:52Z')

    with pytest.raises(ValueError):
        rule.ts_to_dt('Invalid')


def test_ts_to_dt_unknown():
    rule = gen_rule(timestamp_type='foo')
    with pytest.raises(ReactorException):
        rule.ts_to_dt('2019-07-26T09:34:52Z')


def test_dt_to_ts_iso():
    dt = datetime.datetime(2019, 7, 26, 9, 34, 52)
    rule = gen_rule(timestamp_type='iso')
    assert dt_to_ts(dt) == rule.dt_to_ts(dt)


def test_dt_to_ts_unix():
    dt = datetime.datetime(2019, 7, 26, 9, 34, 52)
    rule = gen_rule(timestamp_type='unix')
    assert dt_to_unix(dt) == rule.dt_to_ts(dt)


def test_dt_to_ts_unixms():
    dt = datetime.datetime(2019, 7, 26, 9, 34, 52)
    rule = gen_rule(timestamp_type='unix_ms')
    assert dt_to_unixms(dt) == rule.dt_to_ts(dt)


def test_dt_to_ts_timestamp_format():
    dt = datetime.datetime(2019, 7, 26, 9, 34, 52)
    rule = gen_rule(timestamp_type='custom', timestamp_format='%Y-%m-%dT%H:%M:%SZ')
    assert dt_to_ts_with_format(dt, '%Y-%m-%dT%H:%M:%SZ') == rule.dt_to_ts(dt)


def test_dt_to_ts_unknown():
    dt = datetime.datetime(2019, 7, 26, 9, 34, 52)
    rule = gen_rule(timestamp_type='foo')
    with pytest.raises(ReactorException):
        rule.dt_to_ts(dt)


def test_get_alert_body_with_timestamp():
    rule = gen_rule(name='Test', timestamp_field='ts')
    expected_keys = {'uuid', 'rule_uuid', 'rule_name', 'match_data', 'match_body',
                     'alert_info', 'alert_time', 'modify_time', 'num_matches', 'match_time'}

    alert = rule.get_alert_body({}, {'ts': ''}, datetime.datetime(2019, 7, 26, 9, 34, 52))
    assert expected_keys == set(alert.keys())


def test_get_alert_body_without_timestamp():
    rule = gen_rule(name='Test', timestamp_field='ts')
    expected_keys = {'uuid', 'rule_uuid', 'rule_name', 'match_data', 'match_body',
                     'alert_info', 'alert_time', 'modify_time', 'num_matches'}

    alert = rule.get_alert_body({}, {}, datetime.datetime(2019, 7, 26, 9, 34, 52))
    assert expected_keys == set(alert.keys())


def test_merge_alert_body():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts')
    orig_data = {'num_events': 1,
                 'began_at': datetime.datetime(2019, 7, 26, 9, 34, 52),
                 'ended_at': datetime.datetime(2019, 7, 26, 9, 34, 52)}
    new_data = {'num_events': 2,
                'began_at': datetime.datetime(2019, 7, 26, 9, 34, 53),
                'ended_at': datetime.datetime(2019, 7, 26, 9, 34, 53)}

    orig_alert = rule.get_alert_body(orig_data, {'ts': ''}, datetime.datetime(2019, 7, 26, 9, 34, 52))
    new_alert = rule.get_alert_body(new_data, {'ts': ''}, datetime.datetime(2019, 7, 26, 9, 34, 53))

    rule.merge_alert_body(orig_alert, new_alert)

    assert orig_alert['num_matches'] == 2
    assert orig_alert['modify_time'] == new_alert['modify_time']
    assert orig_alert['match_data']['num_events'] == 3
    assert orig_alert['match_data']['began_at'] == datetime.datetime(2019, 7, 26, 9, 34, 52)
    assert orig_alert['match_data']['ended_at'] == datetime.datetime(2019, 7, 26, 9, 34, 53)


def test_get_query():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': []}}}}, 'sort': [{'@timestamp': {'order': 'asc'}}]}
        assert expected == rule.get_query([])


def test_get_query_with_timestamp_field():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': []}}}}, 'sort': [{'ts': {'order': 'asc'}}]}
        assert expected == rule.get_query([], timestamp_field='ts')


def test_get_query_with_sort():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': []}}}}, 'sort': [{'@timestamp': {'order': 'desc'}}]}
        assert expected == rule.get_query([], sort='desc')


def test_get_query_with_start_time():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': [
            {'range': {'@timestamp': {'gt': '2019-07-26T09:34:52Z'}}}
        ]}}}}, 'sort': [{'@timestamp': {'order': 'asc'}}]}
        assert expected == rule.get_query([], start_time=datetime.datetime(2019, 7, 26, 9, 34, 52))


def test_get_query_with_end_time():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': [
            {'range': {'@timestamp': {'lte': '2019-07-26T09:34:52Z'}}}
        ]}}}}, 'sort': [{'@timestamp': {'order': 'asc'}}]}
        assert expected == rule.get_query([], end_time=datetime.datetime(2019, 7, 26, 9, 34, 52))


def test_get_query_with_start_and_end_time():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': [
            {'range': {'@timestamp': {'gt': '2019-07-26T09:34:52Z',
                                      'lte': '2019-07-26T09:34:53Z'}}}
        ]}}}}, 'sort': [{'@timestamp': {'order': 'asc'}}]}
        assert expected == rule.get_query([],
                                          start_time=datetime.datetime(2019, 7, 26, 9, 34, 52),
                                          end_time=datetime.datetime(2019, 7, 26, 9, 34, 53))


def test_get_terms_query():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': []}}}},
                    'aggs': {'counts': {'terms': {'field': 'field_name',
                                                  'size': 1,
                                                  'min_doc_count': 1}}},
                    'sort': [{'@timestamp': {'order': 'asc'}}]}

        assert expected == rule.get_terms_query(rule.get_query([]), 1, 'field_name')


@pytest.mark.skip(reason='Test not yet implemented')
def test_get_aggregation_query():
    rule = gen_rule(RuleType({}), name='Test', timestamp_field='ts', timestamp_type='iso')
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        es_client.return_value = mock.Mock()
        es_client.return_value.es_version_at_least = mock.MagicMock(return_value=True)
        rule.es_client = es_client

        expected = {'query': {'bool': {'filter': {'bool': {'must': []}}}},
                    'sort': [{'@timestamp': {'order': 'asc'}}]}


def test_remove_duplicate_events():
    rule = gen_rule(timestamp_field='ts')
    events = [{'_id': i, 'ts': i+1} for i in range(10)]

    assert rule.remove_duplicate_events(events[0:1]) == events[0:1]
    assert rule.remove_duplicate_events(events[0:2]) == events[1:2]
    assert rule.remove_duplicate_events(events[0:5]) == events[2:5]
    assert rule.remove_duplicate_events(events) == events[5:]
    assert rule.remove_duplicate_events(events) == []


@pytest.mark.skip(reason='Test not yet implemented')
def test_adjust_start_time_for_overlapping_agg_query():
    pass


@pytest.mark.skip(reason='Test not yet implemented')
def test_adjust_start_time_for_interval_sync():
    pass


def test_get_query_key_value_with_query_key():
    rule = gen_rule(RuleType({}), query_key='qk_field')
    match = {'qk_field': 'qk_value'}
    assert rule.get_query_key_value(match) == 'qk_value'


def test_get_query_key_value_with_missing_query_key():
    rule = gen_rule(RuleType({}), query_key='qk_field')
    match = {}
    assert rule.get_query_key_value(match) == '_missing'


def test_get_query_key_value_flatline_with_query_key():
    rule = gen_rule(FlatlineRuleType({'attach_related': False, 'threshold': 1}), query_key='qk_field')
    match = {'qk_field': 'qk_value', 'key': 'key_value'}
    assert rule.get_query_key_value(match) == 'key_value'


def test_get_query_key_value_without_query_key():
    rule = gen_rule(RuleType({}))
    match = {'qk_field': 'qk_value'}
    assert rule.get_query_key_value(match) is None


def test_get_aggregation_key_value_with_key():
    rule = gen_rule(RuleType({}), aggregation_key='agg_field')
    match = {'agg_field': 'agg_value'}
    assert rule.get_aggregation_key_value(match) == 'agg_value'


def test_get_aggregation_key_value_with_missing_key():
    rule = gen_rule(RuleType({}), aggregation_key='agg_field')
    match = {}
    assert rule.get_aggregation_key_value(match) == '_missing'


def test_get_aggregation_key_value_without_key():
    rule = gen_rule(RuleType({}))
    match = {'agg_field': 'agg_value'}
    assert rule.get_aggregation_key_value(match) is None


@pytest.mark.skip(reason='Test not yet implemented')
def test_get_hits():
    pass


@pytest.mark.skip(reason='Test not yet implemented')
def test_get_hits_count():
    pass


@pytest.mark.skip(reason='Test not yet implemented')
def test_get_hits_terms():
    pass


@pytest.mark.skip(reason='Test not yet implemented')
def test_get_hits_aggregation():
    pass


def test_process_hits():
    rule = gen_rule(timestamp_field='@timestamp', timestamp_type='iso')
    for hit in rule.process_hits(gen_hits(10, data_field='data_value')):
        assert '_source' not in hit
        assert '_id' in hit
        assert '_index' in hit
        assert '_type' in hit
        assert 'data_field' in hit and hit['data_field'] == 'data_value'


def test_process_hits_with_fields():
    rule = gen_rule(timestamp_field='@timestamp', timestamp_type='iso')
    for hit in rule.process_hits(gen_hits(10, fields={'field_name': 'field_value'}, data_field='data_value')):
        assert '_source' not in hit
        assert '_id' in hit
        assert '_index' in hit
        assert '_type' in hit
        assert 'data_field' in hit and hit['data_field'] == 'data_value'
        assert 'field_name' in hit and hit['field_name'] == 'field_value'


def test_process_hits_with_source():
    rule = gen_rule(timestamp_field='@timestamp', timestamp_type='iso')
    with pytest.raises(ReactorException):
        rule.process_hits([{}])


def test_process_hits_with_compound_query_key():
    rule = gen_rule(timestamp_field='@timestamp', timestamp_type='iso',
                    compound_query_key=['qk_field1', 'qk_field2'], query_key='qk_field1,qk_field2')
    for hit in rule.process_hits(gen_hits(10, qk_field1='qk_value1', qk_field2='qk_value2')):
        assert '_source' not in hit
        assert '_id' in hit
        assert '_index' in hit
        assert '_type' in hit
        assert 'qk_field1' in hit and hit['qk_field1'] == 'qk_value1'
        assert 'qk_field2' in hit and hit['qk_field2'] == 'qk_value2'
        assert 'qk_field1,qk_field2' in hit and hit['qk_field1,qk_field2'] == 'qk_value1,qk_value2'


def test_process_hits_with_compound_aggregation_key():
    rule = gen_rule(timestamp_field='@timestamp', timestamp_type='iso',
                    compound_aggregation_key=['agg_field1', 'agg_field2'], aggregation_key='agg_field1,agg_field2')
    for hit in rule.process_hits(gen_hits(10, agg_field1='agg_value1', agg_field2='agg_value2')):
        assert '_source' not in hit
        assert '_id' in hit
        assert '_index' in hit
        assert '_type' in hit
        assert 'agg_field1' in hit and hit['agg_field1'] == 'agg_value1'
        assert 'agg_field2' in hit and hit['agg_field2'] == 'agg_value2'
        assert 'agg_field1,agg_field2' in hit and hit['agg_field1,agg_field2'] == 'agg_value1,agg_value2'
