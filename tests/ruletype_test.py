import copy
import datetime
import mock
import pytest
from itertools import zip_longest

from reactor.exceptions import ReactorException
from reactor.ruletype import (
    EventWindow,
    AnyRuleType,
    BlacklistRuleType,
    WhitelistRuleType,
    ChangeRuleType,
    FrequencyRuleType,
    FlatlineRuleType,
    SpikeRuleType,
    NewTermRuleType,
    CardinalityRuleType,
    BaseAggregationRuleType,
    MetricAggregationRuleType,
    SpikeMetricAggregationRuleType,
    PercentageMatchRuleType,
)
from reactor.util import ts_to_dt, dt_to_ts


def gen_timestamp(n: int = 0, offset=None) -> datetime.datetime:
    ts = ts_to_dt('2019-07-26T12:%s:%sZ' % (n // 60, n % 60))
    if offset:
        offset = datetime.timedelta(**offset) if isinstance(offset, dict) else offset
        ts += offset
    return ts


def gen_hits(size, timestamp_field='@timestamp', fields=None, offset=None, **kwargs):
    hits = []
    size = range(size) if isinstance(size, int) else size
    if offset:
        if isinstance(offset, int):
            offset = datetime.timedelta(seconds=offset)
        elif isinstance(offset, dict):
            offset = datetime.timedelta(**offset)
    for n, v in enumerate(size):
        hit = {
            '_id': str(n),
            '_index': 'reactor_logs',
            '_type': '_doc',
            timestamp_field: gen_timestamp(n, offset),
        }
        for kw_key, kw_value in kwargs.items():
            # If the value is a callable
            hit[kw_key] = kw_value(v) if callable(kw_value) else kw_value
        if fields:
            for f_name, f_value in fields:
                hit.setdefault(f_name, f_value)
        hits.append(hit)
    return hits


def gen_bucket_aggregation(agg_name, buckets, query_key=None):
    bucket_agg = {agg_name: {'buckets': buckets}}
    if query_key:
        bucket_agg['key'] = query_key
    return bucket_agg


def gen_percentage_match_agg(match_count, other_count):
    return gen_bucket_aggregation('percentage_match_aggs', {
        'match_bucket': {'doc_count': match_count},
        '_other_': {'doc_count': other_count},
    })


def assert_matches_have(matches, event_terms):
    assert len(matches) == len(event_terms)
    for (extra, event), event_term in zip(matches, event_terms):
        for key, value in zip_longest(*[iter(event_term)]*2, fillvalue=None):
            assert key in event
            assert event[key] == value


def assert_match_has(match, extra_terms, event_terms):
    extra, event = match
    assert len(extra) == len(extra_terms)
    assert len(event) == len(event_terms)
    assert (extra.keys() ^ set(extra_terms)) == set()
    assert (event.keys() ^ set(event_terms.keys())) == set()


def test_event_window():
    timeframe = datetime.timedelta(minutes=10)
    window = EventWindow(timeframe)
    timestamps = [ts_to_dt(x) for x in ['2019-07-26T12:00:00Z',
                                        '2019-07-26T12:05:00Z',
                                        '2019-07-26T12:03:00Z',
                                        '2019-07-26T11:55:00Z',
                                        '2019-07-26T12:09:00Z']]

    for ts in timestamps:
        window.append([{'@timestamp': ts}, 1])

    timestamps.sort()
    for expected, actual in zip(timestamps[1:], window.data):
        assert actual[0]['@timestamp'] == expected

    window.append([{'@timestamp': ts_to_dt('2019-07-26T12:14:00Z')}, 1])
    timestamps.append(ts_to_dt('2019-07-26T12:14:00Z'))

    for expected, actual in zip(timestamps[3:], window.data):
        assert actual[0]['@timestamp'] == expected


def test_any():
    hits = gen_hits(1)
    rule = AnyRuleType({})
    rule.add_hits_data(hits)
    assert len(rule.matches) == 1
    for hit, match in zip(hits, rule.matches):
        assert_match_has(match, {'key', 'num_events', 'began_at', 'ended_at'}, hit)


def test_blacklist():
    hits = gen_hits(['good', 'bad', 'also good', 'really bad'], term=lambda x: x) + gen_hits(1, no_term='bad')
    conf = {'blacklist': ['bad', 'really bad'],
            'compare_key': 'term',
            'timestamp_field': '@timestamp'}
    rule = BlacklistRuleType(conf)
    rule.add_hits_data(hits)
    assert len(rule.matches) == 2
    assert_matches_have(rule.matches, [('term', 'bad'), ('term', 'really bad')])


def test_whitelist_ignore_null_true():
    hits = gen_hits(['good', 'bad', 'also good', 'really bad'], term=lambda x: x) + gen_hits(1, no_term='bad')
    conf = {'whitelist': ['good', 'also good'],
            'compare_key': 'term',
            'ignore_null': True,
            'timestamp_field': '@timestamp'}
    rule = WhitelistRuleType(conf)
    rule.add_hits_data(hits)
    assert len(rule.matches) == 2
    assert_matches_have(rule.matches, [('term', 'bad'), ('term', 'really bad')])


def test_whitelist_ignore_null_false():
    hits = gen_hits(['good', 'bad', 'also good', 'really bad'], term=lambda x: x) + gen_hits(1, no_term='bad')
    conf = {'whitelist': ['good', 'also good'],
            'compare_key': 'term',
            'ignore_null': False,
            'timestamp_field': '@timestamp'}
    rule = WhitelistRuleType(conf)
    rule.add_hits_data(hits)
    assert len(rule.matches) == 3
    assert_matches_have(rule.matches, [('term', 'bad'), ('term', 'really bad'), ('no_term', 'bad')])


def test_change():
    hits = gen_hits(10, username='reactor', term='good', second_term='yes')
    hits[8].pop('term')
    hits[8].pop('second_term')
    hits[9]['term'] = 'bad'
    hits[9]['second_term'] = 'no'

    conf = {'compound_compare_key': ['term', 'second_term'],
            'query_key': 'username',
            'ignore_null': True,
            'timestamp_field': '@timestamp'}
    rule = ChangeRuleType(conf)
    rule.add_hits_data(hits)
    assert_matches_have(rule.matches, [('term', 'bad', 'second_term', 'no')])


def test_change_unhashable_qk():
    hits = gen_hits(10, username=['reactor'], term='good', second_term='yes')
    hits[8].pop('term')
    hits[8].pop('second_term')
    hits[9]['term'] = 'bad'
    hits[9]['second_term'] = 'no'

    conf = {'compound_compare_key': ['term', 'second_term'],
            'query_key': 'username',
            'ignore_null': True,
            'timestamp_field': '@timestamp'}
    rule = ChangeRuleType(conf)
    rule.add_hits_data(hits)
    assert_matches_have(rule.matches, [('term', 'bad', 'second_term', 'no')])


def test_change_ignore_null_false():
    hits = gen_hits(10, username='reactor', term='good', second_term='yes')
    hits[8].pop('term')
    hits[8].pop('second_term')
    hits[9]['term'] = 'bad'
    hits[9]['second_term'] = 'no'

    conf = {'compound_compare_key': ['term', 'second_term'],
            'query_key': 'username',
            'ignore_null': False,
            'timestamp_field': '@timestamp'}
    rule = ChangeRuleType(conf)
    rule.add_hits_data(hits)
    assert_matches_have(rule.matches, [('username', 'reactor'), ('term', 'bad', 'second_term', 'no')])


def test_change_with_timeframe():
    hits = gen_hits(10, username='reactor', term='good', second_term='yes')
    hits[8].pop('term')
    hits[8].pop('second_term')
    hits[9]['term'] = 'bad'
    hits[9]['second_term'] = 'no'

    conf = {'compound_compare_key': ['term', 'second_term'],
            'query_key': 'username',
            'ignore_null': True,
            'timeframe': datetime.timedelta(seconds=2),
            'timestamp_field': '@timestamp'}
    rule = ChangeRuleType(conf)
    rule.add_hits_data(hits)
    assert_matches_have(rule.matches, [('term', 'bad', 'second_term', 'no')])


def test_change_with_timeframe_no_matches():
    hits = gen_hits(10, username='reactor', term='good', second_term='yes')
    hits[8].pop('term')
    hits[8].pop('second_term')
    hits[9]['term'] = 'bad'
    hits[9]['second_term'] = 'no'

    conf = {'compound_compare_key': ['term', 'second_term'],
            'query_key': 'username',
            'ignore_null': True,
            'timeframe': datetime.timedelta(seconds=1),
            'timestamp_field': '@timestamp'}
    rule = ChangeRuleType(conf)
    rule.add_hits_data(hits)
    assert rule.matches == []


def test_frequency():
    hits = gen_hits(60, timestamp_field='blah', username='reactor')
    conf = {'num_events': 59,
            'timeframe': datetime.timedelta(hours=1),
            'timestamp_field': 'blah',
            'attach_related': False}
    rule = FrequencyRuleType(conf)
    rule.add_hits_data(hits)
    assert len(rule.matches) == 1

    # garbage collection
    assert 'all' in rule.occurrences
    rule.garbage_collect(ts_to_dt('2019-07-27T12:00:00Z'))
    assert rule.occurrences == {}


def test_frequency_with_qk():
    hits = gen_hits(60, timestamp_field='blah', username='reactor')
    conf = {'num_events': 59,
            'query_key': 'username',
            'timeframe': datetime.timedelta(hours=1),
            'timestamp_field': 'blah',
            'attach_related': False}
    rule = FrequencyRuleType(conf)
    rule.add_hits_data(hits)
    assert len(rule.matches) == 1

    # garbage collection
    assert 'reactor' in rule.occurrences
    rule.garbage_collect(ts_to_dt('2019-07-27T12:00:00Z'))
    assert rule.occurrences == {}


def test_frequency_with_no_matches():
    hits = gen_hits(60, timestamp_field='blah', username='reactor')
    conf = {'num_events': 61,
            'query_key': 'username',
            'timeframe': datetime.timedelta(hours=1),
            'timestamp_field': 'blah',
            'attach_related': False}
    rule = FrequencyRuleType(conf)
    rule.add_hits_data(hits)
    assert len(rule.matches) == 0


def test_frequency_out_of_order():
    hits = gen_hits(60, timestamp_field='blah', username='reactor')
    conf = {'num_events': 59,
            'timeframe': datetime.timedelta(hours=1),
            'timestamp_field': 'blah',
            'attach_related': False}
    rule = FrequencyRuleType(conf)
    rule.add_hits_data(hits[:10])
    assert len(rule.matches) == 0

    # Try to add hits from before the first occurrence
    old_hits = gen_hits(1, timestamp_field='blah', offset={'hours': -1})
    rule.add_hits_data(old_hits)
    assert len(rule.matches) == 0

    rule.add_hits_data(hits[15:20])
    assert len(rule.matches) == 0
    rule.add_hits_data(hits[10:15])
    assert len(rule.matches) == 0
    rule.add_hits_data(hits[20:55])
    assert len(rule.matches) == 0
    rule.add_hits_data(hits[57:])
    assert len(rule.matches) == 0
    rule.add_hits_data(hits[55:57])
    assert len(rule.matches) == 1

    # garbage collection
    assert 'all' in rule.occurrences
    rule.garbage_collect(ts_to_dt('2019-07-27T12:00:00Z'))
    assert rule.occurrences == {}


def test_frequency_count():
    conf = {'num_events': 100,
            'timeframe': datetime.timedelta(hours=1),
            'use_count_query': True,
            'attach_related': False}
    rule = FrequencyRuleType(conf)

    rule.add_count_data({gen_timestamp(0): 75})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(1): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(2): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(3): 6})
    assert len(rule.matches) == 1


def test_frequency_count_not_immediate():
    conf = {'num_events': 100,
            'timeframe': datetime.timedelta(seconds=3),
            'use_count_query': True,
            'attach_related': False}
    rule = FrequencyRuleType(conf)
    rule.add_count_data({gen_timestamp(0): 75})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(1): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(2): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(3): 6})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(4): 95})
    assert len(rule.matches) == 1


def test_frequency_terms():
    conf = {'num_events': 10,
            'timeframe': datetime.timedelta(hours=1),
            'query_key': 'username',
            'attach_related': False}
    rule = FrequencyRuleType(conf)
    terms = [
        {ts_to_dt('2019-07-26T12:01:00Z'): [{'key': 'userA', 'doc_count': 1},
                                            {'key': 'userB', 'doc_count': 5}]},
        {ts_to_dt('2019-07-26T12:10:00Z'): [{'key': 'userA', 'doc_count': 8},
                                            {'key': 'userB', 'doc_count': 5}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userA', 'doc_count': 3},
                                            {'key': 'userB', 'doc_count': 0}]},
    ]
    # Initial data
    rule.add_terms_data(terms[0])
    assert len(rule.matches) == 0

    # Match for user B
    rule.add_terms_data(terms[1])
    assert len(rule.matches) == 1
    assert rule.matches[0][1].get('username') == 'userB'

    # Match for user A
    rule.add_terms_data(terms[2])
    assert len(rule.matches) == 2
    assert rule.matches[1][1].get('username') == 'userA'


def test_flatline():
    conf = {'timeframe': datetime.timedelta(seconds=30),
            'threshold': 2,
            'timestamp_field': '@timestamp',
            'attach_related': False,
            'forget_keys': False}
    rule = FlatlineRuleType(conf)

    # 1 hit should not cause an alert until after at least 30 seconds pass
    rule.add_hits_data(gen_hits(1))
    assert rule.matches == []

    # Add hits with timestamps 12:00:00 --> 12:00:09
    rule.add_hits_data(gen_hits(10))

    # This will be run at the end of the hits
    rule.garbage_collect(gen_timestamp(11))
    assert rule.matches == []

    # This would be run if the query returned nothing for a future timestamp
    rule.garbage_collect(gen_timestamp(45))
    assert len(rule.matches) == 1

    # After another garbage collection, since there are still no events, a new match is added
    rule.garbage_collect(gen_timestamp(50))
    assert len(rule.matches) == 2

    # Add hits with timestamps 12:00:30 --> 12:00:39
    rule.add_hits_data(gen_hits(10, offset={'seconds': 30}))
    assert len(rule.matches) == 2

    # Now that there is data in the last 30 seconds, no more matches should be added
    rule.garbage_collect(gen_timestamp(55))
    assert len(rule.matches) == 2

    # After that window passes with no more data, a new match is added
    rule.garbage_collect(gen_timestamp(11, offset={'minutes': 1}))
    assert len(rule.matches) == 3


def test_flatline_no_data():
    conf = {'timeframe': datetime.timedelta(seconds=30),
            'threshold': 2,
            'timestamp_field': '@timestamp',
            'attach_related': False,
            'forget_keys': False}
    rule = FlatlineRuleType(conf)

    # Initial lack of data
    rule.garbage_collect(gen_timestamp(0))
    assert len(rule.matches) == 0

    # Passed the timeframe, still no events
    rule.garbage_collect(gen_timestamp(35))
    assert len(rule.matches) == 1


def test_flatline_with_qk():
    conf = {'timeframe': datetime.timedelta(seconds=30),
            'threshold': 1,
            'timestamp_field': '@timestamp',
            'use_query_key': True,
            'query_key': 'qk',
            'attach_related': False,
            'forget_keys': False}
    rule = FlatlineRuleType(conf)

    # Adding two separate query keys, the flatline rule should not trigger for any
    rule.add_hits_data(gen_hits(1, qk='key1'))
    rule.add_hits_data(gen_hits(1, qk='key2'))
    rule.add_hits_data(gen_hits(1, qk='key3'))
    assert rule.matches == []

    # This will be run at the end of the hits
    rule.garbage_collect(gen_timestamp(11))
    assert rule.matches == []

    # Add new data from key3. It will not immediately cause an alert
    rule.add_hits_data(gen_hits(1, offset={'seconds': 20}, qk='key3'))

    # key1 and key2 have not had any new data, so they will trigger the flatline alert
    rule.garbage_collect(gen_timestamp(45))
    assert len(rule.matches) == 2
    assert {'key1', 'key2'} == set([m[0]['key'] for m in rule.matches])

    # Next time the rule runs, all 3 keys still have no data, so all three will cause an alert
    rule.garbage_collect(gen_timestamp(20, offset={'minutes': 1}))
    assert len(rule.matches) == 5
    assert {'key1', 'key2', 'key3'} == set([m[0]['key'] for m in rule.matches])


def test_flatline_with_forget_qk():
    conf = {'timeframe': datetime.timedelta(seconds=30),
            'threshold': 1,
            'timestamp_field': '@timestamp',
            'use_query_key': True,
            'query_key': 'qk',
            'attach_related': False,
            'forget_keys': True}
    rule = FlatlineRuleType(conf)

    # Adding two separate query keys, the flatline rule should not trigger for either
    rule.add_hits_data(gen_hits(1, qk='key1'))
    assert rule.matches == []

    # This will be run at the end of the hits
    rule.garbage_collect(gen_timestamp(11))
    assert rule.matches == []

    # key2 should not alert
    rule.garbage_collect(gen_timestamp(45))
    assert len(rule.matches) == 1
    rule.matches = []

    # key1 was forgotten, so no more alerts
    rule.garbage_collect(gen_timestamp(11, offset={'minutes': 1}))
    assert len(rule.matches) == 0


def test_flatline_count():
    conf = {'timeframe': datetime.timedelta(seconds=30),
            'threshold': 2,
            'timestamp_field': '@timestamp',
            'attach_related': False,
            'forget_keys': False}
    rule = FlatlineRuleType(conf)

    rule.add_count_data({gen_timestamp(0): 1})
    rule.garbage_collect(gen_timestamp(10))
    assert len(rule.matches) == 0

    rule.add_count_data({gen_timestamp(15): 0})
    rule.garbage_collect(gen_timestamp(20))
    assert len(rule.matches) == 0

    rule.add_count_data({gen_timestamp(35): 0})
    assert len(rule.matches) == 1


def test_spike():
    hits = gen_hits(90)
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(seconds=20),
            'use_count_query': False,
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Half rate of hits until
    rule.add_hits_data(hits[:20:2])
    assert len(rule.matches) == 0

    # Double the rate of hits - must wait 2 * timeframe before will alert again
    rule.add_hits_data(hits[20:61])
    assert len(rule.matches) == 1

    # Halve the rate of hits
    rule.add_hits_data(hits[61::2])
    assert len(rule.matches) == 2


def test_spike_up():
    hits = gen_hits(90)
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'up',
            'timeframe': datetime.timedelta(seconds=20),
            'use_count_query': False,
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Half rate of hits until
    rule.add_hits_data(hits[:20:2])
    assert len(rule.matches) == 0

    # Double the rate of hits - must wait 2 * timeframe before will alert again
    rule.add_hits_data(hits[20:61])
    assert len(rule.matches) == 1

    # Halve the rate of hits
    rule.add_hits_data(hits[61::2])
    assert len(rule.matches) == 1


def test_spike_down():
    hits = gen_hits(90)
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'down',
            'timeframe': datetime.timedelta(seconds=20),
            'use_count_query': False,
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Half rate of hits until
    rule.add_hits_data(hits[:20:2])
    assert len(rule.matches) == 0

    # Double the rate of hits - must wait 2 * timeframe before will alert again
    rule.add_hits_data(hits[20:61])
    assert len(rule.matches) == 0

    # Halve the rate of hits
    rule.add_hits_data(hits[61::2])
    assert len(rule.matches) == 1


def test_spike_with_threshold_ref():
    hits = gen_hits(400)
    conf = {'threshold_ref': 25,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(minutes=1),
            'use_count_query': False,
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Start with a
    rule.add_hits_data(hits[:50:8])
    assert len(rule.matches) == 0

    rule.add_hits_data(hits[50:100:4])
    assert len(rule.matches) == 0

    rule.add_hits_data(hits[100:300:2])
    assert len(rule.matches) == 0

    rule.add_hits_data(hits[300:])
    assert len(rule.matches) == 1


def test_spike_with_threshold_cur():
    hits = gen_hits(200)
    conf = {'threshold_ref': 1,
            'threshold_cur': 25,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(seconds=30),
            'use_count_query': False,
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Start with a
    rule.add_hits_data(hits[:50:4])
    assert len(rule.matches) == 0

    rule.add_hits_data(hits[50:100:2])
    assert len(rule.matches) == 0

    rule.add_hits_data(hits[100:])
    assert len(rule.matches) == 1


def test_spike_with_alert_on_new_data():
    hits = gen_hits(100, username='reactor')
    conf = {'threshold_ref': 0,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(seconds=30),
            'use_count_query': False,
            'timestamp_field': '@timestamp',
            'query_key': 'username',
            'alert_on_new_data': True}
    rule = SpikeRuleType(conf)

    # Fill up the baseline ref and cur windows
    rule.add_hits_data(hits)
    assert len(rule.matches) == 0

    # Trigger an alert with new data from another username
    rule.add_hits_data(gen_hits(30, username='not-reactor', offset={'seconds': 100}))
    assert len(rule.matches) == 1


def test_spike_with_qk():
    hits = gen_hits(90, username='reactor')
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(seconds=20),
            'use_count_query': False,
            'timestamp_field': '@timestamp',
            'query_key': 'username'}
    rule = SpikeRuleType(conf)
    rule = SpikeRuleType(conf)

    # Half rate of hits until
    rule.add_hits_data(hits[:20:2])
    assert len(rule.matches) == 0

    # Double the rate of hits - must wait 2 * timeframe before will alert again
    rule.add_hits_data(hits[20:61])
    assert len(rule.matches) == 1

    # Halve the rate of hits
    rule.add_hits_data(hits[61::2])
    assert len(rule.matches) == 2


def test_spike_deep_key():
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(seconds=10),
            'timestamp_field': '@timestamp',
            'query_key': 'foo.bar.baz'}
    rule = SpikeRuleType(conf)

    rule.add_hits_data(gen_hits(1, foo={'bar': {'baz': 'qux'}}))
    assert 'qux' in rule.cur_windows


def test_spike_count():
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(seconds=10),
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Double rate of hits at 20 seconds
    rule.add_count_data({gen_timestamp(0): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(10): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(20): 20})
    assert len(rule.matches) == 1

    # Downward spike
    rule.add_count_data({gen_timestamp(30): 20})
    assert len(rule.matches) == 1
    rule.add_count_data({gen_timestamp(40): 20})
    assert len(rule.matches) == 1
    rule.add_count_data({gen_timestamp(50): 10})
    assert len(rule.matches) == 2


def test_spike_count_spike_up():
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'up',
            'timeframe': datetime.timedelta(seconds=10),
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Double rate of hits at 20 seconds
    rule.add_count_data({gen_timestamp(0): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(10): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(20): 20})
    assert len(rule.matches) == 1

    # Downward spike
    rule.add_count_data({gen_timestamp(30): 20})
    assert len(rule.matches) == 1
    rule.add_count_data({gen_timestamp(40): 20})
    assert len(rule.matches) == 1
    rule.add_count_data({gen_timestamp(50): 10})
    assert len(rule.matches) == 1


def test_spike_count_spike_down():
    conf = {'threshold_ref': 10,
            'spike_height': 2,
            'spike_type': 'down',
            'timeframe': datetime.timedelta(seconds=10),
            'timestamp_field': '@timestamp'}
    rule = SpikeRuleType(conf)

    # Double rate of hits at 20 seconds
    rule.add_count_data({gen_timestamp(0): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(10): 10})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(20): 20})
    assert len(rule.matches) == 0

    # Downward spike
    rule.add_count_data({gen_timestamp(30): 20})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(40): 20})
    assert len(rule.matches) == 0
    rule.add_count_data({gen_timestamp(50): 10})
    assert len(rule.matches) == 1


def test_spike_terms():
    conf = {'threshold_ref': 0,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp',
            'use_count_query': False,
            'query_key': 'username',
            'use_terms_query': True}
    rule = SpikeRuleType(conf)

    terms = [
        {ts_to_dt('2019-07-26T12:01:00Z'): [{'key': 'userA', 'doc_count': 10},
                                            {'key': 'userB', 'doc_count': 5}]},
        {ts_to_dt('2019-07-26T12:10:00Z'): [{'key': 'userA', 'doc_count': 22},
                                            {'key': 'userB', 'doc_count': 5}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userA', 'doc_count': 25},
                                            {'key': 'userB', 'doc_count': 27}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userA', 'doc_count': 10},
                                            {'key': 'userB', 'doc_count': 12},
                                            {'key': 'userC', 'doc_count': 100}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userC', 'doc_count': 100},
                                            {'key': 'userD', 'doc_count': 100}]},
    ]

    # Initial input
    rule.add_terms_data(terms[0])
    assert len(rule.matches) == 0

    # No spike for userA because windows not filled
    rule.add_terms_data(terms[1])
    assert len(rule.matches) == 0

    # Spike for userB only
    rule.add_terms_data(terms[2])
    assert len(rule.matches) == 1
    assert rule.matches[0][1].get('username') == 'userB'

    # Test no alert for new user over threshold
    rule.add_terms_data(terms[3])
    assert len(rule.matches) == 1

    # Test no alert for new user over threshold
    rule.add_terms_data(terms[4])
    assert len(rule.matches) == 1


def test_spike_terms_with_alert_on_new_data():
    conf = {'threshold_ref': 0,
            'spike_height': 2,
            'spike_type': 'both',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp',
            'use_count_query': False,
            'query_key': 'username',
            'use_terms_query': True,
            'alert_on_new_data': True}
    rule = SpikeRuleType(conf)

    terms = [
        {ts_to_dt('2019-07-26T12:01:00Z'): [{'key': 'userA', 'doc_count': 10},
                                            {'key': 'userB', 'doc_count': 5}]},
        {ts_to_dt('2019-07-26T12:10:00Z'): [{'key': 'userA', 'doc_count': 22},
                                            {'key': 'userB', 'doc_count': 5}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userA', 'doc_count': 25},
                                            {'key': 'userB', 'doc_count': 27}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userA', 'doc_count': 10},
                                            {'key': 'userB', 'doc_count': 12},
                                            {'key': 'userC', 'doc_count': 100}]},
        {ts_to_dt('2019-07-26T12:25:00Z'): [{'key': 'userC', 'doc_count': 100},
                                            {'key': 'userD', 'doc_count': 100}]},
    ]

    # Initial input
    rule.add_terms_data(terms[0])
    assert len(rule.matches) == 0

    # No spike for userA because windows not filled
    rule.add_terms_data(terms[1])
    assert len(rule.matches) == 0

    # Spike for userB only
    rule.add_terms_data(terms[2])
    assert len(rule.matches) == 1
    assert rule.matches[0][1].get('username') == 'userB'

    # Test no alert for new user over threshold
    rule.add_terms_data(terms[3])
    assert len(rule.matches) == 2

    # Test no alert for new user over threshold
    rule.add_terms_data(terms[4])
    assert len(rule.matches) == 3


def test_new_term():
    conf = {'fields': ['a', 'b'],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)
    # Mock the result of prepare
    rule.seen_values = {'a': {'key1', 'key2'}, 'b': {'key1', 'key2'}}

    # key1 and key2 shouldn't cause a match
    rule.add_hits_data(gen_hits(1, a='key1', b='key2'))
    assert len(rule.matches) == 0

    # Neither will missing values
    rule.add_hits_data(gen_hits(1, a='key2', offset={'seconds': 1}))
    assert len(rule.matches) == 0

    # key3 causes an alert for field b
    rule.add_hits_data(gen_hits(1, b='key3', offset={'seconds': 2}))
    assert len(rule.matches) == 1
    assert rule.matches[0][0]['new_field'] == 'b'
    assert rule.matches[0][0]['new_value'] == 'key3'

    # key3 doesn't cause another alert for field b but does for field a
    rule.add_hits_data(gen_hits(1, b='key3', offset={'seconds': 3}))
    assert len(rule.matches) == 1
    rule.add_hits_data(gen_hits(1, a='key3', offset={'seconds': 4}))
    assert len(rule.matches) == 2


def test_new_term_with_alert_on_missing_field():
    conf = {'fields': ['a', 'b'],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts,
            'alert_on_missing_field': True}
    rule = NewTermRuleType(conf)
    # Mock the result of prepare
    rule.seen_values = {'a': {'key1', 'key2'}, 'b': {'key1', 'key2'}}

    # key1 and key2 shouldn't cause a match
    rule.add_hits_data(gen_hits(1, a='key1', b='key2'))
    assert len(rule.matches) == 0

    # Missing values should cause a match
    rule.add_hits_data(gen_hits(1, a='key2', offset={'seconds': 1}))
    assert len(rule.matches) == 1
    assert rule.matches[0][0]['missing_field'] == 'b'


def test_new_term_with_nested_fields():
    conf = {'fields': ['a', 'b.c'],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)
    # Mock the result of prepare
    rule.seen_values = {'a': {'key1', 'key2'}, 'b.c': {'key1', 'key2'}}

    # key1 and key2 shouldn't cause a match
    rule.add_hits_data(gen_hits(1, a='key1', b={'c': 'key2'}))
    assert len(rule.matches) == 0

    # Neither will missing values
    rule.add_hits_data(gen_hits(1, a='key2', offset={'seconds': 1}))
    assert len(rule.matches) == 0

    # key3 causes an alert for field b.c
    rule.add_hits_data(gen_hits(1, b={'c': 'key3'}, offset={'seconds': 2}))
    assert len(rule.matches) == 1
    assert rule.matches[0][0]['new_field'] == 'b.c'
    assert rule.matches[0][0]['new_value'] == 'key3'

    # key3 doesn't cause another alert for field b.c but does for field a
    rule.add_hits_data(gen_hits(1, b={'c': 'key3'}, offset={'seconds': 3}))
    assert len(rule.matches) == 1
    rule.add_hits_data(gen_hits(1, a='key3', offset={'seconds': 4}))
    assert len(rule.matches) == 2


def test_new_term_with_composite_fields():
    conf = {'fields': [['a', 'b', 'c'], ['d', 'e.f']],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)
    # Mock the result of prepare
    rule.seen_values = {('a', 'b', 'c'): {('key1', 'key2', 'key3'), ('key1', 'key2', 'key4')},
                        ('d', 'e.f'): {('key1', 'key2', 'key3'), ('key1', 'key2', 'key4')}}

    # Composite `key1, key2, key3` already exists, thus no match
    rule.add_hits_data(gen_hits(1, a='key1', b='key2', c='key3'))
    assert len(rule.matches) == 0

    # Composite `key1, key2, key5` is a new term, thus a match
    rule.add_hits_data(gen_hits(1, a='key1', b='key2', c='key5'))
    assert len(rule.matches) == 1
    assert rule.matches[0][0]['new_field'] == ('a', 'b', 'c')
    assert rule.matches[0][1]['a'] == 'key1'
    assert rule.matches[0][1]['b'] == 'key2'
    assert rule.matches[0][1]['c'] == 'key5'

    # New values in other fields that are not part of the composite key should not cause a match
    rule.add_hits_data(gen_hits(1, a='key1', b='key2', c='key4', d='unrelated_value'))
    assert len(rule.matches) == 1

    # Verify nested fields work properly, key6 causes a match for nested field `e.f`
    rule.add_hits_data(gen_hits(1, d='key4', e={'f': 'key6'}))
    assert len(rule.matches) == 2
    assert rule.matches[1][0]['new_field'] == ('d', 'e.f')
    assert rule.matches[1][1]['d'] == 'key4'
    assert rule.matches[1][1]['e'] == {'f': 'key6'}


def test_new_term_with_composite_fields_and_alert_on_missing_field():
    conf = {'fields': [['a', 'b', 'c'], ['d', 'e.f']],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts,
            'alert_on_missing_field': True}
    rule = NewTermRuleType(conf)
    # Mock the result of prepare
    rule.seen_values = {('a', 'b', 'c'): {('key1', 'key2', 'key3'), ('key1', 'key2', 'key4')},
                        ('d', 'e.f'): {('key1', 'key2', 'key3'), ('key1', 'key2', 'key4')}}

    # Composite `key1, key2, key5` is a new term, thus a match
    rule.add_hits_data(gen_hits(1, a='key1', b='key2'))
    assert len(rule.matches) == 2
    assert rule.matches[0][0]['missing_field'] == ('a', 'b', 'c')
    assert rule.matches[1][0]['missing_field'] == ('d', 'e.f')


def test_new_term_prepare():
    conf = {'fields': ['a', 'b', 'c.d'],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)

    # Mock preparing the rule
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        mock_search_res = {'aggregations': {'filtered': {'values': {'buckets': [{'key': 'key1', 'doc_count': 1},
                                                                                {'key': 'key2', 'doc_count': 5}]}}}}

        es_client.info.return_value = {'version': {'number': '6.0.0'}}
        es_client.search.return_value = mock_search_res
        call_args = []

        # search is called with a mutable dict containing timestamps, this is required to test
        def record_args(*args, **kwargs):
            call_args.append((copy.deepcopy(args), copy.deepcopy(kwargs)))
            return mock_search_res

        es_client.search.side_effect = record_args
        rule.prepare(es_client)

    # 30 day range, 1 day default step, times 3 fields
    assert es_client.search.call_count == 90
    assert len(call_args) == 90

    # Assert that all calls have the proper ordering of time ranges
    old_ts = dt_to_ts(datetime.datetime.min)
    old_field = ''
    for call in call_args:
        field = call[1]['body']['aggs']['filtered']['aggs']['values']['terms']['field']
        if old_field != field:
            old_field = field
            old_ts = dt_to_ts(datetime.datetime.min)
        gte = call[1]['body']['aggs']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte']
        assert gte > old_ts
        lt = call[1]['body']['aggs']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lt']
        assert lt > gte
        old_ts = gte

    assert rule.seen_values == {'a': {'key2', 'key1'}, 'b': {'key2', 'key1'}, 'c.d': {'key2', 'key1'}}


def test_new_term_prepare_window_step_size():
    conf = {'fields': ['a', 'b'],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'window_step_size': {'days': 2},
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)

    # Mock preparing the rule
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        mock_search_res = {'aggregations': {'filtered': {'values': {'buckets': [{'key': 'key1', 'doc_count': 1},
                                                                                {'key': 'key2', 'doc_count': 5}]}}}}

        es_client.info.return_value = {'version': {'number': '6.0.0'}}
        es_client.search.return_value = mock_search_res
        call_args = []

        # search is called with a mutable dict containing timestamps, this is required to test
        def record_args(*args, **kwargs):
            call_args.append((copy.deepcopy(args), copy.deepcopy(kwargs)))
            return mock_search_res

        es_client.search.side_effect = record_args
        rule.prepare(es_client)

    # 30 day range, 2 day step, times 2 fields
    assert es_client.search.call_count == 30
    assert len(call_args) == 30

    # Assert that all calls have the proper ordering of time ranges
    old_ts = dt_to_ts(datetime.datetime.min)
    old_field = ''
    for call in call_args:
        field = call[1]['body']['aggs']['filtered']['aggs']['values']['terms']['field']
        if old_field != field:
            old_field = field
            old_ts = dt_to_ts(datetime.datetime.min)
        gte = call[1]['body']['aggs']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte']
        assert gte > old_ts
        lt = call[1]['body']['aggs']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lt']
        assert lt > gte
        old_ts = gte

    assert rule.seen_values == {'a': {'key2', 'key1'}, 'b': {'key2', 'key1'}}


def test_new_term_prepare_with_composite_fields():
    conf = {'fields': [['a', 'b', 'c'], ['d', 'e.f']],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)

    # Mock preparing the rule
    with mock.patch('reactor.util.ElasticSearchClient') as es_client:
        mock_search_res = \
            {'aggregations': {'filtered': {'values': {'buckets': [
                {'key': 'key1', 'doc_count': 5, 'values': {
                    'buckets': [
                        {
                            'key': 'key2',
                            'doc_count': 5,
                            'values': {
                                'buckets': [
                                    {'key': 'key3', 'doc_count': 3},
                                    {'key': 'key4', 'doc_count': 2},
                                ]
                            }
                        }
                    ]
                }}]}}}}

        es_client.info.return_value = {'version': {'number': '6.0.0'}}
        es_client.search.return_value = mock_search_res
        call_args = []

        # search is called with a mutable dict containing timestamps, this is required to test
        def record_args(*args, **kwargs):
            call_args.append((copy.deepcopy(args), copy.deepcopy(kwargs)))
            return mock_search_res

        es_client.search.side_effect = record_args
        rule.prepare(es_client)

    # 30 day range, 1 day default step, times 2 fields
    assert es_client.search.call_count == 60
    assert len(call_args) == 60

    # Assert that all calls have the proper ordering of time ranges
    old_ts = dt_to_ts(datetime.datetime.min)
    old_field = ''
    for call in call_args:
        field = call[1]['body']['aggs']['filtered']['aggs']['values']['terms']['field']
        if old_field != field:
            old_field = field
            old_ts = dt_to_ts(datetime.datetime.min)
        gte = call[1]['body']['aggs']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte']
        assert gte > old_ts
        lt = call[1]['body']['aggs']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lt']
        assert lt > gte
        old_ts = gte

    assert rule.seen_values == {('a', 'b', 'c'): {('key1', 'key2', 'key3'), ('key1', 'key2', 'key4')},
                                ('d', 'e.f'): {('key1', 'key2', 'key3'), ('key1', 'key2', 'key4')}}


def test_new_term_terms():
    conf = {'fields': ['a'],
            'timestamp_field': '@timestamp',
            'index': 'reactor_logs',
            'ts_to_dt': ts_to_dt,
            'dt_to_ts': dt_to_ts}
    rule = NewTermRuleType(conf)
    # Mock the result of prepare
    rule.seen_values = {'a': {'key1', 'key2'}}

    # key1 and key2 shouldn't cause a match
    terms = {gen_timestamp(0): [{'key': 'key1', 'doc_count': 1},
                                {'key': 'key2', 'doc_count': 1}]}
    rule.add_terms_data(terms)
    assert len(rule.matches) == 0

    # key3 causes an alert for field a
    terms = {gen_timestamp(1): [{'key': 'key3', 'doc_count': 1}]}
    rule.add_terms_data(terms)
    assert len(rule.matches) == 1
    assert rule.matches[0][0]['new_field'] == 'a'
    assert rule.matches[0][0]['new_value'] == 'key3'

    # key3 doesn't cause another alert for field b
    terms = {gen_timestamp(1): [{'key': 'key3', 'doc_count': 1}]}
    rule.add_terms_data(terms)
    assert len(rule.matches) == 1


def test_cardinality_max():
    conf = {'max_cardinality': 4,
            'cardinality_field': 'username',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp'}
    rule = CardinalityRuleType(conf)

    # Add 4 different user names
    rule.add_hits_data(gen_hits(['userA', 'userB', 'userC', 'userD'], username=lambda x: x))
    assert len(rule.matches) == 0
    rule.garbage_collect(gen_timestamp(4))

    # Add a duplicate, stay at 4 cardinality
    rule.add_hits_data(gen_hits(1, username='userB', offset={'seconds': 6}))
    rule.garbage_collect(gen_timestamp(6))
    assert len(rule.matches) == 0

    # Next unique will trigger
    rule.add_hits_data(gen_hits(1, username='new-user', offset={'seconds': 8}))
    rule.garbage_collect(gen_timestamp(8))
    assert len(rule.matches) == 1

    # 15 minutes later, adding more will not trigger an alert
    rule.add_hits_data(gen_hits(['userX', 'userY', 'userZ'], username=lambda x: x, offset={'minutes': 15}))
    assert len(rule.matches) == 1
    rule.garbage_collect(gen_timestamp(4, offset={'minutes': 15}))


def test_cardinality_max_with_qk():
    conf = {'max_cardinality': 2,
            'cardinality_field': 'foo',
            'query_key': 'username',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp'}
    rule = CardinalityRuleType(conf)

    # Add 3 different user names, one value each
    rule.add_hits_data(gen_hits(['userA', 'userB', 'userC'], username=lambda x: x, foo=lambda x: 'foo' + x))
    rule.garbage_collect(gen_timestamp(3))
    assert len(rule.matches) == 0

    # Add 3 more unique values for `userC`, should cause two matches
    rule.add_hits_data(gen_hits(['bar', 'baz', 'bux'], username='userC', foo=lambda x: x, offset={'minutes': 5}))
    rule.garbage_collect(gen_timestamp(3, offset={'minutes': 5}))
    assert len(rule.matches) == 2
    assert rule.matches[0][1]['username'] == 'userC'
    assert rule.matches[1][1]['username'] == 'userC'
    assert rule.matches[0][1]['foo'] == 'baz'
    assert rule.matches[1][1]['foo'] == 'bux'


def test_cardinality_min():
    conf = {'min_cardinality': 4,
            'cardinality_field': 'username',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp'}
    rule = CardinalityRuleType(conf)

    # Add 2 different user names, no alert because time hasn't elapsed
    rule.add_hits_data(gen_hits(['userA', 'userB'], username=lambda x: x))
    assert len(rule.matches) == 0
    rule.garbage_collect(gen_timestamp(2))

    # Add 3 more user names at +5 minutes
    rule.add_hits_data(gen_hits(['userC', 'userD', 'userE'], username=lambda x: x, offset={'minutes': 5}))
    rule.garbage_collect(gen_timestamp(3, offset={'minutes': 5}))
    assert len(rule.matches) == 0

    # 15 minutes later, adding an existing user name will cause a match
    rule.add_hits_data(gen_hits(1, username='userE', offset={'minutes': 16}))
    rule.garbage_collect(gen_timestamp(1, offset={'minutes': 16}))
    assert len(rule.matches) == 1


def test_cardinality_min_with_qk():
    conf = {'min_cardinality': 2,
            'cardinality_field': 'foo',
            'query_key': 'username',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp'}
    rule = CardinalityRuleType(conf)

    # Add 3 different user names, one value each, no alert because time hasn't elapsed
    rule.add_hits_data(gen_hits(['userA', 'userB', 'userC'], username=lambda x: x, foo=lambda x: 'foo' + x))
    rule.garbage_collect(gen_timestamp(3))
    assert len(rule.matches) == 0

    # Add 3 more user names at +5 minutes (300 seconds)
    rule.add_hits_data(gen_hits(['userA', 'userB', 'userC'], username=lambda x: x, foo=lambda x: 'foo' + x, offset=300))
    rule.garbage_collect(gen_timestamp(3, offset={'minutes': 5}))
    assert len(rule.matches) == 0

    # 15 minutes later, adding an existing user name will cause 3 matches (one for each query key)
    rule.garbage_collect(gen_timestamp(3, offset={'minutes': 15}))
    assert len(rule.matches) == 3


def test_cardinality_nested_cardinality_field():
    conf = {'max_cardinality': 4,
            'cardinality_field': 'd.ip',
            'timeframe': datetime.timedelta(minutes=10),
            'timestamp_field': '@timestamp'}
    rule = CardinalityRuleType(conf)

    # Add 4 different IPs
    ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']
    rule.add_hits_data(gen_hits(ips, d=lambda x: {'ip': x}))
    rule.garbage_collect(gen_timestamp(4))
    assert len(rule.matches) == 0

    # Add a duplicate, stay at a cardinality of 4
    rule.add_hits_data(gen_hits(1, d={'ip': ips[3]}, offset=5))
    rule.garbage_collect(gen_timestamp(5))
    assert len(rule.matches) == 0

    # Add an event with no IP, stay at a cardinality of 4
    rule.add_hits_data(gen_hits(1, offset=6))
    rule.garbage_collect(gen_timestamp(6))
    assert len(rule.matches) == 0

    # Unique IP will cause a match
    rule.add_hits_data(gen_hits(1, d={'ip': '10.0.0.5'}, offset=10))
    rule.garbage_collect(gen_timestamp(10))
    assert len(rule.matches) == 1

    # 15 minutes later, adding more will not trigger a match
    rule.add_hits_data(gen_hits(['10.0.0.6', '10.0.0.7', '10.0.0.7'], d=lambda x: {'ip': x}, offset={'minutes': 15}))
    assert len(rule.matches) == 1


def test_base_aggregation_constructor_bucket_interval():
    conf = {'bucket_interval_timedelta': datetime.timedelta(seconds=10),
            'buffer_time': datetime.timedelta(minutes=1),
            'bucket_interval': None,
            'timestamp_field': '@timestamp'}

    # Test time period constructor logic
    pairs = [({'seconds': 10}, '10s'), ({'minutes': 5}, '5m'), ({'hours': 4}, '4h'),
             ({'days': 3}, '3d'), ({'weeks': 2}, '2w')]
    for bucket_interval, bucket_interval_period in pairs:
        conf['bucket_interval'] = bucket_interval
        rule = BaseAggregationRuleType(conf)
        assert rule.conf['bucket_interval_period'] == bucket_interval_period

    with pytest.raises(ReactorException):
        conf['bucket_interval'] = {'month': 1}
        rule = BaseAggregationRuleType(conf)


def test_base_aggregation_constructor_buffer_time():
    conf = {'bucket_interval_timedelta': datetime.timedelta(seconds=10),
            'buffer_time': datetime.timedelta(minutes=1),
            'bucket_interval': {'seconds': 10},
            'timestamp_field': '@timestamp'}

    # `buffer_time` evenly divisible by bucket period
    with pytest.raises(ReactorException):
        conf['bucket_interval_timedelta'] = datetime.timedelta(seconds=13)
        rule = BaseAggregationRuleType(conf)


def test_base_aggregation_constructor_run_every():
    conf = {'bucket_interval_timedelta': datetime.timedelta(seconds=10),
            'buffer_time': datetime.timedelta(minutes=1),
            'bucket_interval': {'seconds': 10},
            'timestamp_field': '@timestamp',
            'use_run_every_query_size': True,
            'run_every': datetime.timedelta(minutes=2)}

    # `run_every` evenly divisible by `bucket_interval`
    rule = BaseAggregationRuleType(conf)

    with pytest.raises(ReactorException):
        conf['bucket_interval_timedelta'] = datetime.timedelta(seconds=13)
        rule = BaseAggregationRuleType(conf)


def test_base_aggregation_payload_not_wrapped():
    with mock.patch.object(BaseAggregationRuleType, 'check_for_matches', return_value=None) as mock_check_matches:
        conf = {'bucket_interval': {'seconds': 10},
                'bucket_interval_timedelta': datetime.timedelta(seconds=10),
                'buffer_time': datetime.timedelta(minutes=5),
                'timestamp_field': '@timestamp'}

        rule = BaseAggregationRuleType(conf)
        timestamp = gen_timestamp(60)

        # Payload not wrapped
        rule.add_aggregation_data({timestamp: {}})
        mock_check_matches.assert_called_once_with(timestamp, None, {})


def test_base_aggregation_payload_wrapped_by_date_histogram():
    with mock.patch.object(BaseAggregationRuleType, 'check_for_matches', return_value=None) as mock_check_matches:
        conf = {'bucket_interval': {'seconds': 10},
                'bucket_interval_timedelta': datetime.timedelta(seconds=10),
                'buffer_time': datetime.timedelta(minutes=5),
                'timestamp_field': '@timestamp'}

        rule = BaseAggregationRuleType(conf)
        timestamp = gen_timestamp(60)
        interval_agg = gen_bucket_aggregation('interval_aggs', [{'key_as_string': dt_to_ts(gen_timestamp())}])

        # Payload wrapped by date_histogram
        interval_agg_data = {timestamp: interval_agg}
        rule.add_aggregation_data(interval_agg_data)
        mock_check_matches.assert_called_once_with(gen_timestamp(), None, {'key_as_string': dt_to_ts(gen_timestamp())})


def test_base_aggregation_payload_wrapped_by_terms():
    with mock.patch.object(BaseAggregationRuleType, 'check_for_matches', return_value=None) as mock_check_matches:
        conf = {'bucket_interval': {'seconds': 10},
                'bucket_interval_timedelta': datetime.timedelta(seconds=10),
                'buffer_time': datetime.timedelta(minutes=5),
                'timestamp_field': '@timestamp'}

        rule = BaseAggregationRuleType(conf)
        timestamp = gen_timestamp(60)

        # Payload wrapped by terms
        bucket_agg_data = {timestamp: gen_bucket_aggregation('bucket_aggs', [{'key': 'qk'}])}
        rule.add_aggregation_data(bucket_agg_data)
        mock_check_matches.assert_called_once_with(timestamp, 'qk', {'key': 'qk'})


def test_base_aggregation_payload_wrapped_by_terms_and_date_histogram():
    with mock.patch.object(BaseAggregationRuleType, 'check_for_matches', return_value=None) as mock_check_matches:
        conf = {'bucket_interval': {'seconds': 10},
                'bucket_interval_timedelta': datetime.timedelta(seconds=10),
                'buffer_time': datetime.timedelta(minutes=5),
                'timestamp_field': '@timestamp'}

        rule = BaseAggregationRuleType(conf)
        ts = gen_timestamp(60)
        interval_agg = gen_bucket_aggregation('interval_aggs', [{'key_as_string': dt_to_ts(gen_timestamp())}])
        interval_aggs = interval_agg['interval_aggs']

        # Payload wrapped by terms and date histogram
        bucket_interval_data = {ts: gen_bucket_aggregation('bucket_aggs', [{'key': 'qk',
                                                                            'interval_aggs': interval_aggs}])}
        rule.add_aggregation_data(bucket_interval_data)
        mock_check_matches.assert_called_once_with(gen_timestamp(), 'qk', {'key_as_string': dt_to_ts(gen_timestamp())})


def test_metric_aggregation_constructor_threshold():
    conf = {'buffer_time': datetime.timedelta(minutes=5),
            'metric_agg_type': 'avg',
            'metric_agg_key': 'cpu_pct',
            'timestamp_field': '@timestamp'}

    # No thresholds in conf
    with pytest.raises(ReactorException):
        MetricAggregationRuleType(conf)

    # `max_threshold` only
    conf.pop('min_threshold', None)
    conf['max_threshold'] = 0.8
    MetricAggregationRuleType(conf)

    # `min_threshold` only
    conf['min_threshold'] = 0.1
    conf.pop('max_threshold', None)
    MetricAggregationRuleType(conf)


def test_metric_aggregation_constructor_metric_agg_type():
    conf = {'buffer_time': datetime.timedelta(minutes=5),
            'min_threshold': 0.1,
            'max_threshold': 0.8,
            'metric_agg_type': None,
            'metric_agg_key': 'cpu_pct',
            'timestamp_field': '@timestamp'}

    # Valid aggregation types
    for agg_type in BaseAggregationRuleType.allowed_aggregations:
        conf['metric_agg_type'] = agg_type
        MetricAggregationRuleType(conf)

    with pytest.raises(ReactorException):
        conf['metric_agg_type'] = 'invalid-agg-type'
        MetricAggregationRuleType(conf)


def test_metric_aggregation():
    conf = {'buffer_time': datetime.timedelta(minutes=5),
            'min_threshold': 0.1,
            'max_threshold': 0.8,
            'metric_agg_type': 'avg',
            'metric_agg_key': 'cpu_pct',
            'timestamp_field': '@timestamp'}
    rule = MetricAggregationRuleType(conf)

    assert rule.conf['aggregation_query_element'] == {'metric_cpu_pct_avg': {'avg': {'field': 'cpu_pct'}}}

    assert rule.crossed_thresholds(None) is False
    assert rule.crossed_thresholds(0.09) is True
    assert rule.crossed_thresholds(0.10) is False
    assert rule.crossed_thresholds(0.79) is False
    assert rule.crossed_thresholds(0.81) is True

    rule.check_for_matches(gen_timestamp(), None, {'metric_cpu_pct_avg': {'value': None}})
    rule.check_for_matches(gen_timestamp(), None, {'metric_cpu_pct_avg': {'value': 0.5}})
    assert len(rule.matches) == 0

    rule.check_for_matches(gen_timestamp(), None, {'metric_cpu_pct_avg': {'value': 0.05}})
    assert len(rule.matches) == 1
    rule.check_for_matches(gen_timestamp(), None, {'metric_cpu_pct_avg': {'value': 0.95}})
    assert len(rule.matches) == 2


def test_metric_aggregation_with_qk():
    conf = {'buffer_time': datetime.timedelta(minutes=5),
            'min_threshold': 0.1,
            'max_threshold': 0.8,
            'metric_agg_type': 'avg',
            'metric_agg_key': 'cpu_pct',
            'query_key': 'qk',
            'timestamp_field': '@timestamp'}
    rule = MetricAggregationRuleType(conf)

    rule.check_for_matches(gen_timestamp(), 'qk_val', {'metric_cpu_pct_avg': {'value': 0.95}})
    assert len(rule.matches) == 1
    assert rule.matches[0][1]['qk'] == 'qk_val'


def test_metric_aggregation_with_complex_qk():
    conf = {'buffer_time': datetime.timedelta(minutes=5),
            'min_threshold': 0.1,
            'max_threshold': 0.8,
            'metric_agg_type': 'avg',
            'metric_agg_key': 'cpu_pct',
            'compound_query_key': ['qk', 'sub_qk'],
            'query_key': 'qk,sub_qk',
            'timestamp_field': '@timestamp'}
    rule = MetricAggregationRuleType(conf)

    query = gen_bucket_aggregation('bucket_aggs', [{'metric_cpu_pct_avg': {'value': 0.91}, 'key': 'sub_qk_val1'},
                                                   {'metric_cpu_pct_avg': {'value': 0.95}, 'key': 'sub_qk_val2'},
                                                   {'metric_cpu_pct_avg': {'value': 0.89}, 'key': 'sub_qk_val3'}],
                                   query_key='qk_val')

    rule.check_for_matches(gen_timestamp(), 'qk_val', query)
    assert len(rule.matches) == 3
    assert rule.matches[0][1]['qk'] == 'qk_val'
    assert rule.matches[1][1]['qk'] == 'qk_val'
    assert rule.matches[2][1]['qk'] == 'qk_val'
    assert rule.matches[0][1]['sub_qk'] == 'sub_qk_val1'
    assert rule.matches[1][1]['sub_qk'] == 'sub_qk_val2'
    assert rule.matches[2][1]['sub_qk'] == 'sub_qk_val3'


@pytest.mark.skip(reason='Test not yet implemented')
def test_spike_metric_aggregation():
    conf = {}
    rule = SpikeMetricAggregationRuleType(conf)


def test_percentage_match_constructor_percentage():
    conf = {'match_bucket_filter': {'term': 'term_val'},
            'buffer_time': datetime.timedelta(minutes=5),
            'timestamp_field': '@timestamp'}

    # No percentages in conf
    with pytest.raises(ReactorException):
        PercentageMatchRuleType(conf)

    # `max_percentage` only
    conf.pop('min_percentage', None)
    conf['max_percentage'] = 75
    PercentageMatchRuleType(conf)

    # `min_percentage` only
    conf['min_percentage'] = 25
    conf.pop('max_percentage', None)
    PercentageMatchRuleType(conf)


def test_percentage_match():
    conf = {'match_bucket_filter': {'term': 'term_val'},
            'buffer_time': datetime.timedelta(minutes=5),
            'min_percentage': 25,
            'max_percentage': 75,
            'timestamp_field': '@timestamp'}
    rule = PercentageMatchRuleType(conf)

    # Check `aggregation_query_element` is correct
    assert rule.conf['aggregation_query_element'] == {
        'percentage_match_aggs': {'filters': {'other_bucket': True,
                                              'filters': {'match_bucket': {'bool': {'must': {'term': 'term_val'}}}}}}
    }

    # Check boundaries for `percentage_violation`
    assert rule.percentage_violation(25) is False
    assert rule.percentage_violation(50) is False
    assert rule.percentage_violation(75) is False
    assert rule.percentage_violation(24.9) is True
    assert rule.percentage_violation(75.1) is True

    # Check `check_for_matches`
    rule.check_for_matches(gen_timestamp(), None, gen_percentage_match_agg(0, 0))
    rule.check_for_matches(gen_timestamp(), None, gen_percentage_match_agg(None, 100))
    rule.check_for_matches(gen_timestamp(), None, gen_percentage_match_agg(26, 74))
    rule.check_for_matches(gen_timestamp(), None, gen_percentage_match_agg(74, 26))
    assert len(rule.matches) == 0

    rule.check_for_matches(gen_timestamp(), None, gen_percentage_match_agg(24, 76))
    assert len(rule.matches) == 1
    rule.check_for_matches(gen_timestamp(), None, gen_percentage_match_agg(76, 24))
    assert len(rule.matches) == 2


def test_percentage_match_with_qk():
    conf = {'match_bucket_filter': {'term': 'term_val'},
            'buffer_time': datetime.timedelta(minutes=5),
            'min_percentage': 25,
            'max_percentage': 75,
            'query_key': 'qk',
            'timestamp_field': '@timestamp'}
    rule = PercentageMatchRuleType(conf)

    rule.check_for_matches(gen_timestamp(), 'qk_val', gen_percentage_match_agg(76.666666667, 24))
    assert len(rule.matches) == 1
    assert rule.matches[0][1]['qk'] == 'qk_val'
    assert '76.1589403974' in rule.get_match_str(*rule.matches[0])
    conf['percentage_format_string'] = '%.2f'
    assert '76.16' in rule.get_match_str(*rule.matches[0])

