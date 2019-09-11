import copy
import datetime
from typing import Generator

from reactor.exceptions import ReactorException, ConfigException
from reactor.util import (
    dt_to_ts, ts_to_dt, dt_now, pretty_ts, total_seconds,
    dots_get, hashable,
    reactor_logger,
    add_raw_postfix, format_index,
    ElasticSearchClient
)
from reactor.validator import yaml_schema, SetDefaultsDraft7Validator
from sortedcontainers import SortedKeyList


def get_ts_lambda(ts_field):
    """
    Constructs a lambda that may be called to extract the timestamp field from a given (event, count) pair.
    """
    return lambda pair: dots_get(pair[0], ts_field)


class EventWindow(object):
    """ A container to hold event counts for rules which need a chronological ordered event window. """

    def __init__(self, timeframe, get_timestamp=get_ts_lambda('@timestamp'), on_remove=None):
        self.timeframe = timeframe
        self.on_remove = on_remove
        self.get_ts = get_timestamp
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
            return datetime.timedelta(0)
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


class RuleType(object):
    rule_schema = None

    def __init__(self, conf: dict):
        self.num_matches = 0
        self.conf = conf
        self.occurrences = {}
        self.ts_field = self.conf.get('timestamp_field', '@timestamp')
        self.silences = {}

    def set_silence(self, key: str, until: datetime.datetime):
        """ Mark a silence cache key as silenced until """
        self.silences[key] = until

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        """ Prepare the RuleType for receiving data. Should be called before running a rule. """
        pass

    def add_match(self, extra: dict, event: dict) -> (dict, dict):
        """
        :param extra: Extra data about the triggered alert
        :param event: Event that triggered the rule match
        :return: Tuple of `extra` and a deep copy of `event`
        """
        event = copy.deepcopy(event)
        if self.ts_field in event:
            event[self.ts_field] = dt_to_ts(event[self.ts_field])

        self.num_matches += 1
        return extra, event

    def get_match_str(self, extra: dict, match: dict) -> str:
        return ''

    def garbage_collect(self, timestamp: datetime.datetime) -> Generator[dict, None, None]:
        yield from ()

    def merge_alert_body(self, orig_alert: dict, new_alert: dict):
        """ Merge `new_alert` into `orig_alert`. """
        orig_alert['match_data']['num_events'] += new_alert['match_data']['num_events']
        orig_alert['match_data']['began_at'] = min(ts_to_dt(orig_alert['match_data']['began_at']),
                                                   ts_to_dt(new_alert['match_data']['began_at']))
        orig_alert['match_data']['ended_at'] = max(ts_to_dt(orig_alert['match_data']['ended_at']),
                                                   ts_to_dt(new_alert['match_data']['ended_at']))


class AcceptsHitsDataMixin(object):
    """ A mixin class to denote that the RuleType accepts hits data. """
    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AcceptsCountDataMixin(object):
    """ A mixin class to denote that the RuleType accepts count data. """
    def add_count_data(self, counts) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AcceptsTermsDataMixin(object):
    """ A mixin class to denote that the RuleType accepts terms data. """
    def add_terms_data(self, counts) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AcceptsAggregationDataMixin(object):
    """ A mixin class to denote that the RuleType accepts aggregation data. """
    def add_aggregation_data(self, payload) -> Generator[dict, None, None]:
        raise NotImplementedError()


class AnyRuleType(RuleType, AcceptsHitsDataMixin):
    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-any.yaml', __file__)

    """ A rule that will match on any input data. """
    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self.conf.get('query_key', None)
        for event in data:
            extra = {'key': hashable(dots_get(event, qk)) if qk else 'all',
                     'num_events': 1,
                     'began_at': ts_to_dt(dots_get(event, self.ts_field)),
                     'ended_at': ts_to_dt(dots_get(event, self.ts_field))}
            yield self.add_match(extra, event)


class CompareRuleType(RuleType, AcceptsHitsDataMixin):
    """ A base class for matching a specific term by passing it to a compare function. """
    required_options = frozenset(['compound_compare_key'])

    def expand_entries(self, list_type: str):
        """
        Expand entries specified in files using the '!file' directive, if there are
        any, then add everything to a set.
        """
        entries_set = set()
        for entry in self.conf[list_type]:
            if entry.startswith('!file'):  # - "!file /path/to/list"
                filename = entry.split()[1]
                with open(filename, 'r') as f:
                    for line in f:
                        entries_set.add(line.rstrip())
            else:
                entries_set.add(entry)
        self.conf[list_type] = entries_set

    def compare(self, event: dict) -> bool:
        """ An event is a match if this returns true """
        raise NotImplementedError()

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        for event in data:
            if self.compare(event):
                yield self.add_match(*self.generate_match(event))

    def generate_match(self, event: dict) -> (dict, dict):
        raise NotImplementedError()


class BlacklistRuleType(CompareRuleType):
    """ A CompareRuleType where the compare function checks a given key against a blacklist. """
    required_options = frozenset(['compare_key', 'blacklist'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-blacklist.yaml', __file__)

    def __init__(self, conf: dict):
        super(BlacklistRuleType, self).__init__(conf)
        self.expand_entries('blacklist')

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        # Add the blacklist to the filter
        self.conf.setdefault('original_filter', self.conf['filter'])
        self.conf['filter'] = self.conf['original_filter']
        terms_query = {'terms': {self.conf['compare_key']: list(self.conf['blacklist'])}}
        if es_client.es_version_at_least(6):
            self.conf['filter'].append(terms_query)
        else:
            self.conf['filter'].append({'constant_score': {'filter': terms_query}})

    def compare(self, event: dict) -> bool:
        term = dots_get(event, self.conf['compare_key'])
        return term in self.conf['blacklist']

    def generate_match(self, event: dict) -> (dict, dict):
        extra = {'compare_key': self.conf['compare_key'],
                 'num_events': 1,
                 'began_at': dots_get(event, self.ts_field),
                 'ended_at': dots_get(event, self.ts_field)}
        return extra, event


class WhitelistRuleType(CompareRuleType):
    """ A CompareRuleType where the compare function checks a given key against a whitelist. """
    required_options = frozenset(['compare_key', 'whitelist', 'ignore_null'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-whitelist.yaml', __file__)

    def __init__(self, conf: dict):
        super(WhitelistRuleType, self).__init__(conf)
        self.expand_entries('whitelist')

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        # Add the whitelist to the filter
        self.conf.setdefault('original_filter', self.conf['filter'])
        self.conf['filter'] = self.conf['original_filter']
        terms_query = {'bool': {'must_not': {'terms': {self.conf['compare_key']: list(self.conf['whitelist'])}}}}
        if es_client.es_version_at_least(6):
            self.conf['filter'].append(terms_query)
        else:
            self.conf['filter'].append({'constant_score': {'filter': terms_query}})

    def compare(self, event: dict) -> bool:
        term = dots_get(event, self.conf['compare_key'])
        if term is None:
            return not self.conf['ignore_null']
        else:
            return term not in self.conf['whitelist']

    def generate_match(self, event: dict) -> (dict, dict):
        extra = {'compare_key': self.conf['compare_key'],
                 'num_events': 1,
                 'began_at': dots_get(event, self.ts_field),
                 'ended_at': dots_get(event, self.ts_field)}
        return extra, event


class ChangeRuleType(CompareRuleType):
    """ A rule that will store values for a certain term and match if those values change. """
    required_options = frozenset(['query_key', 'compound_compare_key', 'ignore_null'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-change.yaml', __file__)

    change_map = {}
    occurrence_time = {}

    def compare(self, event: dict) -> bool:
        key = hashable(dots_get(event, self.conf['query_key']))
        values = []
        reactor_logger.debug('Previous values of compare keys: %s', self.occurrences)
        for val in self.conf['compound_compare_key']:
            lookup_value = dots_get(event, val)
            values.append(lookup_value)
        reactor_logger.debug('Current values of compare keys: %s', values)

        changed = False
        for val in values:
            if not isinstance(val, bool) and not val and self.conf['ignore_null']:
                return False
        # If we have seen this key before, compare it to the new value
        if key in self.occurrences:
            for idx, previous_values in enumerate(self.occurrences[key]):
                reactor_logger.debug('%s  %s', previous_values, values[idx])
                changed = previous_values != values[idx]
                if changed:
                    break
            if changed:
                self.change_map[key] = (self.occurrences[key], values)
                # If using timeframe, only return if the time delta is < timeframe
                if key in self.occurrence_time:
                    changed = event[self.ts_field] - self.occurrence_time[key] <= self.conf['timeframe']

        # Update the current value and time
        reactor_logger.debug('Setting current value of compare keys values: %s', values)
        self.occurrences[key] = values
        if 'timeframe' in self.conf:
            self.occurrence_time[key] = event[self.ts_field]
        reactor_logger.debug('Final result of comparison between previous and current values: %r', changed)
        return changed

    def generate_match(self, event: dict) -> (dict, dict):
        # TODO: this is not technically correct
        # if the term changes multiple times before an alert is sent
        # this data will be overwritten with the most recent change
        change = self.change_map.get(hashable(dots_get(event, self.conf['query_key'])))
        extra = {}
        if change:
            extra = {'old_value': change[0],
                     'new_value': change[1],
                     'key': hashable(dots_get(event, self.conf['query_key'])),
                     'num_events': 1,
                     'began_at': dots_get(event, self.ts_field),
                     'ended_at': dots_get(event, self.ts_field)}
            reactor_logger.debug('Description of the changed records (%s): %s', extra, event)
        return extra, event


class FrequencyRuleType(RuleType, AcceptsHitsDataMixin, AcceptsCountDataMixin, AcceptsTermsDataMixin):
    """ A RuleType that matches if num_events number of events occur within a timeframe. """
    required_options = frozenset(['num_events', 'timeframe'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator,  'schemas/ruletype-frequency.yaml', __file__)

    def __init__(self, conf: dict):
        super(FrequencyRuleType, self).__init__(conf)
        self.get_ts = get_ts_lambda(self.ts_field)
        self.get_ts = get_ts_lambda(self.ts_field)
        self.attach_related = self.conf['attach_related']

    def check_for_match(self, key, end=False):
        # Match if, after removing old events, we hit num_events.
        # the 'end' parameter depends on whether this was called from the
        # middle or end of an add_data call and is used in sub classes
        if self.occurrences[key].count() >= self.conf['num_events']:
            # Check if the occurrences are split across a silence
            e = self.occurrences[key].data[0][0]
            silence_cache_key = dots_get(e, self.conf.get('query_key'), '_missing') if self.conf.get('query_key') else '_silence'
            if silence_cache_key in self.silences:
                if ts_to_dt(self.get_ts(self.occurrences[key].data[0])) <= self.silences[silence_cache_key] < ts_to_dt(self.get_ts(self.occurrences[key].data[-1])):
                    while self.get_ts(self.occurrences[key].data[0]) <= self.silences[silence_cache_key]:
                        self.occurrences[key].data.pop(0)
                    return

            extra = {'key': key, 'num_events': self.occurrences[key].count(),
                     'began_at': self.get_ts(self.occurrences[key].data[0]),
                     'ended_at': self.get_ts(self.occurrences[key].data[-1])}
            event = self.occurrences[key].data[-1][0]
            if self.attach_related:
                event['related_events'] = [data[0] for data in self.occurrences[key].data[:-1]]
            self.occurrences.pop(key)
            yield self.add_match(extra, event)

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self.conf.get('query_key', None)
        keys = set()
        for event in data:
            # If no query_key, we use the key 'all' for all events
            key = hashable(dots_get(event, qk)) if qk else 'all'

            # Store the timestamps of recent occurrences, per key
            self.occurrences.setdefault(key, EventWindow(self.conf['timeframe'], self.get_ts)).append((event, 1))
            yield from self.check_for_match(key, end=False)
            keys.add(key)

            # TODO: will potentially need to accept that any solution for now will not be able to easily handle historic
            #  data added after future alerts have been fired, or that it requires data to come in order

        # We call this multiple times with the 'end' parameter because subclasses
        # may or may not want to check while only partial data has been added
        for key in keys:
            if key in self.occurrences:
                yield from self.check_for_match(key, end=True)

    def get_match_str(self, extra: dict, match: dict) -> str:
        lt = self.conf['use_local_time']
        match_ts = dots_get(match, self.ts_field)
        start_time = pretty_ts(ts_to_dt(match_ts) - self.conf['timeframe'], lt)
        end_time = pretty_ts(match_ts, lt)
        return 'At least %d events occurred between %s and %s\n\n' % (self.conf['num_events'], start_time, end_time)

    def garbage_collect(self, timestamp: datetime.datetime) -> Generator[dict, None, None]:
        """ Remove all occurrence data that is beyond the timeframe away. """
        stale_keys = []
        for key, window in self.occurrences.items():
            if timestamp - dots_get(window.data[-1][0], self.ts_field) > self.conf['timeframe']:
                stale_keys.append(key)
        list(map(self.occurrences.pop, stale_keys))
        yield from ()

    def add_count_data(self, counts) -> Generator[dict, None, None]:
        """ Add count data to the rule. Data should be of the form {ts: count}. """
        if len(counts) > 1:
            raise ReactorException('add_count_data can only accept one count at a time')
        (ts, count), = list(counts.items())

        event = ({self.ts_field: ts}, count)
        self.occurrences.setdefault('all', EventWindow(self.conf['timeframe'], self.get_ts)).append(event)
        yield from self.check_for_match('all')

    def add_terms_data(self, terms) -> Generator[dict, None, None]:
        for timestamp, buckets in terms.items():
            for bucket in buckets:
                event = ({self.ts_field: timestamp,
                          self.conf['query_key']: bucket['key']}, bucket['doc_count'])
                self.occurrences.setdefault(bucket['key'],
                                            EventWindow(self.conf['timeframe'], self.get_ts)).append(event)
                yield from self.check_for_match(bucket['key'])


class FlatlineRuleType(FrequencyRuleType):
    """ A FrequencyRuleType that matches when there is low number of events within a timeframe. """
    required_options = frozenset(['timeframe', 'threshold'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-flatline.yaml', __file__)

    def __init__(self, conf: dict):
        super(FlatlineRuleType, self).__init__(conf)
        self.threshold = self.conf['threshold']

        # Dictionary mapping query keys to the first event observed
        self.first_event = {}

    def check_for_match(self, key, end=True):
        # This function gets called between every added document with end=True after the last
        # We ignore the calls before the end because it may trigger false positives
        if not end:
            return

        most_recent_ts = self.get_ts(self.occurrences[key].data[-1])
        if self.first_event.get(key) is None:
            self.first_event[key] = most_recent_ts

        # Don't check for matches until timeframe has elapsed
        if most_recent_ts - self.first_event[key] < self.conf['timeframe']:
            return

        # Match if, after removing old events, we hit num_events
        count = self.occurrences[key].count()
        if count < self.conf['threshold']:
            # Do a deep-copy, otherwise we lose the datetime type in the timestamp field of the last event
            extra = {'key': key,
                     'count': count,
                     'num_events': count,
                     'began_at': self.first_event[key],
                     'ended_at': most_recent_ts}
            event = copy.deepcopy(self.occurrences[key].data[-1][0])
            yield self.add_match(extra, event)

            if not self.conf['forget_keys']:
                # After adding this match, leave the occurrences window alone since it will
                # be pruned in the next add_data or garbage_collect, but reset the first_event
                # so that alerts continue to fire until the threshold is passed again.
                least_recent_ts = self.get_ts(self.occurrences[key].data[0])
                timeframe_ago = most_recent_ts - self.conf['timeframe']
                self.first_event[key] = min(least_recent_ts, timeframe_ago)
            else:
                # Forget about this key until we see it again
                self.first_event.pop(key)
                self.occurrences.pop(key)

    def get_match_str(self, extra: dict, match: dict) -> str:
        ts = match[self.ts_field]
        lt = self.conf.get('use_local_time')
        message = 'An abnormally low number of events occurred around %s.\n' % pretty_ts(ts, lt)
        message += 'Between %s and %s, there were less than %s events.\n\n' % (
            pretty_ts(ts_to_dt(ts) - self.conf['timeframe'], lt),
            pretty_ts(ts, lt),
            self.conf['threshold']
        )
        return message

    def garbage_collect(self, timestamp: datetime.datetime) -> Generator[dict, None, None]:
        # We add an event with a count of zero to the EventWindow for each key. This will cause the EventWindow
        # to remove events that occurred more than one timeframe ago, and call on_remove on them.
        default = ['all'] if 'query_key' not in self.conf else []
        for key in list(self.occurrences.keys()) or default:
            event = ({self.ts_field: timestamp}, 0)
            self.occurrences.setdefault(key, EventWindow(self.conf['timeframe'], self.get_ts)).append(event)
            self.first_event.setdefault(key, timestamp)
            yield from self.check_for_match(key, end=True)


class SpikeRuleType(RuleType, AcceptsHitsDataMixin, AcceptsCountDataMixin, AcceptsTermsDataMixin):
    """ A RuleType that uses two sliding windows to compare relative event frequency. """
    required_options = frozenset(['timeframe', 'spike_height', 'spike_type'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-spike.yaml', __file__)

    def __init__(self, *args):
        super(SpikeRuleType, self).__init__(*args)
        self.timeframe = self.conf['timeframe']

        self.ref_windows = {}
        self.cur_windows = {}

        self.get_ts = get_ts_lambda(self.ts_field)
        self.first_event = {}
        self.skip_checks = {}

        self.field_value = self.conf.get('field_value')

        self.ref_window_filled_once = False

    def clear_windows(self, qk, event):
        """ Reset the state and prevent alerts until windows are filled again """
        self.ref_windows[qk].clear()
        self.first_event.pop(qk)
        self.skip_checks[qk] = dots_get(event, self.ts_field) + self.conf['timeframe'] * 2

    def handle_event(self, event: dict, count: int, qk: str = 'all'):
        self.first_event.setdefault(qk, event)

        self.ref_windows.setdefault(qk, EventWindow(self.timeframe, self.get_ts))
        self.cur_windows.setdefault(qk, EventWindow(self.timeframe, self.get_ts, self.ref_windows[qk].append))

        self.cur_windows[qk].append((event, count))

        # Don't alert if ref window has not yet been filled for this key AND
        if dots_get(event, self.ts_field) - self.first_event[qk][self.ts_field] < self.conf['timeframe'] * 2:
            # Reactor has not been running long enough for any alerts OR
            if not self.ref_window_filled_once:
                return
            # This rule is not using alert_on_new_data (with query_key) OR
            if not (self.conf.get('query_key') and self.conf.get('alert_on_new_data')):
                return
            # An alert for this qk has recently fired
            if qk in self.skip_checks and dots_get(event, self.ts_field) < self.skip_checks[qk]:
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
            if cur < self.conf.get('threshold_cur', 0) or ref < self.conf.get('threshold_ref', 0):
                return False
        elif ref is None or ref == 0 or cur is None or cur == 0:
            return False

        spike_dn = cur <= (ref / self.conf['spike_height'])
        spike_up = cur >= (ref * self.conf['spike_height'])

        return (spike_up and self.conf['spike_type'] in ['both', 'up']) or \
               (spike_dn and self.conf['spike_type'] in ['both', 'down'])

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self.conf.get('query_key', None)
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
        """ Generate a SpikeRuleType event. """
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
        ts_str = pretty_ts(match[self.ts_field], self.conf['use_local_time'])
        timeframe = self.conf['timeframe']
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

    def garbage_collect(self, timestamp: datetime.datetime) -> Generator[dict, None, None]:
        # Windows are sized according to their newest event
        # This is a placeholder to accurately size windows in the absence of events
        for qk in self.cur_windows.keys():
            if qk != 'all' and self.ref_windows[qk].count() == 0 and self.cur_windows[qk].count() == 0:
                self.cur_windows.pop(qk)
                self.ref_windows.pop(qk)
                continue
            placeholder = {self.ts_field: timestamp, "placeholder": True}
            # The placeholder may trigger an alert, in which case, qk will be expected
            if qk != 'all':
                placeholder.update({self.conf['query_key']: qk})
            yield from self.handle_event(placeholder, 0, qk)

    def add_count_data(self, counts) -> Generator[dict, None, None]:
        """ Add count data to the rule. Data should be of the form {ts: count}. """
        if len(counts) > 1:
            raise ReactorException('SpikeRuleType.add_count_data can only accept one count at a time')
        for ts, count in counts.items():
            yield from self.handle_event({self.ts_field: ts}, count, 'all')

    def add_terms_data(self, terms) -> Generator[dict, None, None]:
        for timestamp, buckets in terms.items():
            for bucket in buckets:
                count = bucket['doc_count']
                event = {self.ts_field: timestamp,
                         self.conf['query_key']: bucket['key']}
                key = bucket['key']
                yield from self.handle_event(event, count, key)


class NewTermRuleType(RuleType, AcceptsHitsDataMixin, AcceptsTermsDataMixin):
    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-new_term.yaml', __file__)

    """ A RuleType that detects a new value in a list of fields. """
    # TODO: alter self.seen_values to be a mapping of value to timestamp of last seen - add option to forget old terms
    #  outside timeframe
    def __init__(self, conf: dict):
        super(NewTermRuleType, self).__init__(conf)
        self.seen_values = {}
        # Allow the use of query_key or fields
        if 'fields' not in self.conf and 'query_key' not in self.conf:
            raise ConfigException('NewTermRuleType fields or query_key must be specified')
        self.fields = self.conf.get('query_key', self.conf.get('fields'))

        if not self.fields:
            raise ConfigException('NewTermRuleType requires fields or query_key to not be empty')
        if type(self.fields) != list:
            self.fields = [self.fields]
        if self.conf.get('use_terms_query') and (len(self.fields) != 1 or type(self.fields[0]) == list):
            raise ConfigException('use_terms_query can only be used with a single non-composite field')
        if self.conf.get('use_terms_query'):
            if [self.conf['query_key']] != self.fields:
                raise ConfigException('If use_terms_query is specified, you cannot specify different query_key and fields')
            if not self.conf.get('query_key').endswith('.keyword') and not self.conf.get('query_key').endswith('.raw'):
                if self.conf.get('use_keyword_postfix', True):
                    reactor_logger.warning('If query_key is a non-keyword field, you must set '
                                           'use_keyword_postfix to false, or add .keyword/.raw to your query_key')

    def prepare(self, es_client: ElasticSearchClient, start_time: str = None) -> None:
        """ Performs a terms aggregation for each field to get every existing term. """
        # Check if already prepared
        if self.seen_values:
            return

        # Get the version of ElasticSearch
        es_version = es_client.info()['version']['number'].split('.')

        window_size = datetime.timedelta(**self.conf.get('terms_window_size', {'days': 30}))
        field_name = {'field': '', 'size': 2**31 - 1}  # Maximum 32 bit integer value
        query_template = {'aggs': {'values': {'terms': field_name}}}  # type: dict
        if start_time:
            end = ts_to_dt(start_time)
        elif 'start_date' in self.conf:
            end = ts_to_dt(self.conf['start_date'])
        else:
            end = dt_now()
        start = end - window_size
        step = datetime.timedelta(**self.conf.get('window_step_size', {'days': 1}))

        for field in self.fields:
            tmp_start = start
            tmp_end = min(start + step, end)
            time_filter = {self.ts_field: {'lt': dt_to_ts(tmp_end),
                                           'gte': dt_to_ts(tmp_start)}}
            query_template['filter'] = {'bool': {'must': [{'range': time_filter}]}}
            query = {'aggs': {'filtered': query_template}}
            if 'filter' in self.conf:
                for item in self.conf['filter']:
                    query_template['filter']['bool']['must'].append(item)

            # For composite keys, we will need to perform sub-aggregations
            if type(field) == list:
                self.seen_values.setdefault(tuple(field), set())
                level = query_template['aggs']
                # Iterate on each part of the composite key and add a sub-aggs clause to the elasticsearch query
                for i, sub_field in enumerate(field):
                    if self.conf.get('use_keyword_postfix', True):
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
                if self.conf.get('use_keyword_postfix', True):
                    field_name['field'] = add_raw_postfix(field, es_version)
                else:
                    field_name['field'] = field

            # Query the entire time range in small chunks
            while tmp_start < end:
                if self.conf.get('use_strftime_index'):
                    index = format_index(self.conf['index'], tmp_start, tmp_end)
                else:
                    index = self.conf['index']
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
                time_filter[self.ts_field] = {'lt': dt_to_ts(tmp_end),
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
                if not value and self.conf.get('alert_on_missing_field'):
                    yield self.add_match(*self.generate_match(lookup_field, None, event=event))
                elif value:
                    if value not in self.seen_values[lookup_field]:
                        yield self.add_match(*self.generate_match(lookup_field, value, event=event))

    def generate_match(self, field, value, event: dict = None, timestamp=None) -> (dict, dict):
        """ Generate a match and, if there is a value, store in `self.seen_values[field]`. """
        event = event or {field: value, self.ts_field: timestamp}
        event = copy.deepcopy(event)
        qk = self.conf.get('query_key', None)
        extra = {'key': hashable(dots_get(event, qk)) if qk else 'all',
                 'num_events': 1,
                 'began_at': dots_get(event, self.ts_field),
                 'ended_at': dots_get(event, self.ts_field)}
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


class CardinalityRuleType(RuleType, AcceptsHitsDataMixin):
    """ A RuleType that matches if cardinality of a field is above or below a threshold within a timeframe. """
    required_options = frozenset(['timeframe', 'cardinality_field'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-cardinality.yaml', __file__)

    def __init__(self, conf: dict):
        super(CardinalityRuleType, self).__init__(conf)
        if 'max_cardinality' not in self.conf and 'min_cardinality' not in self.conf:
            raise ConfigException('CardinalityRuleType must have one of either max_cardinality or min_cardinality')
        self.cardinality_field = self.conf['cardinality_field']
        self.cardinality_cache = {}
        self.first_event = {}
        self.timeframe = self.conf['timeframe']

    def add_hits_data(self, data: list) -> Generator[dict, None, None]:
        qk = self.conf.get('query_key', None)
        for event in data:
            # If no query_key, we use the key 'all' for all events
            key = hashable(dots_get(event, qk)) if qk else 'all'
            self.cardinality_cache.setdefault(key, {})
            self.first_event.setdefault(key, dots_get(event, self.ts_field))
            value = hashable(dots_get(event, self.cardinality_field))
            if value is not None:
                # Store this timestamp as most recent occurrence of the term
                self.cardinality_cache[key][value] = dots_get(event, self.ts_field)
                yield from self.check_for_match(key, event)

    def check_for_match(self, key, event, garbage_collect=True):
        time_elapsed = dots_get(event, self.ts_field) - self.first_event.get(key, dots_get(event, self.ts_field))
        timeframe_elapsed = time_elapsed > self.timeframe
        if (len(self.cardinality_cache[key]) > self.conf.get('max_cardinality', float('inf')) or
                (len(self.cardinality_cache[key]) < self.conf.get('min_cardinality', 0.0) and timeframe_elapsed)):
            # If there might be a match, run garbage collect first to remove outdated terms
            # Only run it if there might be a match so it doesn't impact performance
            if garbage_collect:
                yield from self.garbage_collect(dots_get(event, self.ts_field))
                yield from self.check_for_match(key, event, False)
            else:
                self.first_event.pop(key, None)
                extra = {'cardinality': self.cardinality_cache[key],
                         'key': key,
                         'num_events': len(self.cardinality_cache[key]),
                         'began_at': self.first_event.get(key, dots_get(event, self.ts_field)),
                         'ended_at': dots_get(event, self.ts_field)}
                yield self.add_match(extra, event)

    def get_match_str(self, extra: dict, match: dict) -> str:
        lt = self.conf.get('use_local_time')
        began_time = pretty_ts(ts_to_dt(dots_get(match, self.ts_field)) - self.conf['timeframe'], lt)
        ended_time = pretty_ts(dots_get(match, self.ts_field), lt)
        if 'max_cardinality' in self.conf:
            return 'A maximum of %d unique %s(s) occurred since last alert or between %s and %s\n\n' % (
                self.conf['max_cardinality'],
                self.cardinality_field,
                began_time,
                ended_time)
        else:
            return 'Less than  %d unique %s(s) occurred since last alert or between %s and %s\n\n' % (
                self.conf['max_cardinality'],
                self.cardinality_field,
                began_time,
                ended_time)

    def garbage_collect(self, timestamp: datetime.datetime) -> Generator[dict, None, None]:
        """ Remove all occurrence data that is beyond the timeframe away. """
        stale_terms = []
        for qk, terms in self.cardinality_cache.items():
            for term, last_occurrence in terms.items():
                if timestamp - last_occurrence > self.conf['timeframe']:
                    stale_terms.append((qk, term))

        for qk, term in stale_terms:
            self.cardinality_cache[qk].pop(term)

            # Create a placeholder event for min_cardinality match occurred
            if 'min_cardinality' in self.conf:
                event = {self.ts_field: timestamp}
                if 'query_key' in self.conf:
                    event.update({self.conf['query_key']: qk})
                yield from self.check_for_match(qk, event, False)


class BaseAggregationRuleType(RuleType, AcceptsAggregationDataMixin):
    allowed_aggregations = frozenset(['min', 'max', 'avg', 'sum', 'cardinality', 'value_count'])

    def __init__(self, conf: dict):
        super(BaseAggregationRuleType, self).__init__(conf)
        bucket_interval = self.conf.get('bucket_interval')
        if bucket_interval:
            seconds = total_seconds(bucket_interval)
            if seconds % (60 * 60 * 24 * 7) == 0:
                self.conf['bucket_interval_period'] = str(int(seconds) // (60 * 60 * 24 * 7)) + 'w'
            elif seconds % (60 * 60 * 24) == 0:
                self.conf['bucket_interval_period'] = str(int(seconds) // (60 * 60 * 24)) + 'd'
            elif seconds % (60 * 60) == 0:
                self.conf['bucket_interval_period'] = str(int(seconds) // (60 * 60)) + 'h'
            elif seconds % 60 == 0:
                self.conf['bucket_interval_period'] = str(int(seconds) // 60) + 'm'
            elif seconds % 1 == 0:
                self.conf['bucket_interval_period'] = str(int(seconds)) + 's'
            else:
                raise ConfigException('Unsupported window size')

            if self.conf.get('use_run_every_query_size'):
                if total_seconds(self.conf['run_every']) % total_seconds(self.conf['bucket_interval']) != 0:
                    raise ConfigException('run_every must be evenly divisible by bucket_interval if specified')
            else:
                if total_seconds(self.conf['buffer_time']) % total_seconds(self.conf['bucket_interval']) != 0:
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


class MetricAggregationRuleType(BaseAggregationRuleType):
    """ A RuleType that matches when there is a low number of events within a timeframe. """

    required_options = frozenset(['metric_agg_key', 'metric_agg_type'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator,  'schemas/ruletype-metric_aggregation.yaml', __file__)

    def __init__(self, conf: dict):
        super(MetricAggregationRuleType, self).__init__(conf)
        if 'max_threshold' not in self.conf and 'min_threshold' not in self.conf:
            raise ConfigException('MetricAggregationRuleType must have one of either max_threshold or min_threshold')
        if self.conf['metric_agg_type'] not in self.allowed_aggregations:
            raise ConfigException('metric_agg_type must be one of %s' % str(self.allowed_aggregations))

        self.metric_key = 'metric_%s_%s' % (self.conf['metric_agg_key'], self.conf['metric_agg_type'])
        self.conf['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, extra: dict, match: dict) -> str:
        return 'Threshold violation, %s:%s %s (min: %s, max: %s)\n\n' % (
            self.conf['metric_agg_type'],
            self.conf['metric_agg_key'],
            match[self.metric_key],
            self.conf.get('min_threshold'),
            self.conf.get('max_threshold'),
        )

    def generate_aggregation_query(self):
        return {self.metric_key: {self.conf['metric_agg_type']: {'field': self.conf['metric_agg_key']}}}

    def check_for_matches(self, timestamp, query_key, aggregation_data):
        if 'compound_query_key' in self.conf:
            yield from self.check_matches_recursive(timestamp, query_key, aggregation_data, self.conf['compound_query_key'], {})
        else:
            metric_val = aggregation_data[self.metric_key]['value']
            if self.crossed_thresholds(metric_val):
                match = {self.ts_field: timestamp,
                         self.metric_key: metric_val}
                if query_key is not None:
                    match[self.conf['query_key']] = query_key
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
                match_data[self.ts_field] = timestamp
                match_data[self.metric_key] = metric_val

                # Add compound key to payload to allow alerts to trigger for every unique occurrence
                compound_value = [match_data[key] for key in self.conf['compound_query_key']]
                match_data[self.conf['query_key']] = ','.join(compound_value)

                extra = {'num_events': 1,
                         'began_at': timestamp,
                         'ended_at': timestamp}
                yield self.add_match(extra, match_data)

    def crossed_thresholds(self, metric_value):
        if metric_value is None:
            return False
        if 'max_threshold' in self.conf and metric_value > self.conf['max_threshold']:
            return True
        if 'min_threshold' in self.conf and metric_value < self.conf['min_threshold']:
            return True
        return False


class SpikeMetricAggregationRuleType(BaseAggregationRuleType, SpikeRuleType):
    """ A rule that matches when there is a spike in an aggregated event compared to its reference window, """

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-spike_metric_aggregation.yaml', __file__)

    def __init__(self, conf: dict):
        super(SpikeMetricAggregationRuleType, self).__init__(conf)

        # Metric aggregation alert things
        self.metric_key = 'metric_%s_%s' % (self.conf['metric_agg_key'], self.conf['metric_agg_type'])
        if self.conf['metric_agg_type'] not in self.allowed_aggregations:
            raise ConfigException('metric_agg_type must be one of %s' % str(self.allowed_aggregations))

        # Disabling bucket intervals (doesn't make sense in context of spike to split up your time period)
        if self.conf.get('bucket_interval'):
            raise ConfigException('bucket_interval not supported for spike metric aggregations')

        self.conf['aggregation_query_element'] = self.generate_aggregation_query()

    def generate_aggregation_query(self):
        """ Lifted from MetricAggregation, added support for scripted fields. """
        if self.conf.get('metric_agg_script'):
            return {self.metric_key: {self.conf['metric_agg_type']: self.conf['metric_agg_script']}}
        else:
            return {self.metric_key: {self.conf['metric_agg_type']: {'field': self.conf['metric_agg_key']}}}

    def add_aggregation_data(self, payload) -> Generator[dict, None, None]:
        """
        BaseAggregationRuleType.add_aggregation_data unpacks our results and runs checks directly against hardcoded
        cutoffs.
        We, instead, want to use all of our SpikeRuleType.handle_event inherited logic (current/reference) from
        the aggregation's `value` key to determine spikes from aggregations.
        """
        for timestamp, payload_data in payload.items():
            if 'bucket_aggs' in payload_data:
                yield from self.unwrap_term_buckets(timestamp, payload_data['bucket_aggs'])
            else:
                # no time / term split, just focus on aggregations
                event = {self.ts_field: timestamp}
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
            event = {self.ts_field: timestamp,
                     self.conf['query_key']: qk_str}
            # Pass to SpikeRuleType's tracker
            yield from self.handle_event(event, agg_value, qk_str)

            # Handle unpack of lowest level
            del qk[-1]

    def get_match_str(self, extra: dict, match: dict) -> str:
        message = 'An abnormal %s of %s (%s) occurred around %s.\n' % (
            self.conf['metric_agg_type'], self.conf['metric_agg_key'], round(extra['spike_count']),
            pretty_ts(dots_get(match, self.ts_field), self.conf['use_local_time']))
        message += 'Preceding that time, there was %s of %s of (%s) within %s\n\n' % (
            self.conf['metric_agg_type'], self.conf['metric_agg_key'],
            round(extra['reference_count']), self.conf['timeframe'])
        return message

    def check_for_matches(self, timestamp, query_key, aggregation_data):
        raise Exception('Method not used by SpikeMetricAggregationRuleType')


class PercentageMatchRuleType(BaseAggregationRuleType):
    required_options = frozenset(['match_bucket_filter'])

    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype-percentage_match.yaml', __file__)

    def __init__(self, conf: dict):
        super(PercentageMatchRuleType, self).__init__(conf)
        if all([f not in self.conf for f in ['max_percentage', 'min_percentage']]):
            raise ConfigException('PercentageMatchRuleType must have one of either min_percentage or max_percentage')

        self.min_denominator = self.conf.get('min_denominator', 0)
        self.match_bucket_filter = self.conf['match_bucket_filter']
        self.conf['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, extra: dict, match: dict) -> str:
        percentage_format_string = self.conf.get('percentage_format_string', '%s')
        return 'Percentage violation, value: %s (min: %s, max: %s) of %s items.\n\n' % (
            (percentage_format_string % extra['percentage']),
            self.conf.get('min_percentage'),
            self.conf.get('max_percentage'),
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
                    event = {self.ts_field: timestamp}
                    if query_key is not None:
                        event[self.conf['query_key']] = query_key
                    yield self.add_match(extra, event)

    def percentage_violation(self, match_percentage):
        if 'max_percentage' in self.conf and match_percentage > self.conf['max_percentage']:
            return True
        if 'min_percentage' in self.conf and match_percentage < self.conf['min_percentage']:
            return True
        return False
