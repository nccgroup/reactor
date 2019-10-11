import collections
import datetime
import logging
import os
import re
import sys
import time
import uuid

import dateutil
import dateutil.parser
import elasticsearch
import pytz
import yaml

import reactor.auth
from .exceptions import ReactorException

reactor_logger = logging.getLogger('reactor')
reactor_logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
ch.setFormatter(formatter)
reactor_logger.addHandler(ch)


class ElasticSearchClient(elasticsearch.Elasticsearch):
    """ Extension of low level :class:`elasticsearch.ElasticSearch` client with additional version resolving. """

    def __init__(self, *args, **kwargs):
        super(ElasticSearchClient, self).__init__(*args, **kwargs)
        self._es_version = None

    @property
    def es_version(self) -> tuple:
        if self._es_version is None:
            self._es_version = tuple(int(_) for _ in self.info()['version']['number'].split('.'))
        return self._es_version

    def es_version_at_least(self, major: int, minor: int = 0, patch: int = 0) -> bool:
        """ Check whether a semantic version is at least the specified version. """
        return semantic_at_least(self.es_version, major, minor, patch)

    def wait_until_responsive(self, patience: datetime.timedelta):
        """ Wait until ElasticSearch becomes responsive (or too much time passes). """
        patience = patience.total_seconds()

        # Don't poll unless we're asked to
        if patience <= 0.0:
            return True

        # Increase logging level
        cur_level = logging.getLogger("elasticsearch").level
        try:
            logging.getLogger("elasticsearch").setLevel(max(logging.ERROR, cur_level))

            # Periodically poll ElasticSearch. Keep going until ElasticSearch is responds
            # or our patience runs out
            ref = time.time()
            while (time.time() - ref) < patience:
                try:
                    if self.ping():
                        return True
                except elasticsearch.ConnectionError:
                    pass
                time.sleep(1.0)
            reactor_logger.error('Could not reach ElasticSearch at %s', self.transport.hosts)
            return False
        finally:
            logging.getLogger("elasticsearch").setLevel(cur_level)


def semantic_at_least(version: tuple, major: int, minor: int = 0, patch: int = 0) -> bool:
    """ Check whether a semantic version is at least the specified version. """

    return int(version[0]) > major or \
        (int(version[0]) == major and int(version[1]) > minor) or \
        (int(version[0]) == major and int(version[1]) == minor and int(version[2]) >= patch)


def parse_duration(value: str) -> datetime.timedelta:
    """ Convert ``units=val`` spec into a ``timedelta`` object. """
    units, val = value.split('=')
    return datetime.timedelta(**{units: int(val)})


def parse_timestamp(value: str, ts_format: str = '%Y-%m-%dT%H:%M:%S') -> datetime.datetime:
    """ Convert ``YYYY-MM-DDTHH:MM:SS`` str into a ``datetime`` object. """
    timestamp = datetime.datetime.strptime(value, ts_format)
    # If no timezone information provided, assume local timezone
    if not timestamp.tzinfo:
        timestamp = timestamp.astimezone()
    return timestamp


class RangeChoice(object):
    def __init__(self, min_val, max_val):
        self.min = min_val
        self.max = max_val

    def __contains__(self, item):
        return self.min <= item <= self.max

    def __iter__(self):
        yield str(self)

    def __str__(self):
        return '%s..%s' % (self.min, self.max)

    def __repr__(self):
        return '%s..%s' % (self.min, self.max)


def import_class(class_str: str, mappings: dict = None, module=None) -> type:
    """ Based on the class_str return the corresponding class type. """
    if isinstance(class_str, type):
        return class_str

    if module and hasattr(module, class_str):
        return getattr(module, class_str)

    if mappings and class_str in mappings:
        return import_class(mappings[class_str])

    try:
        module_path, module_class = class_str.rsplit('.', 1)
        base_module = __import__(module_path, globals(), locals(), [module_class])
        return getattr(base_module, module_class)
    except (ImportError, AttributeError, ValueError) as e:
        raise ReactorException('Could not import module %s: %s' % (class_str, e))


yaml_loader = None
env_matcher = re.compile(r'\${([a-zA-Z_$][a-zA-Z_$0-9]*)(:[^}]+)?}')


def load_yaml(filename: str) -> dict:
    global yaml_loader
    if not yaml_loader:
        try:
            yaml_loader = yaml.CFullLoader
        except AttributeError:
            yaml_loader = yaml.FullLoader
        yaml_loader.add_implicit_resolver(r'!env', env_matcher, None)
        yaml_loader.add_constructor(r'!env', env_constructor)

    with open(filename) as fh:
        return yaml.load(fh, yaml_loader)


def env_constructor(loader: yaml.BaseLoader, node: yaml.nodes.Node) -> any:
    """
    Extract the matched value, expand env variable, and replace the match.
    https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html
    """
    value = loader.construct_scalar(node)
    match = env_matcher.match(value)
    env_name = match.group(1)
    env_default = None
    if match.group(2) is not None:
        env_default = match.group(2)[1:]
        # Detect the type of the env default and construct the object
        tag = loader.resolve(yaml.nodes.ScalarNode, env_default, (True, False))
        default_node = yaml.nodes.ScalarNode(tag, env_default)
        env_default = loader.construct_object(default_node)
        # If this value is quoted with single or double quotes
        if isinstance(env_default, str) and env_default[0] == env_default[-1] and env_default[0] in ['"', "'"]:
            env_default = env_default[1:-1]

    return os.environ.get(env_name, env_default)


def generate_id() -> str:
    """ Generate a UUID4 string and corresponding urlsafe base64 encoded version. """
    return str(uuid.uuid4())


def dots_set(dots_dict: dict, key: str, value) -> bool:
    """
    Looks up the location that the term maps to and sets it to the given value.
    :return: True if the value was set, False otherwise
    :rtype: bool
    """
    value_dict, value_key = _dots_lookup(dots_dict, key)

    if value_dict is not None:
        value_dict[value_key] = value
    else:
        cursor_dict = dots_dict
        sub_keys = key.split('.')
        for sub_key in sub_keys[:-1]:
            cursor_dict[sub_key] = {} if sub_key not in cursor_dict else cursor_dict[sub_key]
            cursor_dict = cursor_dict[sub_key]
        cursor_dict[sub_keys[-1]] = value

    return True


def dots_set_default(dots_dict: dict, key: str, value) -> bool:
    """
    Looks up the location that the term maps to and sets it to the given value.
    :return: True if the value was set, False otherwise
    :rtype: bool
    """
    value_dict, value_key = _dots_lookup(dots_dict, key)

    if value_dict is not None:
        value_dict.setdefault(value_key, value)
    else:
        cursor_dict = dots_dict
        sub_keys = key.split('.')
        for sub_key in sub_keys[:-1]:
            cursor_dict[sub_key] = {} if sub_key not in cursor_dict else cursor_dict[sub_key]
            cursor_dict = cursor_dict[sub_key]
        cursor_dict.setdefault(sub_keys[-1], value)
    return True


def dots_get(dots_dict: dict, key: str, default: any = None) -> any:
    """
    Performs iterative dictionary search for the given key.
    :return: The value identified by the key or `default` if it cannot be found
    """
    value_dict, value_key = _dots_lookup(dots_dict, key)
    return default if value_key is None else value_dict[value_key]


def dots_del(dots_dict: dict, key: str) -> None:
    """ Deletes the dot-notation specified key from the dict. """
    dict_cursor = dots_dict
    keys = key.split('.')
    while len(keys) > 1:
        dict_cursor = dict_cursor[keys.pop(0)]
    del dict_cursor[keys.pop(0)]


def dots_has(dots_dict: dict, key: str) -> bool:
    """
    Performs iterative dictionary search for the given key.
    :return: True if the dots_dict contains the key
    """
    value_dict, value_key = _dots_lookup(dots_dict, key)
    return value_key is not None


def _dots_lookup(lookup_dict: dict, key: str) -> (dict, str):
    """
    Performs iterative dictionary search based upon the following conditions:

    1.  Sub keys may either appear behind a full stop (.) or at one lookup_dict level lower in the tree
    2.  No wildcards exist within the provided key (`key` is treated as a string literal)

    This is necessary to get around inconsistencies in ES data.

    For example:
      {'ad.account_name': 'bob'}
    Or:
      {'csp_report': {'blocked_uri': 'bob.com'}}
    And even:
      {'juniper_duo.geoip': {'country_name': 'Democratic People\'s Republic of Korea'}}

    We want a search key of the form "key.sub_key.sub_sub_key" to match in all cases.
    :return: A tuple with the first element being the dict that contains the key and the second element
    which is the last sub_key used to access the target specified by the key. None returned for both if the key can not
    be found.
    """
    if key in lookup_dict:
        return lookup_dict, key

    # If the key does not match immediately, perform iterative lookup:
    # 1.  Split the search key into tokens
    # 2.  Recurrently concatenate these together to traverse deeper into the dictionary,
    #     clearing the sub_key at every successful lookup.
    #
    # This greedy approach is correct because sub_keys must always appear in order,
    # preferring full stops and traversal interchangeably.
    #
    # Sub_keys will NEVER be duplicated between an alias and a traversal.
    #
    # For example:
    #   {'foo.bar': {'bar': 'ray'}} to look up 'foo.bar' will return {'bar': 'ray'}, not 'ray'
    dict_cursor = lookup_dict
    sub_keys = key.split('.')
    sub_key = ''

    while len(sub_keys) > 0:
        if not dict_cursor:
            return None, None

        sub_key += sub_keys.pop(0)

        if sub_key in dict_cursor:
            if len(sub_keys) == 0:
                break

            dict_cursor = dict_cursor[sub_key]
            sub_key = ''
        elif len(sub_keys) == 0:
            # If there are no keys left to match, return None values
            dict_cursor = None
            sub_key = None
        else:
            sub_key += '.'

    return dict_cursor, sub_key


def flatten_dict(dct, delimiter: str = '.', prefix: str = '') -> dict:
    ret = {}
    for key, val in dct.items():
        if type(val) == dict:
            ret.update(flatten_dict(val, prefix=prefix + key + delimiter))
        else:
            ret[prefix + key] = val
    return ret


def expand_dict(dct, delimiter: str = '.') -> dict:
    """ Expand a (partially) flattened dot-notation dictionary. """
    if isinstance(dct, dict):
        stale_keys = []
        fresh_keys = {}
        for key in dct:
            if delimiter in key:
                # Make note of the stale key
                stale_keys.append(key)

                # Detect the root and sub keys
                root_key, sub_key = key.split(delimiter, 1)
                next_key, rest_key = sub_key.split(delimiter, 1) if delimiter in sub_key else (sub_key, None)

                # Set the default for root_key
                if isinstance(dct.get(root_key), list) or (next_key and next_key.isdigit()):
                    fresh_keys.setdefault(root_key, dct.get(root_key, []))
                else:
                    fresh_keys.setdefault(root_key, dct.get(root_key, {}))

                # Add the value
                if next_key and next_key.isdigit():
                    next_key = int(next_key or '0')
                    sub_val = {rest_key: dct[key]} if rest_key else dct[key]
                    if len(fresh_keys[root_key]) > next_key:
                        fresh_keys[root_key][next_key].update(sub_val)
                    else:
                        fresh_keys[root_key].insert(next_key, sub_val)
                elif isinstance(fresh_keys[root_key], dict):
                    fresh_keys[root_key].update({sub_key: dct[key]})
                elif isinstance(fresh_keys[root_key], list):
                    fresh_keys[root_key].append({sub_key: dct[key]})

                expand_dict(fresh_keys, delimiter)
            else:
                expand_dict(dct[key], delimiter)

        # Remove the flat keys
        for key in stale_keys:
            dct.pop(key)

        # Insert the expanded keys
        dct.update(fresh_keys)

    elif isinstance(dct, list):
        for elem in dct:
            expand_dict(elem, delimiter)

    return dct


def resolve_string(string: str, match: dict, missing_text: str = '[missing]') -> str:
    """
        Given a python string that may contain references to fields on the match dictionary,
            the strings are replaced using the corresponding values.
        However, if the referenced field is not found on the dictionary,
            it is replaced by a default string.
        Strings can be formatted using the old-style format ('%(field)s') or
            the new-style format ('{match[field]}').

        :param string: A string that may contain references to values of the 'match' dictionary.
        :param match: A dictionary with the values to replace where referenced by keys in the string.
        :param missing_text: The default text to replace a formatter with if the field doesnt exist.
    """
    flat_match = flatten_dict(match)
    flat_match.update(match)
    dd_match = collections.defaultdict(lambda: missing_text, flat_match)
    dd_match['_missing_value'] = missing_text
    while True:
        try:
            string = string % dd_match
            string = string.format(**dd_match)
            break
        except KeyError as e:
            if '{%s}' % e not in string:
                break
            string = string.replace('{%s}' % e, '{_missing_value}')

    return string


def ts_to_dt(timestamp) -> datetime.datetime:
    if isinstance(timestamp, datetime.datetime):
        return timestamp
    dt = dateutil.parser.parse(timestamp)
    # Implicitly convert local timestamps to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.utc)
    return dt


def dt_to_ts(dt) -> str:
    if not isinstance(dt, datetime.datetime):
        reactor_logger.warning('Expected datetime, got %s' % (type(dt)))
        return dt
    ts = dt.isoformat()
    # Round microseconds to milliseconds
    if dt.tzinfo is None:
        # Implicitly convert local times to UTC
        return ts + 'Z'
    # isoformat() uses microsecond accuracy and timezone offsets
    # but we should try to use millisecond accuracy and Z to indicate UTC
    return ts.replace('000+00:00', 'Z').replace('+00:00', 'Z')


def ts_to_dt_with_format(timestamp, ts_format) -> datetime.datetime:
    if isinstance(timestamp, datetime.datetime):
        return timestamp
    dt = datetime.datetime.strptime(timestamp, ts_format)
    # Implicitly convert local timestamps to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.utc)
    return dt


def dt_to_ts_with_format(dt, ts_format) -> str:
    if not isinstance(dt, datetime.datetime):
        reactor_logger.warning('Expected datetime, got %s', type(dt))
        return dt
    return dt.strftime(ts_format)


def unix_to_dt(ts) -> datetime.datetime:
    dt = datetime.datetime.utcfromtimestamp(float(ts))
    dt = dt.replace(tzinfo=pytz.utc)
    return dt


def dt_to_unix(dt) -> int:
    return int(dt.timestamp())


def unixms_to_dt(ts) -> datetime.datetime:
    return unix_to_dt(float(ts) / 1000.0)


def dt_to_unixms(dt) -> int:
    return int(dt_to_unix(dt) * 1000)


def dt_now():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)


def pretty_ts(timestamp, tz=True):
    """
    Pretty-format the given timestamp (to be printed or logged hereafter).
    If tx, the timestamp will be converted to local time.
    Format: YYYY-MM-DD HH:MM TZ
    """
    dt = timestamp
    if not isinstance(timestamp, datetime.datetime):
        dt = ts_to_dt(timestamp)
    if tz:
        dt = dt.astimezone()
    return dt.strftime('%Y-%m-%d %H:%M:%S %Z')


def total_seconds(dt):
    # For python 2.6 compatibility
    if dt is None:
        return 0
    elif hasattr(dt, 'total_seconds'):
        return dt.total_seconds()
    else:
        return (dt.microseconds + (dt.seconds + dt.days * 24 * 3600) * 10**6) / 10**6


def hashable(obj):
    """ Convert obj to a hashable obj.
    We use the value of some fields from Elasticsearch as keys for dictionaries. This means
    that whatever Elasticsearch returns must be hashable, and it sometimes returns a list or dict."""
    if not obj.__hash__:
        return str(obj)
    return obj


def elasticsearch_client(conf: dict) -> ElasticSearchClient:
    """ returns an Elasticsearch instance configured using an es_conn_config """
    es_conn_conf = build_es_conn_config(conf)
    auth = reactor.auth.Auth()
    es_conn_conf['http_auth'] = auth(host=es_conn_conf['es_host'],
                                     username=es_conn_conf['es_username'],
                                     password=es_conn_conf['es_password'])

    return ElasticSearchClient(hosts=[{"host": es_conn_conf['es_host'], "port": es_conn_conf['es_port']}],
                               url_prefix=es_conn_conf['es_url_prefix'],
                               use_ssl=es_conn_conf['use_ssl'],
                               verify_certs=es_conn_conf['verify_certs'],
                               ca_certs=es_conn_conf['ca_certs'],
                               connection_class=elasticsearch.RequestsHttpConnection,
                               http_auth=es_conn_conf['http_auth'],
                               timeout=es_conn_conf['es_conn_timeout'],
                               send_get_body_as=es_conn_conf['send_get_body_as'],
                               client_cert=es_conn_conf['client_cert'],
                               client_key=es_conn_conf['client_key'])


def build_es_conn_config(conf: dict) -> dict:
    """ Given a conf dictionary w/ raw config properties 'use_ssl', 'es_host', 'es_port'
    'es_username' and 'es_password', this will return a new dictionary
    with properly initialized values for 'es_host', 'es_port', 'use_ssl' and 'http_auth' which
    will be a basic auth username:password formatted string """
    parsed_conf = {
        'use_ssl': os.environ.get('ES_USE_SSL', False),
        'verify_certs': True,
        'ca_certs': None,
        'client_cert': None,
        'client_key': None,
        'http_auth': None,
        'es_username': None,
        'es_password': None,
        'aws_region': None,
        'profile': None,
        'es_host': conf['host'],
        'es_port': int(conf['port']),
        'es_url_prefix': '',
        'es_conn_timeout': conf.get('conn_timeout', 20),
        'send_get_body_as': conf.get('send_get_body_as', 'GET')
    }

    if 'username' in conf:
        parsed_conf['es_username'] = conf['username']
        parsed_conf['es_password'] = conf['password']

    if 'aws_region' in conf:
        parsed_conf['aws_region'] = conf['aws_region']

    if 'profile' in conf:
        parsed_conf['profile'] = conf['profile']

    if 'ssl' in conf:
        if 'enabled' in conf['ssl']:
            parsed_conf['use_ssl'] = conf['ssl']['enabled']

        if 'verify_certs' in conf['ssl']:
            parsed_conf['verify_certs'] = conf['ssl']['verify_certs']

        if 'ca_certs' in conf['ssl']:
            parsed_conf['ca_certs'] = conf['ssl']['ca_certs']

        if 'client_cert' in conf['ssl']:
            parsed_conf['client_cert'] = conf['ssl']['client_cert']

        if 'client_key' in conf['ssl']:
            parsed_conf['client_key'] = conf['ssl']['client_key']

    if 'url_prefix' in conf:
        parsed_conf['url_prefix'] = conf['url_prefix']

    return parsed_conf


def format_index(index, start, end, add_extra=False):
    """ Takes an index, specified using strftime format, start and end time timestamps,
    and outputs a wildcard based index string to match all possible timestamps. """
    # Convert to UTC
    start -= start.utcoffset()
    end -= end.utcoffset()
    original_start = start
    indices = set()
    while start.date() <= end.date():
        indices.add(start.strftime(index))
        start += datetime.timedelta(days=1)
    num = len(indices)
    if add_extra:
        while len(indices) == num:
            original_start -= datetime.timedelta(days=1)
            new_index = original_start.strftime(index)
            assert new_index != index, "You cannot use a static index with search_extra_index"
            indices.add(new_index)

    return ','.join(indices)


def add_raw_postfix(field, es_version):
    if semantic_at_least(es_version, 5):
        end = '.keyword'
    else:
        end = '.raw'
    if not field.endswith(end):
        field += end
    return field
