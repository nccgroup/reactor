import copy
import datetime
import elasticsearch
import json
import os.path
import urllib.parse

from .util import dt_to_ts, ts_to_dt, dots_get
from .exceptions import ReactorException


def ts_add(ts, td):
    """ Allows a timedelta (td) add operation on a string timestamp (ts) """
    return dt_to_ts(ts_to_dt(ts) + td)


def generate_kibana4_db(rule, match):
    """ Creates a link for a kibana4 dashboard which has time set to the match. """
    db_name = rule.get('use_kibana4_dashboard')
    start = ts_add(
        dots_get(match, rule['timestamp_field']),
        -rule.get('kibana4_start_timedelta', rule.get('timeframe', datetime.timedelta(minutes=10)))
    )
    end = ts_add(
        dots_get(match, rule['timestamp_field']),
        rule.get('kibana4_end_timedelta', rule.get('timeframe', datetime.timedelta(minutes=10)))
    )
    return kibana4_dashboard_link(db_name, start, end)


def generate_kibana_db(rule, match, index):
    """ Uses a template dashboard to upload a temp dashboard showing the match.
    Returns the url to the dashboard. """
    db = copy.deepcopy(dashboard_temp)

    # Set timestamp fields to match our rule especially if
    # we have configured something other than @timestamp
    set_timestamp_field(db, rule['timestamp_field'])

    # Set filters
    for sub_filter in rule['filter']:
        if sub_filter:
            add_filter(db, sub_filter)
    set_included_fields(db, rule['include'])

    # Set index
    set_index_name(db, index)

    return upload_dashboard(db, rule, match)


def upload_dashboard(db, rule, match):
    """ Uploads a dashboard schema to the kibana-int Elasticsearch index associated with rule.
    Returns the url to the dashboard. """
    # Set time range
    start = ts_add(dots_get(match, rule['timestamp_field']), -rule.get('timeframe', datetime.timedelta(minutes=10)))
    end = ts_add(dots_get(match, rule['timestamp_field']), datetime.timedelta(minutes=10))
    set_time(db, start, end)

    # Set dashboard name
    db_name = 'Reactor - %s - %s' % (rule['name'], end)
    set_name(db, db_name)

    # Add filter for query_key value
    if 'query_key' in rule:
        for qk in rule.get('compound_query_key', [rule['query_key']]):
            if qk in match:
                term = {'term': {qk: match[qk]}}
                add_filter(db, term)

    # Add filter for aggregation_key value
    if 'aggregation_key' in rule:
        for qk in rule.get('compound_aggregation_key', [rule['aggregation_key']]):
            if qk in match:
                term = {'term': {qk: match[qk]}}
                add_filter(db, term)

    # Convert to json
    db_js = json.dumps(db)
    db_body = {'user': 'guest',
               'group': 'guest',
               'title': db_name,
               'dashboard': db_js}

    # Upload
    es = elasticsearch_client(rule['elasticsearch'])
    # TODO: doc_type = _doc for elastic >= 6 and index may have changed
    res = es.index(index='kibana-int',
                   doc_type='temp',
                   body=db_body)

    # Return dashboard URL
    kibana_url = rule.get('kibana_url')
    if not kibana_url:
        kibana_url = 'http://%s:%s/_plugin/kibana/' % (rule['es_host'],
                                                       rule['es_port'])
    return kibana_url + '#/dashboard/temp/%s' % (res['_id'])


def get_dashboard(rule, db_name):
    """ Download dashboard which matches use_kibana_dashboard from Elasticsearch. """
    if not db_name:
        raise ReactorException("use_kibana_dashboard undefined")
    query = {'query': {'term': {'_id': db_name}}}
    try:
        # TODO: discover the kibana dashboard index and doc type for newer versions of elasticsearch
        index = 'kibana-int'
        if rule.es_client.es_version_at_least(6, 6):
            res = rule.es_client.search(index=index, size=1, body=query,
                                        _source_includes=['dashboard'])
        elif rule.es_client.es_version_at_least(6):
            res = rule.es_client.search(index=index, size=1, body=query,
                                        _source_include=['dashboard'])
        else:
            res = rule.es_client.search(index=index, doc_type='dashboard',
                                        size=1, body=query, _source_include=['dashboard'])
    except elasticsearch.ElasticsearchException as e:
        raise ReactorException("Error querying for dashboard: %s" % e)

    if res['hits']['hits']:
        return json.loads(res['hits']['hits'][0]['_source']['dashboard'])
    else:
        raise ReactorException("Could not find dashboard named %s" % db_name)


def use_kibana_link(rule, match):
    """ Uploads an existing dashboard as a temp dashboard modified for match time.
    Returns the url to the dashboard. """
    # Download or get cached dashboard
    dashboard = rule.get('dashboard_schema')
    if not dashboard:
        db_name = rule.get('use_kibana_dashboard')
        dashboard = get_dashboard(rule, db_name)
    if dashboard:
        rule['dashboard_schema'] = dashboard
    else:
        return None
    dashboard = copy.deepcopy(dashboard)
    return upload_dashboard(dashboard, rule, match)


def filters_from_kibana(rule, db_name):
    """ Downloads a dashboard from Kibana and returns corresponding filters, None on error. """
    try:
        db = rule.get('dashboard_schema')
        if not db:
            db = get_dashboard(rule, db_name)
        filters = filters_from_dashboard(db)
    except ReactorException:
        return None
    return filters


dashboard_temp = {'editable': True,
                  u'failover': False,
                  u'index': {u'default': u'NO_TIME_FILTER_OR_INDEX_PATTERN_NOT_MATCHED',
                             u'interval': u'none',
                             u'pattern': u'',
                             u'warm_fields': True},
                  u'loader': {u'hide': False,
                              u'load_elasticsearch': True,
                              u'load_elasticsearch_size': 20,
                              u'load_gist': True,
                              u'load_local': True,
                              u'save_default': True,
                              u'save_elasticsearch': True,
                              u'save_gist': False,
                              u'save_local': True,
                              u'save_temp': True,
                              u'save_temp_ttl': u'30d',
                              u'save_temp_ttl_enable': True},
                  u'nav': [{u'collapse': False,
                            u'enable': True,
                            u'filter_id': 0,
                            u'notice': False,
                            u'now': False,
                            u'refresh_intervals': [u'5s',
                                                   u'10s',
                                                   u'30s',
                                                   u'1m',
                                                   u'5m',
                                                   u'15m',
                                                   u'30m',
                                                   u'1h',
                                                   u'2h',
                                                   u'1d'],
                            u'status': u'Stable',
                            u'time_options': [u'5m',
                                              u'15m',
                                              u'1h',
                                              u'6h',
                                              u'12h',
                                              u'24h',
                                              u'2d',
                                              u'7d',
                                              u'30d'],
                            u'timefield': u'@timestamp',
                            u'type': u'timepicker'}],
                  u'panel_hints': True,
                  u'pulldowns': [{u'collapse': False,
                                  u'enable': True,
                                  u'notice': True,
                                  u'type': u'filtering'}],
                  u'refresh': False,
                  u'rows': [{u'collapsable': True,
                             u'collapse': False,
                             u'editable': True,
                             u'height': u'350px',
                             u'notice': False,
                             u'panels': [{u'annotate': {u'enable': False,
                                                        u'field': u'_type',
                                                        u'query': u'*',
                                                        u'size': 20,
                                                        u'sort': [u'_score', u'desc']},
                                          u'auto_int': True,
                                          u'bars': True,
                                          u'derivative': False,
                                          u'editable': True,
                                          u'fill': 3,
                                          u'grid': {u'max': None, u'min': 0},
                                          u'group': [u'default'],
                                          u'interactive': True,
                                          u'interval': u'1m',
                                          u'intervals': [u'auto',
                                                         u'1s',
                                                         u'1m',
                                                         u'5m',
                                                         u'10m',
                                                         u'30m',
                                                         u'1h',
                                                         u'3h',
                                                         u'12h',
                                                         u'1d',
                                                         u'1w',
                                                         u'1M',
                                                         u'1y'],
                                          u'legend': True,
                                          u'legend_counts': True,
                                          u'lines': False,
                                          u'linewidth': 3,
                                          u'mode': u'count',
                                          u'options': True,
                                          u'percentage': False,
                                          u'pointradius': 5,
                                          u'points': False,
                                          u'queries': {u'ids': [0], u'mode': u'all'},
                                          u'resolution': 100,
                                          u'scale': 1,
                                          u'show_query': True,
                                          u'span': 12,
                                          u'spyable': True,
                                          u'stack': True,
                                          u'time_field': u'@timestamp',
                                          u'timezone': u'browser',
                                          u'title': u'Events over time',
                                          u'tooltip': {u'query_as_alias': True,
                                                       u'value_type': u'cumulative'},
                                          u'type': u'histogram',
                                          u'value_field': None,
                                          u'x-axis': True,
                                          u'y-axis': True,
                                          u'y_format': u'none',
                                          u'zerofill': True,
                                          u'zoomlinks': True}],
                             u'title': u'Graph'},
                            {u'collapsable': True,
                             u'collapse': False,
                             u'editable': True,
                             u'height': u'350px',
                             u'notice': False,
                             u'panels': [{u'all_fields': False,
                                          u'editable': True,
                                          u'error': False,
                                          u'field_list': True,
                                          u'fields': [],
                                          u'group': [u'default'],
                                          u'header': True,
                                          u'highlight': [],
                                          u'localTime': True,
                                          u'normTimes': True,
                                          u'offset': 0,
                                          u'overflow': u'min-height',
                                          u'pages': 5,
                                          u'paging': True,
                                          u'queries': {u'ids': [0], u'mode': u'all'},
                                          u'size': 100,
                                          u'sort': [u'@timestamp', u'desc'],
                                          u'sortable': True,
                                          u'span': 12,
                                          u'spyable': True,
                                          u'status': u'Stable',
                                          u'style': {u'font-size': u'9pt'},
                                          u'timeField': u'@timestamp',
                                          u'title': u'All events',
                                          u'trimFactor': 300,
                                          u'type': u'table'}],
                             u'title': u'Events'}],
                  u'services': {u'filter': {u'ids': [0],
                                            u'list': {u'0': {u'active': True,
                                                             u'alias': u'',
                                                             u'field': u'@timestamp',
                                                             u'from': u'now-24h',
                                                             u'id': 0,
                                                             u'mandate': u'must',
                                                             u'to': u'now',
                                                             u'type': u'time'}}},
                                u'query': {u'ids': [0],
                                           u'list': {u'0': {u'alias': u'',
                                                            u'color': u'#7EB26D',
                                                            u'enable': True,
                                                            u'id': 0,
                                                            u'pin': False,
                                                            u'query': u'',
                                                            u'type': u'lucene'}}}},
                  u'style': u'dark',
                  u'title': u'ElastAlert Alert Dashboard'}

kibana4_time_temp = "(refreshInterval:(display:Off,section:0,value:0),time:(from:'%s',mode:absolute,to:'%s'))"


def set_time(dashboard, start, end):
    dashboard['services']['filter']['list']['0']['from'] = start
    dashboard['services']['filter']['list']['0']['to'] = end


def set_index_name(dashboard, name):
    dashboard['index']['default'] = name


def set_timestamp_field(dashboard, field):
    # set the nav timefield if we don't want @timestamp
    dashboard['nav'][0]['timefield'] = field

    # set the time_field for each of our panels
    for row in dashboard.get('rows'):
        for panel in row.get('panels'):
            panel['time_field'] = field

    # set our filter's  time field
    dashboard['services']['filter']['list']['0']['field'] = field


def add_filter(dashboard, es_filter):
    next_id = max(dashboard['services']['filter']['ids']) + 1

    kibana_filter = {'active': True,
                     'alias': '',
                     'id': next_id,
                     'mandate': 'must'}

    if 'not' in es_filter:
        es_filter = es_filter['not']
        kibana_filter['mandate'] = 'mustNot'

    if 'query' in es_filter:
        es_filter = es_filter['query']
        if 'query_string' in es_filter:
            kibana_filter['type'] = 'querystring'
            kibana_filter['query'] = es_filter['query_string']['query']
    elif 'term' in es_filter:
        kibana_filter['type'] = 'field'
        f_field, f_query = es_filter['term'].items()[0]
        # Wrap query in quotes, otherwise certain characters cause Kibana to throw errors
        if isinstance(f_query, str):
            f_query = '"%s"' % (f_query.replace('"', '\\"'))
        if isinstance(f_query, list):
            # Escape quotes
            f_query = [item.replace('"', '\\"') for item in f_query]
            # Wrap in quotes
            f_query = ['"%s"' % item for item in f_query]
            # Convert into joined query
            f_query = '(%s)' % (' AND '.join(f_query))
        kibana_filter['field'] = f_field
        kibana_filter['query'] = f_query
    elif 'range' in es_filter:
        kibana_filter['type'] = 'range'
        f_field, f_range = es_filter['range'].items()[0]
        kibana_filter['field'] = f_field
        kibana_filter.update(f_range)
    else:
        raise ReactorException("Could not parse filter %s for Kibana" % es_filter)

    dashboard['services']['filter']['ids'].append(next_id)
    dashboard['services']['filter']['list'][str(next_id)] = kibana_filter


def set_name(dashboard, name):
    dashboard['title'] = name


def set_included_fields(dashboard, fields):
    dashboard['rows'][1]['panels'][0]['fields'] = list(set(fields))


def filters_from_dashboard(db):
    filters = db['services']['filter']['list']
    config_filters = []
    or_filters = []
    for sub_filter in filters.values():
        filter_type = sub_filter['type']
        if filter_type == 'time':
            continue

        config_filter = {}
        if filter_type == 'querystring':
            config_filter = {'query': {'query_string': {'query': sub_filter['query']}}}

        if filter_type == 'field':
            config_filter = {'term': {sub_filter['field']: sub_filter['query']}}

        if filter_type == 'range':
            config_filter = {'range': {sub_filter['field']: {'from': sub_filter['from'], 'to': sub_filter['to']}}}

        if sub_filter['mandate'] == 'mustNot':
            config_filter = {'not': config_filter}

        if sub_filter['mandate'] == 'either':
            or_filters.append(config_filter)
        else:
            config_filters.append(config_filter)

    if or_filters:
        config_filters.append({'or': or_filters})

    return config_filters


def kibana4_dashboard_link(dashboard, start_time, end_time):
    dashboard = os.path.expandvars(dashboard)
    time_settings = kibana4_time_temp % (start_time, end_time)
    time_settings = urllib.parse.quote(time_settings)
    return "%s?_g=%s" % (dashboard, time_settings)
