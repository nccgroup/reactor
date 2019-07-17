import json
import time
import elasticsearch
import elasticsearch.helpers
import os
import sys
from reactor.exceptions import ReactorException
from reactor.util import reactor_logger, ElasticSearchClient


def create_indices(es_client: ElasticSearchClient, conf: dict, recreate=False, old_index=None, force=False):
    reactor_logger.info('ElasticSearch version: %s', es_client.es_version)

    es_index_mappings = read_es_index_mappings(es_client.es_version[0])
    es_index_settings = read_es_index_settings(es_client.es_version[0])

    es_index = elasticsearch.client.IndicesClient(es_client)
    if not recreate:
        index = conf['index'] + '_alert' if es_client.es_version_at_least(6) else conf['index']
        if es_index.exists(index):
            reactor_logger.warning('Index %s already exists. Skipping index creation.' % index)
            return None
    elif not force:
        if not query_yes_no("Recreating indices will delete ALL existing data. Are you sure you want to recreate?"):
            reactor_logger.warning('Initialisation abandoned.')
            return None

    if es_index.exists_template(conf['index']):
        reactor_logger.info('Template %s already exists. Deleting in preparation for creating indices.' % conf['index'])
        es_index.delete_template(conf['index'])

    # (Re-)Create indices.
    if es_client.es_version_at_least(6):
        index_names = (
            conf['index'] + '_alert',
            conf['index'] + '_status',
            conf['index'] + '_silence',
            conf['index'] + '_error',
        )
    else:
        index_names = (
            conf['index'],
        )
    for index_name in index_names:
        if es_index.exists(index_name):
            reactor_logger.info('Deleting index ' + index_name + '.')
            try:
                es_index.delete(index_name)
            except elasticsearch.NotFoundError:
                # Why does this ever occur?? It shouldn't. But it does.
                pass
        es_index.create(index_name, body={'settings': es_index_settings})
        # Confirm index has been created
        index_created = es_index.exists(index_name)
        timeout = time.time() + 2.0
        while not index_created and time.time() < timeout:
            index_created = es_index.exists(index_name)
        if not index_created:
            raise ReactorException('Failed to create index: %s' % index_name)
    for item in es_client.cat.aliases(format='json'):
        if item['alias'] != conf['alert_alias']:
            continue
        reactor_logger.info('Deleting index ' + item['index'] + '.')
        try:
            es_index.delete(item['index'])
        except elasticsearch.NotFoundError:
            # Why does this ever occur?? It shouldn't. But it does.
            pass

    if es_client.es_version_at_least(7):
        # TODO remove doc_type completely when elasticsearch client allows doc_type=None
        # doc_type is a deprecated feature and will be completely removed in ElasticSearch 8
        reactor_logger.info('Applying mappings for ElasticSearch v7.x')
        es_client.indices.put_mapping(index=conf['index'] + '_alert', doc_type='_doc',
                                      body=es_index_mappings['alert'], include_type_name=True)
        es_client.indices.put_alias(index=conf['index'] + '_alert', name=conf['alert_alias'])
        es_client.indices.put_mapping(index=conf['index'] + '_status', doc_type='_doc',
                                      body=es_index_mappings['status'], include_type_name=True)
        es_client.indices.put_mapping(index=conf['index'] + '_silence', doc_type='_doc',
                                      body=es_index_mappings['silence'], include_type_name=True)
        es_client.indices.put_mapping(index=conf['index'] + '_error', doc_type='_doc',
                                      body=es_index_mappings['error'], include_type_name=True)
        es_client.indices.put_template(name=conf['index'], body={'index_patterns': [conf['index'] + '_alert_*'],
                                                                 'aliases': {conf['alert_alias']: {}},
                                                                 'settings': es_index_settings,
                                                                 'mappings': {'_doc': es_index_mappings['alert']}})
    elif es_client.es_version_at_least(6):
        reactor_logger.info('Applying mappings for ElasticSearch v6.x')
        es_client.indices.put_mapping(index=conf['index'] + '_alert', doc_type='_doc',
                                      body=es_index_mappings['alert'])
        es_client.indices.put_alias(index=conf['index'] + '_alert', name=conf['alert_alias'])
        es_client.indices.put_mapping(index=conf['index'] + '_status', doc_type='_doc',
                                      body=es_index_mappings['status'])
        es_client.indices.put_mapping(index=conf['index'] + '_silence', doc_type='_doc',
                                      body=es_index_mappings['silence'])
        es_client.indices.put_mapping(index=conf['index'] + '_error', doc_type='_doc',
                                      body=es_index_mappings['error'])
        es_client.indices.put_template(name=conf['index'], body={'index_patterns': [conf['index'] + '_alert_*'],
                                                                 'aliases': {conf['alert_alias']: {}},
                                                                 'settings': es_index_settings,
                                                                 'mappings': {'_doc': es_index_mappings['alert']}})
    else:
        reactor_logger.info('Applying mappings for ElasticSearch v5.x')
        es_client.indices.put_mapping(index=conf['index'], doc_type='reactor_alert',
                                      body=es_index_mappings['alert'])
        es_client.indices.put_alias(index=conf['index'] + '_alert', name=conf['alert_alias'])
        es_client.indices.put_mapping(index=conf['index'], doc_type='reactor_status',
                                      body=es_index_mappings['status'])
        es_client.indices.put_mapping(index=conf['index'], doc_type='reactor',
                                      body=es_index_mappings['silence'])
        es_client.indices.put_mapping(index=conf['index'], doc_type='reactor_error',
                                      body=es_index_mappings['error'])
        es_client.indices.put_template(name=conf['index'], body={'template': conf['index'] + '_*',
                                                                 'aliases': {conf['alert_alias']: {}},
                                                                 'settings': es_index_settings,
                                                                 'mappings': {
                                                                     'reactor_alert': es_index_mappings['alert']}})

    reactor_logger.info('New index and template %s created', conf['index'])
    if old_index:
        reactor_logger.info('Copying data from old index %s to new index %s', old_index, conf['index'])
        elasticsearch.helpers.reindex(es_client, old_index, conf['index'])


def read_es_index_mappings(es_version):
    reactor_logger.info('Reading ElasticSearch v%s index mappings:', es_version)
    return {
        'silence': read_es_index_mapping('silence', es_version),
        'status': read_es_index_mapping('status', es_version),
        'alert': read_es_index_mapping('alert', es_version),
        'error': read_es_index_mapping('error', es_version)
    }


def read_es_index_settings(es_version):
    reactor_logger.info('Reading ElasticSearch v%s index settings:', es_version)
    return read_es_index_mapping('settings', es_version)


def read_es_index_mapping(mapping, es_version):
    base_path = os.path.abspath(os.path.dirname(__file__))
    mapping_path = 'mappings/{0}/{1}.json'.format(es_version, mapping)
    path = os.path.join(base_path, mapping_path)
    with open(path, 'r') as f:
        reactor_logger.info("Reading index mapping '%s'", mapping_path)
        return json.load(f)


def query_yes_no(question: str, default="yes") -> bool:
    """
    Ask a yes/no question via `input()` and return their answer.
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("Invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'no').\n")
