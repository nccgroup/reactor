import copy
import datetime
import json
from reactor import elasticsearch_client


with open('./tests/integration/logs.ndjson', 'r') as f:
    logs = [json.loads(line.strip()) for line in f.readlines()]
es_client = elasticsearch_client({
    'host': 'elasticsearch',
    'port': 9200,
    'username': 'elastic',
    'password': 'changeme',
})

# Delete the logs index if it exists
print(' [.] Checking if reactor_logs exists')
if es_client.indices.exists('reactor_logs'):
    es_client.indices.delete('reactor_logs')


# Create `reactor_logs` index with settings and mappings
print(' [.] Creating index')
es_client.indices.create('reactor_logs',
                         body=json.load(open('./tests/integration/logs-index-%d.json' % es_client.es_version[0], 'r')))

# Insert some logs to be tested against
print(' [.] Inserting data')
action_data_pairs = []
action_template = {'index': {'_index': 'reactor_logs', '_type': '_doc'}}
for n, log in enumerate(logs):
    # Add the action
    action = copy.copy(action_template)  # type: dict
    action.update({'_id': str(n)})
    action_data_pairs.append(action)

    # Add the log
    log.update({'@timestamp': '%sT05:%02d:%02dZ' % (datetime.datetime.today().strftime('%Y-%m-%d'), int(n/60), 6//60)})
    action_data_pairs.append(log)
es_client.bulk(action_data_pairs, refresh='wait_for')
