#!/bin/sh
set -e


# Initialise the reactor indices
echo " [x] Initialising the reactor indices (waiting up to 1 minute for ElasticSearch)"
python -m reactor init --config config-test.yaml --patience minutes=1 --recreate -f


# Populate ElasticSearch with some interesting logs
echo " [x] Setting up test data"
python ./tests/integration/test.py


# Execute reactor for a limited period of time
echo " [x] Executing reactor"
python -m reactor test --config config-test.yaml --output devnull any.yaml
python -m reactor test --config config-test.yaml --output devnull frequency.yaml
python -m reactor test --config config-test.yaml --output devnull spike.yaml
python -m reactor test --config config-test.yaml --output devnull blacklist.yaml
python -m reactor test --config config-test.yaml --output devnull whitelist.yaml
python -m reactor test --config config-test.yaml --output devnull change.yaml
python -m reactor test --config config-test.yaml --output devnull flatline.yaml
python -m reactor test --config config-test.yaml --output devnull new_term.yaml
python -m reactor test --config config-test.yaml --output devnull cardinality.yaml
python -m reactor test --config config-test.yaml --output devnull metric_aggregation.yaml
# TODO: Add `python -m reactor test --config config-test.yaml --output devnull spike_metric_aggregation.yaml`
python -m reactor test --config config-test.yaml --output devnull percentage_match.yaml
python -m reactor run --end $(date +%Y-%m-%dT%H:%M:%S) --config config-test.yaml