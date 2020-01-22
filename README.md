# Reactor
Reactor started as a fork of the open source project [ElastAlert](https://github.com/Yelp/elastalert). The maintainer of
this project should keep close tabs on the changes to ElastAlert (excluding their additional alerters) and ensure that
any relevant bugs fixes be patched in and any useful feature ported in.

Reactor is an alerting engine which takes in a set of rules with custom filters and alerts on matches.
Reactor automatically updates silenced alerts with repeat alert information.

#### Patch history from ElastAlert
| Date accessed | Commit                                   | Notes                                             |
|---------------|------------------------------------------|---------------------------------------------------|
| 2019-10-16    | 325f1dfe7a45f3ca2a2cc00127ab71fcd4f9cead | Went back until before Reactor was first created. |
| 2020-01-09    | ec5d03b95708ea0aa3d29c065f9794fdd95a82a1 | We do not use coverage.                           |
|               |                                          |                                                   |


## Supported Versions
Currently Reactor supports ElasticSearch 5.x.x, 6.x.x, and 7.x.x
As new versions of elasticsearch become available Reactor will be updated to support.
There is no intention to add support for older versions of ElasticSearch.
Currently, there is no date to remove support for older versions of ElasticSearch. If ElasticSearch's python library
removes support we are likely to follow suite.

### Compatibility
As per ElasticSearch's python library's guidelines is is recommended to use install the version of the library with the
same major version as the cluster.

For **Elasticsearch 7.0** and later, use the major version 7 (``elasticsearch<8.0.0,>=7.0.0``).

For **Elasticsearch 6.0** and later, use the major version 6 (``elasticsearch<7.0.0,>=6.0.0``).

For **Elasticsearch 5.0** and later, use the major version 5 (``elasticsearch<6.0.0,>=5.0.0``).

Please note there is a [known bug](https://github.com/elastic/elasticsearch-py/issues/971) introduced version ``6.4.0``
of the ElasticSearch library which was fixed in ``7.0.4`` but not in major version 6. The bug puts the ``scroll_id`` in
the query parameter which can cause Elasticsearch to return ``400`` status codes for valid scroll ids.


## Development
You can start an elasticsearch instance inside docker for local development using the following command:
```console
$ docker run -d -p 9200:9200/tcp --name elasticsearch docker.elastic.co/elasticsearch/elasticsearch:<version>
```

And the following configuration in `config.yaml`:
```yaml
writeback_index: reactor
alert_alias: reactor_alerts

elasticsearch: &elasticsearch
  host: localhost
  port: 9200

# Global settings to be applied to every run
rule:
  elasticsearch: *elasticsearch
```

### Git Hooks
This project provides git hooks to stop user error. Please run the following commands:

```console
$ cp .git-hooks-pre-push .git/hooks/pre-push
```

## Running tests
Reactor is covered by two types of testing: unit and integration. Unit testing to ensure
individual functions and logic flows work correctly; integration testing to ensure that the
whole system works in unison.

### Unit
The unit tests are written using PyTest. To run all the tests run the following command:
```console
$ py.test
```

### Integration
The unit tests are executed in docker and require a `.env` file in the project's root directory.
The tests require the follow contents inside the the `.env` file:
```dotenv
# The ElasticSearch version to be tested, reactor supports >= 5.x.x
ES_VERSION=6.3.2
# Basic configuration information so that reactor can query ElasticSearch
ES_HOST=elasticsearch
ES_USER=elastic
ES_PASSWORD=changeme
```

The integration Docker Compose file `test.docker-compose.yml` has 3 environment variables that
are required for >= v7.x.x which will break older versions:
```dotenv
node.name=elasticsearch
discovery.seed_hosts=elasticsearch
cluster.initial_master_nodes=elasticsearch
```

To execute the integration tests run the following command: 
```console
$ docker-compose -f docker-compose-test.yml up --abort-on-container-exit --build  reactor elasticsearch
```


## Create SSL certs for RAFT leadership
The following set of commands (performed in ``./certs/``) will create a set a CA and device certificate for running the mock cluster on localhost:
```console
# Only do once: generate the root CA key:
$ openssl genrsa -out transport-ca.key 4096

# Generate the root CA certificate:
## Country Name (2 letter code) []:GB
## State or Province Name (full name) []:.
## Locality Name (eg, city) []:.
## Organization Name (eg, company) []:.
## Organizational Unit Name (eg, section) []:.
## Common Name (eg, fully qualified host name) []:PyRaftLog
## Email Address []:.
$ openssl req -x509 -new -nodes -key transport-ca.key -sha256 -days 1024 -out transport-ca.pem

# Generate device certificates
# Only do once: generate device key:
$ openssl genrsa -out transport-consensus.key 4096

# Generate device certificate signing request:
## Country Name (2 letter code) []:GB
## State or Province Name (full name) []:.
## Locality Name (eg, city) []:.
## Organization Name (eg, company) []:.
## Organizational Unit Name (eg, section) []:.
## Common Name (eg, fully qualified host name) []:localhost
## Email Address []:.
$ openssl req -new -key transport-consensus.key -out transport-consensus.csr

# Generate a signed device certificate:
$ openssl x509 -req -in transport-consensus.csr -CA transport-ca.pem -CAkey transport-ca.key -CAcreateserial -out transport-consensus.crt -days 500 -sha256
```


## Build documentation
To build the documentation:
```console
$ pip install -r requirements-docs.txt
$ cd docs
$ sphinx-build -b html -d build/doctrees -W source build/html
```
