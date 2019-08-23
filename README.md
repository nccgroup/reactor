# Reactor
Create rules with custom filters and alert on matches.
Automatically update silenced alerts with repeat alert information.

## Supported Versions
Currently Reactor supports ElasticSearch 5.x.x, 6.x.x, and 7.x.x
As new versions of elasticsearch become available Reactor will be updated to support.
There is no intention to add support for older versions of ElasticSearch.
Currently, there is no date to remove support for older versions of ElasticSearch. If ElasticSearch's python package
removes support we are likely to follow suite.


## Development
You can start an elasticsearch instance inside docker for local development using the following command:
```shell script
docker run -d -p 9200:9200/tcp --name elasticsearch docker.elastic.co/elasticsearch/elasticsearch:<version>
```

And the following configuration in `config.yaml`:
```yaml
index: reactor
alert_alias: reactor_alerts

elasticsearch: &elasticsearch
  host: localhost
  port: 9200

# Global settings to be applied to every run
rule:
  elasticsearch: *elasticsearch
```


## Running tests
Reactor is covered by two types of testing: unit and integration. Unit testing to ensure
individual functions and logic flows work correctly; integration testing to ensure that the
whole system works in unison.

### Unit
The unit tests are written using PyTest. To run all the tests run the following command:
```shell script
py.test
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
```shell script
docker-compose -f docker-compose-test.yml up --abort-on-container-exit --build  reactor elasticsearch
```
