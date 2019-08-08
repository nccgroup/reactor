# Reactor
Create rules with custom filters and alert on matches.
Automatically update silenced alerts with repeat alert information.



## Running tests
Reactor is covered by two types of testing: unit and integration. Unit testing to ensure
individual functions and logic flows work correctly; integration testing to ensure that the
whole system works in unison.

### Unit
The unit tests are written using PyTest. To run all the tests run the following command:
```shell script
pt.test
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
