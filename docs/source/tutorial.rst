.. _tutorial:

Running Reactor for the First Time
=====================================

Requirements
------------

- Elasticsearch
- ISO8601 or Unix timestamped data
- Python 3.6
- pip, see requirements.txt
- Packages on Ubuntu 14.x: python-pip python-dev libffi-dev libssl-dev

Downloading and Configuring
---------------------------

You can either install the latest released version of Reactor using pip::

    $ pip install reactor

or you can clone the Reactor repository for the most recent changes::

    $ git clone https://git-man-dev.nccgroup.local/reactlive/reactor.git

Install the module::

    $ pip install "setuptools>=11.3"
    $ python setup.py install

Depending on the version of Elasticsearch, you may need to manually install the correct version of elasticsearch-py.

Elasticsearch 5.0+::

    $ pip install "elasticsearch>=6.0.0,<7.0.0"

Next, open up config.yaml.example. In it, you will find several configuration options. Reactor may be run without changing any of these settings.

``loader.config.rules_folder`` is where Reactor will load rule configuration files from. It will attempt to load every .yaml file in the folder. Without any valid rules, Reactor will not start. Reactor will also load new rules, stop running missing rules, and restart modified rules as the files in this folder change. For this tutorial, we will use the example_rules folder.

``rule.run_every`` is how often Reactor will query Elasticsearch.

``rule.buffer_time`` is the size of the query window, stretching backwards from the time each query is run. This value is ignored for rules where ``use_count_query`` or ``use_terms_query`` is set to true.

``elasticsearch.host`` is the address of an Elasticsearch cluster where Reactor will store data about its state, queries run, alerts, and errors. Each rule may also use a different Elasticsearch host to query against.

``elasticsearch.port`` is the port corresponding to ``elasticsearch.host``.

``elasticsearch.ssl.enabled``: Optional; whether or not to connect to ``elasticsearch.host`` using TLS; set to ``True`` or ``False``.

``elasticsearch.ssl.verify_certs``: Optional; whether or not to verify TLS certificates; set to ``True`` or ``False``. The default is ``True``

``elasticsearch.ssl.client_cert``: Optional; path to a PEM certificate to use as the client certificate

``elasticsearch.ssl.client_key``: Optional; path to a private key file to use as the client key

``elasticsearch.ssl.ca_certs``: Optional; path to a CA cert bundle to use to verify SSL connections

``elasticsearch.username``: Optional; basic-auth username for connecting to ``elasticsearch.host``.

``elasticsearch.password``: Optional; basic-auth password for connecting to ``elasticsearch.host``.

``elasticsearch.url_prefix``: Optional; URL prefix for the Elasticsearch endpoint.

``elasticsearch.send_get_body_as``: Optional; Method for querying Elasticsearch - ``GET``, ``POST`` or ``source``. The default is ``GET``

``writeback_index`` is the name of the index in which Reactor will store data. We will create this index later.

``alert_time_limit`` is the retry window for failed alerts.

Save the file as ``config.yaml``

Setting Up Elasticsearch
------------------------

Reactor saves information and metadata about its queries and its alerts back to Elasticsearch. This is useful for
auditing, debugging, and it allows Reactor to restart and resume exactly where it left off. This is not required for
Reactor to run, but highly recommended.

First, we need to create an index for Reactor to write to by running ``reactor init``

.. code-block:: console

    $ reactor init
    2019-10-08 16:56:22,255 [reactor] INFO: ElasticSearch version: (6, 3, 0)
    2019-10-08 16:56:22,255 [reactor] INFO: Reading ElasticSearch v6 index mappings:
    2019-10-08 16:56:22,255 [reactor] INFO: Reading index mapping 'mappings/6/silence.json'
    2019-10-08 16:56:22,256 [reactor] INFO: Reading index mapping 'mappings/6/status.json'
    2019-10-08 16:56:22,256 [reactor] INFO: Reading index mapping 'mappings/6/alert.json'
    2019-10-08 16:56:22,256 [reactor] INFO: Reading index mapping 'mappings/6/error.json'
    2019-10-08 16:56:22,256 [reactor] INFO: Reading ElasticSearch v6 index settings:
    2019-10-08 16:56:22,256 [reactor] INFO: Reading index mapping 'mappings/6/settings.json'
    2019-10-08 16:56:23,210 [reactor] INFO: Applying mappings for ElasticSearch v6

For information about what data will go here, see :ref:`Reactor Metadata Index <metadata>`.

Creating a Rule
---------------

Each rule defines a query to perform, parameters on what triggers a match, and a list of alerts to fire for each match.
We are going to create an example rule ``my_rules/my_frequency.yaml`` as a template

.. code-block:: yaml

    elasticsearch:
      host: elasticsearch.example.com
      port: 14900

    name: My Frequency Rule
    index: logstash-*

    type: frequency
    num_events: 50
    timeframe:
      hours: 4

    filter:
    - term:
        some_field: "some_value"

    alerters:
      email:
        to:
        - "reactor@example.com"


``elasticsearch.host`` and ``elasticsearch.port``
    Should point to the Elasticsearch cluster we want to query.

``name``
    The unique name for this rule. Reactor will not start if two rules share the same name.

``type``
    Each rule has a different type which may take different parameters. The ``frequency`` type means "Alert when more than
    ``num_events`` occur within ``timeframe``." For information other types, see :ref:`Rule types <ruletypes>`.

``index``
    The name of the index(es) to query. If you are using Logstash, by default the indexes will match ``"logstash-*"``.

``num_events``
    This parameter is specific to ``frequency`` type and is the threshold for when an alert is triggered.

``timeframe``
    Is the time period in which ``num_events`` must occur.

``filter``
    Is a list of Elasticsearch filters that are used to filter results. Here we have a single term filter for documents
    with ``some_field`` matching ``some_value``. See :ref:`Writing Filters For Rules <writing_filters>` for more
    information. If no filters are desired, it should be specified as an empty list: ``filter: []``

``alerters``
    Is a dictionary or list of dictionaries with the alerter as the key and the value is the configuration object for that alerter.
    For more information on alerters, see :ref:`Alerters <configure_alerters>`.
    The email alert requires an SMTP server for sending mail. By default, it will attempt to use localhost. This can be
    changed with the ``smtp_host`` option.

``alerters.email.to``
    Is a list of addresses to which alerts will be sent.

There are many other optional configuration options, see :ref:`Common configuration options <common_config>`.

All documents must have a timestamp field. Reactor will try to use ``@timestamp`` by default, but this can be changed
with the ``timestamp_field`` option. By default, Reactor uses ISO8601 timestamps, though unix timestamps are supported
by setting ``timestamp_type``.

As is, this rule means "Send an email to reactor@example.com when there are more than 50 documents with
``some_field == some_value`` within a 4 hour period."

Validating Your Rule
--------------------

Running the ``reactor validate my_rules/my_frequency.yaml`` command will validate that your global config is valid, then that all
specified rules are valid. This command **only** tests the *syntax* of the rule and will not, for example, test whether
your elasticsearch configuration valid

.. code-block:: console

    $ reactor validate my_rules/my_frequency.yaml
    2019-10-09 10:12:15,409 [reactor] INFO: All specified rules are valid


Testing Your Filter
-------------------

Running the ``reactor hits my_rules/my_frequency.yaml`` command will allow you to test the filter specified rule and will output the hits
returned by Elasticsearch that would be used by the Rule to test for matches

.. code-block:: console

    $ reactor hits my_rules/my_frequency.yaml
    @timestamp: 2019-10-08 10:48:49+00:00
    @version: 1
    _id: 69v8qm0BH2DWqbGHfdsy
    _index: logstash-2019.10.08
    _type: doc
    beat: {
        "hostname": "b7237550a185",
        "name": "b7237550a185",
        "version": "6.3.0"
    }
    some_field: some_value
    message: [08/Oct/2019:10:48:49 +0000] some_field=some_value
    tags: [
        "beats_input_codec_plain_applied"
    ]
    --------------------------------------------------------------------------------
    @timestamp: 2019-10-08 10:48:49+00:00
    @version: 1
    _id: 6Nv8qm0BH2DWqbGHfdsy
    _index: logstash_2b01573f-0048-492f-904e-22abb50500e4_2019.10.08
    _type: doc
    beat: {
        "hostname": "b7237550a185",
        "name": "b7237550a185",
        "version": "6.3.0"
    }
    some_field: some_value
    message: [08/Oct/2019:10:48:49 +0000] some_field=some_value
    }
    tags: [
        "beats_input_codec_plain_applied"
    ]
    --------------------------------------------------------------------------------


The number of hits output can be controlled using ``--max-hits <number>`` option (this defaults to the rule's ``max_query_size`` value if not set).

Testing Your Rule
-----------------

Running the ``reactor test my_rules/my_frequency.yaml`` command will test that your config file successfully loads and run it in debug mode over the last 24 hours

.. code-block:: console

    $ reactor test my_rules/my_frequency.yaml
    My Frequency Rule

    At least 50 events occurred between 2019-10-08 10:56:50 UTC and 2019-10-08 10:57:50 UTC

    @timestamp: 2019-10-08T10:57:50Z
    @version: 1
    _id: FdwEq20BH2DWqbGHwwRk
    _index: logstash_2b01573f-0048-492f-904e-22abb50500e4_2019.10.08
    _type: doc
    beat: {
        "hostname": "b7237550a185",
        "name": "b7237550a185",
        "version": "6.3.0"
    }
    some_field: some_value
    [08/Oct/2019:10:57:50 +0000] some_field=some_value
    tags: [
        "beats_input_codec_plain_applied"
    ]
    --------------------------------------------------------------------------------

If you want to specify a configuration file to use, you can run it with the config flag

.. code-block:: console

    $ reactor test --config <path-to-config-file> my_rules/my_frequency.yaml

The configuration preferences will be loaded as follows:
    1. Configurations specified in the yaml file.
    2. Configurations specified in the config file, if specified.
    3. Default configurations, for the tool to run.

See :ref:`the testing section for more details <testing>`

Running Reactor
------------------

There are two ways of invoking Reactor. As a daemon, through Supervisor (http://supervisord.org/), or directly with Python. For easier debugging purposes in this tutorial, we will invoke it directly

.. code-block:: console

    $ reactor run --verbose my_frequency.yaml  # or use the entry point: reactor run --verbose ...
    2019-10-09 12:09:32,347 [reactor] INFO: ElasticSearch version: (6, 3, 0)
    2019-10-09 12:09:32,347 [reactor] INFO: Starting up (max_processpool=3 cluster_size=1)
    2019-10-09 12:09:32,349 [reactor] INFO: Loading rules
    2019-10-09 12:09:32,405 [reactor] INFO: Cluster leader elected: ('localhost', '7000')
    2019-10-09 12:09:32,405 [reactor] INFO: Rule set updated: ['my_rules/my_frequency.yaml']
    2019-10-09 12:09:33,424 [reactor] INFO: Ran from 2019-10-09 10:09:32 UTC to 2019-10-09 11:09:32 UTC "My Frequency Rule": 5 query hits (0 already seen), 0 matches, 0 alerts sent (0 silenced)

Reactor uses the python logging system and ``--verbose`` sets it to display INFO level messages. ``my_frequency.yaml`` specifies the rule to run, otherwise Reactor will attempt to load the other rules in the my_rules folder.

Let's break down the response to see what's happening. ::

    2019-10-09 12:09:32,347 [reactor] INFO: ElasticSearch version: (6, 3, 0)

Reactor has detected the version of Elasticsearch it will writeback to. ::

    2019-10-09 12:09:32,347 [reactor] INFO: Starting up (max_processpool=3 cluster_size=1)

This line shows that that Reactor is starting up and will be running a process pool with 3 workers and 1 node in the cluster. ::

    2019-10-09 12:09:32,349 [reactor] INFO: Loading rules

This line means Reactor is attempting to load the rules for the first time. ::

    2019-10-09 12:09:32,405 [reactor] INFO: Cluster leader elected: ('localhost', '7000')

This line is showing you that Reactor has decided which node will be the leader (in this case, itself). ::

    2019-10-09 12:09:32,405 [reactor] INFO: Rule set updated: ['my_rules/my_frequency.yaml']

This line is showing you that set of rules that this node will be in charge of executing.
If you are running Reactor in a cluster this may be a subset of the total rules. ::

    2019-10-09 12:09:33,424 [reactor] INFO: Ran from 2019-10-09 10:09:32 UTC to 2019-10-09 11:09:32 UTC "My Frequency Rule": 5 query hits (0 already seen), 0 matches, 0 alerts sent (0 silenced)

This line means Reactor has finished processing the rule. For large time periods, if there are more than
``max_query_size`` hits, multiple queries will be performed using scrolling; the data will be processed together.
``query hits`` is the number of documents that are downloaded from Elasticsearch,
``already seen`` refers to documents that were already counted in a previous overlapping query and will be ignored,
``matches`` is the number of matches the rule type outputted,
``alerts sent`` is the number of alerts actually sent, and
``silenced`` is the number of alerts that were silenced (alerts sent - silenced = the number of new alerts).
Alerts sent may differ from matches because of options like ``realert`` and ``aggregation`` or because of an error.

The rule will be run again after duration specified by the ``run_every`` option which defaults to 5 minutes.

Say, over the next 297 seconds, 46 more matching documents were added to Elasticsearch::


    2019-10-09 12:14:33,424 [reactor] INFO: Sent email to ['reactor@example.com']
    ...
    2019-10-09 12:14:33,424 [reactor] INFO: Ran from 2019-10-09 10:14:32 UTC to 2019-10-09 11:14:32 UTC "My Frequency Rule": 51 query hits (5 already seen), 1 matches, 1 alerts sent (0 silenced)

The body of the email will contain something like::

    Example rule

    At least 50 events occurred between2019-10-09 10:14:32 UTC to 2019-10-09 11:14:32 UTC

    @timestamp: 2019-10-08T10:57:50Z

If an error occurred, such as an unreachable SMTP server, you may see: ::

    2019-10-09 12:14:33,424 [reactor] ERROR: Error while running alert email: Error connecting to SMTP host: [Errno 61] Connection refused

Note that if you stop Reactor and then run it again later, it will look up ``reactor_status`` and begin querying at the
end time of the last query. This is to prevent duplication or skipping of alerts if Reactor is restarted.

By using the ``--debug`` flag instead of ``--verbose``, the body of email will instead be logged and the email will not
be sent. In addition, the queries will not be saved to ``reactor_status``.

Stopping Reactor
------------------

To stop Reactor you should send a ``SIGINT`` signal to the main process, this can be done pressing ``^C`` on the
terminal. Reactor will then attempt a normal shutdown, which will entail:

- Waiting for raft to shutdown
- Removing all jobs from the background scheduler
- Waiting for any executing rules to complete

If you need to shutdown immediately, and unsafely, then you can do so by sending ``SIGINT`` three or more times. Doing
this runs the risk of alerts being fired more than once.

An example output from the logger would look like this:

.. code-block:: console

    ^C2019-10-09 12:09:54,101 [reactor] INFO: Attempting normal shutdown
    2019-10-09 12:09:54,102 [reactor] INFO: Waiting for raft to shutdown
    2019-10-09 12:09:54,102 [reactor] INFO: Removing jobs from scheduler
    2019-10-09 12:09:54,102 [reactor] INFO: Waiting for running jobs to complete (1)
    2019-10-09 12:09:54,109 [reactor] INFO: Shutdown complete!
    2019-10-09 12:09:54,156 [reactor] INFO: Goodbye

or for a force shutdown:

.. code-block:: console

    ^C2019-10-09 12:31:19,868 [reactor] INFO: Attempting normal shutdown
    2019-10-09 12:31:19,868 [reactor] INFO: Waiting for raft to shutdown
    2019-10-09 12:31:19,868 [reactor] INFO: Removing jobs from scheduler
    2019-10-09 12:31:19,869 [reactor] INFO: Waiting for running jobs to complete (1)
    ^C2019-10-09 12:31:20,140 [reactor] INFO: Attempting normal shutdown
    ^C2019-10-09 12:31:20,316 [reactor] CRITICAL: Terminating reactor

