Introduction to Reactor
***********************

Reactor is a simple framework for alerting on anomalies, spikes, or other patterns of interest from data in Elasticsearch.

Elasticsearch, Logstash and Kibana is used for managing our ever increasing amount of data and logs.
Kibana is great for visualizing and querying data, but we quickly realized that it needed a companion tool for alerting
on inconsistencies in our data. Out of this need, Reactor was created.

If you have data being written into Elasticsearch in near real time and want to be alerted when that data matches certain
patterns, Reactor is the tool for you.

Overview
========

We designed Reactor to be :ref:`reliable <reliability>`; highly :ref:`modular <modularity>`;
horizontal :ref:`scalability <scalability>` through clustering; and, easy to :ref:`set up <tutorial>` and
:ref:`configure <configuration>`.

It works by combining Elasticsearch with two main components, rule types and alerters.
There are two additional components of Reactor: :ref:`Notifiers <notifiers>` and :ref:`Plugins <plugins>`.
Elasticsearch is periodically queried and the data is passed to the rule, which determines when
a match is found. When a match occurs, it is given to one or more alerters, which take action based on the match.

This is configured by a set of rules, each of which defines a query, a rule type, and a set of alerters.

Several rule types with common monitoring paradigms are included with Reactor:

- "Match where there are X events in Y time" (``frequency`` type)
- "Match when the rate of events increases or decreases" (``spike`` type)
- "Match when there are less than X events in Y time" (``flatline`` type)
- "Match when a certain field matches a blacklist/whitelist" (``blacklist`` and ``whitelist`` type)
- "Match on any event matching a given filter" (``any`` type)
- "Match when a field has two different values within some time" (``change`` type)

Currently, we have support built in for these alert types:

- Command
- Debug
- Email
- Webhook

Additional rule types and alerters can be easily imported or written. (See :ref:`Adding a new Rule <rules>`
and :ref:`Add a new Alerter <alerters>`)

In addition to this basic usage, there are many other features that make alerters more useful:

- Alerts link to Kibana dashboards
- Aggregate counts for arbitrary fields
- Combine alerts into periodic reports
- Separate alerts by using a unique key field
- Intercept and enhance match data
- Intercept and enhance alerts

To get started, check out :ref:`Running Reactor For The First Time <tutorial>`.

.. _reliability:

Reliability
===========

Reactor has several features to make it more reliable in the event of restarts or Elasticsearch unavailability:

- Reactor :ref:`saves its state to Elasticsearch <metadata>` and, when started, will resume where previously stopped
- If Elasticsearch is unresponsive, Reactor will wait until it recovers before continuing
- Alerts which throw errors may be automatically retried for a period of time

.. _modularity:

Modularity
==========

Reactor has five main components that may be imported as a module or customized:

Rules
-----

The rule is responsible for processing the data returned from Elasticsearch. It is initialized with the rule
configuration, passed data that is returned from querying Elasticsearch with the rule's filters, and outputs matches
based on this data. See :ref:`Adding a new Rule <rules>` for more information.

Alerters
--------

Alerters are responsible for taking action based on a match. A match is a pair: the first element is the ``match_data``,
a dictionary extra of data added by the rule; and, the second element is the ``match_body`` a dictionary containing the
values from a Elasticsearch document (certain rules, like Flatline or Cardinality, generate matches based on no hits
and so sometimes this is an empty dictionary).
See :ref:`Adding a new Alerter <alerters>` for more information.

Enhancements
------------

Enhancements are a way of intercepting an alert and modifying/enhancing it in some way. Enhancements are passed the
alert body (see :py:meth:`Rule.get_alert_body`) before it is given the the alerter(s).
See :ref:`Enhancements` for more information.

Plugins
-------

Plugins provide a way of adding functionality to run alongside Reactor. See :ref:`Adding a new Plugin <plugins>` for
more information.

Notifiers
---------

Notifiers provide a mechanism to alerting Reactor administrators of uncaught exceptions raised by rules.
See :ref:`Adding a new Notifier <notifiers>` for more information.

.. _scalability:

Scalability
===========

Reactor has horizontal scalability out of the box that is easy to configure and increase as and when needed. Reactor uses
part of the `RAFT consensus algorithm <https://raft.github.io/>`_ to perform leadership election between Reactor nodes in
a cluster. The leader then uses a deterministic and stable algorithm to distribute the set of rules across the nodes. Each
node then has a configurable ``ProcessPool`` to spread the execution of rules over multiple processors. Furthermore, for
each rule, Reactor caches a configurable ``writeback_flush`` length list of alerts and uses Elasticsearch's bulk API to
further improve performance.

To configure the size of the ``ProcessPool`` used by Reactor on a particular node use the ``max_processpool`` option, Reactor
defaults to ``multiprocessing.cpu_count`` the option is not specified::

    max_processpool: 10

To configure the maximum size of the writeback cache used by individual rules use the ``writeback_flush`` option, Reactor
defaults to ``1000`` if not specified::

    writeback_flush: 1000

Cluster
-------

Reactor provides a easy to use way to configure a horizontally scalable cluster of nodes by implementing the leadership
election part of the `RAFT consensus algorithm <https://raft.github.io/>`_ to quickly and stably elect a leader amongst
the Reactor nodes. These nodes then pass additional information between themselves like their ``max_processpool`` to
provide the leader with the best information to determine how to distribute the rules across the cluster.

To configure a cluster, each node must have the ``cluster`` option::

    cluster:
      host: "node1.reactor:7000"
      neighbours: ["node1.reactor:7000", "node2.reactor:7000", "node3.reactor:7000"]

where ``host`` is the ``hostname`` and ``port`` that the other nodes can reach that node, and, ``neighbours`` is a list
of members of the cluster which can optionally include ``host``.

Cluster can be of any size, though it is recommend that is an odd number of nodes. A leader can only be elected if there
is a majority of responsive neighbours; a leader can only maintain leadership if a majority of neighbour remain responsive;
a leader will only start distributing rules after it has heard from **all** neighbours at least once.

.. _configuration:

Configuration
=============

Reactor has a global configuration file, ``config.yaml``, which defines several aspects of its operation:

``elasticsearch.host``: The host name of the Elasticsearch cluster where Reactor records metadata about its searches.
When Reactor is started, it will query for information about the time that it was last run. This way,
even if Reactor is stopped and restarted, it will never miss data or look at the same events twice. It will also specify
the default cluster for each rule to run on.
.. The environment variable ``ES_HOST`` will override this field.

``elasticsearch.port``: The port corresponding to ``elasticsearch.host``.
.. The environment variable ``ES_PORT`` will override this field.

``elasticsearch.ssl.enabled``: Optional; whether or not to connect to ``elasticsearch.host`` using TLS; set to ``True`` or ``False``.
.. The environment variable ``ES_USE_SSL`` will override this field.

``elasticsearch.ssl.verify_certs``: Optional; whether or not to verify TLS certificates; set to ``True`` or ``False``. The default is ``True``.

``elasticsearch.ssl.client_cert``: Optional; path to a PEM certificate to use as the client certificate.

``elasticsearch.ssl.client_key``: Optional; path to a private key file to use as the client key.

``elasticsearch.ssl.ca_certs``: Optional; path to a CA cert bundle to use to verify SSL connections

``elasticsearch.username``: Optional; basic-auth username for connecting to ``elasticsearch.host``.
.. The environment variable ``ES_USERNAME`` will override this field.

``elasticsearch.password``: Optional; basic-auth password for connecting to ``elasticsearch.host``.
.. The environment variable ``ES_PASSWORD`` will override this field.

``elasticsearch.url_prefix``: Optional; URL prefix for the Elasticsearch endpoint.
.. The environment variable ``ES_URL_PREFIX`` will override this field.

``elasticsearch.send_get_body_as``: Optional; Method for querying Elasticsearch - ``GET``, ``POST`` or ``source``.
The default is ``GET``

``elasticsearch.conn_timeout``: Optional; sets timeout for connecting to and reading from ``elasticsearch.host``;
defaults to ``20``.

``elasticsearch.aws_region``: This makes Reactor to sign HTTP requests when using Amazon Elasticsearch Service. It'll
use instance role keys to sign the requests.
.. The environment variable ``AWS_DEFAULT_REGION`` will override this field.

``elasticsearch.profile``: AWS profile to use when signing requests to Amazon Elasticsearch Service, if you don't want
to use the instance role keys.
.. The environment variable ``AWS_DEFAULT_PROFILE`` will override this field.


``writeback_index``: Optional; The index on ``elasticsearch.host`` to use. Defaults to ``reactor``.

``alert_alias``: Optional; The alias for all alert indices. Defaults to ``reactor_alerts``.

``writeback_flush``: Optional; The maximum number of writeback actions per rule to cache before flushing.
Defaults to ``1000``.

``max_processpool``: Optional; The maximum number of processes to rule rules (capped between 1 and CPU count).
Defaults to ``multiprocessing.cpu_count()``.


``cluster.host``: Optional (required for clustering); The hostname and port in the form ``<hostname>:<port>`` which the
other cluster nodes can reach this node.

``cluster.neighbours``: Optional (required for clustering); A list of cluster node's hostname and port which all cluster
nodes can reach. Can include the this node.


``alert_time_limit``: Optional; The time limit that for an alert to be sent. The default is 2 days.

``old_query_limit``: Optional; The maximum time between queries for Reactor to start at the most recently run query.
When Reactor starts, for each rule, it will search ``reactor_metadata`` for the most recently run query and start
from that time, unless it is older than ``old_query_limit``, in which case it will start from the present time.
The default is one week.

``max_aggregation``: Optional; The maximum number of alerts to aggregate together. If a rule has ``aggregation`` set,
all alerts occurring within a timeframe will be sent together. The default is 10,000.

``string_multi_field_name``: Optional; If set, the suffix to use for the subfield for string multi-fields in Elasticsearch.
The default value is ``.keyword``.


``reload``: Optional; The duration to wait between checking for rule configuration changes. The default is 3 seconds.

``resend``: Optional; The duration to wait between attempting to send pending alerts. The default is 30 seconds.


``loader.type``: Optional; sets the loader class to be used by Reactor to retrieve rules and hashes.
Defaults to ``FileRulesLoader`` if not set.

``loader.config``: Dictionary configuration for the specified loader.

``loader.config.rules_folder``: The name of the folder which contains rule configuration files. Reactor will load all
files in this folder, and all subdirectories, that end in .yaml. If the contents of this folder change, Reactor will
load, reload or remove rules based on their respective config files. (only required when using ``FileRulesLoader``).

``loader.conf.scan_subdirectories``: Optional; Sets whether or not Reactor should recursively descend the rules directory - ``true``
or ``false``. The default is ``true`` (only required when using ``FileRulesLoader``).


``rule.buffer_time``: Reactor will continuously query against a window from the present to ``buffer_time`` ago.
This way, logs can be back filled up to a certain extent and Reactor will still process the events. This
may be overridden by individual rules. This option is ignored for rules where ``use_count_query`` or ``use_terms_query``
is set to true. Note that back filled data may not always trigger count based alerts as if it was queried in real time.

``rule.run_every``: How often Reactor should query Elasticsearch. Reactor will remember the last time it ran the query for a
given rule, and periodically query from that time until the present. The format of this field is a nested unit of time,
such as ``minutes: 5``. This is how time is defined in every Reactor configuration.

``rule.max_query_size``: The maximum number of documents that will be downloaded from Elasticsearch in a single query. The
default is 10,000, and if you expect to get near this number, consider using ``use_count_query`` for the rule. If this
limit is reached, Reactor will `scroll <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html>`_
using the size of ``max_query_size`` through the set amount of pages, when ``max_scrolling_count`` is set or until
processing all results.

.. TODO these are out of date! Check with latest ElastAlert

``max_scrolling_count``: The maximum amount of pages to scroll through. The default is ``0``, which means the scrolling
has no limit. For example if this value is set to ``5`` and the ``rule.max_query_size`` is set to ``10000`` then ``50000``
documents will be downloaded at most.

``rule.scroll_keepalive``: The maximum time (formatted in `Time Units <https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units>`_)
the scrolling context should be kept alive. Avoid using high values as it abuses resources in Elasticsearch, but be
mindful to allow sufficient time to finish processing all the results.

``rule.disable_rule_on_error``: If true, Reactor will disable rules which throw uncaught (not EAException) exceptions. It
will upload a traceback message to ``reactor_metadata`` and if ``notify_email`` is set, send an email notification. The
rule will no longer be run until either Reactor restarts or the rule file has been modified. This defaults to True.

.. TODO these are out of date! Check with latest ElastAlert

``show_disabled_rules``: If true, Reactor show the disable rules' list when finishes the execution. This defaults to True.


``notifiers``: Optional; A dictionary of notifier names to configuration.
For more information see :ref:`Notifiers <configure_notifiers>`


``plugins``: Optional; A dictionary of plugin names to configuration.
For more information see :ref:`Plugins <configure_plugins>`


``mappings``: Optional; A dictionary of mappings from human friendly name to ``package.file.ClassName``. This is used by
the loader to import the correct class. For example, to add a human friendly version ``awesome`` of a custom rule found
in ``reactor_modules.my_rules.AwesomeRule``:

.. code-block:: yaml

    mappings:
      rule:
        awesome: reactor_modules.my_rules.AwesomeRule


.. _configure_notifiers:

Notifiers
---------

Notifiers are used by Reactor to notify administrators of critical events that require their attention. Specifically,
if a rule raises an uncaught exception. By default, Reactor only logs notifications.

Reactor provides one notifier called ``email`` (``EmailNotifier``) which uses the same configuration as
:ref:`Email Alerter <configure_alerters_email>`. To enable the email notifier simply add the ``notifiers`` option to the
global configuration file:

.. code-block:: yaml

    notifiers:
      email:
        to: admin@example.com


.. _configure_plugins:

Plugins
-------

Plugins are used by Reactor to provide additional functionality which runs alongside Reactor. By default, Reactor does
not have any plugins enabled.

Reactor provides one plugin called ``http_server`` (``HttpServerPlugin``) which creates a threading HTTP server that
listens to port 7100 (can be configured using the ``http_server.port`` option) for ``GET /`` requests and returns a
JSON encoded response, e.g.:

.. code-block:: json

    {
      "up_time": 263.042096626,
      "cluster": {
        "size": 3,
        "leader": "node1.reactor.local:7000",
        "neighbourhood": [
          "node1.reactor.local:7000",
          "node2.reactor.local:7000",
          "node3.reactor.local:7000"
        ],
        "changed": 262.4381868839263916
      },
      "rules": [
        {
          "locator": "my_rules/my_frequency.yaml",
          "running": false,
          "time_taken": 0.2722489833831787
        }
      ]
    }

where:
``up_time`` is the monotonic time since Reactor started;
``cluster.size`` is the number of nodes in the cluster;
``cluster.leader`` is the currently elected cluster leader;
``cluster.neighbourhood`` is the list of all nodes in the cluster;
``cluster.changed`` is the time since this node last changed state;
``rules`` is a list of all loaded and enabled rules running on this node;
``rules.*.locator`` is the rule locator;
``rules.*.running`` is boolean of whether the rule is running;
``rules.*.time_taken`` is the time in seconds that the rule last took to run.

Logging
-------

.. TODO this is out of date. Check with ElastAlert!

By default, Reactor uses a simple basic logging configuration to print log messages to standard error.
You can change the log level to ``INFO`` messages by using the ``--verbose`` or ``--debug`` command line options.

If you need a more sophisticated logging configuration, you can provide a full logging configuration
in the config file. This way you can also configure logging to a file, to Logstash and
adjust the logging format.

For details, see the end of ``config.yaml.example`` where you can find an example logging
configuration.


