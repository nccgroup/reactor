.. _reactor_commands:

Commands
========

Reactor requires Python 3.6+ to run. It provides multiple commands to help you initialise, validate, test, silence, and
run rules. All commands have the same basic way of being called:

``$ reactor <command>``

.. code-block:: console

    $ reactor --help
    usage: reactor [-h] {run,init,validate,test,hits,console,silence} ...

    optional arguments:
      -h, --help            show this help message and exit

    actions:
      {run,init,validate,test,hits,console,silence}
        run                 Run the reactor client
        init                Initialise the reactor indices and templates
        validate            Validate the specified rules
        test                Test the specified rules
        hits                Retrieve the hits for the specified rule
        console             Start the reactor console
        silence             Silence a set of rules

Run Reactor
-----------

``$ reactor run [rules [rules ...]]`` starts Reactor proper.

.. code-block:: console

    $ reactor run --help
    usage: reactor run [-h] [-c my_config.yaml]
                       [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                       [--patience units=val] [--start YYYY-MM-DDTHH:MM:SS]
                       [--end YYYY-MM-DDTHH:MM:SS]
                       [--reload units=val | --pin-rules] [--debug | --verbose]
                       [--es-debug] [--es-debug-trace FILENAME]
                       [rules [rules ...]]

    positional arguments:
      rules                 Limit running to the specified rules

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level
      --patience units=val  Maximum time to wait for ElasticSearch to become
                            responsive (e.g. seconds=30)
      --start YYYY-MM-DDTHH:MM:SS
                            Start querying from this timestamp
      --end YYYY-MM-DDTHH:MM:SS
                            Stop querying after timestamp
      --reload units=val    How frequently to poll configuration location for
                            changes (e.g. seconds=3)Overrides the config setting
      --pin-rules           Stop Alerter from monitoring for rule changes
      --debug               Suppresses alerts and prints information instead
      --verbose             Increase verbosity without suppressing alerts
      --es-debug            Enabled verbose logging from ElasticSearch queries
      --es-debug-trace FILENAME
                            Log ElasticSearch queries as curl commands in
                            specified file

There is one positional argument when running Reactor:

    :rules:                 will only run the given rules. The rule file may be a complete file path or a filename
                            in ``rules_folder`` or its subdirectories.

Several optional arguments are available when running Reactor:

    --config                will specify the configuration file to use. The default is ``config.yaml``.
    --debug                 will run Reactor in debug mode. This will increase the logging verboseness, change all alerts
                            to ``DebugAlerter``, which prints alerts and suppresses their normal action, and skips writing
                            search and alert metadata back to Elasticsearch. Not compatible with `--verbose`.
    --verbose               will increase the logging verboseness, which allows you to see information about the state
                            of queries. Not compatible with `--debug`.
    --start <timestamp>     will force Reactor to begin querying from the given time, instead of the default,
                            querying from the present. The timestamp should be ISO8601, e.g.  ``YYYY-MM-DDTHH:MM:SS``
                            (UTC) or with timezone ``YYYY-MM-DDTHH:MM:SS-08:00`` (PST). Note that if querying over a
                            large date range, no alerts will be sent until that rule has finished querying over the
                            entire time period. To force querying from the current time, use "NOW".
    --end <timestamp>       will cause Reactor to stop querying at the specified timestamp. By default, Reactor
                            will periodically query until the present indefinitely.
    --es_debug              will enable logging for all queries made to Elasticsearch.
    --es_debug_trace <trace.log>    will enable logging curl commands for all queries made to Elasticsearch to the
                                    specified log file. ``--es_debug_trace`` is passed through to `elasticsearch.py
                                    <http://elasticsearch-py.readthedocs.io/en/master/index.html#logging>`_ which
                                    logs `localhost:9200` instead of the actual ``elasticsearch.host``:``elasticsearch.port``.
    --end <timestamp>       will force Reactor to stop querying after the given time, instead of the default, querying
                            to the present time. This really only makes sense when running standalone. The timestamp is
                            formatted as ``YYYY-MM-DDTHH:MM:SS`` (UTC) or with timezone ``YYYY-MM-DDTHH:MM:SS-XX:00``
                            (UTC-XX).
    --pin_rules             will stop Reactor from loading, reloading or removing rules based on changes to their config files.

Silence a rule
---------------

``$ reactor silence`` allows you to silence one or more rules for a specified duration.

.. code-block:: console

    $ reactor silence --help
    usage: reactor silence [-h] [-c my_config.yaml]
                           [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                           [--patience units=val]
                           [--duration units=val | --revoke]
                           rules [rules ...]

    positional arguments:
      rules                 List of rules to silence

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level
      --patience units=val  Maximum time to wait for ElasticSearch to become
                            responsive (e.g. seconds=30)
      --duration units=val  Duration to silence rule for (e.g. hours=1)
      --revoke              Revoke all silences on the specified rules

Several optional arguments are available using the silence command:

    - ``--config`` will specify the configuration file to use. The default is ``config.yaml``.
    - ``--log-level`` will set the logging level to be used
    - ``--patience <unit>=<number>`` will set the maximum time to wait for Elasticsearch to become responsive.
    - ``--duration <unit>=<number>`` will silence the alerts for a given rule for a period of time. <unit> is one of days,
      weeks, hours, minutes or seconds. <number> is an integer. For example, ``--rule noisy_rule.yaml --silence hours=4``
      stop noisy_rule from generating any alerts for 4 hours.


There is one positional argument when using the silence command:

    - ``(<rule.yaml>( <rule.yaml>)*)?`` will only run the given rules. The rule file may be a complete file path or a filename
      in ``rules_folder`` or its subdirectories.

Initialise Reactor
------------------

``$ reactor init`` allows you to initialise the indices and templates used by Reactor.

.. code-block:: console

    $ reactor init --help
    usage: reactor init [-h] [-c my_config.yaml]
                        [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                        [--patience units=val] [-m MAPPINGS_DIR] [--recreate] [-f]
                        [--old-index OLD_INDEX]

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level
      --patience units=val  Maximum time to wait for ElasticSearch to become
                            responsive (e.g. seconds=30)
      -m MAPPINGS_DIR, --mappings MAPPINGS_DIR
                            Path to the directory containing the mapping JSON
                            files
      --recreate            Recreated the indices and template
      -f, --force           Force recreation of indices (no user prompt)
      --old-index OLD_INDEX
                            Name of the old index to copy the data across from

Validate a rule
------------------

``$ reactor validate`` allows you to validate a rule configuration.

.. code-block:: console

    $ reactor validate --help
    usage: reactor validate [-h] [-c my_config.yaml]
                            [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                            rules [rules ...]

    positional arguments:
      rules                 List of rules to validate

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level

Test a rule
------------------

``$ reactor test`` allows you to test a rule.

.. code-block:: console

    $ reactor test --help
    usage: reactor test [-h] [-c my_config.yaml]
                        [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                        [--patience units=val] [--max-hits [1..10000]]
                        [--timeframe units=val] [--start YYYY-MM-DDTHH:MM:SS]
                        [--output stdout|stderr|devnull|FILENAME]
                        [--format {plain,json}]
                        rules [rules ...]

    positional arguments:
      rules                 List of rules to test

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level
      --patience units=val  Maximum time to wait for ElasticSearch to become
                            responsive (e.g. seconds=30)
      --max-hits [1..10000]
                            Maximum number of hits to retrieve
      --timeframe units=val
                            Limit the query to a timeframe (e.g. hours=24)
      --start YYYY-MM-DDTHH:MM:SS
                            Start querying from this timestamp
      --output stdout|stderr|devnull|FILENAME
                            Where to output the alerts
      --format {plain,json}
                            Format to output the alerts

Get rule hits
------------------

``$ reactor hits`` allows you to retrieve the hits of a rule filter.

.. code-block:: console

    $ reactor hits --help
    usage: reactor hits [-h] [-c my_config.yaml]
                        [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                        [--patience units=val] [--max-hits [1..10000]]
                        [--timeframe units=val] [--start YYYY-MM-DDTHH:MM:SS]
                        [--output stdout|stderr|devnull|FILENAME]
                        [--format {plain,json}] [--counts]
                        rule

    positional arguments:
      rule                  The rule to retrieve hits

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level
      --patience units=val  Maximum time to wait for ElasticSearch to become
                            responsive (e.g. seconds=30)
      --max-hits [1..10000]
                            Maximum number of hits to retrieve
      --timeframe units=val
                            Limit the query to a timeframe (e.g. hours=24)
      --start YYYY-MM-DDTHH:MM:SS
                            Start querying from this timestamp
      --output stdout|stderr|devnull|FILENAME
                            Where to output the alerts
      --format {plain,json}
                            Format to output the alerts
      --counts              Only report on the number of hits

Console command
------------------

``$ reactor console`` provides a basic curses view of Reactor. It provides command line access to viewing reactor
indices and should be used for debugging purposes by developers or administrators.

.. code-block:: console

    $ reactor console --help
    usage: reactor console [-h] [-c my_config.yaml]
                           [-l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}]
                           [--patience units=val] [--suppress]
                           [-i {alerts,error,silence,status}] [-r REFRESH]
                           [--max-hits [1..]]

    optional arguments:
      -h, --help            show this help message and exit
      -c my_config.yaml, --config my_config.yaml
                            Global config file
      -l {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}, --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                            Set the logging level
      --patience units=val  Maximum time to wait for ElasticSearch to become
                            responsive (e.g. seconds=30)
      --suppress            Disable warnings from urllib3
      -i {alerts,error,silence,status}, --index {alerts,error,silence,status}
                            Index to retrieve hits from
      -r REFRESH, --refresh REFRESH
                            Number of seconds between automatic refresh
      --max-hits [1..]      Maximum number of hits to retrieve

Several optional arguments are available when running Reactor console:

    --config <my_config.yaml>   will specify the configuration file to use. The default is ``config.yaml`` found in the
                                current working directory.
    --patience <units=value>    will specify the duration to wait for Elasticsearch to become responsive.
    --suppress                  will specify whether to disable urllib3 warnings.
    --index <index>             will specify the starting view of console. The default is ``indices``.
    --refresh <seconds>         will specify number of seconds between automatic refresh of the current view. The
                                default is not have automatic refresh.
    --max-hits <integer>        will specify the maximum number of hits that will be displayed by the console.

Keyboard commands
^^^^^^^^^^^^^^^^^

Reactor console is controlled by keyboard commands. Below is a complete list of the commands:

.. table::
    :widths: 25 75

    +---------------+--------------------------------------------------------------------------------------------------+
    |   Switch views                                                                                                   |
    +===============+==================================================================================================+
    | **Key**       | **Action**                                                                                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``i``         | Switch to view reactor indices                                                                   |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``a``         | Switch to view ``reactor_alerts`` (see global ``writeback_alias`` option)                        |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``e``         | Switch to view ``reactor_error`` (see global ``writeback_index`` option)                         |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``s``         | Switch to view ``reactor_silence`` (see global ``writeback_index`` option)                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``t``         | Switch to view ``reactor_status`` (see global ``writeback_index`` option)                        |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``r``         | Refresh the current view immediately                                                             |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``ESC``       | Switch from item view to item index view or from index view to indices view                      |
    +---------------+--------------------------------------------------------------------------------------------------+

.. table::
    :widths: 25 75

    +---------------+--------------------------------------------------------------------------------------------------+
    |   Selecting a row                                                                                                |
    +===============+==================================================================================================+
    | **Key**       | **Action**                                                                                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``1..9``      | Start to enter a row number to select (**note** disables auto refresh)                           |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``BACKSPACE`` | Delete the last entered digit of the line number                                                 |
    +---------------+                                                                                                  |
    | ``DEL``       |                                                                                                  |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``.``         | Select the current specified row                                                                 |
    +---------------+                                                                                                  |
    | ``Enter``     |                                                                                                  |
    +---------------+--------------------------------------------------------------------------------------------------+

.. table::
    :widths: 25 75

    +---------------+--------------------------------------------------------------------------------------------------+
    |   Scrolling table                                                                                                |
    +===============+==================================================================================================+
    | **Key**       | **Action**                                                                                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_HOME``  | Scroll the view to the first page                                                                |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_LEFT``  | Scroll the view to the previous page                                                             |
    +---------------+                                                                                                  |
    | ``KEY_PPAGE`` |                                                                                                  |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_RIGHT`` | Scroll the view to the next page                                                                 |
    +---------------+                                                                                                  |
    | ``KEY_NPAGE`` |                                                                                                  |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_END``   | Scroll the view to the last page                                                                 |
    +---------------+--------------------------------------------------------------------------------------------------+

.. table::
    :widths: 25 75

    +---------------+--------------------------------------------------------------------------------------------------+
    |   Scrolling item                                                                                                 |
    +===============+==================================================================================================+
    | **Key**       | **Action**                                                                                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_HOME``  | Scroll the view to the top                                                                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_UP``    | Scroll the view up                                                                               |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_DOWN``  | Scroll the view down                                                                             |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``KEY_END``   | Scroll the view to the bottom                                                                    |
    +---------------+--------------------------------------------------------------------------------------------------+

.. table::
    :widths: 25 75

    +---------------+--------------------------------------------------------------------------------------------------+
    |   Exiting Console                                                                                                |
    +===============+==================================================================================================+
    | **Key**       | **Action**                                                                                       |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``q``         | Quit the console                                                                                 |
    +---------------+--------------------------------------------------------------------------------------------------+
    | ``^C``        | Quit the console (if pressed 3 times then will call system exit)                                 |
    +---------------+--------------------------------------------------------------------------------------------------+
