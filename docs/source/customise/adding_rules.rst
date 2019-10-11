.. _rules:

Adding a new Rule
=================

This document describes how to create a new rule type. Built in rule types live in ``reactor/rule.py`` and are
subclasses of :py:class:`Rule`. At the minimum, your rule needs to also subclass ``AcceptsHitsDataMixin`` which requires
that ``add_hits_data`` is implemented.

Your class may implement several functions from :py:class:`Rule`:

.. code-block:: python

    class AwesomeNewRule(AcceptsHitsDataMixin, Rule):
        # ...
        def add_hits_data(self, data: list):
            # ...
        def get_match_str(self, extra: dict, match: dict):
            # ...
        def garbage_collect(self, timestamp: datetime.datetime):
            # ...

You can import new rule types by specifying the type as ``module.file.RuleName``, where module is the name of a Python
module, or folder containing ``__init__.py``, and file is the name of the Python file containing a :py:class:`Rule`
subclass named ``RuleName``.

Basics
------

The :py:class:`Rule` instance remains in memory while Reactor is running, receives data, keeps track of its state, and
generates matches. Several important member properties are created in the ``__init__`` method of :py:class:`Rule`:

.. py:attribute:: Rule._schema_file

    The relative path from ``cls._schema_relative`` (defaults to ``reactor.rule.py``) to the configuration schema
    file. The file should be in a ``.yaml`` format and should describe all options required and optional for using the
    Rule using the `JSON Schema Draft 7 specification <https://json-schema.org/specification-links.html#draft-7>`_.
    Reactor will validate all rule configurations that specify this rule *before* instantiation.

.. py:attribute:: Rule._schema_relative

    The absolute path that ``cls._schema_file`` is relative to (defaults to ``reactor.rule.py``).


.. py:attribute:: Rule._conf

    This dictionary is loaded from the rule configuration file. If there is a ``timeframe`` configuration option, this
    will be automatically converted to a ``datetime.timedelta`` object when the rules are loaded. Values from this
    dictionary can be accessed via ``Rule.conf()`` and set via ``Rule.set_conf()``.

.. py:attribute:: Rule._data

    This is an instance of ``WorkerData`` found in ``reactor/rule.py`` and should contain all the working data required
    for the rule to efficiently be executed again. All the values contained **MUST** be able to be pickled as this is
    passed between processes.


.. py:method:: Rule.add_hits_data(self, data: list):

    When Reactor queries Elasticsearch, it will pass all of the hits to the rule type by calling ``add_hits_data``.
    ``data`` is a list of dictionary objects which contain all of the fields in ``include``, ``query_key`` and ``compare_key``
    if they exist, and ``@timestamp`` as a datetime object. They will always come in chronological order sorted by ``@timestamp``.

    Whilst processing the data provided the Rule must ``yield`` all matches as they are encountered. This allows Reactor to
    immediately alert and feedback to the Rule information such as silences.

    Any information that is relevant to the matching event (generally coming from the field in Elasticsearch) should be put
    in the ``event`` dictionary object and any information relevant to the match should be put in the ``extra`` dictionary
    object, for example:

    .. code-block:: python

        def add_hits_data(self, data: list) -> Generator[dict, None, None]:
            for event in data:
                if self.is_a_match(event):
                    extra = {'reason': self.get_last_reason(),
                             'num_events': self.get_num_events(),
                             'began_at': ts_to_dt(dots_get(self.get_first_event(), self._data.ts_field)),
                             'ended_at': ts_to_dt(dots_get(event, self._data.ts_field)))
                    yield self.add_match(extra, event)

    It is recommended to use ``self.add_match(extra, event)`` during the yield as it increments the number of matches
    counter store in ``self._data`` and will convert the datetime ``@timestamp`` back into a ISO08601 timestamp.
    It is also recommended that ``extra`` include the three properties:

    - ``num_events``: The number of events that it took to trigger this match
    - ``began_at``: The timestamp of the first event
    - ``ended_at``: The timestamp of the last event

.. py:method:: Rule.get_match_str(self, extra: dict, match: dict)

    Alerters will call this function to get a human readable string about a match for an alert. Extra and match will be the
    same objects yielded by your Rule, however they may have been altered by enhancements. The :py:class:`Rule` base implementation
    will return an empty string. Note that by default, the alert text will already contain the key-value pairs from the match.
    This should return a string that gives some information about the match in the context of this specific Rule.

.. py:method:: Rule.garbage_collect(self, timestamp: datetime.datetime)

    This will be called after Reactor has run over a time period ending in ``timestamp`` and should be used to clear any
    state that may be obsolete as of ``timestamp``. ``timestamp`` is a datetime object.


Tutorial
--------

As an example, we are going to create a rule type for detecting suspicious logins. Let's imagine the data we are querying
is login events that contains IP address, username and a timestamp. Our configuration will take a list of usernames and
a time range and alert if a login occurs in the time range. First, create a file in the ``reactor_modules`` folder created
in the :ref:`customise_prerequisites` called ``schema-ruletype-awesome.yaml``:

.. code-block:: yaml

    ---
    $schema: http://json-schema.org/draft-07/schema#
    definitions: {}

    title: My Rule
    type: object
    required: [time_start, time_end, usernames]

    properties:
        type: {enum: [my_rule, reactor_modules.my_rules.AwesomeRule]}
        time_start:
            type: string
            format: time
        time_end:
            type: string
            format: time
        usernames:
            type: array
            items: {type: string}
    ...

This will be used to ensure that any rule configuration used is valid before MyRule is instantiated.
Now, in a file named ``my_rules.py``, add

.. code-block:: python

    import dateutil.parser

    from reactor.rule import Rule, AcceptsHitsDataMixin

    # reactor.util includes useful utility functions
    # such as converting from timestamp to datetime obj
    # and searching through elasticsearch documents using dots notation
    from reactor.util import ts_to_dt, dots_get


    class AwesomeRule(AcceptsHitsDataMixin, Rule):

        # By setting _schema_file and _schema_relative you can ensure that
        # the rule config file specifies all of the options and they are
        # valid. Otherwise, Reactor will throw an exception when trying to
        # load the rule
        _schema_file = 'schema-ruletype-awesome.yaml'
        _schema_relative = __file__

        # add_hits_data will be called each time Elasticsearch is queried.
        # data is a list of documents from Elasticsearch, sorted by timestamp,
        # including all the fields that the config specifies with "include"
        def add_hits_data(self, data: list):
            for document in data:

                # To access config options, use self.rules
                if dots_get(document, 'data.src_user') in self.conf('usernames'):

                    # Convert the timestamp to a time object
                    login_time = document['@timestamp'].time()

                    # Convert time_start and time_end to time objects
                    time_start = dateutil.parser.parse(self.conf('time_start')).time()
                    time_end = dateutil.parser.parse(self.conf('time_end')).time()

                    # If the time falls between start and end
                    if time_start < login_time < time_end:

                        # Create some extra data about the match
                        extra = {'num_events': 1,
                                 'began_at': ts_to_dt(dots_get(document, self.data.ts_field)),
                                 'ended_at': ts_to_dt(dots_get(document, self.data.ts_field))}
                        # To add a match, use self.add_match
                        yield self.add_match(extra, document)

        # The results of get_match_str will appear in the alert text
        def get_match_str(self, extra: dict, match: dict):
            return "%s logged in between %s and %s" % (dots_get(match, 'data.src_user'),
                                                       self.conf('time_start'),
                                                       self.conf('time_end'))

        # garbage_collect is called indicating that Reactor has already been run up to timestamp
        # It is useful for knowing that there were no query results from Elasticsearch because
        # add_hits_data will not be called with an empty list
        # It is possible for matches to be detected during garbage collection but in our case this
        # will never happen so an empty generator is returned
        def garbage_collect(self, timestamp):
            yield from ()


In the rule configuration file, ``rules/example_login_rule.yaml``, we are going to specify this rule by writing:

.. code-block:: yaml

    name: "Example login rule"
    index: logstash-*
    elasticsearch:
      host: elasticsearch.example.com
      port: 14900
    type: "reactor_modules.my_rules.AwesomeRule"
    # Alert if admin, userXYZ or foobaz log in between 8 PM and midnight
    time_start: "20:00"
    time_end: "23:59:59"
    usernames:
    - "admin"
    - "userXYZ"
    - "foobaz"
    # We require the username field from documents
    include:
    - "username"
    alerters:
    - debug: {}

Reactor will attempt to import the rule with ``from reactor_modules.my_rules import AwesomeRule``.
This means that the folder must be in a location where it can be imported as a Python module.

An alert from this rule will look something like::

    Example login rule

    userXYZ logged in between 20:00 and 24:00

    @timestamp: 2015-03-02T22:23:24Z
    username: userXYZ

If you were going to use your new rule a lot you could add a rule mapping to the global configuration file:

.. code-block:: yaml

    mappings:
      rule:
        awesome: reactor_modules.my_rules.AwesomeRule

which would allow you to use the human friendly, and shorter ``type: awesome`` in the rule configuration files.
