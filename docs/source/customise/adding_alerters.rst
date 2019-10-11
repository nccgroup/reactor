.. _alerters:

Adding a new Alerter
====================

This document describes how to create a new alerter. Build in alerters live in ``reactor/alerter.py`` are are subclasses
of :py:class:`Alerter`. At a minimum, your alerter needs to implement two member functions and set the class variable.

Your class may implement several functions from :py:class:`Alerter`:

.. code-block:: python

    class AwesomeAlerter(Alerter):
        _schema_file = '../reactor_modules/schema-alerter-awesome.yaml'

        def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
            # ...
        def get_info(self):
            # ...
        def create_alert_title(self, alerts):
            # ...

You can import alerters by specifying the type as ``module.file.AlerterName``, where module is the name of a Python
module, or folder containing ``__init__.py``, and file is the name of the Python file containing an :py:class:`Alerter`
subclass named ``AlerterName``.

Basics
------

The alerter class will be instantiated when Reactor starts, and be periodically passed matches through the ``alert``
method. Reactor also writes back info about the alert into Elasticsearch that it obtains through ``get_info``. Several
important member properties:

.. py:attribute:: Alerter._schema_file

    The relative path from ``cls._schema_relative`` (defaults to ``reactor.alerter.py``) to the configuration schema
    file. The file should be in a ``.yaml`` format and should describe all options required and optional for using the
    Rule using the `JSON Schema Draft 7 specification <https://json-schema.org/specification-links.html#draft-7>`_.
    Reactor will validate all alerter configurations that specify this alerter *before* instantiation.

.. py:attribute:: Alerter._schema_relative

    The absolute path that ``cls._schema_file`` is relative to (defaults to ``reactor.rule.py``).


.. py:attribute:: Alerter.rule

    The :py:class:`Rule` object that this alerter is attached to. All options specific to that rule can be retrieved
    using the :py:meth:`rule.conf` function.

.. py:attribute:: Alerter.conf

    The dictionary containing the alerter configuration. All options specific to the alerter will be found in here.

.. py:attribute:: Alerter.pipeline

    This is a dictionary object that serves to transfer information between alerts. When an alert is triggered, a new
    empty pipeline object will be created and each alerter can add or receive information from it. Note that alerters
    are called in the order they are defined in the rule file. For example, a custom alerter added a ticket number
    to the pipeline and the email alerter will add that link if it's present in the pipeline.

.. py:method:: Alerter.alert(self, alerts: list, silenced: bool = False, publish: bool = True)

    Reactor will call this function to send an alert. ``alerts`` is a list of dictionary objects with the body that will be
    sent and stored in Elasticsearch. You can get a nice string representation of the match by calling
    ``self.rule.get_match_str(alert['match_data'], alert['match_body'])``. If this method raises an exception, it will
    be caught by Reactor and the alert will be marked as unsent and saved for later.

    ``silenced`` is a boolean flag to inform the alerter as to whether the alert has been silenced, your alerter can then
    choose how to handle silenced alerts (e.g., ignore them, increase the priority, etc).

    ``publish`` is a boolean flag to inform the alerter as to whether Reactor is default or debug mode. If the ``publish``
    is ``False`` then your alerter should not publish the alert but either ignore the alert or output a debug statement.

.. py:method:: Alerter.get_info(self)

    This function is called to get information about the alert to save back to Elasticsearch. It should return a dictionary,
    which is uploaded directly to Elasticsearch, and should contain useful information about the alert such as the type,
    recipients, parameters, etc.

Tutorial
--------

As an example, we are going to create an alerter that will write alerts to a local output file. Our configuration takes
a single option ``output_file_path`` that tells the alerter the path of the output file. First, create a file in the
``reactor_modules`` folder created in the :ref:`customise_prerequisites` called ``schema-ruletype-awesome.yaml``
called ``schema-alerter-awesome.yaml``:

.. code-block:: yaml

    ---
    $schema: http://json-schema.org/draft-07/schema#
    definitions: {}

    title: Awesome Alerter
    type: object
    required: [output_file_path]

    properties:
        type: {enum: [awesome, reactor_modules.my_alerters.AwesomeAlerter]}
        output_file_path: {type: string}
    ...

Now, in a file named ``my_alerters.py``, add

.. code-block:: python

    from reactor.alerter import Alerter, BasicMatchString


    class AwesomeAlerter(Alerter):

        # By setting _schema_file and _schema_relative you can ensure that
        # the alerter config file specifies all of the options and they are
        # valid. Otherwise, Reactor will throw an exception when trying to
        # load the alerter
        _schema_file = 'schema-alerter-awesome.yaml'
        _schema_relative = __file__

        # Alert is called
        def alert(self, alerts: list, silenced: bool = False, publish: bool = True):
            # If the alert is silenced or we are told not to publish
            if silenced or not publish:
                return

            # Alerts is a list of alert dictionaries.
            # It contains more than one alert when the alerter has
            # the aggregation option set
            for alert in alerts:

                # Config options can be accessed with self.conf
                with open(self.conf['output_file_path'], "a") as output_file:

                    # basic_match_string will transform the alert into the default
                    # human readable string format
                    match_string = str(BasicMatchString(self.rule, alert['match_data'], alert['match_body']))

                    output_file.write(match_string)

        # get_info is called after an alert is sent to get data that is written back
        # to Elasticsearch in the field "alert_info"
        # It should return a dict of information relevant to what the alert does
        def get_info(self):
            return {'type': 'Awesome Alerter',
                    'output_file': self.conf['output_file_path']}


In the rule configuration file, we are going to specify the alerter by writing

.. code-block:: yaml

    alerters:
      reactor_modules.my_alerters.AwesomeAlerter:
        output_file_path: "/tmp/alerts.log"

Reactor will attempt to import the alerter with ``from reactor_modules.my_alerters import AwesomeAlerter``.
This means that the folder must be in a location where it can be imported as a python module.
