.. _plugins:

Adding a new Plugin
===================

This document describes how to create a new plugin. Built in plugins live in ``reactor/plugin.py`` and are a subclass
of :py:class:`reactor.plugin.BasePlugin`. At a minimum, your plugin needs to implement the ``start`` and ``shutdown`` functions.
Plugins are instantiated with a Reactor object and their configuration dictionary object.

Plugins are modules which let you add functionality that runs alongside Reactor. They can be added to Reactor using the
``plugins`` option in the global configuration file, ``config.yaml``:

.. code-block:: yaml

    plugins:
      module.file.MyPlugin: {}

where module is the name of a Python module, or folder containing ``__init__.py``,
and file is the name of the Python file containing a :py:class:`reactor.plugin.BasePlugin` subclass named ``MyPlugin``.

A human-readable version the plugin can be added by using the ``mappings.plugin`` option:

.. code-block:: yaml

    mappings:
      plugin:
        my_plugin: module.file.MyEnhancement

    # ...

    plugins:
      my_plugin: {}


.. py:method:: reactor.plugin.BasePlugin.start(self)
    :noindex:

    This is called before Reactor is started and start the plugin. If the plugin starts a process, thread, or executes a
    long running shell command then the plugin should store the reference to that so that it may be terminated when shutdown
    is called.

.. py:method:: reactor.plugin.BasePlugin.shutdown(self, timeout: int = None)
    :noindex:

    This function shuts down the plugin, any process, thread, or running shell command that the plugin started should be
    stopped and cleaned up. If ``timeout`` is provided then the plugin should wait up to that number of seconds for the
    plugin to shutdown.

Tutorial
--------

As an example, we are going to create a plugin that runs a command every 5 minutes. Our configuration will take a list
a command and timeframe. First, create a file in the ``reactor_modules`` folder created in the
:ref:`customise_prerequisites` called ``my_plugins.yaml``:

.. code-block:: python

    import subprocess
    import threading
    import time

    from reactor.plugin import BasePlugin
    from reactor.util import parse_timeframe


    class PeriodicCommandPlugin(BasePlugin):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self._running = None
            self._thread = None
            self._time = 0
            self._command = self.conf['command']
            self._timeframe = parse_timeframe(self.conf['timeframe']).total_seconds()

            if type(self._command) == str:
                self._command = self._command.split(' ')

        def start(self):
            logging.getLogger('reactor.plugin.periodic_command').info('Starting plugin')
            # Clear the time
            self._time = 0
            # Create the threading event
            self._running = threading.Event()
            self._running.set()
            # Create and start the daemon thread
            self._thread = threading.Thread(target=self.execute_command, daemon=True)
            self._thread.start()

        def shutdown(self, timeout: int = None):
            if self._thread is None:
                raise RuntimeException('PeriodicCommandPlugin not started')
            self._running.clear()
            self._thread.join()
            del self._thread
            del self._running

        def execute_command(self):
            while self._running.is_set():
                if time.time() - self._time >= self._timeframe:
                    res = subprocess.run(self._command, capture_output=True)
                    logging.getLogger('reactor.plugin.periodic_command').info(res.stdout)
                self._running.wait(1)

In the global configuration file, ``config.yaml``, we are going to specify this plugin by writing:

.. code-block:: yaml

    plugins:
      reactor_modules.my_plugins.PeriodicCommandPlugin:
        command: ping elasticsearch -oq -W 1000
        timeframe:
          minutes: 5

Plugins will not automatically be run. Inside the Reactor configuration file, you need to point it to the plugin(s)
that it should run.
