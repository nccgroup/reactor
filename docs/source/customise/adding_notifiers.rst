.. _notifiers:

Adding a new Notifier
=====================

This document describes how to create a new notifier. Built in notifiers live in ``reactor/notifier.py`` and are a
subclass of :py:class:`BaseNotifier`. At a minimum, your notifier needs to implement the ``notify`` function. Notifiers
are instantiated with their configuration dictionary object.

Notifiers are modules which let you perform a specified action when a Reactor notification is triggered. They can be
added to Reactor using the ``notifiers`` option:

.. code-block:: yaml

    notifiers:
      module.file.MyNotifier: {}

where module is the name of a Python module, or folder containing ``__init__.py``,
and file is the name of the Python file containing a :py:class:`BaseNotifier` subclass named ``MyNotifier``.

A human-readable version the notifier can be added by using the ``mappings.notifier`` option:

.. code-block:: yaml

    mappings:
      notifier:
        my_notifier: module.file.MyEnhancement

    # ...

    notifiers:
      my_notifier: {}

.. py:method:: BaseNotifier.notify(self, subject: str, body: str)

    This function is called by Reactor when an uncaught exception is raised. ``subject`` is the subject of the notification
    and ``body`` is the message. The notifier should fire the notification whenever this function is called.


Tutorial
--------

As an example, we are going to create a notifier that runs a command. Our configuration will take just the command.
First, create a file in the ``reactor_modules`` folder created in the :ref:`customise_prerequisites` called
``my_notifiers.py``:

.. code-block:: python

    import copy
    import subprocess

    from reactor.notifier import BaseNotifier


    class CommandNotifier(BaseNotifier):

        def __init__(self, *args, **kwargs):
            super(CommandNotifier, self).__init__(*args, **kwargs)

            self._command = self.conf['command']

        def notify(self, subject: str, body: str) -> None:
            command = self._command % (subject, body)
            subprocess.run(command, shell=True, capture_output=True)


In the global configuration file, ``config.yaml``, we are going to specify this notifier be writing:

.. code-block:: yaml

    notifiers:
      example.my_notifier.CommandNotifier:
        command: foo --subject='%s' --body='%s'

Notifiers will not automatically be used. Inside the Reactor configuration file, you need to point it to the notifiers(s)
that it should use.
