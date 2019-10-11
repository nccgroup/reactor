.. _enhancements:

Adding a new Enhancement
========================

This document describes how to create a new enhancement. Built in enhancements live in ``reactor/enhancement.py`` and
are a subclass of :py:class:`BaseEnhancement`. At a minimum, your enhancement needs to implement the ``process`` function.

A special exception class :py:class:`DropAlertException` can be used in an enhancement to drop alerts if custom
conditions are met. For example:

.. code-block:: python

    class AwesomeEnhancement(BaseEnhancement):
        def process(self, alert: dict):
            # Drops a match if "field_1" == "field_2"
            if alert['match_body']['field_1'] == alert['match_body']['field_2']:
                raise DropMatchException()

.. py:method:: BaseEnhancement.process(self, alert: dict)

    Reactor will call this function to allow the enhancement to process the alert. ``alert`` is the result of
    :py:meth:`Rule.get_alert_body`. An enhancement can choose to either do nothing to the alert, alter the alert in some
    way to enhance it, or raise a :py:class:`DropAlertException` to drop the alert.

Tutorial
--------

As an example, we are going to create an enhancement that performs a whois lookup on all alerts with a match body that
contains ``domain`` as a property. First, create a file in the ``reactor_modules`` folder created in the
:ref:`customise_prerequisites` called ``my_enhancements.py``:

.. code-block:: python

    from reactor.enhancement import BaseEnhancement


    class AwesomeEnhancement(BaseEnhancement):

        # The enhancement is run against every alert
        # The alert is passed to the process function where it can be modified in any way
        # Reactor will do this for each enhancement linked to a rule
        def process(self, alert: dict):
            if 'domain' in alert['match_body']:
                url = "http://who.is/whois/%s" % (alert['match_body']['domain'])
                alert['match_data']['domain_whois_link'] = url

Enhancements will not automatically be run. Inside the rule configuration file, you need to point it to the enhancement(s)
that it should run by setting the ``enhancements`` option:

.. code-block:: yaml

    enhancements:
    - "reactor_modules.my_enhancements.AwesomeEnhancement"

