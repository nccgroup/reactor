Code Reference
==============

.. autoclass:: reactor.reactor.Reactor
    :members: running, start, stop, terminate, silence

.. autoclass:: reactor.cluster.RaftNode
    :member-order: bysource
    :members: is_leader, has_leader, neighbours, neighbourhood, set_ssl, start, shutdown, meta, leader_meta

.. autoclass:: reactor.console.Console
    :members: run


Base Classes
------------

.. autoclass:: reactor.rule.Rule
    :member-order: bysource
    :members: add_hits_data, add_count_data, add_terms_data, add_aggregation_data, get_match_str, garbage_collect, conf, set_conf

.. autoclass:: reactor.loader.RuleLoader
    :members: discover, get_hash, get_yaml

.. autoclass:: reactor.alerter.Alerter
    :members: alert, get_info

.. autoclass:: reactor.enhancement.BaseEnhancement
    :members: process

.. autoclass:: reactor.plugin.BasePlugin
    :member-order: bysource
    :members: start, shutdown

.. autoclass:: reactor.notifier.BaseNotifier
    :members: notify


Exceptions
----------

.. autoexception:: reactor.exceptions.ReactorException


.. autoexception:: reactor.exceptions.ConfigException


.. autoexception:: reactor.exceptions.QueryException


.. autoexception:: reactor.cluster.ClusterException


.. autoexception:: reactor.enhancement.DropAlertException

