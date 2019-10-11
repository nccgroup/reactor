.. _loaders:

Adding a new Loader
========================

This document describes how to create a new rule loader. Built in loaders live in ``reactor/loader.py`` and are
subclasses of :py:class:`RuleLoader`. At a minimum, your loader needs to implement ``discover``, ``get_hash``, and
``get_yaml``.

You class may implement several functions from :py:class:`RuleLoader`:

.. code-block:: python

    class AwesomeRuleLoader(RuleLoader):
        # ...
        def discover(self, use_rules: list = None):
            # ...
        def get_hash(self, locator: str) -> str:
            # ...
        def get_yaml(self, locator: str) -> dict:
            # ...
        def resolve_import(self, rule: dict):
            # ...

You can import loaders by specifying the type as ``module.file.RuleLoaderName``, where module is the name of a
python module or folder containing ``__init__.py``, and file is the name of the python file containing a
:py:class:`RuleLoader` subclass named ``RuleLoaderName``.

Basics
------

The :py:class:`RuleLoader` instance remains in memory while Reactor is running, is frequently called to check for updates
(unless the ``--pin_rules`` to the ``reactor run`` command), and loads/validates all the rules that Reactor will execute.
The three functions that must be implemented are:

.. py:method:: RuleLoader.self.discover(self, use_rules\: list = None) -> List[str]

    This function should discover all rules and return a list of locators that can be passed into ``get_hash`` and
    ``get_yaml``. If ``use_rules`` is supplied then the result of should be limited to the union of the set of rule
    locators discovered and the rule locators in ``use_rules``.

.. py:method:: RuleLoader.self.get_hash(self, locator\: str) -> str

    This function should return a uniquely identifying hash for the rule locator. The value of this hash must change if
    the rule configuration has been altered in any way and remain the same if not. Reactor uses the returned value to
    determine whether the rule should be reloaded.

.. py:method:: RuleLoader.self.get_yaml(self, locator\: str) -> dict

    This function should retrieve and parse the YAML of the specified rule. Technically, the store for a custom rule
    loader does not need to be in a YAML format only that this function returns a dictionary that Reactor can use.

Tutorial
--------

As an example, we are going to create a rule loader that can retrieve rules from a MongoDB database rather than a
filesystem. First, create a file in the `reactor_modules` folder created in the :ref:`customise_prerequisites` called
``my_loaders.py``:

.. code-block:: python

    import json
    import yaml

    from pymongo import MongoClient
    from reactor.loader import RuleLoader


    class MongoDbRuleLoader(RuleLoader):

        def __init__(self, conf: dict, rule_defaults: dict, mappings: dict):
            super(MongoDbRuleLoader, self).__init__(conf, rule_defaults, mappings)
            # Pass the MongoClient the keywords it needs
            self._client = MongoClient(**conf['client'])
            # Connect to the database
            self._db = self._client[conf['database']]
            self._cache = {}

        def discover(self, use_rules: list = None) -> List[str]:
            # Clear the cache as want to be able to detect rules that are no longer there
            self._cache = {}
            # Limit the returned rules to those stored in `use_rules` if provided
            find_filter = {'_id': {'$in': use_rules}} if use_rules else {}
            # Retrieve and cache all filtered rules
            for rule in self._db.rules.find(find_filter):
                self._cache[str(rule['_id'])] = rule

            # Return the list rule locators
            return list(self._cache.keys())

        def get_hash(self, locator: str) -> str:
            # Convert the rule dictionary into a consistent string
            rule_str = json.dumps(self._cache[locator], sort_keys=True)
            # Convert the string into a hash
            rule_hash = hashlib.sha256()
            rule_hash.update(rule_str.encode('utf-8'))
            return rule_hash.hexdigest()

        def get_yaml(self, locator: str) -> dict:
            # Return the already cached rule
            return self._cache[locator]

Finally, you need to specify in your Reactor configuration file that MongoRuleLoader should be used instead of the
default FileRuleLoader, so in your global configuration file ``config.yaml`` file:

.. code-block:: yaml

    loader:
      type: "reactor_modules.my_loaders.MongoRuleLoader"
      config:
        client:
          url: mongodb::27017
        database: reactor

