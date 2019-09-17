import copy
import datetime
import hashlib
import os
import typing
import yaml

import jsonschema
import reactor.enhancement
from typing import List, Optional, Iterator
from reactor.exceptions import ReactorException, ConfigException
from reactor.util import (
    reactor_logger,
    load_yaml,
    import_class
)
from reactor.validator import yaml_schema, SetDefaultsDraft7Validator
from reactor.rule import Rule


class RuleLoader(object):
    rule_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/ruletype.yaml', __file__)
    conf_schema = None

    def __init__(self, conf: dict, rule_defaults: dict, mappings: dict):
        """
        :param conf: Configuration for the loader
        :param rule_defaults: Default values for every rule
        :param mappings: Lookup dictionary of alerters and ruletype classes
        """
        self.conf = conf
        self.rule_defaults = rule_defaults
        self.mappings = mappings

        self.rule_imports = {}
        self.rules = {}  # type: typing.Dict[str, Rule]
        self._loaded = False
        self._disabled = {}

    def __iter__(self) -> Iterator[Rule]:
        return iter(self.rules.values())

    def __contains__(self, item):
        return item in self.rules

    def __getitem__(self, item):
        return self.rules[item]

    def keys(self):
        return self.rules.keys()

    @property
    def loaded(self) -> bool:
        """ Whether the rules have been loaded at least once. """
        return self._loaded

    def disable(self, locator: str) -> None:
        """ Disable the rule until it is next updated. """
        self._disabled[locator] = self.get_hash(locator)

    def enabled(self, locator: str) -> None:
        """ Enable the rule. """
        self._disabled.pop(locator, None)

    def load(self, args: dict = None) -> list:
        """
        Discover and load all the rules as defined in the configuration and arguments.
        :return: Tuple of additions, modifications, and removals (list[str], list[str], list[str])
        """
        names = []
        use_rules = args.get('rules', [])
        use_rules = use_rules if isinstance(use_rules, list) else [use_rules]

        # Load each rule configuration
        rules = {}
        for locator in self.discover(use_rules):
            # If this rule has been disabled and the hash has not changed, skip it
            if locator in self._disabled and self.get_hash(locator) == self._disabled[locator]:
                continue

            # If we have already loaded this rule and it hasn't changed, use the existing
            if self.rules.get(locator) and self.rules[locator].hash == self.get_hash(locator):
                rules[locator] = self.rules[locator]
                continue

            # Load the rule
            try:
                rule = self.load_configuration(locator)
                rule.hash = self.get_hash(locator)
                # By setting `is_enabled: False` in rule YAML, a rule is easily disabled
                if not rule.enabled:
                    continue
                if rule.name in names:
                    raise ReactorException('Duplicate rule named %s' % rule.name)
            except ReactorException as e:
                raise ReactorException('Error loading rule: %s: %s' % (locator, e))

            # Does this rule already exist, copy over some properties
            if self.rules.get(locator):
                rule.data = self.rules.get(locator).data

            rules[locator] = rule
            names.append(rule.name)

        # Mark that we have loaded at least once
        self._loaded = True
        self.rules = rules

        return list(self.rules.keys())

    def discover(self, use_rules: list = None) -> List[str]:
        """ Discover all rules and return a list of rule locators. """
        raise NotImplementedError()

    def get_hash(self, locator: str) -> str:
        """ Given a rule locator return the hash. Used to detect configuration changes. """
        raise NotImplementedError()

    def load_configuration(self, locator: str) -> Rule:
        # Load configuration and validate
        try:
            conf = self.load_yaml(locator)
            rule = self.validate(locator, conf)

            # Create the rule object and populate
            self.load_options(rule)
            self.load_modules(rule, self.mappings)

        except ReactorException as e:
            self.disable(locator)
            raise e

        else:
            # Clear the invalid cache
            self.enabled(locator)

            return rule

    def load_yaml(self, locator: str):
        rule = {
            'rule_id': locator,
            'running': False,
        }
        # Clear the dependencies for locator
        self.rule_imports.pop(locator, None)
        while locator is not None:
            loaded = self.get_yaml(locator)

            # Special case for merging filters - if both specify a filter merge (AND) them
            if 'filter' in rule and 'filter' in loaded:
                rule['filter'] = loaded['filter'] + rule['filter']

            # Merge the loaded yaml into the rule dictionary
            loaded.update(rule)
            rule = loaded

            # Check for import dependencies
            locator = self.resolve_import(rule)

        # Copy defaults in from the base config
        for key, val in self.rule_defaults.items():
            rule.setdefault(key, val)

        return rule

    def get_yaml(self, locator: str) -> dict:
        """ Get and parse the YAML of the specified rule. """
        raise NotImplementedError()

    def resolve_import(self, rule: dict) -> Optional[str]:
        # If the rule has not imports
        if 'import' not in rule:
            return None

        # Make a note of the dependency
        dependencies = self.rule_imports.setdefault(rule['rule_id'], [])
        dependencies.append(rule['import'])

        # Resolve the new rule id
        import_id = rule['import']

        # Remove import before we load the import rule (or we could go on forever!)
        del (rule['import'])
        return import_id

    def validate(self, locator: str, conf: dict) -> Rule:
        # Validate the base rule type
        try:
            self.rule_schema.validate(conf)
        except jsonschema.ValidationError as e:
            raise ConfigException('Invalid rule configuration: %s\n%s' % (conf['rule_id'], e))

        # Convert rule type into Rule object
        rule_type = import_class(conf['type'], self.mappings['rule'], reactor.rule)
        if not issubclass(rule_type, reactor.rule.Rule):
            raise ConfigException('Rule module %s is not a subclass of Rule' % rule_type)

        # Validate the specific rule type
        try:
            rule_type.rule_schema.validate(conf)
        except jsonschema.ValidationError as e:
            raise ConfigException('Invalid rule configuration: %s\n%s' % (conf['rule_id'], e))

        # Instantiate Rule
        try:
            return rule_type(locator, self.get_hash(locator), conf)
        except (KeyError, ReactorException) as e:
            raise ReactorException('Error initialising rule %s: %s' % (conf['name'], e))

    @staticmethod
    def load_options(rule: Rule) -> None:
        """ Converts time objects, sets defaults, and validates some settings. """
        # Store compound keys and convert original to string
        for key in ['query_key', 'aggregation_key', 'compare_key']:
            if isinstance(rule.conf(key), list):
                rule.set_conf('compound_' + key, rule.conf(key))
                rule.set_conf(key, ','.join(rule.conf(key)))
            elif rule.conf(key):
                rule.set_conf('compound_' + key, [rule.conf(key)])

        # Add query_key, compare_key, and timestamp to include
        include = rule.conf('include')
        for key in ['query_key', 'aggregation_key', 'compare_key']:
            compound_key = 'compound_' + key
            if rule.conf(compound_key):
                include += rule.conf(compound_key)
        include.append(rule.conf('timestamp_field'))
        rule.set_conf('include', list(set(include)))

        # Check that generate_kibana_link is compatible with the filters
        if rule.conf('generate_kibana_link'):
            for es_filter in rule.conf('filter'):
                if es_filter:
                    if 'not' in es_filter:
                        es_filter = es_filter['not']
                    if 'query' in es_filter:
                        es_filter = es_filter['query']
                    if es_filter.keys()[0] not in ('term', 'query_string', 'range'):
                        raise ConfigException(
                            'generate_kibana_link is incompatible with filters other than term, query_string and range.'
                            'Consider creating a dashboard and using use_kibana_dashboard instead.')

        # Check that doc_type is provided if use_(count/terms)_query
        if rule.conf('use_count_query') or rule.conf('use_terms_query'):
            # TODO: will need make this conditional base on ES version
            if not rule.conf('doc_type'):
                raise ConfigException('doc_type must be specified')

        # Check that query_key is set if use_terms_query
        if rule.conf('use_terms_query') and not rule.conf('query_key'):
            raise ConfigException('query_key must be specified with use_terms_query')

        # Warn if use_strftime_index is used with %y, %M, or %D
        # (%y = short year, %M = minutes, %D = full date)
        if rule.conf('use_strftime_index'):
            for token in ['%y', '%M', '%D']:
                if token in rule.conf('index'):
                    reactor_logger.warning('Did you mean to use %s in the index?'
                                           'The index will for formatted like %s' % (token,
                                                                                     datetime.datetime.now().strftime(
                                                                                         rule.conf('index'))))

    @staticmethod
    def load_modules(rule: Rule, mappings: dict):
        """ Loads things that could be modules. Enhancements, alerters, and rule type. """
        # Load match enhancements
        match_enhancements = []
        for enhancement_name in rule.conf('match_enhancements', []):
            enhancement_class = import_class(enhancement_name, mappings['enhancement'], reactor.enhancement)
            if not issubclass(enhancement_class, reactor.enhancement.MatchEnhancement):
                raise ConfigException('Enhancement module %s not a subclass of MatchEnhancement' % enhancement_name)
            match_enhancements.append(enhancement_class(rule))
        rule.match_enhancements = match_enhancements

        # Load alert enhancements
        alert_enhancements = []
        for enhancement_name in rule.conf('alert_enhancements', []):
            enhancement_class = import_class(enhancement_name, mappings['enhancement'], reactor.enhancement)
            if not issubclass(enhancement_class, reactor.enhancement.AlertEnhancement):
                raise ConfigException('Enhancement module %s not a subclass of AlertEnhancement' % enhancement_name)
            alert_enhancements.append(enhancement_class(rule))
        rule.alert_enhancements = alert_enhancements

        # Load alerters
        alerters = []
        for alerter_name in rule.conf('alerters'):
            alerter_conf = copy.deepcopy(rule.conf('alerters')[alerter_name])
            alerter_class = import_class(alerter_name, mappings['alerter'], reactor.alerter)
            if not issubclass(alerter_class, reactor.alerter.Alerter):
                raise ReactorException('Alerter module %s is not a subclass of Alerter' % alerter_class)
            alerters.append(alerter_class(rule, alerter_conf))
        rule.alerters = alerters


class FileRuleLoader(RuleLoader):
    conf_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/loader-file.yaml', __file__)

    def discover(self, use_rules: list = None) -> List[str]:
        # Passing a list of use rules directly will bypass rules_folder and .yaml checks
        if use_rules and all([os.path.isfile(f) for f in use_rules]):
            return use_rules

        folder = self.conf['rules_folder']
        rules = {}
        if self.conf['scan_subdirectories']:
            for root, folders, files in os.walk(folder):
                for filename in files:
                    if use_rules and filename not in use_rules:
                        continue
                    if self.is_yaml(filename):
                        rules[filename] = filename
        else:
            for filename in os.listdir(folder):
                abs_path = os.path.join(folder, filename)
                if use_rules and filename not in use_rules:
                    continue
                if os.path.isfile(abs_path) and self.is_yaml(filename):
                    rules[filename] = abs_path

        if use_rules and len(use_rules) != len(rules):
            raise ReactorException('Could not find all requested rules: %s' % list(use_rules - rules.keys()))

        return list(rules.values())

    def get_hash(self, locator: str) -> str:
        rule_hash = hashlib.sha256()
        if os.path.exists(locator):
            with open(locator) as fh:
                rule_hash.update(fh.read().encode('utf-8'))
            for import_locator in self.rule_imports.get(locator, []):
                with open(import_locator) as fh:
                    rule_hash.update(fh.read().encode('utf-8'))
        return rule_hash.hexdigest()

    def get_yaml(self, locator: str) -> dict:
        try:
            return load_yaml(locator)
        except yaml.YAMLError as e:
            raise ConfigException(str(e))

    def resolve_import(self, rule: dict) -> Optional[str]:
        """ Allow for paths relative to the rule. """
        import_id = super(FileRuleLoader, self).resolve_import(rule)
        if import_id is None:
            return None
        elif os.path.isabs(import_id):
            return import_id
        else:
            return os.path.join(os.path.dirname(rule['rule_id']), import_id)

    @staticmethod
    def is_yaml(filename):
        return filename.endswith('.yaml') or filename.endswith('.yml')
