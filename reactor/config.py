import os

import jsonschema

import reactor
from reactor.util import dots_set_default, dots_set, dots_get, import_class, load_yaml
from reactor.validator import yaml_schema, SetDefaultsDraft7Validator

required_config = frozenset(['elasticsearch.host', 'elasticsearch.port'])

default_mappings = {
    # Default rule loaders
    'loader.file': reactor.loader.FileRuleLoader,

    # Default alerts
    'alerter.debug': reactor.alerter.DebugAlerter,
    'alerter.test': reactor.alerter.TestAlerter,
    'alerter.command': reactor.alerter.CommandAlerter,
    'alerter.email': reactor.alerter.EmailAlerter,
    'alerter.webhook': reactor.alerter.WebhookAlerter,

    # Default rule types
    'ruletype.any': reactor.ruletype.AnyRuleType,
    'ruletype.frequency': reactor.ruletype.FrequencyRuleType,
    'ruletype.spike': reactor.ruletype.SpikeRuleType,
    'ruletype.blacklist': reactor.ruletype.BlacklistRuleType,
    'ruletype.whitelist': reactor.ruletype.WhitelistRuleType,
    'ruletype.change': reactor.ruletype.ChangeRuleType,
    'ruletype.flatline': reactor.ruletype.FlatlineRuleType,
    'ruletype.new_term': reactor.ruletype.NewTermRuleType,
    'ruletype.cardinality': reactor.ruletype.CardinalityRuleType,
    'ruletype.metric_aggregation': reactor.ruletype.MetricAggregationRuleType,
    'ruletype.percentage_match': reactor.ruletype.PercentageMatchRuleType,
}


config_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/config.yaml', __file__)


def parse_config(filename: str, defaults: dict = None, overwrites: dict = None) -> dict:
    conf = load_yaml(filename)

    # Set user defaults
    for key, value in (defaults or {}).items():
        dots_set_default(conf, key, value)

    # Set overwrites
    for key, value in (overwrites or {}).items():
        dots_set(conf, key, value)

    # Validate config - and set defaults specified in the schema
    try:
        config_schema.validate(conf)
    except jsonschema.ValidationError as e:
        raise reactor.ReactorException("Invalid config file: %s\n%s" % (filename, e))

    # Set mapping defaults
    for key, value in default_mappings.items():
        dots_set_default(conf['mappings'], key, value)

    # Initialise the rule loader
    rule_loader_class = dots_get(conf, 'loader.type')
    rule_loader_class = import_class(rule_loader_class, conf['mappings']['loader'], reactor.loader)
    if not issubclass(rule_loader_class, reactor.RuleLoader):
        raise reactor.ConfigException('Loader type %s not a subclass of RuleLoader' % rule_loader_class)
    loader_config = dots_get(conf, 'loader.config')
    rule_defaults = dots_get(conf, 'rule', {})
    if hasattr(rule_loader_class, 'conf_schema') and rule_loader_class.conf_schema:
        try:
            rule_loader_class.conf_schema.validate(loader_config)
        except jsonschema.ValidationError as e:
            raise reactor.ReactorException("Invalid loader config: %s\n%s" % (filename, e))
    conf['loader'] = rule_loader_class(loader_config, rule_defaults, conf['mappings'])

    return conf
