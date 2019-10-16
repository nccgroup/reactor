import jsonschema
import logging
import logging.config

import reactor
from .util import dots_set_default, dots_set, dots_get, import_class, load_yaml, reactor_logger
from .validator import yaml_schema, SetDefaultsDraft7Validator

required_config = frozenset(['elasticsearch.host', 'elasticsearch.port'])

default_mappings = {
    # Default rule loaders
    'loader': {
        'file': reactor.loader.FileRuleLoader,
    },

    # Default alerts
    'alerter': {
        'debug': reactor.alerter.DebugAlerter,
        'test': reactor.alerter.TestAlerter,
        'command': reactor.alerter.CommandAlerter,
        'email': reactor.alerter.EmailAlerter,
        'webhook': reactor.alerter.WebhookAlerter,
    },

    # Default rule types
    'rule': {
        'any': reactor.rule.AnyRule,
        'frequency': reactor.rule.FrequencyRule,
        'spike': reactor.rule.SpikeRule,
        'blacklist': reactor.rule.BlacklistRule,
        'whitelist': reactor.rule.WhitelistRule,
        'change': reactor.rule.ChangeRule,
        'flatline': reactor.rule.FlatlineRule,
        'new_term': reactor.rule.NewTermRule,
        'cardinality': reactor.rule.CardinalityRule,
        'metric_aggregation': reactor.rule.MetricAggregationRule,
        'percentage_match': reactor.rule.PercentageMatchRule,
    },

    # Default enhancements
    'enhancement': {
        'metadata': reactor.enhancement.MetaDataEnhancement,
    },

    # Default notifiers
    'notifier': {
        'email': reactor.notifier.EmailNotifier,
    },

    # Default plugin types
    'plugin': {
        'http_server': reactor.plugin.HttpServerPlugin,
    },
}


config_schema = yaml_schema(SetDefaultsDraft7Validator, 'schemas/config.yaml', __file__)


def parse_config(filename: str, args: dict, defaults: dict = None, overwrites: dict = None) -> dict:
    conf = load_yaml(filename)

    # Configure logging
    configure_logging(conf, args)

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
        raise reactor.ReactorException('Invalid config file "%s":\n%s' % (filename, e))

    # Set mapping defaults
    conf.setdefault('mappings', {})
    for key in default_mappings:
        conf['mappings'].setdefault(key, {})
        for name, value in default_mappings[key].items():
            conf['mappings'][key].setdefault(name, value)

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


def configure_logging(conf: dict, args: dict):
    """ Configure logging from the global config file if provided. """
    if 'logging' in conf:
        # load new logging configuration
        logging.config.dictConfig(conf['logging'])

    # re-enable INFO log level on reactor_logger in verbose/debug mode
    # (but don't touch it if it is already set to INFO or below by config)
    if args.get('verbose') or args.get('debug'):
        if reactor_logger.level > logging.INFO or reactor_logger.level == logging.NOTSET:
            reactor_logger.setLevel(logging.INFO)

    if args.get('es_debug', False):
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    if args.get('es_debug_trace', False):
        tracer = logging.getLogger('elasticsearch.trace')
        tracer.setLevel(logging.INFO)
        tracer.addHandler(logging.FileHandler(args['es_debug_trace']))
