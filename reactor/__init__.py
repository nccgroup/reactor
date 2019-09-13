__version__ = '1.0.0'
__author__ = 'Pete Scopes'

from reactor.alerter import (
    Alerter, DebugAlerter, TestAlerter, CommandAlerter, EmailAlerter, WebhookAlerter
)
from .auth import Auth
from .core import Core
from .enhancement import AlertEnhancement, MatchEnhancement, DropException
from .exceptions import *
from .loader import Rule, RuleLoader, FileRuleLoader
from .rule import Rule
from .rule import (
    AnyRule,
    CompareRule, BlacklistRule, WhitelistRule, ChangeRule,
    FrequencyRule, FlatlineRule,
    SpikeRule,
    NewTermRule,
    CardinalityRule,
    BaseAggregationRule, MetricAggregationRule, PercentageMatchRule
)
from .util import ElasticSearchClient, elasticsearch_client
