import argparse
import logging
import signal
import sys
import time
import traceback

import urllib3

import reactor.plugin as reactor_plugin
from reactor.alerter import TestAlerter, BasicHitString
from reactor.config import parse_config
from reactor.exceptions import ReactorException, ConfigException
from reactor.reactor import Reactor
from reactor.util import (
    parse_duration,
    parse_timestamp,
    parse_positive_int,
    elasticsearch_client,
    dt_now,
    reactor_logger,
    pretty_ts,
    import_class,
)


def parse_args(args: dict) -> (argparse.ArgumentParser, dict):
    config = argparse.ArgumentParser(add_help=False)
    config.add_argument('-c', '--config',
                        action='store',
                        dest='config',
                        metavar='my_config.yaml',
                        default='config.yaml',
                        help='Global config file')
    config.add_argument('-l', '--log-level',
                        action='store',
                        dest='log_level',
                        default=None,
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
                        help='Set the logging level')

    patience = argparse.ArgumentParser(add_help=False)
    patience.add_argument('--patience',
                          action='store',
                          dest='timeout',
                          type=parse_duration,
                          default='seconds=20',
                          metavar='units=val',
                          help='Maximum time to wait for ElasticSearch to become responsive (e.g. seconds=30)')

    disable_warnings = argparse.ArgumentParser(add_help=False)
    disable_warnings.add_argument('--suppress',
                                  action='store_true',
                                  dest='disable_warnings',
                                  help='Disable warnings from urllib3')

    timestamps = argparse.ArgumentParser(add_help=False)
    timestamps.add_argument('--start',
                            action='store',
                            type=parse_timestamp,
                            default=None,
                            metavar='YYYY-MM-DDTHH:MM:SS',
                            help='Start querying from this timestamp')
    timestamps.add_argument('--end',
                            action='store',
                            type=parse_timestamp,
                            default=None,
                            metavar='YYYY-MM-DDTHH:MM:SS',
                            help='Stop querying after timestamp')

    run_rule = argparse.ArgumentParser(add_help=False)
    run_rule.add_argument('--max-hits',
                          type=parse_positive_int,
                          metavar='[1..]',
                          default=None,
                          help='Maximum number of hits to retrieve (overrides max_scrolling_count)')
    run_rule.add_argument('--timeframe',
                          type=parse_duration,
                          metavar='units=val',
                          default='hours=24',
                          help='Limit the query to a timeframe (e.g. hours=24)')
    run_rule.add_argument('--start',
                          type=parse_timestamp,
                          default=None,
                          metavar='YYYY-MM-DDTHH:MM:SS',
                          help='Start querying from this timestamp')
    run_rule.add_argument('--output',
                          type=str,
                          metavar='stdout|stderr|devnull|FILENAME',
                          default='stdout',
                          help='Where to output the alerts')
    run_rule.add_argument('--format',
                          type=str,
                          choices=['plain', 'json'],
                          default='plain',
                          help='Format to output the alerts')

    # Define the sub parsers
    parser = argparse.ArgumentParser('reactor')
    parser.set_defaults(action=None)
    sub_parser = parser.add_subparsers(title='actions')

    # Normal run
    run_sp = sub_parser.add_parser('run', parents=[config, patience, timestamps, disable_warnings],
                                   help='Run the reactor client')
    run_sp.set_defaults(action='run')
    run_sp_group = run_sp.add_mutually_exclusive_group()
    run_sp_group.add_argument('--reload',
                              default='seconds=3',
                              type=parse_duration,
                              metavar='units=val',
                              help='How frequently to poll configuration location for changes (e.g. seconds=3)'
                                   'Overrides the config setting')
    run_sp_group.add_argument('--pin-rules',
                              action='store_true',
                              dest='pin_rules',
                              help='Stop Alerter from monitoring for rule changes')
    run_sp_group = run_sp.add_mutually_exclusive_group()
    run_sp_group.add_argument('--debug',
                              action='store_const',
                              const='debug',
                              default='default',
                              dest='mode',
                              help='Suppresses alerts and prints information instead')
    run_sp_group.add_argument('--verbose',
                              action='store_true',
                              dest='verbose',
                              help='Increase verbosity without suppressing alerts')
    run_sp.add_argument('--es-debug',
                        action='store_true',
                        dest='es_debug',
                        help='Enabled verbose logging from ElasticSearch queries')
    run_sp.add_argument('--es-debug-trace',
                        action='store',
                        dest='es_debug_trace',
                        metavar='FILENAME',
                        help='Log ElasticSearch queries as curl commands in specified file')
    run_sp.add_argument('--plugins-only',
                        action='store_true',
                        dest='plugins_only',
                        help='Only run the plugins specified in the config file')
    run_sp.add_argument('rules',
                        nargs='*',
                        help='Limit running to the specified rules')

    # Initialise command
    init_sp = sub_parser.add_parser('init', parents=[config, patience, disable_warnings],
                                    help='Initialise the reactor indices and templates')
    init_sp.set_defaults(action='init')
    init_sp.add_argument('-m', '--mappings',
                         dest='mappings_dir',
                         help='Path to the directory containing the mapping JSON files')
    init_sp.add_argument('--recreate',
                         action='store_true',
                         help='Recreated the indices and template')
    init_sp.add_argument('-f', '--force',
                         action='store_true',
                         help='Force recreation of indices (no user prompt)')
    init_sp.add_argument('--old-index',
                         dest='old_index',
                         help='Name of the old index to copy the data across from')

    # Validate command
    test_sp = sub_parser.add_parser('validate', parents=[config],
                                    help='Validate the specified rules')
    test_sp.set_defaults(action='validate', mode='test')
    test_sp.add_argument('rules',
                         nargs='+',
                         help='List of rules to validate')

    # Test command
    test_sp = sub_parser.add_parser('test', parents=[config, patience, run_rule, disable_warnings],
                                    help='Test the specified rules')
    test_sp.set_defaults(action='test', mode='test')
    test_sp.add_argument('rules',
                         nargs='+',
                         help='List of rules to test')

    # Hits command
    hits_sp = sub_parser.add_parser('hits', parents=[config, patience, run_rule, disable_warnings],
                                    help='Retrieve the hits for the specified rule')
    hits_sp.set_defaults(action='hits', mode='test')
    hits_sp.add_argument('--counts',
                         action='store_true',
                         help='Only report on the number of hits')
    hits_sp.add_argument('rule',
                         help='The rule to retrieve hits')

    # Console command
    console_sp = sub_parser.add_parser('console', parents=[config, patience, disable_warnings],
                                       help='Start the reactor console')
    console_sp.set_defaults(action='console')
    console_sp.add_argument('-i', '--index',
                            default=None,
                            choices=['alert', 'error', 'silence', 'status'],
                            help='Index to retrieve hits from')
    # Silence command
    silence_sp = sub_parser.add_parser('silence', parents=[config, patience, disable_warnings],
                                       help='Silence a set of rules')
    silence_sp.set_defaults(action='silence')
    silence_sp.add_argument('rules',
                            nargs='+',
                            help='List of rules to silence')
    silence_sp_group = silence_sp.add_mutually_exclusive_group()
    silence_sp_group.add_argument('--duration',
                                  type=parse_duration,
                                  metavar='units=val',
                                  default='hours=1',
                                  help='Duration to silence rule for (e.g. hours=1)')
    silence_sp_group.add_argument('--revoke',
                                  action='store_true',
                                  help='Revoke all silences on the specified rules')

    # Parse the arguments
    return parser, vars(parser.parse_args(args))


def handle_signal(recv_signal, frame):
    sys.exit(recv_signal)


def perform_init(config: dict, args: dict) -> int:
    """ Perform the initialise action. """
    es_client = elasticsearch_client(config['elasticsearch'])
    if not es_client.wait_until_responsive(args['timeout']):
        return 1
    if not es_client.es_version_at_least(5):
        reactor_logger.fatal('Unsupported version of ElasticSearch: %s', es_client.es_version)
        return 2

    from reactor.init import create_indices
    create_indices(es_client, config, args['recreate'], args['old_index'], args['force'])
    return 0


def perform_validate(config: dict, args: dict) -> int:
    try:
        reactor = Reactor(config, args)
        reactor.loader.load(args)
        reactor_logger.info('All specified rules are valid')
        return 0
    except ReactorException as e:
        print(e)
        return 1


def perform_test(config: dict, args: dict) -> int:
    """ Perform the test action. """
    start_time = args['start'] or (dt_now() - args['timeframe'])
    end_time = start_time + args['timeframe']

    reactor = Reactor(config, args)
    reactor.loader.load(args)

    for rule in reactor.loader:
        rule.alerters = [TestAlerter(rule, {'format': args['format'], 'output': args['output']})]
        rule.set_conf('segment_size', args['timeframe'])
        rule.max_hits = args['max_hits']
        reactor.test_rule(rule, end_time, start_time=start_time)
    return 0


def perform_hits(config: dict, args: dict) -> int:
    """ Perform the hits action. """
    args['rules'] = [args['rule']]
    start_time = args['start'] or (dt_now() - args['timeframe'])
    end_time = start_time + args['timeframe']

    reactor = Reactor(config, args)
    reactor.loader.load(args)

    for rule in reactor.loader:
        if args['counts']:
            hits = rule.get_hits_count(start_time, end_time, rule.get_index(start_time, end_time))
            reactor_logger.info('Queried from %s to %s "%s": %s query hits',
                                pretty_ts(start_time, rule.conf('use_local_time')),
                                pretty_ts(start_time, rule.conf('use_local_time')),
                                rule.name,
                                hits[list(hits.keys())[0]])

        else:
            rule.alerters = []
            rule.set_conf('segment_size', args['timeframe'])
            rule.max_hits = args['max_hits']

            hits = []
            while rule.data.num_hits < min(rule.data.total_hits or float('inf'), rule.max_hits):
                hits.extend(rule.get_hits(start_time, end_time, rule.get_index(start_time, end_time)))
            rule.clear_scroll()

            # Format the hits
            formatted_str = []
            for hit in hits:
                hit_str = str(BasicHitString(args['format'], hit)) + (('-' * 80) if args['format'] != 'json' else '')
                formatted_str.append(hit_str)

            # Print the formatted hits
            if args['output'] == 'stdout':
                print(*formatted_str, sep='\n', end='\n', file=sys.stdout)
            elif args['output'] == 'stderr':
                print(*formatted_str, sep='\n', end='\n', file=sys.stderr)
            elif args['output'] == 'devnull':
                pass
            else:
                with open(args['output'], 'w') as f:
                    f.writelines([line + '\n' for line in formatted_str])

            reactor_logger.info('Queried from %s to %s "%s": %s query hits',
                                pretty_ts(start_time, rule.conf('use_local_time')),
                                pretty_ts(start_time, rule.conf('use_local_time')),
                                rule.name,
                                len(hits))

    return 0


def perform_console(config: dict, args: dict) -> int:
    from reactor.console import run_console
    run_console(Reactor(config, args))
    return 0


def perform_silence(config: dict, args: dict) -> int:
    """ Perform the silence action. """
    reactor = Reactor(config, args)
    reactor.loader.load(args)
    for rule in reactor.loader:
        reactor.silence(rule, duration=args['duration'], revoke=args['revoke'])

    return 0


def perform_run(config: dict, args: dict) -> int:
    # Create the reactor
    reactor = Reactor(config, args)

    # Set up the signal handler
    signal.signal(signal.SIGINT, reactor.handle_signal)
    if hasattr(signal, 'SIGINFO'):
        signal.signal(signal.SIGINFO, reactor.handle_signal)

    # Start the plugins
    plugins = {}
    for plugin_type in config['plugins']:
        plugin_conf = config['plugins'][plugin_type]
        plugin_class = import_class(plugin_type, config['mappings']['plugin'], reactor_plugin)
        if not issubclass(plugin_class, reactor_plugin.BasePlugin):
            raise ConfigException('Plugin type %s not a subclass of BasePlugin' % plugin_class)
        plugins[plugin_type] = plugin_class(reactor, plugin_conf)
        plugins[plugin_type].start()

    # Start reactor
    if not args['plugins_only']:
        exit_code = reactor.start()
    else:
        while reactor.terminate_called == 0:
            time.sleep(0.5)
        exit_code = 0

    # Shutdown the plugins
    for plugin in plugins.values():
        plugin.shutdown()

    return exit_code


def main(args: list = None):
    args = args or sys.argv[1:]
    signal.signal(signal.SIGINT, handle_signal)

    parser, args = parse_args(args)
    if args['action'] is None:
        parser.print_help()
        return 0

    if args['log_level']:
        reactor_logger.setLevel(args['log_level'])

    # Silence the APScheduler logs if not in DEBUG mode
    if reactor_logger.level > logging.DEBUG:
        logging.getLogger('apscheduler').addHandler(logging.NullHandler())

    if args['disable_warnings']:
        urllib3.disable_warnings()

    try:
        config = parse_config(args['config'], args)

        # Initial Reactor writeback database
        if args['action'] == 'init':
            exit_code = perform_init(config, args)

        # Validate the specified rules
        elif args['action'] == 'validate':
            exit_code = perform_validate(config, args)
            pass

        # Test the specified rules
        elif args['action'] == 'test':
            exit_code = perform_test(config, args)

        # Retrieve hits for the specified rule
        elif args['action'] == 'hits':
            exit_code = perform_hits(config, args)

        # Start the reactor console
        elif args['action'] == 'console':
            exit_code = perform_console(config, args)

        # Silence the set of specified rules
        elif args['action'] == 'silence':
            exit_code = perform_silence(config, args)

        # Run Reactor
        else:
            exit_code = perform_run(config, args)

    except Exception as e:
        reactor_logger.fatal(str(e))
        reactor_logger.debug(traceback.format_exc())
        return 1

    else:
        return exit_code


if __name__ == '__main__':
    main(sys.argv[1:])
