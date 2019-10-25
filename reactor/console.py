import curses
import math
import signal
import sys
import threading
from datetime import timedelta
from functools import lru_cache

import texttable as tt

from .alerter import BasicHitString
from .reactor import Reactor
from .util import ts_to_dt, pretty_ts, dt_now, dots_get, reactor_logger

YELLOW_ON_BLACK = 1
GREEN_ON_BLACK = 2
RED_ON_BLACK = 3
BLUE_ON_BLACK = 4
BLACK_ON_YELLOW = 5
BLACK_ON_GREEN = 6


class Console(object):
    """
    A simple ncurses application to provide a system administrator/developer a view of the current state of the reactor
    indices in Elasticsearch. Provides 3 types of view: a table of indices, a table of index entries, and a index item.

    :param reactor: Instance of :py:class:`Reactor`
    """
    settings = {'indices': {'mappings': {'uuid': 'UUID',
                                         'index': 'Index Name',
                                         'status': 'Status',
                                         'health': 'Health',
                                         'docs.count': 'Num Documents',
                                         'shards': 'Shards',
                                         'store.size': 'Size'},
                            'timestamps': [],
                            'types': ['t', 't', 't', 't', 'i', 't', 't'],
                            'alignments': ['l', 'l', 'l', 'l', 'r', 'r', 'r'],
                            'query': None},
                'alerts': {'mappings': {'match_data.began_at': 'Began At',
                                        'match_data.ended_at': 'Ended At',
                                        'uuid': 'UUID',
                                        'rule_name': 'Rule Name',
                                        'priority': 'Priority',
                                        'duration': 'Duration',
                                        'num_matches': 'Num Matches',
                                        'match_data.num_events': 'Num Events',
                                        '_version': 'Version'},
                           'timestamps': ['match_data.began_at', 'match_data.ended_at'],
                           'types': ['t', 't', 't', 't', 't', 't', 'i', 'i', 'i'],
                           'alignments': ['l', 'l', 'c', 'l', 'r', 'r', 'r', 'r', 'r'],
                           'query': {'sort': {'match_time': 'desc'}}},
                'error': {'mappings': {'@timestamp': 'Timestamp',
                                       'rule_uuid': 'Rule UUID',
                                       'data': 'Data',
                                       'message': 'Message',
                                       'traceback': 'Traceback'},
                          'timestamps': ['@timestamp'],
                          'types:': ['t', 't', 'a', 't', 't'],
                          'alignments': ['l', 't', 'l', 'l', 'l'],
                          'query': {'sort': {'@timestamp': 'desc'}}},
                'silence': {'mappings': {'@timestamp': 'Timestamp',
                                         'rule_uuid': 'Rule UUID',
                                         'until': 'Until',
                                         'exponent': 'Exponent',
                                         'silence_key': 'Silence Key',
                                         'alert_uuid': 'Alert UUID'},
                            'timestamps': ['@timestamp', 'until'],
                            'types': ['t', 't', 't', 'i', 't', 't'],
                            'alignments': ['l', 't', 'l', 'r', 'l', 'l'],
                            'query': {'sort': {'@timestamp': 'desc'}}},
                'status': {'mappings': {'@timestamp': 'Timestamp',
                                        'rule_uuid': 'Rule UUID',
                                        'rule_name': 'Rule Name',
                                        'start_time': 'Start Time',
                                        'end_time': 'End Time',
                                        'time_taken': 'Time Taken (secs)',
                                        'matches': 'Matches',
                                        'hits': 'Hits'},
                           'timestamps': ['@timestamp', 'start_time', 'end_time'],
                           'types': ['t', 't', 't', 't', 't', 'f', 'i', 'i'],
                           'alignments': ['l', 'l', 'l', 'l', 'l', 'r', 'r', 'r'],
                           'query': {'sort': {'@timestamp': 'desc'}}}}
    shortcuts = {'i': 'indices',
                 'a': 'alerts',
                 'e': 'error',
                 's': 'silence',
                 't': 'status',
                 'r': 'refresh',
                 'q': 'quit'}

    def __init__(self, reactor: Reactor):
        self.reactor = reactor
        self.highlight = {}
        self.cache = {}

        self.refresh = timedelta(seconds=self.reactor.args['refresh']) if self.reactor.args['refresh'] else None

        index = None if reactor.args['index'] == 'indices' else reactor.args['index']
        self.display = {'index': self.reactor.core.get_writeback_index(index) if index else None,
                        'type': reactor.args['index'],
                        'item': None,
                        'offset': 0,
                        'max_offset': 0,
                        'page': 1,
                        'total': 0,
                        'refreshed_at': {},
                        'input': ''}

        self.window = None
        self.header_window = None
        self.footer_window = None
        self.outer_window = None

        self._is_terminated = threading.Event()
        self._terminate_count = 0

    def handle_signal(self, signal_num, _):
        if signal_num == signal.SIGWINCH:
            self.handle_resize()
        else:
            self.handle_terminate(signal_num)

    def handle_terminate(self, signal_num):
        # Increment terminate count
        self._terminate_count += 1

        # If reached limit exit immediately
        if self._terminate_count >= 3:
            sys.exit(signal_num)

        # Set the is_terminated event
        self._is_terminated.set()

    def handle_resize(self):
        curses.endwin()
        self.window.refresh()
        # Update the other windows
        self.header_window.resize(1, self.window.getmaxyx()[1])
        self.footer_window.resize(1, self.window.getmaxyx()[1])
        self.footer_window.mvwin(self.window.getmaxyx()[0] - 1, 0)
        self.outer_window.resize(self.window.getmaxyx()[0] - 2, self.window.getmaxyx()[1])
        self.inner_window.resize(self.window.getmaxyx()[0] - 4, self.window.getmaxyx()[1] - 2)

    def run(self) -> int:
        """ Run the console application. """
        if not self.reactor.wait_until_responsive(timeout=self.reactor.args['timeout']):
            return 1

        curses.wrapper(self._main)
        return 0

    @staticmethod
    def right(window, text):
        return window.getmaxyx()[1] - len(text) - 1

    def _init(self):
        # Clear signal handling
        self._is_terminated.clear()
        self._terminate_count = 0

        # Clear highlight and cache
        self.highlight = {}
        self.cache = {}

        # Clear screen
        self.window.clear()

        # Set not delay on user input
        self.window.nodelay(True)

        # Hide the cursor
        curses.curs_set(0)
        curses.noecho()

        # Initialise the color combinations we're going to use
        curses.init_pair(YELLOW_ON_BLACK, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(GREEN_ON_BLACK, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(RED_ON_BLACK, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(BLUE_ON_BLACK, curses.COLOR_BLUE, curses.COLOR_BLACK)
        curses.init_pair(BLACK_ON_YELLOW, curses.COLOR_BLACK, curses.COLOR_YELLOW)
        curses.init_pair(BLACK_ON_GREEN, curses.COLOR_BLACK, curses.COLOR_GREEN)

    def _main(self, window):
        self.window = window
        self._init()
        window.refresh()

        self.header_window = curses.newwin(1, self.window.getmaxyx()[1], 0, 0)
        self.footer_window = curses.newwin(1, self.window.getmaxyx()[1], self.window.getmaxyx()[0] - 1, 0)
        self.outer_window = curses.newwin(self.window.getmaxyx()[0] - 2, self.window.getmaxyx()[1], 1, 0)
        self.inner_window = curses.newwin(self.window.getmaxyx()[0] - 4, self.window.getmaxyx()[1] - 2, 2, 1)

        # Draw the basics
        self._draw_header()
        self._draw_footer()
        self._draw_outer()
        self._draw_inner()

        while not self._is_terminated.is_set():
            c = window.getch()
            if c == curses.KEY_RESIZE:
                self._refresh(header=True, footer=True, outer=True, inner=True)

            elif c in list(map(ord, self.shortcuts.keys())):
                if chr(c) == 'r':
                    self._retrieve.cache_clear()
                    self.display['input'] = ''
                    self._refresh(footer=True, inner=True)
                elif chr(c) == 'q':
                    self._is_terminated.set()
                    continue
                else:
                    self._switch_view(index_type=self.shortcuts[chr(c)])

            elif c in list(map(ord, ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0'])):
                self.display['input'] += chr(c)
                self._refresh(footer=True)
            elif c == ord('.') or c == curses.KEY_ENTER or c == 10:  # Enter key
                try:
                    idx = int(self.display['input']) - 1
                    items = self._items(self.display['index'], self.display['type'])
                    item = items[idx]
                    if self.display['type'] == 'indices':
                        self.display['index'] = item['index']
                        for index_type in ['alert', 'error', 'silence', 'status']:
                            if self.display['index'].startswith(self.reactor.core.get_writeback_index(index_type)):
                                self.display['type'] = 'alerts' if index_type == 'alert' else index_type
                        if self.display['type'] == 'indices':
                            self.display['index'] = None
                    else:
                        self.display['item'] = item
                    pass
                except (KeyError, ValueError):
                    # ValueError means the input was not a valid integer
                    # KeyError means `idx` was not a valid item
                    # In either case, we can safely ignore the error
                    pass
                finally:
                    self.display['input'] = ''
                    self._refresh(header=True, footer=True, inner=True)
            elif c in (curses.KEY_BACKSPACE, curses.KEY_DC, 127):
                if len(self.display['input']):
                    self.display['input'] = self.display['input'][:-1]
                    self._refresh(footer=True)
            elif c == 27:  # Escape key
                if self.display['item']:
                    self._switch_view(index_name=self.display['index'], index_type=self.display['type'])
                else:
                    self._switch_view(index_type='indices')

            elif c == curses.KEY_UP:
                self.display['offset'] = max(0, self.display['offset'] - 1)
                self._refresh(inner=True)
            elif c == curses.KEY_DOWN:
                self.display['offset'] = min(self.display['max_offset'], self.display['offset'] + 1)
                self._refresh(inner=True)

            elif c == curses.KEY_PPAGE or c == curses.KEY_LEFT:
                self.display['page'] = max(1, self.display['page'] - 1)
                self._refresh(inner=True)
            elif c == curses.KEY_HOME:
                if self.display['item']:
                    self.display['offset'] = 0
                else:
                    self.display['page'] = 1
                self._refresh(inner=True)
            elif c == curses.KEY_NPAGE or c == curses.KEY_RIGHT:
                max_hits = max(0, self.inner_window.getmaxyx()[0] - 3)
                max_page = int(math.ceil(self.display['total'] / max_hits))
                self.display['page'] = min(max_page, self.display['page'] + 1)
                self._refresh(inner=True)
            elif c == curses.KEY_END:
                if self.display['item']:
                    self.display['offset'] = self.display['max_offset']
                else:
                    max_hits = max(0, self.inner_window.getmaxyx()[0] - 3)
                    max_page = int(math.ceil(self.display['total'] / max_hits))
                    self.display['page'] = max_page
                self._refresh(inner=True)

            elif self.refresh and dt_now() - self._refreshed_at() > self.refresh:
                if len(self.display['input']) == 0:
                    self._retrieve.cache_clear()
                    self._refresh(inner=True)

            self._is_terminated.wait(0.01)

    def _switch_view(self, index_name: str = None, index_type: str = None):
        if not index_name and index_type != 'indices':
            index_name = self.reactor.core.get_writeback_index(index_type)
        self.display['index'] = index_name
        self.display['type'] = index_type or 'alerts'
        self.display['item'] = None
        self.display['page'] = 1
        self.display['input'] = ''
        self._refresh(header=True, inner=True, footer=True)

    def _refresh(self, header=False, footer=False, outer=False, inner=False):
        if header:
            self._draw_header()
        if footer:
            self._draw_footer()
        if outer:
            self._draw_outer()
        if inner:
            self._draw_inner()
        self.window.refresh()

    def _draw_header(self):
        try:
            title = ' Reactor Console'
            index = self.display['type'].capitalize()

            self.header_window.clear()
            self.header_window.chgat(-1, curses.A_REVERSE)
            self.header_window.addstr(0, 0, title, curses.A_REVERSE)
            self.header_window.addstr(0, self.right(self.header_window, index), index, curses.A_REVERSE)
        except curses.error as e:
            reactor_logger.error('Error drawing header: %s %s', type(e), str(e))
        finally:
            self.header_window.refresh()

    def _draw_footer(self):
        try:

            help_str = ' | '.join([f'[{k}] {v}' for k, v in self.shortcuts.items()])
            help_pos = self.right(self.footer_window, help_str)
            self.footer_window.clear()
            self.footer_window.addstr(0, help_pos, help_str, curses.A_NORMAL)
            pos = 1
            for v in self.shortcuts.values():
                self.footer_window.chgat(0, help_pos + pos, 1, curses.A_BOLD | curses.color_pair(YELLOW_ON_BLACK))
                pos += 7 + len(v)

            self.footer_window.addstr(0, 0, self.display['input'], curses.color_pair(GREEN_ON_BLACK))
        except curses.error as e:
            reactor_logger.error('Error drawing footer: %s %s', type(e), str(e))
        finally:
            self.footer_window.refresh()

    def _draw_outer(self):
        try:
            self.outer_window.clear()
            self.outer_window.box()
        except curses.error as e:
            reactor_logger.error('Error drawing outer: %s %s', type(e), str(e))
        finally:
            self.outer_window.refresh()

    def _draw_inner(self):
        try:
            self.inner_window.clear()
            if self.display['index'] is not None and self.display['item'] is not None:
                self._draw_item()
            else:
                self._draw_table()
        except curses.error as e:
            reactor_logger.error('Error drawing inner: %s %s', type(e), str(e))
        finally:
            self.inner_window.refresh()

    def _draw_item(self):
        self.inner_window.clear()
        offset = self.display['offset']
        max_width = self.inner_window.getmaxyx()[1]-2
        max_lines = max(0, self.inner_window.getmaxyx()[0])
        item = self.display['item']
        item_str = []
        for l in str(BasicHitString('plain', item)).split('\n'):
            for i in range(0, len(l), max_width):
                item_str.append(l[i:i+max_width])
        self.display['max_offset'] = max(0, len(item_str) - max_lines)
        item_str = '\n'.join(item_str[offset:offset + max_lines])
        self.inner_window.addstr(0, 0, item_str)

    def _draw_table(self):
        data = self._retrieve(self.display['index'], self.display['type'])

        self.display['items'] = {i: h for i, h in enumerate(data['hits']['hits'], 1)}
        self.display['total'] = data['hits']['total']

        max_hits = self.inner_window.getmaxyx()[0]
        table, count, total = self._generate_table(self.inner_window, self.settings[self.display['type']], data)
        table = table.split('\n')
        table_header, table_body = table[0:2], table[2:]
        refresh_str = f'''Refresh {int(self.refresh.total_seconds())}s''' if self.refresh else ''
        page_str = f'''Page: {self.display['page']} of {int(total/max_hits) + 1}'''
        total_str = f'''Total: {count} of {total}'''
        info_str = ' | '.join([refresh_str, page_str, total_str])

        # Draw the table header
        refreshed_at = self.display['refreshed_at'].get(self.display.get('index', 'indices'), dt_now())
        self.inner_window.addstr(0, 0, pretty_ts(refreshed_at, False), curses.color_pair(BLUE_ON_BLACK))
        self.inner_window.addstr(0, 24, self.display['index'] or '', curses.A_BOLD)
        self.inner_window.addstr(0, self.right(self.inner_window, info_str), info_str)
        self.inner_window.addstr(1, 0, table_header[0])
        self.inner_window.addstr(2, 0, table_header[1])
        self.inner_window.redrawln(0, 3)

        # Draw the table body
        cur_page = self.display['page'] - 1
        per_page = max(0, self.inner_window.getmaxyx()[0] - 3)
        offset = cur_page * per_page
        _highlight = self.highlight[self.display['index'] or 'indices']

        line_num = 0
        row_first = True
        for line in table_body:
            if row_first:
                self.inner_window.addstr(3 + line_num, 0, line)
                self.inner_window.chgat(3 + line_num, 0, 5, curses.color_pair(YELLOW_ON_BLACK))
                if _highlight.get(line_num + offset) == 'create':
                    self.inner_window.chgat(3 + line_num, 8, len(line)-8, curses.color_pair(BLACK_ON_GREEN))
                elif _highlight.get(line_num + offset) == 'modify':
                    self.inner_window.chgat(3 + line_num, 8, len(line)-8, curses.A_REVERSE)
            row_first = line.startswith('-')
            line_num += 1 if row_first else 0
        self.inner_window.redrawln(3, 3 + line_num)

    def _generate_table(self, window, settings: dict, data: dict):
        table = tt.Texttable(window.getmaxyx()[1]-2)
        table.set_deco(table.HEADER | table.VLINES | table.HLINES)
        table.set_chars(['-', '|', '+', '~'])
        table.header([' '*5] + list(settings['mappings'].values()))
        table.set_cols_dtype(['t'] + (settings.get('types') or (['t'] * len(settings['mappings']))))
        table.set_cols_align(['r'] + (settings.get('alignments') or (['l'] * len(settings['mappings']))))

        cur_page = self.display['page'] - 1
        per_page = max(0, window.getmaxyx()[0] - 3)
        offset = cur_page * per_page
        hits = data['hits']['hits'][offset:offset+per_page]

        row_num = 0
        for row_num, hit in enumerate(hits, 1 + offset):
            source = hit['_source'] if '_source' in hit else hit
            row = [f'{row_num:5d}.']
            for prop in settings['mappings'].keys():
                value = dots_get(hit if prop.startswith('_') else source, prop, '')
                if prop in settings['timestamps']:
                    try:
                        value = pretty_ts(ts_to_dt(value), False)
                        pass
                    except ValueError:
                        pass
                row.append(value)
            table.add_row(row)

        table_str = table.draw()
        table_width = len(table_str.split('\n')[-1])
        if not data['hits']['hits']:
            table_str += '\n' + '=== No hits ==='.center(table_width)
        # If reached max_hits limit
        if row_num < int(data['hits']['total']) and (row_num - offset) < per_page:
            table_str += '\n' + '---' if row_num > 0 else ''
            table_str += '\n' + '=== Reached max hits limit ==='.center(table_width)

        return table_str, len(data['hits']['hits']), int(data['hits']['total'])

    def _refreshed_at(self, index: str = None):
        index = index or self.display['index'] or 'indices'
        return self.display['refreshed_at'][index]

    def _items(self, index: str = None, index_type: str = None):
        return self._retrieve(index, index_type)['hits']['hits']

    @lru_cache(maxsize=10)
    def _retrieve(self, index: str = None, index_type: str = None):
        refreshed_at = self.display['refreshed_at'].get(index or 'indices', dt_now())
        _highlight = {}
        create_field = '_source.@timestamp'
        modify_field = None

        if index is None:
            res = self.reactor.es_client.cat.indices(index=self.reactor.writeback_index + '_*', format='json')
            res = {'hits': {'total': len(res),
                            'hits': sorted(res, key=lambda h: h['index'])}}
            for i, hit in enumerate(res['hits']['hits']):
                res['hits']['hits'][i]['shards'] = f'{hit["pri"]} * {hit["rep"]}'
        else:
            query = self.settings[index_type]['query']
            res = self.reactor.es_client.search(index=index, body=query,
                                                size=self.reactor.args['max_hits'], version=True)
            # Modify the result depending on the index
            if index_type == 'alerts':
                create_field = '_source.alert_time'
                modify_field = '_source.modify_time'
                for i, hit in enumerate(res['hits']['hits']):
                    ended_at = ts_to_dt(dots_get(hit['_source'], 'match_data.ended_at'))
                    began_at = ts_to_dt(dots_get(hit['_source'], 'match_data.began_at'))
                    res['hits']['hits'][i]['_source']['duration'] = str(ended_at - began_at).split('.')[0]

        for i, hit in enumerate(res['hits']['hits']):
            if create_field and ts_to_dt(dots_get(hit, create_field, '1970')) > refreshed_at:
                _highlight[i] = 'create'
            elif modify_field and ts_to_dt(dots_get(hit, modify_field, '1970')) > refreshed_at:
                _highlight[i] = 'modify'

        # Make note of when this happened
        self.display['refreshed_at'][index or 'indices'] = dt_now()
        self.highlight[index or 'indices'] = _highlight
        return res
