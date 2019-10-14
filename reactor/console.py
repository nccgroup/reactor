import curses
import time

import texttable as tt

from .reactor import Reactor
from .util import ts_to_dt, pretty_ts, dt_now

highlight = []
cache = {}


def run_console(client: Reactor):
    curses.wrapper(console_main, client=client)


def console_main(stdscr, client: Reactor):
    # Hide the cursor
    curses.curs_set(0)

    running = True

    # Initialise the color combinations we're going to use
    curses.init_pair(1, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_BLACK, curses.COLOR_YELLOW)
    curses.init_pair(6, curses.COLOR_BLACK, curses.COLOR_GREEN)
    stdscr.nodelay(True)

    # Begin the program
    stdscr.addstr(0, 0, 'Reactor Console', curses.A_REVERSE)
    stdscr.chgat(-1, curses.A_REVERSE)

    # Set up windows
    help_window = curses.newwin(1, curses.COLS, curses.LINES-1, 0)
    table_window = curses.newwin(curses.LINES-2, curses.COLS, 1, 0)
    table_header_window = table_window.subwin(3, curses.COLS-2, 2, 1)
    table_content_window = table_window.subwin(curses.LINES-8, curses.COLS-2, 5, 1)
    table_nav_window = table_window.subwin(1, curses.COLS-2, table_window.getmaxyx()[0]-1, 1)
    table_nav_window.addstr(0, 0, 'First Page (Home)  |  Previous Page (<- or PgUp)')
    table_nav_window.addstr(0, table_nav_window.getmaxyx()[1]-43, 'Next Page (-> or PgDn)  |  Last Page (End)')
    table_nav_window.chgat(0, 0, -1, curses.color_pair(5))

    help_window.addstr(0, 0, '(i)ndices | (a)lert | (e)rror | (s)ilence | s(t)atus | re(f)resh | (q)uit')
    help_window.chgat(0, 1, 1, curses.A_BOLD | curses.color_pair(1))
    help_window.chgat(0, 13, 1, curses.A_BOLD | curses.color_pair(1))
    help_window.chgat(0, 23, 1, curses.A_BOLD | curses.color_pair(1))
    help_window.chgat(0, 33, 1, curses.A_BOLD | curses.color_pair(1))
    help_window.chgat(0, 46, 1, curses.A_BOLD | curses.color_pair(1))
    help_window.chgat(0, 58, 1, curses.A_BOLD | curses.color_pair(2))
    help_window.chgat(0, 68, 1, curses.A_BOLD | curses.color_pair(3))

    # Draw a border around the table window
    table_window.box()

    # Refresh/display the application
    stdscr.noutrefresh()
    help_window.noutrefresh()
    table_window.noutrefresh()
    table_nav_window.noutrefresh()
    curses.doupdate()

    page = 0

    index = client.args['index']
    refreshed_at = 0
    redraw_header = True
    redraw_content = True
    while running:
        if refreshed_at + 10 < time.time():
            refreshed_at = time.time()
            table, count, total = generate_table(client, index, table_content_window.getmaxyx()[0], page)
            table = table.split('\n')
            header, table = table[0:2], table[2:]
            total_str = 'Page: %s of %s | Total: %s of %s' % (page+1, int(total/table_content_window.getmaxyx()[0])+1, count, total)
            redraw_header = True
            redraw_content = True

        if redraw_header:
            # Draw the header
            table_header_window.clear()
            table_header_window.addstr(0, 0, (index or 'indices').upper(), curses.A_BOLD)
            table_header_window.addstr(0, 10, pretty_ts(dt_now(), False), curses.color_pair(4))
            table_header_window.addstr(0, curses.COLS-(2+len(total_str)), total_str)
            table_header_window.addstr(1, 0, header[0])
            table_header_window.addstr(2, 0, header[1])
            table_header_window.refresh()
            redraw_header = False

        if redraw_content:
            # Draw the content
            table_content_window.clear()
            line_num = 0
            first = True
            for line in table:
                if first:
                    table_content_window.addstr(line_num, 0, line)
                    first = False
                if line.startswith('----'):
                    first = True
                    line_num += 1
            for line, new_alert in highlight:  # type: int, bool
                table_content_window.chgat(line, 0, -1, curses.color_pair(6) if new_alert else curses.A_REVERSE)
            table_content_window.refresh()
            redraw_content = False

        c = stdscr.getch()
        if c == ord('q'):
            running = False
        elif c == ord('f'):
            refreshed_at = 0

        elif c == ord('i'):
            page = 0
            refreshed_at = 0
            index = None
        elif c == ord('a'):
            page = 0
            refreshed_at = 0
            index = 'alert'
        elif c == ord('e'):
            page = 0
            refreshed_at = 0
            index = 'error'
        elif c == ord('s'):
            page = 0
            refreshed_at = 0
            index = 'silence'
        elif c == ord('t'):
            page = 0
            refreshed_at = 0
            index = 'status'

        elif c == curses.KEY_PPAGE or c == curses.KEY_LEFT:
            page = max(0, page-1)
            refreshed_at = 0
        elif c == curses.KEY_HOME:
            page = 0
            refreshed_at = 0
        elif c == curses.KEY_NPAGE or c == curses.KEY_RIGHT:
            page = min(int(total/table_content_window.getmaxyx()[0]), page+1)
            refreshed_at = 0
        elif c == curses.KEY_END:
            page = int(total/table_content_window.getmaxyx()[0])
            refreshed_at = 0

        else:
            time.sleep(0.1)

        # Refresh the window from the bottom up (to stop flickering)
        stdscr.noutrefresh()
        table_window.noutrefresh()
        table_header_window.noutrefresh()
        table_content_window.noutrefresh()
        table_nav_window.noutrefresh()
        help_window.noutrefresh()
        curses.doupdate()


def generate_table(client: Reactor, index: str, max_hits: int, page: int = 0):
    global highlight
    highlight = []
    try:
        offset = page * max_hits

        tab = tt.Texttable()
        tab.set_max_width(curses.COLS - 4)
        # tab.set_max_width(80)
        tab.set_deco(tab.HEADER | tab.VLINES | tab.HLINES)

        if index == 'status':
            index = client.core.get_writeback_index('status')
            tab.header(['Timestamp', 'Rule UUID', 'Rule Name', 'Start Time', 'End Time', 'Time Taken (s)', 'Matches', 'Hits'])
            tab.set_cols_dtype(['t', 't', 't', 't', 't', 'f', 'i', 'i'])
            tab.set_cols_align(['l', 'l', 'l', 'l', 'l', 'r', 'r', 'r'])

            query = {'sort': {'@timestamp': 'desc'}}
            res = client.es_client.search(index=index, size=max_hits, from_=offset, body=query)
            for hit in res['hits']['hits']:
                source = hit['_source']  # type: dict
                try:
                    row = [
                        pretty_ts(ts_to_dt(source.pop('@timestamp')), False),
                        source.pop('rule_uuid'),
                        source.pop('rule_name'),
                        pretty_ts(ts_to_dt(source.pop('start_time')), False),
                        pretty_ts(ts_to_dt(source.pop('end_time')), False),
                        source.pop('time_taken'),
                        source.pop('matches'),
                        source.pop('hits'),
                    ]
                except Exception as e:
                    row = [str(e), repr(hit['_source']), hit['_id'], '', '', '', '', '']
                tab.add_row(row)  # + [source]

            count = len(res['hits']['hits'])
            total = int(res['hits']['total'])

        elif index == 'error':
            index = client.core.get_writeback_index('error')
            tab.header(['Timestamp', 'Rule UUID', 'Data', 'Message', 'Traceback'])
            column = ['@timestamp', 'rule_uuid', 'data', 'message', 'traceback']
            tab.set_cols_dtype(['t', 't', 'a', 't', 't'])
            tab.set_cols_align(['l', 't', 'l', 'l', 'l'])

            query = {'sort': {column[0]: 'desc'}}
            res = client.es_client.search(index=index, size=max_hits, from_=offset, body=query)
            for hit in res['hits']['hits']:
                source = hit['_source']  # type: dict
                row = [pretty_ts(ts_to_dt(source.pop(column[0])), False)] + [source.pop(key) for key in column[1:]]
                tab.add_row(row)  # + [source]

            count = len(res['hits']['hits'])
            total = int(res['hits']['total'])

        elif index == 'silence':
            index = client.core.get_writeback_index('silence')
            tab.header(['Timestamp', 'Rule UUID', 'Until', 'Exponent', 'Silence Key', 'Alert UUID'])
            tab.set_cols_dtype(['t', 't', 't', 'i', 't', 't'])
            tab.set_cols_align(['l', 't', 'l', 'r', 'l', 'l'])

            query = {'sort': {'@timestamp': 'desc'}}
            res = client.es_client.search(index=index, size=max_hits, from_=offset, body=query)
            for hit in res['hits']['hits']:
                source = hit['_source']  # type: dict
                row = [
                    pretty_ts(ts_to_dt(source.pop('@timestamp')), False),
                    source.pop('rule_uuid'),
                    pretty_ts(ts_to_dt(source.pop('until')), False),
                    source.pop('exponent'),
                    source.pop('silence_key'),
                    source.pop('alert_uuid'),
                ]
                tab.add_row(row)  # + [source]

            count = len(res['hits']['hits'])
            total = int(res['hits']['total'])

        elif index == 'alert':
            cache.setdefault('alert', dt_now())
            index = client.conf['alert_alias']
            tab.header(['',
                        'Began At',
                        'Ended At',
                        # 'Alert Time',
                        # 'Match Time',
                        'Alert UUID',
                        'Rule Name',
                        'Priority',
                        'Duration',
                        'Num Matches',
                        'Num Events'])
            tab.set_cols_dtype(['t', 't', 't', 't', 't', 't', 't', 'i', 'i'])
            tab.set_cols_align(['r', 'l', 'l', 'c', 'l', 'r', 'r', 'r', 'r'])

            query = {'sort': {'match_time': 'desc'}}
            res = client.es_client.search(index=index, size=max_hits, from_=offset, body=query)
            for i, hit in enumerate(res['hits']['hits']):
                source = hit['_source']  # type: dict
                if ts_to_dt(source['modify_time']) > cache['alert']:
                    highlight.append((i, ts_to_dt(source['alert_time']) > cache['alert']))
                row = [
                    '(' + str(i + 1 + offset) + ')',
                    pretty_ts(ts_to_dt(source['match_data']['began_at']), False),
                    pretty_ts(ts_to_dt(source['match_data']['ended_at']), False),
                    # pretty_ts(ts_to_dt(source.pop('alert_time')), False),
                    # pretty_ts(ts_to_dt(source.pop('match_time')), False),
                    source.pop('uuid'),
                    source.pop('rule_name'),
                    source.pop('priority'),
                    str(ts_to_dt(source['match_data']['ended_at'])-ts_to_dt(source['match_data']['began_at'])).split('.')[0],
                    source['num_matches'],
                    source['match_data']['num_events'],
                ]
                tab.add_row(row)  # + [source]

            count = len(res['hits']['hits'])
            total = int(res['hits']['total'])

            # Update cache
            cache['alert'] = dt_now()

        else:
            tab.header(['UUID', 'Index Name', 'Status', 'Health', 'Num Documents', 'Shards', 'Size'])
            tab.set_cols_dtype(['t', 't', 't', 't', 'i', 't', 't'])
            tab.set_cols_align(['l', 'l', 'l', 'l', 'r', 'r', 'r'])

            res = client.es_client.cat.indices(index=client.writeback_index + '_*', format='json')
            for hit in sorted(res, key=lambda h: h['index'])[offset:offset+max_hits]:
                tab.add_row([
                    hit.pop('uuid'),
                    hit.pop('index'),
                    hit.pop('status'),
                    hit.pop('health'),
                    hit.pop('docs.count'),
                    '%s * %s' % (hit.pop('pri'), hit.pop('rep')),
                    hit.pop('store.size')
                ])
                pass
            count = len(res[offset:offset+max_hits])
            total = len(res)

        return tab.draw(), count, total

    except Exception as e:
        return '%s\n\n' % str(e), 0, 0

