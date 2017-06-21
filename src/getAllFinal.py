from __future__ import print_function
# from collections import defaultdict
import gc
import json
from multiprocessing import Manager, Pipe, Process
import sqlite3
from requests import ConnectionError
from sys import argv
from wotconsole import WOTXResponseError, player_tank_statistics

try:
    range = xrange
except NameError:
    pass


def query(player, error_queue, api_key, result_queue,
          delay=0.00000001, timeout=10, max_retries=5, debug=False):
    '''
    Pull data from WG's servers. This allows us to retry pages up until a
    certain point
    '''
    try:
        retries = max_retries
        while retries:
            try:
                # Try Xbox first
                ans = player_tank_statistics(
                    player,
                    application_id=api_key,
                    fields='tank_id',
                    timeout=timeout)
                if not ans[str(player)]:
                    ans = player_tank_statistics(
                        player,
                        application_id=api_key,
                        fields='tank_id',
                        timeout=timeout,
                        api_realm='ps4')
                if ans[str(player)]:
                    result_queue.put((
                        player,
                        set(map(lambda x: x['tank_id'], ans[str(player)]))
                        # ans[str(player)]
                    ))
                break
            # Patch: until WOTXResponseError is updated, exceeding the API
            # request limit is not properly handled by the library
            except (TypeError, ConnectionError) as ce:
                # print('Error for page {}'.format(page_no))
                # print(ce)
                if 'Max retries exceeded with url' in str(ce):
                    retries -= 1
                else:
                    # parent_pipe.send(ce)
                    error_queue.put(ce)
                    break
            except WOTXResponseError as wg:
                # parent_pipe.send(wg)
                error_queue.put(wg)
                break
    except Exception as e:
        print('Worker: Unknown error: {}'.format(e))
        try:
            error_queue.put(e)
            # parent_pipe.send(e)
        except:
            pass


def update_stats_database(data_queue, outfile, conn,
                          delay=0.0000000001, debug=False):
    # This delay float is overkill, but we don't want to block for long!

    # with open(outfile, 'w') as f:
    with sqlite3.connect(outfile) as db:
        buffer = 0
        c = db.cursor()
        tanks = set()
        # noname = ''
        # SELECT name FROM sqlite_master WHERE type='table' AND name=?;
        c.execute("Create table if not exists tanks(id INTEGER PRIMARY KEY)")
        # We won't add names to this right now
        c.execute("""Create table if not exists players(id INTEGER PRIMARY KEY,
                   name TEXT)""")
        try:
            while not conn.poll(delay):
                while not data_queue.empty():
                    player, playertanks = data_queue.get()
                    if not playertanks:
                        continue
                    c.execute("insert or ignore into players values (?, ?)",
                              # (player, noname))
                              (player, None))
                    for tank in playertanks:
                        if tank not in tanks:
                            tanks.add(tank)
                            c.execute(("create table if not exists _{}(player "
                                       "INTEGER PRIMARY KEY)").format(tank))
                        c.execute("insert or ignore into _{} values (?)".format(tank),
                                  (player,))
                    buffer += 1
                    if debug:
                        print('Added tanks for', player)
                    if buffer >= 2500:
                        db.commit()
                        gc.collect()
                        buffer = 0
                        if debug:
                            print('Cleared buffer and memory')
            if debug:
                print('CK: Parent signal received. Clearing queue')
            while not data_queue.empty():
                player, playertanks = data_queue.get()
                if not playertanks:
                    continue
                c.execute("insert or ignore into players values (?, ?)",
                          # (player, noname))
                          (player, None))
                for tank in playertanks:
                    if tank not in tanks:
                        tanks.add(tank)
                        c.execute(("create table if not exists _{}(player "
                                   "INTEGER PRIMARY KEY)").format(tank))
                    c.execute("insert or ignore into _{} values (?)".format(tank),
                              (player,))
                buffer += 1
                if debug:
                    print('Added tanks for', player)
                if buffer >= 2500:
                    db.commit()
                    gc.collect()
                    buffer = 0
                    if debug:
                        print('Cleared buffer and memory')

        except (IOError, EOFError):
            print('Did the parent terminate?')

        finally:
            db.commit()


def update_csv(queue, stats_csv, player_csv, conn, delay, debug):
    pass


def generate_players(sqldb, start, finish):
    '''
    Create the list of players to query for. If the SQLite database already has
    a list, we'll use it; otherwise we'll generate a range.

    :param sqldb: File name of SQLite database
    :param int start: Starting account ID number
    :param int finish: Ending account ID number
    '''
    with sqlite3.connect(sqldb) as db:
        try:
            return filter(lambda x: start <= x <= finish,
                          map(lambda p: p[0],
                              db.execute('select id from players').fetchall()))
        except (ValueError, sqlite3.OperationalError):
            return range(start, finish)

# 1 Worker to export to CSV
# 1 Worker to export errors to file
# 1 Worker to handle database
# N-number of workers to handle requests to API

if __name__ == '__main__':
    with open(argv[1]) as f:
        config = json.load(f)

    manager = Manager()
    error_queue = manager.Queue()

    process_count = 4 if 'pool size' not in config else config['pool size']
    start_account = 1 if 'start account' not in config else config[
        'start account']
    max_account = 14000000 if 'max account' not in config else config[
        'max account']
    max_retries = 5 if 'max retries' not in config else config[
        'max retries']
    timeout = 15 if 'timeout' not in config else config['timeout']
    delay = 0.0000000001 if 'delay' not in config else config['delay']
    join_wait = 1800 if 'join wait' not in config else config['join wait']
    debug = False if 'debug' not in config else config['debug']
    if 'database' not in config:
        config['database'] = None

    player_ids = generate_players(
        config['database'],
        start_account,
        max_account)

    handlers = []
    handler_conns = []
    queues = []

    if 'sql' in config['output']:
        sql_handler_conn, sql_child_conn = Pipe()
        sql_queue = manager.Queue()
        sql_handler = Process(
            name='Stats DB Handler',
            target=update_stats_database,
            args=(
                sql_queue,
                # config['input'],
                config['output']['sql'],
                sql_child_conn,
                delay,
                debug))
        handlers.append(sql_handler)
        handler_conns.append(sql_child_conn)
        queues.append(sql_queue)

    if 'csv' in config['output']:
        csv_handler_conn, csv_child_conn = Pipe()
        csv_queue = manager.Queue()
        csv_handler = Process(
            name='Player DB Handler',
            target=update_csv,
            args=(
                csv_queue,
                csv_child_conn,
                # config['input'],
                config['output']['csv'],
                config['output']['players'],
                config['output']['tanks'],
                delay,
                debug))
        handlers.append(csv_handler)
        handler_conns.append(csv_child_conn)
        queues.append(csv_queue)

    pipes = []
    processes = []
    waiting = []
    for ps in range(0, process_count):
        parent_conn, child_conn = Pipe()
        processes.append(
            Process(
                target=query,
                args=(
                    ps,
                    child_conn,
                    config['application_id'],
                    queues,
                    delay,
                    timeout,
                    max_retries,
                    debug)))
        pipes.append(parent_conn)
        waiting.append(True)

    try:
        # pool_handler_conn, pool_child_conn = Pipe()
        if debug:
            print('Starting data handlers')
        for handler in handlers:
            handler.start()
        for p in processes:
            p.start()
        not_done = True
        player_iter = iter(player_ids)
        if debug:
            print('Adding work to pool')
        while not_done:
            for n, process in enumerate(processes):
                if pipes[n].poll(delay):
                    received = pipes[n].recv()
                    if isinstance(received, int):
                        if received > 0:
                            if debug:
                                print(
                                    'Parent: Worker {} got page {}'.format(
                                        n, received))
                            waiting[n] = True
                        else:
                            if debug:
                                print(
                                    'Parent: Worker {} hit page max'.format(n))
                            not_done = False
                            break
                    else:
                        print('Exception from child {}:'.format(n), received)
                        not_done = False
                        break
                if waiting[n]:
                    try:
                        pipes[n].send(player_iter.next())
                        waiting[n] = False
                    except StopIteration:
                        if debug:
                            print('No more player IDs to process')
                        not_done = False
                        break
    except (KeyboardInterrupt, SystemExit):
        print('Attempting to prematurely terminate processes')
    finally:
        for n, p in enumerate(processes):
            pipes[n].send(0)
            p.join()
        # player_queue.close()
        for conn in handler_conns:
            conn.send(-1)
        # sql_handler_conn.send(-1)
        # csv_handler_conn.send(-1)
        if debug:
            print('Sending signal to queue handler(s)')
        for handler in handlers:
            handler.join(join_wait)
        # sql_handler.join(join_wait)
        # csv_handler.join(join_wait)
