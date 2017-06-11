from __future__ import print_function
# from collections import defaultdict
import gc
import json
from multiprocessing import Manager, Pipe, Process, Pool
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
            except ConnectionError as ce:
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


def combine_keys(data_queue, outfile, conn, delay=0.0000000001, debug=False):
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

    # CSV file gets too long!
    # with open(outfile, 'w') as f:
    #     buffer = 0
    #     try:
    #         while not conn.poll(delay):
    #             while not data_queue.empty():
    #                 data = data_queue.get()
    #                 f.write(
    #                     str(data[0]) + ',' + ','.join(
    #                         map(lambda x: str(x['tank_id']), data[1])) + '\n')
    #                 if debug:
    #                     print('Added tanks for', data[0])
    #                 buffer += 1
    #                 if buffer >= 250:
    #                     f.flush()
    #                     buffer = 0
    #                     if debug:
    #                         print('Flushed to file')
    #         while not data_queue.empty():
    #             data = data_queue.get()
    #             f.write(
    #                 str(data[0]) + ',' + ','.join(
    #                     map(lambda x: str(x['tank_id']), data[1])) + '\n')
    #             if debug:
    #                 print('Added tanks for', data[0])
    #             buffer += 1
    #             if buffer >= 250:
    #                 f.flush()
    #                 buffer = 0
    #                 if debug:
    #                     print('Flushed to file')

    #     except (IOError, EOFError):
    #         print('Did the parent terminate?')

    # Pickle to file is too large!
    # tanks = defaultdict(set)
    # try:
    #     while not conn.poll(delay):
    #         while not data_queue.empty():
    #             data = data_queue.get()
    #             for entry in data[1]:
    #                 tanks[entry['tank_id']].add(data[0])
    #             if debug:
    #                 print('Added tanks for', data[0])
    #     while not data_queue.empty():
    #         data = data_queue.get()
    #         for entry in data[1]:
    #             tanks[entry['tank_id']].add(data[0])
    #         if debug:
    #             print('Added tanks for', data[0])
    # except (IOError, EOFError):
    #     print('Did the parent terminate?')
    # finally:
    #     with open(outfile, 'wb') as f:
    #         if debug:
    #             print('CK: Starting the pickling process . . .')
    #         pickle.dump(tanks, f, pickle.HIGHEST_PROTOCOL)
    #         if debug:
    #             print('CK: Pickling complete!')

if __name__ == '__main__':
    with open(argv[1]) as f:
        config = json.load(f)

    manager = Manager()
    player_queue = manager.Queue()
    error_queue = manager.Queue()

    process_count = 4 if 'pool size' not in config else config['pool size']
    delay = 0.0000000001 if 'delay' not in config else config['delay']
    timeout = 15 if 'timeout' not in config else config['timeout']
    max_account = 14000000 if 'max account' not in config else config[
        'max account']
    max_retries = 5 if 'max retries' not in config else config[
        'max retries']
    debug = False if 'debug' not in config else config['debug']
    start_account = 1 if 'start account' not in config else config[
        'start account']
    queue_handler_conn, queue_child_conn = Pipe()
    pool = Pool(process_count, maxtasksperchild=10)
    queue_handler = Process(
        name='Tanks DB Handler',
        target=combine_keys,
        args=(
            player_queue,
            # config['input'],
            config['output'],
            queue_child_conn,
            delay,
            debug))
    try:
        # pool_handler_conn, pool_child_conn = Pipe()
        if debug:
            print('Starting queue handler')
        queue_handler.start()
        if debug:
            print('Adding work to pool')
        for i in range(start_account, max_account):
            pool.apply_async(
                query,
                (
                    i,
                    # pool_child_conn,
                    error_queue,
                    config['application_id'],
                    player_queue,
                    delay,
                    timeout,
                    max_retries,
                    debug
                ))
    except (KeyboardInterrupt, SystemExit):
        print('Attempting to prematurely terminate processes')
        pool.terminate()
    finally:
        pool.close()
        pool.join()
        # player_queue.close()
        queue_child_conn.send(-1)
        if debug:
            print('Sending signal to queue handler')
            print('Pickling can take a *long* time')
        queue_handler.join(1800)
        if not error_queue.empty() and debug:
            print('Errors received from pool:')
        while error_queue.qsize() and debug:
            print('\t', error_queue.get())
