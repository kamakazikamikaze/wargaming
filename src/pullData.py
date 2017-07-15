from __future__ import print_function
import cPickle as pickle
# from collections import defaultdict
from datetime import datetime
from itertools import chain
import gc
import json
import logging
from multiprocessing import Manager, Pipe, Process
import sqlite3
from requests import ConnectionError
import socket
from sys import argv
import signal
from wotconsole import WOTXResponseError, player_tank_statistics, player_data
from wotconsole import vehicle_info

try:
    range = xrange
except NameError:
    pass


def query(worker_number, parent_pipe, api_key, result_queues, error_queue,
          delay=0.00000001, timeout=10, max_retries=5, debug=False):
    '''
    Pull data from WG's servers. This allows us to retry pages up until a
    certain point
    '''
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    buf = 0
    not_done = True
    while not_done:
        try:
            if not parent_pipe.poll(delay):
                # No work received yet. Wait.
                continue
            received = parent_pipe.recv()
            # If we get a number lower than one, we exit the process
            if not received:
                not_done = False
                break
            if not isinstance(received, int):
                raise TypeError(
                    '{}: Bad entry from pipe'.format(worker_number))
            retries = max_retries
            realm = 'xbox' if received < 1000000000 else 'ps4'
            ans = None
            player_queued = False
            queried = False
            while retries:
                try:
                    # We can get a query limit error from the server in this
                    # loop. To prevent duplicate player retrieval, we'll check
                    # if we've already received a response for it and move on
                    if not queried:
                        # Try Xbox first
                        ans = player_data(
                            received,
                            api_key,
                            fields=[
                                '-statistics.company',
                                '-statistics.frags',
                                '-private'],
                            timeout=timeout,
                            api_realm=realm)
                        # Xbox and PS4 account ID ranges do not overlap
                        # if not ans[str(received)]:
                        #     realm = 'ps4'
                        #     ans = player_data(
                        #         received,
                        #         api_key,
                        #         fields=[
                        #             '-statistics.company',
                        #             '-statistics.all.frags',
                        #             '-statistics.frags',
                        #             '-private'],
                        #         timeout=timeout,
                        #         api_realm=realm)
                        queried = True
                    # An empty resposne = player was removed or ID not yet made
                    if ans.data and ans[str(received)]:
                        # Prevent duplicate entries into Queues
                        if not player_queued:
                            player_queued = True
                            p = ans[str(received)]
                            p.update(p['statistics']['all'])
                            del p['statistics']['all']
                            p.update(p['statistics'])
                            del p['statistics']
                            p['console'] = realm
                            p['created_at_raw'] = p['created_at']
                            p['last_battle_time_raw'] = p['last_battle_time']
                            p['updated_at_raw'] = p['updated_at']
                            p['created_at'] = datetime.strftime(
                                datetime.utcfromtimestamp(p['created_at']),
                                '%Y-%m-%d')
                            p['last_battle_time'] = datetime.strftime(
                                datetime.utcfromtimestamp(
                                    p['last_battle_time']),
                                '%Y-%m-%d')
                            p['updated_at'] = datetime.strftime(
                                datetime.utcfromtimestamp(p['updated_at']),
                                '%Y-%m-%d')
                            for result_queue in result_queues:
                                result_queue.put(('player', p))
                        tanks = player_tank_statistics(
                            received,
                            api_key,
                            fields=[
                                '-company',
                                '-frags',
                                '-in_garage',
                                '-in_garage_updated'],
                            timeout=timeout,
                            api_realm=realm)
                        # Some player IDs return an empty dict
                        if str(received) not in tanks.data:
                            error_queue.put(
                                (received, Exception(
                                    "Method 'player_tank_statistics' did not yield any tanks")))
                        elif tanks[str(received)] is None:
                            pass
                        else:
                            for tank in tanks[str(received)]:
                                tank.update(tank['all'])
                                tank['last_battle_time_raw'] = tank[
                                    'last_battle_time']
                                tank['last_battle_time'] = datetime.strftime(
                                    datetime.utcfromtimestamp(
                                        tank['last_battle_time']),
                                    '%Y-%m-%d')
                                del tank['all']
                                for result_queue in result_queues:
                                    result_queue.put(('tank', tank))
                    buf += 1
                    if buf >= 1000:
                        gc.collect()
                        buf = 0
                        if debug:
                            print(
                                'QT{:2}: Cleared memory'.format(worker_number))
                    parent_pipe.send(received)
                    break
                # Patch: until WOTXResponseError is updated, exceeding the API
                # request limit is not properly handled by the library
                except (TypeError, ConnectionError) as ce:
                    # print('Error for page {}'.format(page_no))
                    # print(ce)
                    if 'Max retries exceeded with url' in str(ce):
                        retries -= 1
                    else:
                        parent_pipe.send('{} (Error)'.format(received))
                        error_queue.put((received, ce))
                        break
                except WOTXResponseError as wg:
                    if 'REQUEST_LIMIT_EXCEEDED' in wg.message:
                        retries -= 1
                    else:
                        parent_pipe.send('{} (Error)'.format(received))
                        error_queue.put((received, wg))
                        break
            if not retries:
                parent_pipe.send('{} (Retry limit exceeded'.format(received))
                error_queue.put((received, Exception('Retry limit exceeded')))
            # Just in case!
            del ans
            received = None
        except Exception as e:
            print('{}: Unknown error: {}'.format(ps, e))
            try:
                error_queue.put((received, e))
                parent_pipe.send('Unknown (Error)')
            except:
                pass
    print('QT{:2}: Exiting'.format(worker_number))


def update_stats_database(data_queue, conn, tanks, outfile, error_queue,
                          delay=0.0000000001, debug=False):
    with sqlite3.connect(outfile) as db:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        buf = 0
        c = db.cursor()
        # setup database
        c.execute("""Create table if not exists tanks(
            name TEXT,
            short_name TEXT,
            tank_id INTEGER PRIMARY KEY,
            tier INTEGER,
            is_premium INTEGER,
            type TEXT,
            nation TEXT,
            price_gold INTEGER)""")
        for _, t in tanks.iteritems():
            c.execute('insert or ignore into tanks values (?,?,?,?,?,?,?,?)',
                      tuple(t[k] for k in (
                          'name',
                          'short_name',
                          'tank_id',
                          'tier',
                          'is_premium',
                          'type',
                          'nation',
                          'price_gold')))
        db.commit()
        c.execute("""Create table if not exists players(
            account_id INTEGER PRIMARY KEY,
            battles INTEGER,
            capture_points INTEGER,
            console TEXT,
            created_at TEXT,
            created_at_raw INTEGER,
            damage_assisted_radio INTEGER,
            damage_assisted_track INTEGER,
            damage_dealt INTEGER,
            damage_received INTEGER,
            direct_hits_received INTEGER,
            dropped_capture_points INTEGER,
            explosion_hits INTEGER,
            explosion_hits_received INTEGER,
            frags INTEGER,
            global_rating INTEGER,
            hits INTEGER,
            last_battle_time TEXT,
            last_battle_time_raw INTEGER,
            losses INTEGER,
            max_damage INTEGER,
            max_damage_tank_id INTEGER,
            max_frags INTEGER,
            max_frags_tank_id INTEGER,
            max_xp INTEGER,
            max_xp_tank_id INTEGER,
            nickname TEXT,
            no_damage_direct_hits_received INTEGER,
            piercings INTEGER,
            piercings_received INTEGER,
            shots INTEGER,
            spotted INTEGER,
            survived_battles INTEGER,
            trees_cut INTEGER,
            updated_at TEXT,
            updated_at_raw INTEGER,
            wins INTEGER,
            xp INTEGER)""")
        # faster than sorting keys, even if it adds a lot of lines
        player_keys = (
            'account_id',
            'battles',
            'capture_points',
            'console',
            'created_at',
            'created_at_raw',
            'damage_assisted_radio',
            'damage_assisted_track',
            'damage_dealt',
            'damage_received',
            'direct_hits_received',
            'dropped_capture_points',
            'explosion_hits',
            'explosion_hits_received',
            'frags',
            'global_rating',
            'hits',
            'last_battle_time',
            'last_battle_time_raw',
            'losses',
            'max_damage',
            'max_damage_tank_id',
            'max_frags',
            'max_frags_tank_id',
            'max_xp',
            'max_xp_tank_id',
            'nickname',
            'no_damage_direct_hits_received',
            'piercings',
            'piercings_received',
            'shots',
            'spotted',
            'survived_battles',
            'trees_cut',
            'updated_at',
            'updated_at_raw',
            'wins',
            'xp'
        )
        c.execute("""Create table if not exists stats(
            account_id INTEGER,
            battle_life_time INTEGER,
            battles INTEGER,
            capture_points INTEGER,
            damage_assisted_radio INTEGER,
            damage_assisted_track INTEGER,
            damage_dealt INTEGER,
            damage_received INTEGER,
            direct_hits_received INTEGER,
            dropped_capture_points INTEGER,
            explosion_hits INTEGER,
            explosion_hits_received INTEGER,
            frags INTEGER,
            hits INTEGER,
            last_battle_time TEXT,
            last_battle_time_raw INTEGER,
            losses INTEGER,
            mark_of_mastery INTEGER,
            max_frags INTEGER,
            max_xp INTEGER,
            no_damage_direct_hits_received INTEGER,
            piercings INTEGER,
            piercings_received INTEGER,
            shots INTEGER,
            spotted INTEGER,
            survived_battles INTEGER,
            tank_id INTEGER,
            trees_cut INTEGER,
            wins INTEGER,
            xp INTEGER)""")
        tank_keys = (
            'account_id',
            'battle_life_time',
            'battles',
            'capture_points',
            'damage_assisted_radio',
            'damage_assisted_track',
            'damage_dealt',
            'damage_received',
            'direct_hits_received',
            'dropped_capture_points',
            'explosion_hits',
            'explosion_hits_received',
            'frags',
            'hits',
            'last_battle_time',
            'last_battle_time_raw',
            'losses',
            'mark_of_mastery',
            'max_frags',
            'max_xp',
            'no_damage_direct_hits_received',
            'piercings',
            'piercings_received',
            'shots',
            'spotted',
            'survived_battles',
            'tank_id',
            'trees_cut',
            'wins',
            'xp'
        )
        try:
            while not conn.poll(delay):
                try:
                    while not data_queue.empty():
                        classification, data = data_queue.get()
                        if classification == 'tank':
                            c.execute(
                                'insert into stats values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                tuple(data[t] for t in tank_keys))
                        elif classification == 'player':
                            try:
                                c.execute(
                                    'insert into players values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                    tuple(data[p] for p in player_keys))
                            except sqlite3.IntegrityError:
                                pass
                        else:
                            print('SQL : Uh, got some bad data?')
                        buf += 1
                        # if debug:
                        #     print('SQL: Got data for', data['account_id'])
                        if buf >= 2500:
                            db.commit()
                            gc.collect()
                            buf = 0
                            if debug:
                                print('SQL : Cleared buffer')  # and memory')
                except (ValueError, sqlite3.OperationalError) as oe:
                    error_queue.put(
                        ('(sql) {}'.format(data['account_id']), oe))
            if debug:
                print('SQL : Parent signal received. Clearing queue')
            while not data_queue.empty():
                try:
                    classification, data = data_queue.get()
                    if classification == 'tank':
                        c.execute(
                            'insert into stats values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            tuple(data[t] for t in tank_keys))
                    elif classification == 'player':
                        try:
                            c.execute(
                                'insert into players values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                tuple(data[p] for p in player_keys))
                        except sqlite3.IntegrityError:
                            pass
                    else:
                        print('SQL : Uh, got some bad data?')
                    buf += 1
                    # if debug:
                    #     print('SQL: Got data for', data['account_id'])
                    if buf >= 2500:
                        db.commit()
                        gc.collect()
                        buf = 0
                        if debug:
                            print('SQL : Cleared buffer')  # and memory')
                except (ValueError, sqlite3.OperationalError):
                    error_queue.put(
                        ('(sql) {}'.format(data['account_id']), oe))

        except (IOError, EOFError):
            print('SQL : Did the parent terminate?')
        except Exception as e:
            print('SQL :', e)
            error_queue.put(('sql', e))
        finally:
            db.commit()
            print('SQL : Exiting')


def update_csv(data_queue, conn, tanks, stats_filename,
               player_filename, tankinfo_filename, error_queue,
               delay=0.0000000001, debug=False):
    buf = 0
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    stats_csv = open(stats_filename, 'a')
    player_csv = open(player_filename, 'a')
    with open(tankinfo_filename, 'w') as tankinfo_csv:
        tankinfo_csv.write(
            'name,short_name,tank_id,tier,is_premium,type,nation,price_gold\n')
        for _, t in tanks.iteritems():
            tankinfo_csv.write(','.join(
                map(
                    lambda x: str(x) if not isinstance(
                        x, unicode) else x.encode('utf-8'),
                    tuple(t[k] for k in (
                        'name',
                        'short_name',
                        'tank_id',
                        'tier',
                        'is_premium',
                        'type',
                        'nation',
                        'price_gold')
                    )
                )) + '\n')
    player_keys = (
        'account_id',
        'battles',
        'capture_points',
        'console',
        'created_at',
        'created_at_raw',
        'damage_assisted_radio',
        'damage_assisted_track',
        'damage_dealt',
        'damage_received',
        'direct_hits_received',
        'dropped_capture_points',
        'explosion_hits',
        'explosion_hits_received',
        'frags',
        'global_rating',
        'hits',
        'last_battle_time',
        'last_battle_time_raw',
        'losses',
        'max_damage',
        'max_damage_tank_id',
        'max_frags',
        'max_frags_tank_id',
        'max_xp',
        'max_xp_tank_id',
        'nickname',
        'no_damage_direct_hits_received',
        'piercings',
        'piercings_received',
        'shots',
        'spotted',
        'survived_battles',
        'trees_cut',
        'updated_at',
        'updated_at_raw',
        'wins',
        'xp'
    )
    player_csv.write(','.join(player_keys) + '\n')
    tank_keys = (
        'account_id',
        'battle_life_time',
        'battles',
        'capture_points',
        'damage_assisted_radio',
        'damage_assisted_track',
        'damage_dealt',
        'damage_received',
        'direct_hits_received',
        'dropped_capture_points',
        'explosion_hits',
        'explosion_hits_received',
        'frags',
        'hits',
        'last_battle_time',
        'last_battle_time_raw',
        'losses',
        'mark_of_mastery',
        'max_frags',
        'max_xp',
        'no_damage_direct_hits_received',
        'piercings',
        'piercings_received',
        'shots',
        'spotted',
        'survived_battles',
        'tank_id',
        'trees_cut',
        'wins',
        'xp'
    )
    stats_csv.write(','.join(tank_keys) + '\n')
    try:
        while not conn.poll(delay):
            while not data_queue.empty():
                classification, data = data_queue.get()
                if classification == 'tank':
                    stats_csv.write(','.join(
                        tuple(unicode(data[t]) for t in tank_keys)) + '\n')
                elif classification == 'player':
                    player_csv.write(','.join(
                        tuple(unicode(data[p]) for p in player_keys)) + '\n')
                else:
                    print('CSV : Uh, got some bad data?')
                # if debug:
                #     print('CSV: Got data for', data['account_id'])
                buf += 1
                if buf >= 2500:
                    gc.collect()
                    buf = 0
                    if debug:
                        print('CSV : Cleared memory')
        if debug:
            print('CSV : Parent signal received. Clearing queue')
        while not data_queue.empty():
            classification, data = data_queue.get()
            if classification == 'tank':
                stats_csv.write(','.join(
                    tuple(unicode(data[t]) for t in tank_keys)) + '\n')
            elif classification == 'player':
                player_csv.write(','.join(
                    tuple(unicode(data[p]) for p in player_keys)) + '\n')
            else:
                print('CSV : Uh, got some bad data?')
            buf += 1
            if buf >= 2500:
                gc.collect()
                buf = 0
                if debug:
                    print('CSV : Cleared memory')
            # if debug:
            #     print('CSV: Got data for', data['account_id'])

    except (IOError, EOFError):
        print('CSV: Did the parent terminate?')
    except Exception as e:
        error_queue.put(('csv', e))
    finally:
        player_csv.close()
        stats_csv.close()
        print('CSV : Exiting')


def logserver(outfile, conn, logport=50000, errorport=50001,
              delay=0.0000000001, debug=False):
    try:
        buf = 0
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        not_done = True
        handler = logging.FileHandler(outfile)
        handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
        logger = logging.getLogger('WG Script')
        logger.addHandler(handler)

        logsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logsock.bind(('localhost', logport))
        logsock.settimeout(0.2)
        errsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        errsock.bind(('localhost', errorport))
        errsock.settimeout(0.2)
        socks = (logsock, errsock)
        while not_done:
            for sock in socks:
                try:
                    logger.handle(
                        logging.makeLogRecord(
                            pickle.loads(
                                sock.recv(4096))))
                    buf += 1
                except socket.timeout:
                    pass
            if buf >= 2500:
                gc.collect()
                buf = 0
                logging.debug('Log: Ran GC')
            if conn.poll(delay):
                not_done = False
                logsock.close()
                errsock.close()
    except (IOError, EOFError):
        print('Log : Did the parent terminate?')
    except Exception as e:
        print('Log :', e)
        print('Log : Exiting')
    finally:
        logger.info('Log: Cleaning up')
        logsock.close()
        errsock.close()


def generate_players(sqldb, xbox_start, xbox_finish, ps4_start, ps4_finish):
    '''
    Create the list of players to query for. If the SQLite database already has
    a list, we'll use it; otherwise we'll generate a range.

    :param sqldb: File name of SQLite database
    :param int xbox_start: Starting Xbox account ID number
    :param int xbox_finish: Ending Xbox account ID number
    :param int ps4_start: Starting PS4 account ID number
    :param int ps4_finish: Ending PS4 account ID number
    '''
    with sqlite3.connect(sqldb) as db:
        try:
            return filter(
                lambda x: (
                    xbox_start <= x <= xbox_finish) or (
                    ps4_start <= x <= ps4_finish),
                map(lambda p: p[0],
                    db.execute('select id from players').fetchall()))
        except (ValueError, sqlite3.OperationalError):
            return chain(range(xbox_start, xbox_finish),
                         range(ps4_start, ps4_finish))

# 1 Worker to export to CSV
# 1 Worker to export errors to file
# 1 Worker to handle database
# N-number of workers to handle requests to API

if __name__ == '__main__':
    with open(argv[1]) as f:
        config = json.load(f)

    manager = Manager()

    process_count = 4 if 'pool size' not in config else config['pool size']

    xbox_start_account = 5000 if 'start account' not in config[
        'xbox'] else config['xbox']['start account']
    xbox_max_account = 12020000 if 'max account' not in config[
        'xbox'] else config['xbox']['max account']

    ps4_start_account = 1073740000 if 'start account' not in config[
        'ps4'] else config['ps4']['start account']
    ps4_max_account = 1079000000 if 'max account' not in config[
        'ps4'] else config['ps4']['max account']

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
        xbox_start_account,
        xbox_max_account,
        ps4_start_account,
        ps4_max_account)

    handlers = []
    handler_conns = []
    queues = []

    vehicles = vehicle_info(
        config['application_id'],
        fields=[
            'name',
            'price_gold',
            'short_name',
            'is_premium',
            'tier',
            'type',
            'tank_id',
            'nation']).data

    if 'logging' in config and 'log file' in config['logging']:
        log_handler_conn, log_child_conn = Pipe()
        if 'log port' not in config['logging']:
            config['logging']['log port'] = 50000
        if 'error log port' not in config['logging']:
            config['logging']['error log port'] = 50001
        error_queue = manager.Queue()
        error_handler = Process(
            name='Error Handler',
            target=logserver,
            args=(
                config['logging']['log file'],
                log_child_conn,
                config['logging']['error log'],
                config['logging']['error log port'],
                delay,
                debug))
        handlers.append(error_handler)
        handler_conns.append(log_handler_conn)

    if 'sql' in config['output']:
        sql_handler_conn, sql_child_conn = Pipe()
        sql_queue = manager.Queue()
        sql_handler = Process(
            name='Stats DB Handler',
            target=update_stats_database,
            args=(
                sql_queue,
                sql_child_conn,
                vehicles,
                config['output']['sql'],
                error_queue,
                delay,
                debug))
        handlers.append(sql_handler)
        handler_conns.append(sql_handler_conn)
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
                vehicles,
                config['output']['csv'],
                config['output']['players'],
                config['output']['tanks'],
                error_queue,
                delay,
                debug))
        handlers.append(csv_handler)
        handler_conns.append(csv_handler_conn)
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
                    ps + 1,
                    child_conn,
                    config['application_id'],
                    queues,
                    error_queue,
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
            print('Main: Adding work to pool')
        buf = 0
        while not_done:
            for n, process in enumerate(processes):
                if pipes[n].poll(delay):
                    received = pipes[n].recv()
                    if debug and received:
                        print(
                            'Main: Worker {:2} got account {}'.format(
                                n + 1, received))
                    waiting[n] = True
                if waiting[n]:
                    try:
                        pipes[n].send(player_iter.next())
                        waiting[n] = False
                    except StopIteration:
                        if debug:
                            print('Main: No more player IDs to process')
                        not_done = False
                        break
            buf += 1
            if buf >= 5000000:
                gc.collect()
                buf = 0
                if debug:
                    print('Main: Cleared memory')
    except (KeyboardInterrupt, SystemExit):
        print('Main: Attempting to prematurely terminate processes')
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
            print('Main: Sending signal to queue handler(s)')
        for handler in handlers:
            handler.join(join_wait)
        # sql_handler.join(join_wait)
        # csv_handler.join(join_wait)
