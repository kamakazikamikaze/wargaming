from __future__ import print_function
import json
from multiprocessing import Manager, Pipe, Process, Pool
import cPickle as pickle
from requests import ConnectionError
from sys import argv
from wotconsole import WOTXResponseError, player_data

try:
    range = xrange
except NameError:
    pass


def query(players, error_queue, api_key, result_queue, fields, api_realm,
          delay=0.00000001, timeout=10, max_retries=5, debug=False):
    '''
    Pull data from WG's servers. This allows us to retry pages up until a
    certain point
    '''
    try:
        retries = max_retries
        while retries:
            try:
                ans = player_data(
                    application_id=api_key,
                    account_id=players,
                    fields=fields,
                    timeout=timeout,
                    api_realm=api_realm)
                # if debug:
                #     print('Worker: Got players {}'.format(players))
                for pid, player in ans.data.iteritems():
                    if player is None:
                        result_queue.put((int(pid), player))
                        continue
                    result_queue.put((
                        player['account_id'],
                        {
                            'created_at': player['created_at'],
                            'last_battle_time': player['last_battle_time'],
                            'nickname': player['nickname'],
                            'updated_at': player['updated_at']
                        }
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
            # except ValueError as e:
            #     if 'No more players' == str(e):
            #         parent_pipe.send(0)
            #     else:
            #         parent_pipe.send(e)
            #     not_done = False
            #     break
    except Exception as e:
        print('Worker: Unknown error: {}'.format(e))
        try:
            error_queue.put(e)
            # parent_pipe.send(e)
        except:
            pass


def combine_keys(players, data_queue, outfile, conn,
                 delay=0.0000000001, debug=False):

    # with open(infile) as f:
    #     players = pickle.load(f)

    players['None'] = set()
    conn.send('Ready')
    # This delay float is overkill, but we don't want to block for long!
    try:
        while not conn.poll(delay):
            while not data_queue.empty():
                data = data_queue.get()
                if data[1] is None:
                    players['None'].add(data[0])
                    continue
                players[data[0]].update(data[1])
        while not data_queue.empty():
            data = data_queue.get()
            if data[1] is None:
                players['None'].add(data[0])
                continue
            players[data[0]].update(data[1])
            # if debug and data_queue.qsize() % 100 == 1:
            #     print(
            #         'CK: Queue size to handle: >{}'.format(
            #             data_queue.qsize()))
    except (IOError, EOFError):
        print('Did the parent terminate?')
        # while not data_queue.empty():
        #     data = data_queue.get()
        #     if data[1] is None:
        #         players['None'].add(data[0])
        #         continue
        #     players[data[0]].update(data[1])
        #     if debug and data_queue.qsize() % 1000 == 1:
        #         print(
        #             'CK: Queue size to handle: >{}'.format(
        #                 data_queue.qsize()))
    finally:
        with open(outfile, 'wb') as f:
            if debug:
                print('CK: Starting the pickling process . . .')
            pickle.dump(players, f, pickle.HIGHEST_PROTOCOL)
            if debug:
                print('CK: Pickling complete!')

if __name__ == '__main__':
    with open(argv[1]) as f:
        config = json.load(f)

    with open(config['input'], 'rb') as f:
        print('Loading pickle. This may take a while.')
        players = pickle.load(f)

    manager = Manager()
    player_queue = manager.Queue()
    error_queue = manager.Queue()
    # pkeys = players.keys()
    # del pkeys[pkeys.index('xbox')]
    # del pkeys[pkeys.index('ps4')]
    info_fields = [
        'account_id',
        'created_at',
        'last_battle_time',
        'nickname',
        'updated_at'
    ]
    process_count = 4 if 'pool size' not in config else config['pool size']
    delay = 0.0000000001 if 'delay' not in config else config['delay']
    timeout = 15 if 'timeout' not in config else config['timeout']
    max_pages = 10000 if 'max pages' not in config else config['max pages']
    max_retries = 5 if 'max retries' not in config else config[
        'max retries']
    debug = False if 'debug' not in config else config['debug']
    queue_handler_conn, queue_child_conn = Pipe()
    pool = Pool(process_count)
    try:
        queue_handler = Process(
            target=combine_keys,
            args=(
                players,
                player_queue,
                # config['input'],
                config['output'],
                queue_child_conn,
                delay,
                debug))
        # pool_handler_conn, pool_child_conn = Pipe()
        queue_handler.start()
        for realm in ('xbox', 'ps4'):
            i_prev = 0
            pkeys = list(players[realm])
            for i in range(100, len(pkeys), 100):
                pool.apply_async(
                    query,
                    (
                        pkeys[i_prev:i],
                        # pool_child_conn,
                        error_queue,
                        config['application_id'],
                        player_queue,
                        info_fields,
                        realm,
                        delay,
                        timeout,
                        max_retries,
                        debug
                    ))
                print(
                    'Applying {} accounts {} to {} to pool . . .'.format(
                        realm, i_prev, i))
                i_prev = i  # i + 1
            if i + 100 != len(pkeys):
                i += 100
                print(
                    'Applying {} accounts {} to {} . . .'.format(
                        realm, i_prev, i))
                pool.apply_async(
                    query,
                    (
                        pkeys[i_prev:i],
                        # pool_child_conn,
                        error_queue,
                        config['application_id'],
                        player_queue,
                        info_fields,
                        realm,
                        delay,
                        timeout,
                        max_retries,
                        debug
                    ))
        # pool.join()
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
