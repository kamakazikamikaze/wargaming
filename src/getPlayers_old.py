from __future__ import print_function
import json
from multiprocessing import Manager, Pipe, Pool, Process
from requests.exceptions import ConnectionError
import pickle
from sys import argv
from wotconsole import WOTXResponseError, top_players


def query(page_no, api_key, result_queue, fields, timeout=10, max_retries=5):
    '''
    Pull data from WG's servers. This allows us to retry pages up until a
    certain point
    '''
    retries = max_retries
    while retries:
        try:
            # print('Trying page {}'.format(page_no))
            ans = top_players(
                application_id=api_key,
                rank_field='global_rating',
                rating='all',
                fields=fields,
                limit=1000,
                page_no=page_no,
                timeout=timeout)
            # print(
            #     'Got page {}. Player count: {}'.format(
            #         page_no, len(
            #             ans.data)))
            for player in ans.data:
                result_queue.put(
                    (
                        player['account_id'],
                        {
                            'platform': player['platform'],
                            'battles': {
                                'rank': player['battles_count']['rank'],
                                'total': player['battles_count']['value']
                            },
                            'wins': {
                                'rank': player['wins_ratio']['rank'],
                                'total': player['wins_ratio']['value']
                            },
                            'survived': {
                                'rank': player['survived_ratio']['rank'],
                                'total': player['survived_ratio']['value']
                            }
                        }
                    ))
            break
        except ConnectionError as ce:
            # print('Error for page {}'.format(page_no))
            # print(ce)
            if 'Max retries exceeded with url' in str(ce):
                retries -= 1
            else:
                result_queue.put(Exception(str(ce)))
        except WOTXResponseError as e:
            # print('Error for page {}'.format(page_no))
            # print(e)
            result_queue.put(e)
            return

    if retries == 0:
        result_queue.put(
            Exception('Retries exceeded for page {}'.format(page_no)))


def combine_keys(data_queue, players, outfile, conn, delay=0.0000000001):
    # players = dict()
    players['xbox'] = set()
    players['ps4'] = set()

    # This delay float is overkill, but we don't want to block for long!
    try:
        while not conn.poll(delay):
            while not data_queue.empty():
                data = data_queue.get()
                if isinstance(data, WOTXResponseError):
                    try:
                        # if data.error['message'] == 'RATINGS_NOT_FOUND'
                        conn.send(data)
                        # print('WOTXResponseError found!')
                        # return players
                    except IOError:
                        # We found an exception but the parent is already dead.
                        # Just move on
                        pass
                elif isinstance(data, Exception):
                    print('Exception: {}'.format(data))
                else:
                    players[data[0]] = data[1]
                    players[data[1]['platform']].add(data[0])
                    # print('Player count: {}'.format(len(players) - 2))
        while not data_queue.empty():
            data = data_queue.get()
            if isinstance(data, WOTXResponseError):
                try:
                    conn.send(data)
                    # print('WOTXResponseError found!')
                    # return players
                except IOError:
                    pass
            elif isinstance(data, Exception):
                print('Exception: {}'.format(data))
            else:
                players[data[0]] = data[1]
                players[data[1]['platform']].add(data[0])
    except IOError:
        print('Did the parent terminate?')
    except Exception as e:
        print('Unknown error: {}'.format(e))
    finally:
        with open(outfile, 'w') as f:
            pickle.dump(dict(players), f)

if __name__ == '__main__':

    with open(argv[1]) as f:
        config = json.load(f)

    page = 0

    manager = Manager()
    player_queue = manager.Queue()
    results = manager.dict()
    # I usually do not create Proxy objects. This is for testing to see if it
    # cuts down on memory since each process would be creating this list each
    # time the function is called
    info_fields = manager.list([
        'account_id',
        'battles_count.rank',
        'battles_count.value',
        'wins_ratio.rank',
        'wins_ratio.value',
        'survived_ratio.rank',
        'survived_ratio.value',
        'platform'])
    pool = Pool(
        processes=4 if 'pool size' not in config else config['pool size'])
    parent_conn, child_conn = Pipe()
    delay = 0.0000000001 if 'delay' not in config else config['delay']
    timeout = 15 if 'timeout' not in config else config['timeout']
    max_pages = 10000 if 'max pages' not in config else config['max pages']
    queue_handler = Process(
        target=combine_keys,
        args=(
            player_queue,
            results,
            config['output'],
            child_conn))
    try:
        queue_handler.start()

        while not parent_conn.poll(delay) and page < max_pages:
            page += 1
            if page % 10 == 1:
                print('Querying page {}'.format(page))
            pool.apply_async(
                query,
                (
                    page,
                    config['application_id'],
                    player_queue,
                    info_fields,
                    timeout
                ))
        pool.close()
        pool.join()
    except (KeyboardInterrupt, SystemExit):
        print('Attempting to prematurely terminate pool.')
        pool.terminate()
        pool.join()
        # queue_handler.terminate()
        # exit('KeyboardInterrupt raised')
    finally:
        # pool.join()
        parent_conn.send(1)
        # while queue_handler.is_alive():
        #     pass
        queue_handler.join(30)

    # if parent_conn.poll(delay):
    #     print('Exited because of exception.')
    #     print(parent_conn.recv())
