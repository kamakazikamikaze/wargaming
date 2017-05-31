from __future__ import print_function
import json
from multiprocessing import Manager, Pipe, Process
from requests.exceptions import ConnectionError
import pickle
from sys import argv
from wotconsole import WOTXResponseError, top_players


def query(worker_number, parent_pipe, api_key, result_queue, fields,
          delay=0.00000001, timeout=10, max_retries=5, debug=False):
    '''
    Pull data from WG's servers. This allows us to retry pages up until a
    certain point
    '''
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
                raise TypeError('The boy ain\'t right.')
            retries = max_retries
            while retries:
                try:
                    ans = top_players(
                        application_id=api_key,
                        rank_field='global_rating',
                        rating='all',
                        fields=fields,
                        limit=1000,
                        page_no=received,
                        timeout=timeout)
                    if debug:
                        print(
                            '{}: Got page {}. Player count: {}'.format(
                                worker_number,
                                received,
                                len(ans.data)))
                    if len(ans.data) == 0:
                        raise ValueError('No more players')
                    for player in ans.data:
                        result_queue.put((
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
                    # Signal to the parent that our page finished just fine
                    parent_pipe.send(received)
                    break
                except ConnectionError as ce:
                    # print('Error for page {}'.format(page_no))
                    # print(ce)
                    if 'Max retries exceeded with url' in str(ce):
                        retries -= 1
                    else:
                        parent_pipe.send(ce)
                        not_done = False
                        break
                except WOTXResponseError as wg:
                    if 'RATINGS_NOT_FOUND' == wg.error['message']:
                        # We've hit the page limit
                        parent_pipe.send(0)
                    else:
                        parent_pipe.send(wg)
                    not_done = False
                    break
                except ValueError as e:
                    if 'No more players' == str(e):
                        parent_pipe.send(0)
                    else:
                        parent_pipe.send(e)
                    not_done = False
                    break
        except IOError as e:
            print('{}: Parent die prematurely?'.format(worker_number))
            print(e)
            break
        except EOFError as e:
            print('{}: Parent kill pipe prematurely?'.format(worker_number))
            print(e)
            break
        except TypeError:
            print(
                '{}: Data from pipe in incorrect format'.format(worker_number))
            break
        except Exception as e:
            print('{}: Unknown error: {}'.format(worker_number, e))
            try:
                parent_pipe.send(wg)
            except:
                pass
            break
    print('{}: Exiting'.format(worker_number))


def combine_keys(data_queue, outfile, conn, delay=0.0000000001, debug=False):
    players = dict()
    players['xbox'] = set()
    players['ps4'] = set()

    # This delay float is overkill, but we don't want to block for long!
    try:
        while not conn.poll(delay):
            while not data_queue.empty():
                data = data_queue.get()
                players[data[0]] = data[1]
                players[data[1]['platform']].add(data[0])
                if debug and len(players) % 1000 == 1:
                    print('CK: Player count: {}'.format(len(players) - 2))
        while not data_queue.empty():
            data = data_queue.get()
            players[data[0]] = data[1]
            players[data[1]['platform']].add(data[0])
            if debug and len(players) % 1000 == 1:
                print('CK: Player count: {}'.format(len(players) - 2))
    except IOError:
        print('Did the parent terminate?')
    except Exception as e:
        print('Unknown error: {}'.format(e))
    finally:
        with open(outfile, 'w') as f:
            pickle.dump(players, f)


if __name__ == '__main__':

    with open(argv[1]) as f:
        config = json.load(f)

    page = 0

    manager = Manager()
    player_queue = manager.Queue()
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
    process_count = 4 if 'pool size' not in config else config['pool size']
    delay = 0.0000000001 if 'delay' not in config else config['delay']
    timeout = 15 if 'timeout' not in config else config['timeout']
    max_pages = 10000 if 'max pages' not in config else config['max pages']
    max_retries = 5 if 'max retries' not in config else config['max retries']
    debug = False if 'debug' not in config else config['debug']
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
                    player_queue,
                    info_fields,
                    delay,
                    timeout,
                    max_retries,
                    debug)))
        pipes.append(parent_conn)
        waiting.append(True)
    queue_handler_conn, child_conn = Pipe()
    queue_handler = Process(
        target=combine_keys,
        args=(
            player_queue,
            config['output'],
            child_conn,
            delay,
            debug))
    try:
        queue_handler.start()
        for p in processes:
            p.start()
        no_page_errors = True
        page_count = 0
        while no_page_errors and page_count <= max_pages:
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
                            no_page_errors = False
                            break
                    else:
                        print('Exception from child {}:'.format(n), received)
                        no_page_errors = False
                        break
                if waiting[n]:
                    page_count += 1
                    pipes[n].send(page_count)
                    waiting[n] = False

    except (KeyboardInterrupt, SystemExit):
        print('Attempting to prematurely terminate processes')

    finally:
        for n, p in enumerate(processes):
            pipes[n].send(0)
            p.join()
        player_queue.close()
        queue_handler_conn.send(-1)
        queue_handler.join(180)

    # if parent_conn.poll(delay):
    #     print('Exited because of exception.')
    #     print(parent_conn.recv())
