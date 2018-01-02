from __future__ import print_function
from datetime import datetime
import gc
import json
from multiprocessing import Manager, Process, Pipe
from Queue import Empty
from requests import ConnectionError
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sys import argv
from time import sleep
from wotconsole import player_data, WOTXResponseError

from database import Diff_Battles, Player, setup_trigger, Total_Battles
from sendtoindexer import create_generator_diffs, create_generator_players
from sendtoindexer import create_generator_totals, send_data
from utils import generate_players

try:
    range = xrange
except NameError:
    pass


def query(worker_number, work, dbconf, token='demo', lang='en', timeout=15,
          max_retries=5, debug=False, err_queue=None, debug_queue=None):
    try:
        engine = create_engine(
            "{protocol}://{user}:{password}@{address}/{name}".format(**dbconf),
            echo=False
        )
        Session = sessionmaker(bind=engine)
        session = Session()
        data_fields = (
            'created_at',
            'account_id',
            'last_battle_time',
            'nickname',
            'updated_at',
            'statistics.all.battles')
        while not work.empty():
            t_players, realm = work.get(False, 0.000001)
            retries = max_retries
            while retries:
                try:
                    response = player_data(
                        t_players,
                        token,
                        fields=data_fields,
                        language=lang,
                        api_realm=realm,
                        timeout=timeout
                    )
                    pulltime = datetime.utcnow()
                    for _, player in response.iteritems():
                        if player is None or len(player) == 0:
                            continue
                        try:
                            p = session.query(Player).filter(
                                Player.account_id == player['account_id']
                            ).one()
                            p.battles = player['statistics']['all']['battles']
                            p.last_battle_time = datetime.utcfromtimestamp(
                                player['last_battle_time'])
                            p.updated_at = datetime.utcfromtimestamp(
                                player['updated_at'])
                            p._last_api_pull = pulltime
                        except NoResultFound:
                            session.add(Player(
                                account_id=player['account_id'],
                                nickname=player['nickname'],
                                console=realm,
                                created_at=datetime.utcfromtimestamp(
                                    player['created_at']),
                                last_battle_time=datetime.utcfromtimestamp(
                                    player['last_battle_time']),
                                updated_at=datetime.utcfromtimestamp(
                                    player['updated_at']),
                                battles=player['statistics'][
                                    'all']['battles'],
                                _last_api_pull=pulltime))
                            # print(player['account_id'], ':', m)
                    session.commit()

                    if debug and debug_queue is not None:
                        debug_queue.put(
                            'Worker {}: Success pulling players {}'.format(
                                worker_number, map(
                                    str, t_players)))
                    del response
                    break
                except (TypeError, ConnectionError) as ce:
                    if 'Max retries exceeded with url' in str(ce):
                        retries -= 1
                    else:
                        if err_queue is not None:
                            err_queue.put((t_players, ce))
                        break
                except WOTXResponseError as wg:
                    if 'REQUEST_LIMIT_EXCEEDED' in wg.message:
                        retries -= 1
                        sleep(0.1)
                    else:
                        if err_queue is not None:
                            err_queue.put((t_players, wg))
                        break
            if not retries:
                if err_queue is not None:
                    err_queue.put(
                        (t_players, Exception('Retry limit exceeded')))
            t_players = None
    except (KeyboardInterrupt, Empty):
        pass
    except Exception as e:
        try:
            if err_queue is not None:
                err_queue.put((t_players, e))
        except:
            pass
    finally:
        try:
            session.commit()
        except:
            pass
    print(
        'Worker{:3}: Exiting at {}'.format(
            worker_number,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


def expand_max_players(config):
    dbconf = config['database']
    update = False
    engine = create_engine(
        "{protocol}://{user}:{password}@{address}/{name}".format(**dbconf),
        echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    max_xbox = int(session.query(Player, func.max(Player.account_id)
                                 ).filter(Player.console == 'xbox').one()[1])
    max_ps4 = int(session.query(Player, func.max(Player.account_id)
                                ).filter(Player.console == 'ps4').one()[1])
    if 'max account' not in config['xbox']:
        config['xbox']['max account'] = max_xbox + 200000
        update = True
    elif config['xbox']['max account'] - max_xbox < 50000:
        config['xbox']['max account'] += 100000
        update = True
    if 'max account' not in config['ps4']:
        config['ps4']['max account'] = max_ps4 + 200000
        update = True
    elif config['ps4']['max account'] - max_ps4 < 50000:
        config['ps4']['max account'] += 100000
        update = True

    if update:
        if 'debug' in config and config['debug']:
            print('Updating configuration.')
            print('Max Xbox account:', max_xbox)
            print('Max PS4 account:', max_ps4)
        with open(argv[1], 'w') as f:
            json.dump(config, f)


def log_worker(queue, filename, conn):
    try:
        with open(datetime.now().strftime(filename), 'w') as f:
            count = 0
            while not conn.poll(0.00000001):
                if not queue.empty():
                    msg = queue.get()
                    if isinstance(msg, tuple):
                        f.write('Error for players: {}'.format(
                            map(str, msg[0])))
                        f.write('\n')
                        f.write(str(msg[1]))
                    else:
                        f.write(msg)
                    f.write('\n')
                    f.flush()
                    count += 1
                if count >= 10000:
                    gc.collect()
    except (KeyboardInterrupt):
        pass


def send_to_elasticsearch(conf):
    dbconf = conf['database']
    engine = create_engine(
        "{protocol}://{user}:{password}@{address}/{name}".format(**dbconf),
        echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    totals = list(
        create_generator_totals(
            datetime.utcnow(),
            session.query(Total_Battles).all()))
    print(
        'ES: Sending totals at',
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    send_data(conf, totals)
    diffs = list(
        create_generator_diffs(
            datetime.utcnow(),
            session.query(Diff_Battles).all()))
    print(
        'ES: Sending diffs at',
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    send_data(conf, diffs)
    # player_ids = set.union(
    #     set(map(lambda p: int(p.account_id), totals)),
    #     set(map(lambda p: int(p.account_id), diffs)))
    player_ids = set.union(
        set(map(lambda p: int(p['_source']['account_id']), totals)),
        set(map(lambda p: int(p['_source']['account_id']), diffs)))
    players = list(
        create_generator_players(session.query(Player).filter(
            Player.account_id.in_(player_ids)).all()))
    print(
        'ES: Sending players at',
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    send_data(conf, players, 'update')
    print('ES: Finished at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def send_everything(conf):
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    dbconf = conf['database']
    engine = create_engine(
        "{protocol}://{user}:{password}@{address}/{name}".format(**dbconf),
        echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.reflect(engine)
    for table in Base.metadata.tables.keys():
        print('ES: Sending', table, 'at',
              datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        if 'diff' in table:
            diffs = list(
                create_generator_diffs(
                    datetime.strptime(table, 'diff_battles_%Y_%m_%d'),
                    session.query(Base.metadata.tables[table]).all()))
            send_data(conf, diffs)
        elif 'total' in table:
            totals = list(
                create_generator_totals(
                    datetime.strptime(table, 'total_battles_%Y_%m_%d'),
                    session.query(Base.metadata.tables[table]).all()))
            send_data(conf, totals)
        elif 'player' in table:
            players = list(
                create_generator_players(
                    session.query(Base.metadata.tables[table]).all()))
            send_data(conf, players, 'update')


def start(config):
    apikey = config['application_id']
    language = 'en' if 'language' not in config else config['language']
    process_count = 12 if 'processes' not in config else config[
        'processes']
    if 'xbox' not in config:
        config['xbox'] = dict()
    if 'ps4' not in config:
        config['ps4'] = dict()
    xbox_start_account = 5000 if 'start account' not in config[
        'xbox'] else config['xbox']['start account']
    xbox_max_account = 13325000 if 'max account' not in config[
        'xbox'] else config['xbox']['max account']
    ps4_start_account = 1073740000 if 'start account' not in config[
        'ps4'] else config['ps4']['start account']
    ps4_max_account = 1080500000 if 'max account' not in config[
        'ps4'] else config['ps4']['max account']
    max_retries = 5 if 'max retries' not in config else config[
        'max retries']
    timeout = 15 if 'timeout' not in config else config['timeout']
    debug = False if 'debug' not in config else config['debug']

    timestamp = '%Y-%m-%d %H:%M:%S'

    manager = Manager()
    work_queue = manager.Queue()
    errors = manager.Queue()
    debug_messages = manager.Queue()

    playerschain = generate_players(
        xbox_start_account,
        xbox_max_account,
        ps4_start_account,
        ps4_max_account
    )

    realm = 'xbox'
    plist = []
    p = playerschain.next()
    while p <= xbox_max_account:
        if len(plist) == 100:
            work_queue.put((tuple(plist), realm))
            plist = []
        plist.append(p)
        p = playerschain.next()
    if plist:
        work_queue.put((tuple(plist), realm))
    plist = []
    realm = 'ps4'
    try:
        # Replace with `while True`?
        while p <= ps4_max_account:
            if len(plist) == 100:
                work_queue.put((tuple(plist), realm))
                plist = []
            plist.append(p)
            p = playerschain.next()
    except StopIteration:
        if plist:
            work_queue.put((tuple(plist), realm))

    setup_trigger(config['database'])

    processes = []
    loggers = []
    pipes = []

    if 'logging' in config:
        if 'errors' in config['logging']:
            par_conn, child_conn = Pipe()
            loggers.append(
                Process(
                    target=log_worker,
                    args=(
                        errors,
                        config['logging']['errors'],
                        child_conn
                    )
                )
            )
            pipes.append(par_conn)
        if 'debug' in config['logging']:
            par_conn, child_conn = Pipe()
            loggers.append(
                Process(
                    target=log_worker,
                    args=(
                        debug_messages,
                        config['logging']['debug'],
                        child_conn
                    )
                )
            )
            pipes.append(par_conn)

    for number in range(0, process_count):
        processes.append(
            Process(
                target=query,
                args=(
                    number,
                    work_queue,
                    config['database'],
                    apikey,
                    language,
                    timeout,
                    max_retries,
                    debug,
                    errors,
                    debug_messages
                )
            )
        )

    try:
        print('Started pulling at:', datetime.now().strftime(timestamp))
        for logger in loggers:
            logger.start()
        for process in processes:
            process.start()
        for process in processes:
            process.join()
    except (KeyboardInterrupt):
        for process in processes:
            process.terminate()
    finally:
        for conn in pipes:
            conn.send(-1)
        for logger in loggers:
            logger.join()
        print('Finished pulling at:', datetime.now().strftime(timestamp))
        expand_max_players(config)

    if 'elasticsearch' in config:
        print('Preparing process to send data to Elasticsearch')
        send_to_elasticsearch(config)

if __name__ == '__main__':

    with open(argv[1]) as f:
        config = json.load(f)
    start(config)
