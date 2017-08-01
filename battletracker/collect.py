from __future__ import print_function
from datetime import datetime
import gc
from itertools import chain
import json
from multiprocessing import Manager, Process, Pipe
from Queue import Empty
from requests import ConnectionError
from sqlalchemy import Column, create_engine, event, DateTime, DDL
from sqlalchemy import ForeignKey, Integer, String
# from _mysql_exceptions import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound  # , MultipleResultsFound
from sys import argv
from time import sleep
from wotconsole import player_data, WOTXResponseError

try:
    range = xrange
except NameError:
    pass

Base = declarative_base()


class Player(Base):
    __tablename__ = 'players'

    account_id = Column(Integer, primary_key=True)
    # Biggest I've seen is 26 thanks to the "_old_######" accounts
    nickname = Column(String(34), nullable=False)
    console = Column(String(4), nullable=False)
    created_at = Column(DateTime, nullable=False)
    last_battle_time = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    battles = Column(Integer, nullable=False)
    _last_api_pull = Column(DateTime, nullable=False)

    def __repr__(self):
        return "<Player(account_id={}, nickname='{}', console='{}', battles={})>".format(
            self.account_id,
            self.nickname,
            self.console,
            self.battles
        )


class Diff_Battles(Base):
    __tablename__ = datetime.utcnow().strftime('diff_battles_%Y_%m_%d')

    account_id = Column(
        Integer,
        ForeignKey('players.account_id'),
        primary_key=True)
    battles = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Diff Battles(account_id={}, battles={})".format(
            self.account_id, self.battles)


class Total_Battles(Base):
    __tablename__ = datetime.utcnow().strftime('total_battles_%Y_%m_%d')

    account_id = Column(
        Integer,
        ForeignKey('players.account_id'),
        primary_key=True)
    battles = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Total Battles(account_id={}, battles={})".format(
            self.account_id, self.battles)


def query(worker_number, work, dbconf, token='demo', lang='en', timeout=15,
          max_retries=5, debug=False, err_queue=None, debug_queue=None):
    try:
        engine = create_engine(
            "{protocol}://{user}:{password}@{address}/{name}".format(**dbconf),
            echo=False
        )
        Session = sessionmaker(bind=engine)
        session = Session()
        while not work.empty():
            t_players, realm = work.get(False, 0.000001)
            retries = max_retries
            while retries:
                try:
                    response = player_data(
                        t_players,
                        token,
                        fields=(
                            'created_at',
                            'account_id',
                            'last_battle_time',
                            'nickname',
                            'updated_at',
                            'statistics.all.battles'
                        ),
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
                                Player.account_id == player['account_id']).one()
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
    print('Worker{:3}: Exiting'.format(worker_number))


def log_worker(queue, filename, conn):
    try:
        with open(filename, 'w') as f:
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


def setup_trigger(db):
    engine = create_engine(
        "{protocol}://{user}:{password}@{address}/{name}".format(**db),
        echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    battle_ddl = DDL("""
        CREATE TRIGGER update_battles BEFORE UPDATE ON players
        FOR EACH ROW
        BEGIN
            IF (OLD.battles < NEW.battles) THEN
                INSERT INTO {} VALUES (NEW.account_id, NEW.battles);
                INSERT INTO {} VALUES (NEW.account_id, NEW.battles - OLD.battles);
            END IF;
        END
    """.format(Total_Battles.__tablename__, Diff_Battles.__tablename__))
    event.listen(
        Player.__table__,
        'after_create',
        battle_ddl.execute_if(
            dialect='mysql'))
    newplayer_ddl = DDL("""
        CREATE TRIGGER new_player AFTER INSERT ON players
        FOR EACH ROW INSERT INTO {} VALUES (NEW.account_id, NEW.battles);
    """.format(Total_Battles.__tablename__))
    event.listen(
        Player.__table__,
        'after_create',
        newplayer_ddl.execute_if(
            dialect='mysql'))
    Base.metadata.create_all(engine)
    session.execute("""
        DROP TRIGGER IF EXISTS new_player;
        DROP TRIGGER IF EXISTS update_battles;
    """)
    session.execute(battle_ddl)
    session.execute(newplayer_ddl)
    session.commit()


def generate_players(xbox_start, xbox_finish, ps4_start, ps4_finish):
    '''
    Create the list of players to query for

    :param int xbox_start: Starting Xbox account ID number
    :param int xbox_finish: Ending Xbox account ID number
    :param int ps4_start: Starting PS4 account ID number
    :param int ps4_finish: Ending PS4 account ID number
    '''
    return chain(range(xbox_start, xbox_finish + 1),
                 range(ps4_start, ps4_finish + 1))

if __name__ == '__main__':

    with open(argv[1]) as f:
        config = json.load(f)

    apikey = config['application_id']
    language = 'en' if 'language' not in config else config['language']
    process_count = 12 if 'processes' not in config else config[
        'processes']
    xbox_start_account = 5000 if 'start account' not in config[
        'xbox'] else config['xbox']['start account']
    xbox_max_account = 13000000 if 'max account' not in config[
        'xbox'] else config['xbox']['max account']
    ps4_start_account = 1073740000 if 'start account' not in config[
        'ps4'] else config['ps4']['start account']
    ps4_max_account = 1079000000 if 'max account' not in config[
        'ps4'] else config['ps4']['max account']
    max_retries = 5 if 'max retries' not in config else config[
        'max retries']
    timeout = 15 if 'timeout' not in config else config['timeout']
    debug = False if 'debug' not in config else config['debug']

    # user = 'root' if 'user' not in config[
    #     'database'] else config['database']['user']
    # password = config['database']['password']
    # address = 'localhost' if 'address' not in config[
    #     'database'] else config['database']['address']
    # proto = 'mysql' if 'protocol' not in config[
    #     'database'] else config['database']['protocol']
    # databasename = config['database']['name']

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
        print('Started at:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
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
        print('Finished at:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
