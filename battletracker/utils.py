from collections import Iterable
from itertools import chain, islice
from json import dump as jdump


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


def create_config(filename):
    newconfig = {
        'application_id': 'demo',
        'language': 'en',
        'xbox': {
            'start account': 5000,
            'max account': 13325000
        },
        'ps4': {
            'start account': 1073740000,
            'max account': 1080500000
        },
        'max retries': 5,
        'timeout': 15,
        'debug': False,
        'processes': 12,
        'logging': {
            'errors': 'logs/error-%Y_%m_%d'
        },
        'database': {
            'protocol': 'mysql',
            'user': 'root',
            'password': 'password',
            'address': 'localhost',
            'name': 'battletracker'
        },
        'elasticsearch': {
            'clusters': {
                '<cluster1>': {
                    'hosts': [],
                    'sniff_on_start': True,
                    'sniff_on_connection_fail': True,
                    'sniffer_timeout': 30
                }
            },
            'offload': {
                'data folder': '/srv/battletracker/offload/dumps',
                'delete old index on reload': True,
                'index': '/srv/battletracker/offload/index.txt'
            }
        }
    }
    with open(filename, 'w') as f:
        jdump(newconfig, f)


def chunker(seq, size):
    r"""
    Break data down into sizable chunks.
    All credit goes to https://stackoverflow.com/a/434328
    :param seq: Iterable data
    :type seq: list or tuple
    :param int size: Maximum length per chunk
    :return: Segmented data
    :rtype: list
    """
    for pos in range(0, len(seq), size):
        if isinstance(seq, Iterable):
            yield [s for s in islice(seq, pos, pos + size)]
        else:
            yield seq[pos:pos + size]
