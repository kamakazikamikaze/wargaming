from itertools import chain
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
        'logging': {
            'errors': 'logs/error-%Y_%m_%d'
        },
        'database': {
            'protocol': 'mysql',
            'user': 'root',
            'password': 'password',
            'address': 'localhost',
            'name': 'battletracker'
        }
    }
    with open(filename, 'w') as f:
        jdump(newconfig, f)
