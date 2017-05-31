from __future__ import print_function
import json
import pickle
from requests import ConnectionError
from sys import argv
from wotconsole import WOTXResponseError, player_data

try:
    range = xrange
except NameError:
    pass


def fetch(playernames, all_players, app_id):
    '''
    A very crude method of pulling data, but it works fine for this short-term
    need. Since ``all_players`` is expected to be a ``dict``, it is mutable and
    will be updated in each call. No need to return anything.
    '''
    ans = player_data(
        application_id=app_id,
        account_id=playernames,
        fields=[
            'account_id',
            'created_at',
            'last_battle_time',
            'nickname',
            'updated_at'],
        timeout=60
    )
    for pid, player in ans.data.iteritems():
        if player is None:
            all_players['None'].add(int(pid))
            continue
        all_players[int(player['account_id'])].update({
            'created_at': player['created_at'],
            'last_battle_time': player['last_battle_time'],
            'nickname': player['nickname'],
            'updated_at': player['updated_at']
        })

if __name__ == '__main__':
    with open(argv[1]) as f:
        config = json.load(f)

    with open(config['input']) as f:
        print('Loading pickle. This may take a while.')
        players = pickle.load(f)

    try:
        i_prev = 0
        pkeys = players.keys()
        fields = [
            'account_id',
            'created_at',
            'last_battle_time',
            'nickname',
            'updated_at'
        ]
        players['None'] = set()
        for i in range(99, len(players), 100):
            print('Fetching accounts {} to {} . . .'.format(i_prev, i))
            try:
                fetch(pkeys[i_prev:i], players, config['application_id'])
            # except WOTXResponseError as e:
            except WOTXResponseError as e:
                print('Error: {}. Skipping.'.format(e))
                print('Account IDs: {}'.format(
                    ','.join(map(lambda k: str(k), pkeys[i_prev:i]))))
            except ConnectionError as e:
                print('Timeout: {}. Skipping.'.format(e))
                print('Account IDs: {}'.format(
                    ','.join(map(lambda k: str(k), pkeys[i_prev:i]))))
            except Exception as e:
                print('Eh waddafak: {}. Skipping.'.format(e))
                print('Account IDs: {}'.format(
                    ','.join(map(lambda k: str(k), pkeys[i_prev:i]))))
            finally:
                i_prev = i + 1

        if i + 100 != len(players):
            i += 100
            print('Fetching accounts {} to {} . . .'.format(i_prev, i))
            fetch(pkeys[i_prev:i], players)
    except Exception as e:
        print(e)
        print('Account IDs: {}'.format(
            ','.join(map(lambda k: str(k), pkeys[i_prev:i]))))

    with open(config['output'], 'w') as f:
        pickle.dump(players, f)
