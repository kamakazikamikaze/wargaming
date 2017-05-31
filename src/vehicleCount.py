from __future__ import print_function
import json
import pickle
from sys import argv
from wotconsole import vehicle_statistics

try:
    range = xrange
except NameError:
    pass

if __name__ == '__main__':
    with open(argv[1]) as f:
        api = json.load(f)

    with open(argv[2]) as f:
        players = pickle.load(f)

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
        ans = player_data(
            application_id=api['application_id'],
            account_id=pkeys[i_prev:i],
            fields=[
                'account_id',
                'created_at',
                'last_battle_time',
                'nickname',
                'updated_at']
        )
        for pid, player in ans.data.iteritems():
            if player is None:
                players['None'].add(int(pid))
                continue
            players[int(player['account_id'])].update({
                'created_at': player['created_at'],
                'last_battle_time': player['last_battle_time'],
                'nickname': player['nickname'],
                'updated_at': player['updated_at']
            })
        i_prev = i

    if i + 100 != len(players):
        i += 100
        print('Fetching accounts {} to {} . . .'.format(i_prev, i))
        ans = player_data(
            application_id=api['application_id'],
            account_id=pkeys[i_prev:i],
            fields=[
                'account_id',
                'created_at',
                'last_battle_time',
                'nickname',
                'updated_at']
        )
        for pid, player in ans.data.iteritems():
            if player is None:
                players['None'].add(int(pid))
                continue
            players[int(player['account_id'])].update({
                'created_at': player['created_at'],
                'last_battle_time': player['last_battle_time'],
                'nickname': player['nickname'],
                'updated_at': player['updated_at']
            })

    with open(argv[3], 'w') as f:
        pickle.dump(players, f)
