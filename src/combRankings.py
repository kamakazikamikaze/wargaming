from __future__ import print_function
import json
import pickle
from sys import argv
from wotconsole import WOTXResponseError, top_players


def fetch(page_no, all_players):
    '''
    A very crude method of pulling data, but it works fine for this short-term
    need. Since ``all_players`` is expected to be a ``dict``, it is mutable and
    will be updated in each call. No need to return anything.
    '''
    ans = top_players(
        application_id=api['application_id'],
        rank_field='global_rating',
        rating='all',
        fields=[
            'account_id',
            'battles_count.rank',
            'battles_count.value',
            'wins_ratio.rank',
            'wins_ratio.value',
            'survived_ratio.rank',
            'survived_ratio.value',
            'platform'],
        limit=1000,
        page_no=page,
        timeout=60)
    for player in ans.data:
        players[player['account_id']] = {
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
        players[player['platform']].add(player['account_id'])

if __name__ == '__main__':
    with open(argv[1]) as f:
        api = json.load(f)

    players = dict()
    players['xbox'] = set()
    players['ps4'] = set()
    page = 0

    try:
        while True:
            page += 1
            if page % 10 == 1:
                print('Fetching page {} . . .'.format(page))
            fetch(page, players)

    except WOTXResponseError:
        pass

    with open(argv[2], 'w') as f:
        pickle.dump(players, f)
