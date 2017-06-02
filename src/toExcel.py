import cPickle as pickle
import datetime
import openpyxl


def gettime(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp)

if __name__ == '__main__':

    with open('output/alldata_mult.pickle', 'rb') as f:
        players = pickle.load(f)

    pkeys = players.keys()
    del pkeys['xbox']
    del pkeys[pkeys.index('xbox')]
    del pkeys[pkeys.index('ps4')]
    del pkeys[pkeys.index('None')]

    wb = openpyxl.Workbook()
    ws = wb.active

    ws.append(['Player ID',
               'Console',
               'Battles rank',
               'Battles total',
               'Survived rank',
               'Survived total',
               'Wins rank',
               'Wins total',
               'Nickname',
               'Created at',
               'Created at timestamp',
               'Updated at',
               'Updated at timestamp',
               'Last battle at',
               'Last battle at timestamp'])

    for key in pkeys:
        try:
            ws.append([key,
                       players[key]['platform'],
                       players[key]['battles']['rank'],
                       players[key]['battles']['total'],
                       players[key]['survived']['rank'],
                       players[key]['survived']['total'],
                       players[key]['wins']['rank'],
                       players[key]['wins']['total'],
                       players[key]['nickname'],
                       gettime(players[key]['created_at']),
                       players[key]['created_at'],
                       gettime(players[key]['updated_at']),
                       players[key]['updated_at'],
                       gettime(players[key]['last_battle_time']),
                       players[key]['last_battle_time']])
        except KeyError:
            ws.append([key,
                       players[key]['platform'],
                       players[key]['battles']['rank'],
                       players[key]['battles']['total'],
                       players[key]['survived']['rank'],
                       players[key]['survived']['total'],
                       players[key]['wins']['rank'],
                       players[key]['wins']['total']])

    wb.save('output/players.xlsx')
