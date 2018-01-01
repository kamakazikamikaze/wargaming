from elasticsearch import Elasticsearch, helpers
from elasticsearch import TransportError
from elasticsearch.helpers import BulkIndexError
import cPickle as pickle
import json
from uuid import uuid4
from os import path, remove


def check_index(conf):
    pass


def create_index(conf):
    pass


def send_data(conf, data):
    for name, cluster in conf['elasticsearch']['clusters'].items():
        try:
            _send_to_cluster(cluster, data)
        except (BulkIndexError, TransportError):
            offload_local(
                name,
                cluster,
                conf['elasticsearch']['offload'],
                data)


def _send_to_cluster(conf, data):
    r"""
    Stream data to an Elasticsearch cluster.

    .. note: This method does **not** catch exceptions

    :param dict conf: Connection parameters for `elasticsearch.Elasticsearch`
    :param list(dict) data: ES documents to send
    """
    # chunk = None
    es = Elasticsearch(**conf)
    # for chunk in chunker(data, conf['elasticsearch']['max bulk']):
    #     # body = '\n'.join(map(lambda doc: jdumps(doc), chunk))
    #     es.bulk(body='\n'.join(map(jdumps, chunk)),
    #             index=conf['elasticsearch']['index'],
    #             doc_type=conf['elasticsearch']['type'],
    #             timeout=conf['elasticsearch']['timeout'])
    helpers.bulk(es, data)


def offload_local(name, clusterconf, dumpconf, data):
    r"""
    When the Elasticsearch cluster/node is unavailable, offload data to disk
    and try to resend the next time the script is run.

    :param str name: Cluster (nick)name
    :param dict clusterconf: Connection configuration for the cluster
    :param dict dumpconf: `configuration['elasticsearch']['offload']`
    :param list(dict) data: All ES documents to be flushed to disk
    """
    dumpuuid = str(uuid4())
    with open(path.join(dumpconf['data folder'], dumpuuid), 'wb') as outfile:
        pickle.dump(data, outfile, pickle.HIGHEST_PROTOCOL)
    # with open(conf['index'], 'a') as indexfile:
    #     indexfile.write(dumpuuid + '\n')
    if path.exists(dumpconf['index']):
        with open(dumpconf['index']) as f:
            indexdata = json.load(f)
    else:
        indexdata = {'clusters': {}, 'dumps': {}}
    if name not in indexdata['clusters']:
        indexdata['clusters'][name] = clusterconf
        indexdata['dumps'][name] = []
    indexdata['dumps'][name].append(dumpuuid)
    with open(dumpconf['index'], 'w') as f:
        json.dump(indexdata, f)


def load_local(conf):
    r"""
    Check if Elasticsearch clusters are online and attempt to send data.

    :param dict conf: `configuration['elasticsearch']['offload']`
    """
    with open(conf['index']) as f:
        indexdata = json.load(f)
    for name, cluster in indexdata['clusters']:
        success = set()
        for dump in indexdata['dumps'][name]:
            data = pickle.load(path.join(conf['data folder'], dump))
            try:
                if conf['delete old index on reload']:
                    es = Elasticsearch(**cluster)
                    es.delete(data[0]['_index'], ignore_unavailable=True)
                _send_to_cluster(cluster, data)
                success.add(dump)
                remove(path.join(conf['data folder'], dump))
            except BulkIndexError:
                pass
            except TransportError:
                # Cannot reach. Skip
                break
        indexdata['dumps'][name] = list(
            set(indexdata['dumps'][name]) - success)
    with open(conf['index'], 'w') as f:
        json.dump(indexdata, f)

# generators


def create_generator_totals(day, query_results):
    return ({
        "_index": day.strftime("total_battles-%Y.%m.%d"),
        "_type": "total",
        "_id": player.account_id,
        "_source": {
            "account_id": player.account_id,
            "battles": player.battles,
            "date": day.strftime("%Y-%m-%d")
        }
    } for player in query_results)


def create_generator_diffs(day, query_results):
    return ({
        "_index": day.strftime("diff_battles-%Y.%m.%d"),
        "_type": "diff",
        "_id": player.account_id,
        "_source": {
            "account_id": player.account_id,
            "battles": player.battles,
            "date": day.strftime("%Y-%m-%d")
        }
    } for player in query_results)


def create_generator_players(query_results):
    return ({
        "_index": "players",
        "_type": "player",
        "_id": player.account_id,
        "_source": {
            "account_id": player.account_id,
            "nickname": player.nickname,
            "console": player.console,
            "created_at": player.created_at,
            "last_battle_time": player.last_battle_time,
            "updated_at": player.updated_at,
            "battles": player.battles,
            "last_api_pull": player.last_api_pull
        }
    } for player in query_results)
