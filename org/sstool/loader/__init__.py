from cassandra.cluster import Cluster


def load_cassandra_table(keyspace, query, params_coll):
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect(keyspace)

    for params in params_coll:
        session.execute(query, params)

    session.shutdown()
