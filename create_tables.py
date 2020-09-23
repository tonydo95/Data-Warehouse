import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in 'drop_table_queries list'
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in 'create_table_queries' list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read the dwh.cfg file to get all the information of the database
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connects with the Redshift cluster and gets cursor to it
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drops all the tables and Creates all tables needed
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Closes the connection
    conn.close()


if __name__ == "__main__":
    main()