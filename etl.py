import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to staging tables on Redshift in 'copy_table_queries'
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables to analytics tables on Redshift in 'insert_table_queries'
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read the dwh.cfg file to get all the information of the database
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connects with the Redshift cluster and gets cursor to it
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load all the staging tables and insert all the analytics tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Closes the connection
    conn.close()


if __name__ == "__main__":
    main()