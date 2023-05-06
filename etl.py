import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """Load staging tables from JSON in S3 to the DWH.

    Args:
        cur  (): psycopg2 cursor
        conn (): psycopg2 connector

    Return:
        staging_events (table in Redshift) with log_data  (from JSON in S3)
        staging_songs  (table in Redshift) with song_data (from JSON in S3)
    """

    print('\nS3 -> Staging\n')
    queries = copy_table_queries
    counter = 0
    queries_count = len(queries)
    for query in queries:
        counter += 1
        start_time = time.time()
        try:
            print(f'({counter}/{queries_count}) Query: loading {query}...',end = ' ')
            cur.execute(queries.get(query))
            conn.commit()
            print('success', end = ' ')
        except Exception as e:
            # print('failed')
            print(e)
        finally:
            print(f'(elapsed: {time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start_time))})')

def insert_tables(cur, conn):
    """Insert data from the staging tables into the fact and dimension tables.

    Args:
        cur  (): psycopg2 cursor
        conn (): psycopg2 connector

    Return:
        songplays (fact      table)
        users     (dimension table)
        songs     (dimension table)
        artists   (dimension table)
        time      (dimension table)
    """

    print('\nStaging -> Analytics\n')
    queries = insert_table_queries
    counter = 0
    queries_count = len(queries)
    for query in queries:
        counter += 1
        start_time = time.time()
        try:
            print(f'({counter}/{queries_count}) Query: inserting {query}...',end='')
            cur.execute(queries.get(query))
            conn.commit()
            print('success', end = ' ')
        except Exception as e:
            # print('failed')
            print(e)
        finally:
            print(f'(elapsed: {time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start_time))})')


def main():
    """Connect to AWS Redshift, load staging tables and insert data from staging into fact/dim tables.

    Args:
        dwh.cfg
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()