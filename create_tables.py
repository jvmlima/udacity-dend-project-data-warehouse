import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import time


def drop_tables(cur, conn):
    """Drop tables if exist.

    Args:
        cur  (): psycopg2 cursor
        conn (): psycopg2 connector
    """

    print('\nDropping tables\n')
    queries = drop_table_queries
    counter = 0
    queries_count = len(queries)
    for query in queries:
        counter += 1
        start_time = time.time()
        try:
            print(f'({counter}/{queries_count}) Query: dropping {query}...',end='')
            cur.execute(queries.get(query))
            conn.commit()
            print('success', end = ' ')
        except Exception as e:
            # print('failed')
            print(e)
        finally:
            print(f'(elapsed: {time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start_time))})')

def create_tables(cur, conn):
    """Create tables if not exist.

    Args:
        cur  (): psycopg2 cursor
        conn (): psycopg2 connector
    """

    print('\nCreating tables\n')
    queries = create_table_queries
    counter = 0
    queries_count = len(queries)
    for query in queries:
        counter += 1
        start_time = time.time()
        try:
            print(f'({counter}/{queries_count}) Query: creating {query}...',end='')
            cur.execute(queries.get(query))
            conn.commit()
            print('success', end = ' ')
        except Exception as e:
            # print('failed')
            print(e)
        finally:
            print(f'(elapsed: {time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start_time))})')



def main():
    """Connect to AWS Redshift, drop tables and create tables.

    Args:
        dwh.cfg

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()