import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Description:
            Drop all tables

        Parameters:
            cur (cursor) : database cursor
            conn (connection) : connection to a database

        Returns:
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description:
            Create all tables

        Parameters:
            cur (cursor) : database cursor
            conn (connection) : connection to a database

        Returns:
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Description:
            Load configuration parameters, establishes database connection,
            drop all tables and then create them

        Parameters:

        Returns:
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