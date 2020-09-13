import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description:
        Load S3 data into into the staging tables
    
    Parameters:
        cur (cursor) : database cursor
        conn (connection) : connection to a database
    
    Returns:
    """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Description:
            Reads data from the staging tables and insert it
            into the final tables

        Parameters:
            cur (cursor) : database cursor
            conn (connection) : connection to a database

        Returns:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Description:
            Load configuration parameters, establishes database connection
            and call function performing ETL tasks

        Parameters:

        Returns:
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