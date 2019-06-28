import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """
    Description: This function can be used to copy json file data from S3 into
    Redshift staging tables.
    Arguments:
        cur: the cursor object.
        conn: the connection object.
    Returns: None
    """
    for query in copy_table_queries:
        t0 = time.time()
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Description: This function can be used to transfer and load data
    from staging table to final tables.
    Arguments:
        cur: the cursor object.
        conn: the connection object.
    Returns: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Description: This function can be used to do the ETL processes
    from S3 json sourse files to Redshift schema.
    Arguments: None
    Returns: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Step 1: Copy data from S3 into Redshift staging tables.
    load_staging_tables(cur, conn)

    # Step 2: Transfer data from staging tables into final tables.
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
