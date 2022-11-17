import configparser
from sql_queries import create_table_queries, drop_table_queries
import psycopg2


def drop_tables(cur, conn):
    """
    Drop all tables if they exist
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging, fact and dimension tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Create connection to database and get cursor to it
    Drop the tables if they exist
    Create the staging, fact and dimension tables
    Close the cursor and connection to the database
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()