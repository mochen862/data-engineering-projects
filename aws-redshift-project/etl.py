import configparser
import psycopg2
from sql_queries import *

def load_staging_tables(cur, conn):
    """
    Copy song data and log data to the staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def insert_tables(cur, conn):
    """
    Insert data from the staging tables into the fact and dimension tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    """
    Create connection to database and get cursor to it
    
    Copy song data and log data to the staging tables
    
    Insert data from the staging tables into the fact and dimension tables
    
    Close the cursor and the connection to the database
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()