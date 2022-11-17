import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import configparser

def create_database():
    """
    - Creates and connects to sparkifydb
    - Returns the connections and cursor to sparkifydb
    """
    # Read parameters from file
    config = configparser.ConfigParser()
    config.read('rdm.cfg')
    DB_NAME_DEFAULT = config.get('RDM', 'DB_NAME_DEFAULT')
    DB_USER = config.get('RDM', 'DB_USER')
    DB_PASSWORD = config.get('RDM', 'DB_PASSWORD')
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname={} user={} password={}".format(DB_NAME_DEFAULT, DB_USER, DB_PASSWORD))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database
    cur.execute('DROP DATABASE IF EXISTS sparkifydb')
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    
    # close connection to default database
    conn.close()
    
    # connect to sparkify database
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user={} password={}'.format(DB_USER, DB_PASSWORD))
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in the drop_table_queries list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    Creates each table using the queries in the create_table_queries list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main(): 
    """
    - Drops (if exists) and creates sparkify database
    - Establishes connection with sparkify database and gets cursor to it
    - Drop all the tables
    - Creates all the tables
    - Finally, closes the connection
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()
    
if __name__ == '__main__':
    main()