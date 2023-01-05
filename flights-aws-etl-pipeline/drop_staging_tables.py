"""
This script will clean up the staging tables by dropping them once the analytics tables have been built.
"""

import configparser
import psycopg2


## CONFIG
config = configparser.ConfigParser()
config.read('aws.cfg') 

#------------------------------------------------------------------------------------------------------------------------------------------------

# DROP ANY EXISTING STAGING TABLES 
drop_aircraft_staging = "DROP TABLE IF EXISTS aircraft_staging" 
drop_airlines_staging = "DROP TABLE IF EXISTS airlines_staging" 
drop_airports_staging = "DROP TABLE IF EXISTS airports_staging" 
drop_flights_staging = "DROP TABLE IF EXISTS flights_staging" 

drop_staging_table_queries = [drop_aircraft_staging, drop_airlines_staging, drop_airports_staging, drop_flights_staging]

#------------------------------------------------------------------------------------------------------------------------------------------------

def drop_staging(cur, conn):
    """
    This function will process each of the drop queries that are written above. It thus wipes any pre-existing tables should they exist.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in drop_staging_table_queries:
        cur.execute(query)
        conn.commit() 


def main():
    """
    This function will collect the required params from a config file, open a DB connection, then process
    1. drop staging tables
    before closing the connection. 
    
    No parameters are required to be passed to this function when called.
    """
    config = configparser.ConfigParser() 
    config.read('aws.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(config.get('CLUSTER','host'), config.get('DWH','dwh_db'), config.get('DWH','dwh_db_user'), 
                            config.get('DWH','dwh_db_password'), config.get('DWH','dwh_port'))
                            )
    cur = conn.cursor()

    drop_staging(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()


# SCIPRT END

#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------
