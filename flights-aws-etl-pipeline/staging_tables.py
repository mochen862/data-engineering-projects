"""
Script that will build the staging tables for the ETL process
"""
#------------------------------------------------------------------------------------------------------------------------------------------------

import configparser
import psycopg2


# CONFIG
config = configparser.ConfigParser()
config.read('aws.cfg') 

#------------------------------------------------------------------------------------------------------------------------------------------------

# DROP STAGING TABLES IF THEY ALREADY EXIST
drop_airports_staging = "DROP TABLE IF EXISTS airports_staging"
drop_airlines_staging = "DROP TABLE IF EXISTS airlines_staging"
drop_aircraft_staging = "DROP TABLE IF EXISTS aircraft_staging"
drop_flights_staging =  "DROP TABLE IF EXISTS flights_staging"

drop_staging_table_queries = [drop_airports_staging, drop_airlines_staging, drop_aircraft_staging, drop_flights_staging]

#------------------------------------------------------------------------------------------------------------------------------------------------

# CREATE STAGING TABLES
create_airports_staging = ("""
CREATE TABLE IF NOT EXISTS airports_staging
(
    iata_airport_code   VARCHAR,
    icao                VARCHAR,
    airport_name        VARCHAR,
    city                VARCHAR,
    country             VARCHAR,
    latitude            VARCHAR,
    longitude           VARCHAR,
    altitude            VARCHAR,
    timezone            VARCHAR,
    dst                 VARCHAR,
    tz                  VARCHAR
)
""")


create_airlines_staging = ("""
CREATE TABLE IF NOT EXISTS airlines_staging
(
    iata_airline_code  VARCHAR,
    airline_name       VARCHAR
)
""")


create_aircraft_staging = ("""
CREATE TABLE IF NOT EXISTS aircraft_staging
(
    n_number                VARCHAR,
    serial_number           VARCHAR,
    aircraft_mfr_model_code VARCHAR,
    engine_mfr_code         VARCHAR,
    manufacture_year        VARCHAR,
    aircraft_type           VARCHAR,
    engine_type             VARCHAR
)
""")


create_flights_staging = ("""
CREATE TABLE IF NOT EXISTS flights_staging
(
    date VARCHAR,
    airline VARCHAR,
    flight_number VARCHAR,
    n_number VARCHAR,
    origin_aiport VARCHAR,
    destination_airport VARCHAR,
    scheduled_departure VARCHAR,
    departure_time VARCHAR,
    departure_delay VARCHAR,
    taxi_out VARCHAR,
    wheels_off VARCHAR,
    scheduled_time VARCHAR,
    elapsed_time VARCHAR,
    air_time VARCHAR,
    distance VARCHAR,
    wheels_on VARCHAR,
    taxi_in VARCHAR,
    scheduled_arrival VARCHAR,
    arrival_time VARCHAR,
    arrival_delay VARCHAR,
    diverted VARCHAR,
    cancelled VARCHAR,
    cancellation_reason VARCHAR
)
""")


create_staging_table_queries = [create_airports_staging, create_airlines_staging, create_aircraft_staging, create_flights_staging]


#------------------------------------------------------------------------------------------------------------------------------------------------


# COPY INTO STAGING TABLES FROM S3 BUCKET

copy_airports_staging = ("""
COPY airports_staging 
FROM '{}'
iam_role '{}'
CSV
delimiter ','
IGNOREHEADER 1
compupdate off region '{}';
"""
).format(config.get('S3','airports'), config.get('CLUSTER', 'role_arn'), config.get('BUCKET', 'region'))


copy_airlines_staging = ("""
COPY airlines_staging 
FROM '{}'
iam_role '{}'
CSV
delimiter ','
IGNOREHEADER 1
compupdate off region '{}';
"""
).format(config.get('S3','airlines'), config.get('CLUSTER', 'role_arn'), config.get('BUCKET', 'region'))


copy_aircraft_staging = ("""
COPY aircraft_staging 
FROM '{}'
iam_role '{}'
CSV
delimiter ','
IGNOREHEADER 1
compupdate off region '{}';
"""
).format(config.get('S3','aircraft'), config.get('CLUSTER', 'role_arn'), config.get('BUCKET', 'region'))


copy_flights_staging = ("""
COPY flights_staging 
FROM '{}'
iam_role '{}'
CSV
delimiter ','
IGNOREHEADER 1
compupdate off region '{}';
"""
).format(config.get('S3','flights'), config.get('CLUSTER', 'role_arn'), config.get('BUCKET', 'region'))


copy_staging_table_queries = [copy_airports_staging, copy_airlines_staging, copy_aircraft_staging, copy_flights_staging]


#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------

# The follwing function are created to drop, create and then load data for each of the staging tables above 

def drop_staging(cur, conn):
    """
    This function will process each of the drop staging table queries that are written above. It thus wipes any pre-existing tables should they exist.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in drop_staging_table_queries:
        cur.execute(query)
        conn.commit() 


def create_staging(cur, conn):
    """
    This function will process each of the create staging table queries that are written above. It builds empty tables should they not already exist.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()


def copy_staging(cur, conn):
    """
    This function will process each of the copy staging table queries that are written above. It copies data from S3 buckets to the redshift table.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in copy_staging_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function runs the execution of the above three functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. drop staging tables
    2. create staging tables
    3. copy into staing tables
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
    create_staging(cur, conn)
    copy_staging(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

# SCIPRT END

#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------