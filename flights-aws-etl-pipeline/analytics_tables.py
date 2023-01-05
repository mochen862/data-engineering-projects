
"""
Script that will build the analytical tables from the staging tables
"""
#------------------------------------------------------------------------------------------------------------------------------------------------

import configparser
import psycopg2


# CONFIG
config = configparser.ConfigParser()
config.read('aws.cfg') 

#------------------------------------------------------------------------------------------------------------------------------------------------

# DROP ANY EXISTING ANALYTICS TABLES 
drop_aircraft = "DROP TABLE IF EXISTS aircraft" 
drop_airlines = "DROP TABLE IF EXISTS airlines" 
drop_airports = "DROP TABLE IF EXISTS airports" 
drop_date = "DROP TABLE IF EXISTS date" 
drop_time = "DROP TABLE IF EXISTS time" 
drop_flights = "DROP TABLE IF EXISTS flights" 

drop_analytics_queries = [drop_aircraft, drop_airlines, drop_airports, drop_date, drop_time, drop_flights] 

#------------------------------------------------------------------------------------------------------------------------------------------------

# CREATE ANALYTICS TABLES

create_aircraft = ("""
CREATE TABLE IF NOT EXISTS aircraft
(
    n_number                VARCHAR NOT NULL sortkey,
    serial_number           VARCHAR,
    aircraft_mfr_model_code VARCHAR,
    engine_mfr_code         VARCHAR
)
""")

create_airlines = ("""
CREATE TABLE IF NOT EXISTS airlines
(
    iata_airline_code  VARCHAR NOT NULL,
    airline_name       VARCHAR
)
diststyle all
""")

create_airports = ("""
CREATE TABLE IF NOT EXISTS airports
(
    iata_airport_code   VARCHAR,
    airport_name        VARCHAR,
    city                VARCHAR,
    latitude            VARCHAR,
    longitude           VARCHAR,
    altitude            VARCHAR
)
diststyle all
""")

create_date = ("""
CREATE TABLE IF NOT EXISTS date
(
    date DATE sortkey,
    year INT,
    month INT,
    day INT,
    weekday INT
)
diststyle all
""")


create_time = ("""
CREATE TABLE IF NOT EXISTS time
(
    time VARCHAR sortkey,
    hour VARCHAR,
    minute VARCHAR
)
diststyle all
""")


create_flights = ("""
CREATE TABLE IF NOT EXISTS flights
(
    flight_id BIGINT IDENTITY(0,1) sortkey,
    date DATE,
    iata_airline_code VARCHAR,
    flight_number VARCHAR,
    n_number VARCHAR,
    origin_airport VARCHAR,
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
    cancellation_reason VARCHAR,
    aircraft_manufacture_year VARCHAR,
    aircraft_type VARCHAR,
    engine_type VARCHAR
)
""")

create_analytics_queries = [create_aircraft, create_airlines, create_airports, create_date, create_time, create_flights]

#------------------------------------------------------------------------------------------------------------------------------------------------

# INSERT INTO ANALYTICS TABLES FROM STAGING TABLES

insert_aircraft = ("""
INSERT INTO aircraft
(n_number, serial_number, aircraft_mfr_model_code, engine_mfr_code)
SELECT DISTINCT
    n_number,
    serial_number,
    aircraft_mfr_model_code,
    engine_mfr_code
FROM aircraft_staging
""")


insert_airlines = ("""
INSERT INTO airlines
(iata_airline_code, airline_name)
SELECT DISTINCT
    iata_airline_code,
    airline_name
FROM airlines_staging
""")


insert_airports = ("""
INSERT INTO airports
(iata_airport_code, airport_name, city, latitude, longitude, altitude)
SELECT DISTINCT
    iata_airport_code,
    airport_name,
    city,
    latitude,
    longitude,
    altitude
FROM airports_staging
""")


insert_date = ("""
INSERT INTO date
(date, year, month, day, weekday)
SELECT
    f.date_,
    EXTRACT(YEAR FROM f.date_),
    EXTRACT(MONTH FROM f.date_),
    EXTRACT(DAY FROM f.date_),
    EXTRACT(WEEKDAY FROM f.date_)
FROM
    (
        SELECT DISTINCT
            TO_DATE(date, 'YYYY-MM-DD') as date_
        FROM flights_staging
    ) as f
""")


insert_time = ("""
INSERT INTO time
(time, hour, minute)
SELECT
    f.time,
    SUBSTRING(f.time, 1, 2),
    SUBSTRING(f.time, 4, 2)

FROM
    (
        SELECT scheduled_departure as time FROM flights_staging
        UNION
        SELECT departure_time FROM flights_staging
        UNION
        SELECT wheels_off FROM flights_staging
        UNION
        SELECT wheels_on FROM flights_staging
        UNION
        SELECT scheduled_arrival FROM flights_staging
        UNION
        SELECT arrival_time FROM flights_staging
    ) as f
""")


insert_flights = ("""
INSERT INTO flights
(date, iata_airline_code, flight_number, n_number, origin_airport, destination_airport, scheduled_departure, departure_time,
departure_delay, taxi_out, wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason, aircraft_manufacture_year,
aircraft_type, engine_type)
SELECT
    TO_DATE(f.date, 'YYYY-MM-DD'),
    f.airline,
    f.flight_number, 
    ac.n_number, 
    f.origin_aiport, 
    f.destination_airport,
    f.scheduled_departure,
    f.departure_time,
    f.departure_delay, 
    f.taxi_out, 
    f.wheels_off, 
    f.scheduled_time, 
    f.elapsed_time, 
    f.air_time, 
    f.distance, 
    f.wheels_on, 
    f.taxi_in,
    f.scheduled_arrival, 
    f.arrival_time, 
    f.arrival_delay, 
    f.diverted, 
    f.cancelled, 
    f.cancellation_reason, 
    ac.manufacture_year,
    ac.aircraft_type,
    ac.engine_type
FROM
    flights_staging as f
INNER JOIN
    aircraft_staging as ac
        ON f.n_number = ac.n_number
""")

insert_analytics_queries = [insert_aircraft, insert_airlines, insert_airports, insert_date, insert_time, insert_flights]

#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------

# The follwing function are created to drop, create and then load data for each of the staging tables above 

def drop_analytics_tables(cur, conn):
    """
    This function will process each of the drop analytics queries that are written above. It thus wipes any pre-existing tables should they exist.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in drop_analytics_queries:
        cur.execute(query)
        conn.commit() 


def create_analytics_tables(cur, conn):
    """
    This function will process each of the create analytics table queries that are written above. It builds empty tables should they not already exist.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in create_analytics_queries:
        cur.execute(query)
        conn.commit()


def insert_analytics_tables(cur, conn):
    """
    This function will process each of the insert into analytics table queries that are written above. It takes data from staging tables to the redshift table referenced.
    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in insert_analytics_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function runs the execution of the above three functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. drop analytics tables
    2. create analytics tables
    3. insert analytics tables
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

    drop_analytics_tables(cur, conn)
    create_analytics_tables(cur, conn)
    insert_analytics_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()


# SCIPRT END

#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------

