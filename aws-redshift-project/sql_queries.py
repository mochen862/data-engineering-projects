# CONFIG

import configparser
config=configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop= "DROP TABLE IF EXISTS users"
songs_table_drop="DROP TABLE IF EXISTS songs"
artists_table_drop="DROP TABLE IF EXISTS artists"
time_table_drop="DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create = """CREATE TABLE IF NOT EXISTS staging_events
                                (artist varchar, 
                                auth varchar,
                                firstName varchar,
                                gender varchar,
                                ItemInSession bigint,
                                lastName varchar,
                                length float,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration varchar,
                                sessionId bigint,
                                song varchar,
                                status bigint,
                                ts bigint,
                                userAgent varchar,
                                userId bigint
                                )"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs
                                    (num_songs bigint,
                                    artist_id varchar,
                                    artist_latitude decimal(8, 5),
                                    artist_longitude decimal (8, 5),
                                    artist_location varchar,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar,
                                    duration float,
                                    year int
                                    )"""

songplays_table_create = """CREATE TABLE IF NOT EXISTS songplays
                            (
                            songplay_id int IDENTITY(0,1) PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id int NOT NULL,
                            level varchar,
                            song_id varchar,
                            artist_id varchar,
                            session_id int,
                            location varchar,
                            user_agent varchar
                            )"""

users_table_create = """CREATE TABLE IF NOT EXISTS users
                        (
                        user_id int PRIMARY KEY, 
                        first_name varchar,
                        last_name varchar,
                        gender varchar, 
                        level varchar
                        )"""

songs_table_create = """CREATE TABLE IF NOT EXISTS songs
                        (
                        song_id varchar PRIMARY KEY,
                        title varchar, 
                        artist_id varchar,
                        year int,
                        duration float
                        )"""

artists_table_create = """CREATE TABLE IF NOT EXISTS artists 
                            (
                            artist_id varchar PRIMARY KEY,
                            name varchar,
                            location varchar,
                            latitude decimal (8, 5),
                            longitude decimal (8, 5)
                            )"""

time_table_create = """CREATE TABLE IF NOT EXISTS time 
                        (
                        start_time TIMESTAMP PRIMARY KEY,
                        hour int,
                        day int,
                        week int,
                        month int, 
                        year int,
                        weekday int
                        )"""


# STAGING TABLES

staging_events_copy = """
COPY staging_events
FROM {}
iam_role {}
region 'us-west-2'
format as json {}
""".format(config.get('S3', 'LOG_DATA'),
           config.get('IAM_ROLE', 'ARN'),
           config.get('S3', 'LOG_JSONPATH')
          )

staging_songs_copy = """
copy staging_songs
from {}
iam_role {}
json 'auto'
region 'us-west-2';
""".format(config['S3']['SONG_DATA'],
           config['IAM_ROLE']['ARN'])


# FINAL TABLES 

songplays_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
    e.userId, 
    e.level, 
    s.song_id,
    s.artist_id, 
    e.sessionId,
    e.location, 
    e.userAgent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong' 
AND e.song = s.title 
AND e.artist = s.artist_name 
AND e.length = s.duration
""")


# AWS does not support ON CONFLICT (colname) DO UPDATE SET colname = EXCLUDED.colname
# hence the CTE to load only the latest rowfrom each user
users_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
WITH uniq_staging_events AS (
    SELECT userId, firstName, lastName, gender, level,
        ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
    FROM staging_events
        WHERE userId IS NOT NULL AND page = 'NextSong'
)
SELECT userId, firstName, lastName, gender, level
FROM uniq_staging_events
WHERE rank = 1;
""")

songs_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artists_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays
""")



# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, artists_table_insert, time_table_insert]