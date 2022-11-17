import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    
    """
    Reads song file and insert records into the songs and artists tables
    
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    
def process_log_file(cur, filepath):
    
    """
    Reads the log file and insert records into the time, users and songplays tables.
    Matches the song title, the artist name and the song duration form the songs and artists tables with the song, artist and length from the songplays table to find a matching song id and artists id (as the log data does not contain song id or artist id)
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df.page == 'NextSong']
    df.reset_index(drop=True, inplace=True)
    
    # convert timestamp to datetime column
    df.ts = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    timestamp = df.ts
    hour = df.ts.dt.hour
    day = df.ts.dt.day
    week_of_year = df.ts.dt.weekofyear
    month = df.ts.dt.month
    year = df.ts.dt.year
    weekday = df.ts.dt.weekday
    time_data =  [timestamp, hour, day, week_of_year, month, year, weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    
    d = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(d)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insert songplay records
    for i, row in df.iterrows():
        
        # get songgid and artistid form song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # insert songplay record
        songplay_data =  (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
def process_data(cur, conn, filepath, func):
    
    """
    Gets all the filepaths and loops through them
    """
    
    # get all files matching extension from library
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(f)
    
    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed".format(i, num_files))
        
def main():
    
    """
    1. Creates a connection to the sparkify database
    2. Gets the cursor
    3. Then gets all of the filepaths for song data, loops through them and inserts the records into the songs and artists tables
    4. Then gets all of the filepaths for the log data, loops through them and inserts the records into the time, users and songplays table. Finds the matching song id and artist id between the songs & artists tables and the songplays table
    5. closes the connection to sparkify database
    """
    
    # load parameters
    import configparser
    config = configparser.ConfigParser()
    config.read('rdm.cfg')
    DB_USER = config.get('RDM', 'DB_USER')
    DB_PASSWORD = config.get('RDM', 'DB_PASSWORD')
    
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user={} password={}'.format(DB_USER, DB_PASSWORD))
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data/', \
                 func = process_song_file)
    process_data(cur, conn, filepath='data/log_data/', \
                 func = process_log_file)
    
    conn.close()
    
if __name__ == "__main__":
    main()
