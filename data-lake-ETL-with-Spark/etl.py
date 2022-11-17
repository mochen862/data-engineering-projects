from datetime import datetime
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, expr, desc, asc
import configparser
import os
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Creating spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Load song data, create songs and artists tables and write them out to s3 bucket
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    song_df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = song_df.filter("song_id != 'null'").select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + "songs/songs_table.parquet")
    
    # extract columns to create artists table
    artists_table = song_df.filter("artist_id != 'null'").select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists/artists_table.parquet")
    
def process_log_data(spark, input_data, output_data):
    """
    Load log data, create users, time and songplays tables, and write them out to s3 bucket
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.where(log_df.page == 'NextSong')

    # extract columns for users table    
    log_df.createOrReplaceTempView("log_data")
    users_table = spark.sql("""WITH uniq_log_data AS (SELECT userId, firstName, lastName, gender, level, ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank FROM log_data WHERE userId IS NOT NULL)
SELECT userId, firstName, lastName, gender, level
FROM uniq_log_data
WHERE rank = 1
""")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users/users_table.parquet")

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn('timestamp', expr("cast(ts/1000 as timestamp)"))
    
    # create datetime column from original timestamp column
    log_df = log_df.withColumn('datetime', date_format('timestamp', "YYYY-MM-dd")) 
    
    # extract columns to create time table
    time_table = log_df.select('timestamp', hour('timestamp').alias('hour'), dayofmonth('timestamp').alias('day'),
                       weekofyear('timestamp').alias('week'), month('timestamp').alias('month'), 
                       year('timestamp').alias('year'), dayofweek('timestamp').alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + "time/time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('song_data')
    log_df.createOrReplaceTempView('log_data')
    songplays_table = spark.sql("""select row_number() over(order by l.ts) as songplay_id,
    l.ts as start_time, l.userId, l.level, s.song_id, s.artist_id, l.sessionId, l.location, l.userAgent, 
    year(l.timestamp) as year, month(l.timestamp) as month
    from log_data l 
    join song_data s 
    on l.artist = s.artist_name and l.song = s.title and l.length = s.duration""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + "songplays/songplays_table.parquet")
    
def main():
    """
    Create spark session
    Load song data, create songs and artists tables and write them out to s3 bucket
    Load log data, create users, time and songplays tables, and write them out to s3 bucket
    """
    spark = create_spark_session()
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    input_data = config.get('IO', 'INPUT_DATA')
    output_data = config.get('IO', 'OUTPUT_DATA')
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()
