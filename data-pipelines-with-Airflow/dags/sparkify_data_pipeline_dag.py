from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1 ),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False}

dag = DAG('sparkify_automate_data_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
)

start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='staging_events',
    table='staging_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}",
    json_format="'s3://udacity-dend/log_json_path.json'",
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='staging_songs',
    table='staging_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_format="'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    fact_table='songplays',
    source_tbl_query=SqlQueries.songplay_table_insert,
    truncate_table=True,
    aws_credentials="aws_credentials",
    dag=dag    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    dim_table='users',
    source_tbl_query=SqlQueries.user_table_insert,
    truncate_table=True,
    aws_credentials="aws_credentials",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    dim_table='songs',
    source_tbl_query=SqlQueries.song_table_insert,
    truncate_table=True,
    aws_credentials="aws_credentials",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    dim_table='artists',
    source_tbl_query=SqlQueries.artist_table_insert,
    truncate_table=True,
    aws_credentials="aws_credentials",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    dim_table='time',
    source_tbl_query=SqlQueries.time_table_insert,
    truncate_table=True,
    aws_credentials="aws_credentials",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    dag=dag,
    dq_checks=[
        {'check_query': "SELECT COUNT(*) FROM staging_events WHERE song is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_events WHERE artist is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_events WHERE length is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_songs WHERE title is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_songs WHERE artist_name is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_songs WHERE duration is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM songplays WHERE sessionid is null", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM songplays WHERE start_time is null", 'expected_result':0},
    ]
        
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]

[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator

