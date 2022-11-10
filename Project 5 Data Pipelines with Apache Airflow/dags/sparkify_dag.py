from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "BinariesGoalls",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
    "start_date": datetime.now()
}

dag = DAG("sparkify_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval='0 * * * *'
        )

# Dummy operator to start the DAG execution
start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

# Copying the events/log data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

# Copying the songs data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto"
)

# Loading the data from staging area tables into songplays fact table
load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

# Loading the data from staging area tables into users dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    table="users",
    query=SqlQueries.user_table_insert
)

# Loading the data from staging area tables into songs dimension fact table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    table="songs",
    query=SqlQueries.song_table_insert
)

# Loading the data from staging area tables into artists dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    table="artists",
    query=SqlQueries.artist_table_insert
)

# Loading the data from staging area tables into time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    conn_id="redshift",
    table="time",
    query=SqlQueries.time_table_insert
)

# Running data quality checks to ensure that the tables were loaded correctly
run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    conn_id="redshift",
    tables=[ "songplays", "songs", "artists",  "time", "users"]
)

# Dummy operator to finish the DAG execution
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Defining the task dependencies

#2nd layer
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

#3rd layer
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

#4th layer
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

#5th
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

#6th layer
run_quality_checks >> end_operator
