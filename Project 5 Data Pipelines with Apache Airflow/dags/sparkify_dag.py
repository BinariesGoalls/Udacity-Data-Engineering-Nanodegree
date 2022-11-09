from datetime import datetime, timedelta
import datetime
import os
from airflow import conf
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator, 
                               CreateTablesOperator)

from helpers import SqlQueries

start_date = datetime.datetime(2018, 11, 1)
end_date = datetime.datetime(2018, 12, 31)

# Default arguments
default_args = {
    'owner': 'Alisson Lima',
    'start_date': start_date,
    'end_date': end_date,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# DAG specification
dag = DAG('sparkify_dag',
          default_args=default_args,
          description="Loads and transforms the data in Redshift with Airflow",
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



# Create tables using an operator
create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

# Stage events data to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

# Stage songs data to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON"
)

# Stage songplays data to Redshift
load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert
)

# Load users dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert
)

# Load songs dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert
)

# Load artists dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert
)

# Load time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert
)

# Run quality checks on Redshift tables
run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    tables=["songplays", "users", "songs", "artists", "times"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting tasks dependencies
start_operator  \
    >> create_redshift_tables \
    >> [stage_songs_to_redshift, stage_events_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator