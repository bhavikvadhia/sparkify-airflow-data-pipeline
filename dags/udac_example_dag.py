from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.udacity_plugin import StageToRedshiftOperator
from airflow.operators.udacity_plugin import LoadFactOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator
from airflow.operators.udacity_plugin import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'bhavik',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1) ,
    'depends_on_past': False
}

dag = DAG('sparkify_data_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *' ,
          catchup = False 
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_events",
    dag = dag ,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    s3_bucket =  "udacity-dend" ,
    s3_key = "log_data/{execution_date.year}/{execution_date.month}/{ds}" ,
    table_name = "staging_events" ,
    json_structure = "s3://udacity-dend/log_json_path.json"
)



stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag ,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    s3_bucket = 'udacity-dend' ,
    s3_key = 'song_data' ,
    table_name = 'staging_songs'
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag ,
    redshift_conn_id = 'redshift',
    database_name = 'public',
    table_name = 'songplays',
    select_sql = SqlQueries.songplay_table_insert  
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag ,
    redshift_conn_id = "redshift" ,
    database_name = "public" ,
    table_name = "users" ,
    select_sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag ,
    redshift_conn_id = "redshift" ,
    database_name = "public" ,
    table_name = "songs" ,
    select_sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag ,
    redshift_conn_id = "redshift" ,
    database_name = "public" ,
    table_name = "artists" ,
    select_sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag ,
    redshift_conn_id = "redshift" ,
    database_name = "public" ,
    table_name = "time" ,
    select_sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag ,
    redshift_conn_id = "redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
