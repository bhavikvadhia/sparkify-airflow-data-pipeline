from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

"""

This is a sub DAG created for running data quality checks on the tables created for Sparkify

redshift_conn_id : redshift connection id

"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "" ,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"running the DQ - check Query - {SqlQueries.invalid_level_dq}")
        records = redshift.get_records(SqlQueries.invalid_level_dq)
        if len(records) > 0 and len(records[0]) > 0:
            if records[0][0]  > 0:
                raise ValueError(f"Number of Records with invalid 'level' in songplays table - {records[0][0]}")

        self.log.info(f"running the DQ - check Query - {SqlQueries.ri_check_dq}")
        records = redshift.get_records(SqlQueries.ri_check_dq)
        if len(records) > 0 and len(records[0]) > 0:
            if records[0][0]  > 0:
                raise ValueError(f"Number of Records with invalid 'songid' in songplays table - {records[0][0]}")