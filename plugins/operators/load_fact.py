from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""

Sub DAG for loading data from songplay staging table to SONGPLAYS fact table

Parameters:
redshift_conn_id : connection id for AWS Redshift connection
database_name    : target database name
table_name       : target table name
select_sql       : select sql for inserting data from staging table into target fact table

"""

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "" ,
                 database_name = "" ,
                 table_name = "" ,
                 select_sql = "" ,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database_name = database_name
        self.table_name = table_name
        self.select_sql = select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_query = f"INSERT INTO {self.database_name}.{self.table_name} {self.select_sql};"
        redshift.run(insert_query)
