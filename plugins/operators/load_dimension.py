from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""

Sub DAG for loading data from staging table to Dimension tables

Parameters:
redshift_conn_id : connection id for AWS Redshift connection
database_name    : target database name
table_name       : target table name
select_sql       : select sql for inserting data from staging table into target fact table
insert_mode      : by default - TRUNCATE_AND_LOAD - will truncate the table and then load, if any different value - will append

"""

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "" ,
                 database_name = "" ,
                 table_name = "" ,
                 select_sql = "" ,
                 insert_mode = "TRUNCATE_AND_LOAD" ,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database_name = database_name
        self.table_name = table_name
        self.select_sql = select_sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_query = f"INSERT INTO {self.database_name}.{self.table_name} {self.select_sql};"
        if self.insert_mode == "TRUNCATE_AND_LOAD":
            redshift.run(f"DELETE FROM {self.database_name}.{self.table_name};")
        redshift.run(insert_query)
