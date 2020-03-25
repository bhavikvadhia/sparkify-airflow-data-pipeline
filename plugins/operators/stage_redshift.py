from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

"""

Sub DAG created for copying data from S3 files to Redshift Staging tables.

Parameters:
redshift_conn_id : connection id for AWS Redshift connection
aws_credentials  : aws credentials
table_name       : target table name
s3_bucket        : S3 bucket name for the source file
s3_key           : S3 file location for the source file
json_structure   : structure file for the json,, Default = AUTO

"""

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 table_name = "" ,
                 s3_bucket = "" ,
                 s3_key = "" , 
                 json_structure = "auto" ,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_structure = json_structure

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Emptying the {self.table_name} table")
        redshift.run(f"DELETE FROM {self.table_name}")
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = SqlQueries.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_structure
        )
        redshift.run(formatted_sql)
