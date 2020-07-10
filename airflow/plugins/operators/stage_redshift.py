from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS json '{}'
    
    """"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json="auto",
                 aws_region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
            self.redshift_conn_id = redshift_conn_id 
            self.aws_credential_id = aws_credential_id
            self.table = table
            self.s3_bucket = s3_bucket
            self.s3_key = s3_key
            self.copy_json = copy_json
            self.aws_region = aws_region
            
    def execute(self, context):
        self.log.info(f"Getting AWS credentials and setting up for {self.table}.")
        
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Deleting data from destination Redshift {self.table}.")
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info(f"Copying data from S3 bucket to Redshift {self.table}.")
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        sql_query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.aws_region,
            self.copy_json
        )
        
        redshift.run(sql_query)
        self.log.info(f"Data copied into {self.table} successfully.")
        





