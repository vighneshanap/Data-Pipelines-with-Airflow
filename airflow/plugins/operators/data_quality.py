from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Performing data quality')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.table:
            self.log.info(f"Checking {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed for {table} no result returned")
        
            num_records = records[0][0]    
            if num_records < 1:
                raise ValueError(f"Data quality check failed {table} contained 0 rows")
           
            self.log.info(f"Data quality check on {table} passed with {num_records}")
            
        self.log.info(f"Data quality check passed")