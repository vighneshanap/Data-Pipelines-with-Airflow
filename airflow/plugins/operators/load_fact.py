from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt 

    def execute(self, context):
        
        self.log.info(f'Loading data into fact {self.table}')
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        sql_query = f"INSERT INTO {self.table} {self.sql_stmt}"
        
        redshift.run(sql_query)
        self.log.info(f"Loading fact {self.table} is completed")
