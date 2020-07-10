from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 trunc_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.trunc_insert = trunc_insert
        
        
    def execute(self, context):
        self.log.info(f"Loading data into {self.table}")
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.trunc_insert:
            self.log.info(f"Deleting rows from {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
          
        sql_insert = f"INSERT INTO {self.table} {sql_stmt}"
        
        self.log.info(f"Insert Statement {sql_insert}")
        redshift.run(sql_insert)
        
        self.log.onfo(f"Inserting into {self.table} done.")