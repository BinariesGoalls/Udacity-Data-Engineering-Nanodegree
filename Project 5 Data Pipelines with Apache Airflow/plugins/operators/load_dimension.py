from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    # Defining the operator parameters
    def __init__(self,
                 conn_id,
                 table,
                 query,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        # Mapping the parameters
        self.conn_id = conn_id
        self.table = table
        self.query = query

        

    def execute(self, context):
        
        # Using the Postgres Hook to get the Postgres connection
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info(f"Inserting data from staging table into {self.table} dimension table")
        # Loading the data from Staging to Fact table
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        
        # Logging success
        self.log.info(f"Sucessfully loaded the data from staging into {self.table}")
