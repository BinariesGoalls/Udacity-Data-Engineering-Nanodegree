from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    # Defining the operator parameters
    def __init__(self,
                 conn_id,
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        # Mapping the parameters
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        
        # Using the Postgres Hook to get the Postgres connection
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        # Execute for every table on the list
        for table in self.tables:
            
            # Selecting the number of records
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")  
            
            # Validating results
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            # Catching number of records    
            num_records = records[0][0]
            
            # Validating the number of records
            if num_records ==  0:
                self.log.error(f"No records present in destination table {table}")
                raise ValueError(f"Data quality check failed. {table} don't have any records")
            
            # Data quality check results
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")