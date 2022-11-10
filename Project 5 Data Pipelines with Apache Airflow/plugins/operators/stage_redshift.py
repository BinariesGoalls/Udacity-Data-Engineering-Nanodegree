from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    @apply_defaults
    # Defining the operator parameters
    def __init__(self,
                 conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        # Mapping the parameters
        self.conn_id = conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        
        # Using the AWS Hook to get the AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Using the Postgres Hook to get the Postgres connection
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info("Cleaning the data from the destination Redshift table")
        # Cleaning the destination table
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f"Successfully deleted the data from {self.table} Redshift table")
        
        self.log.info("Copying data from S3 to Redshift")
        
        # Formatting the files path
        self.s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        
        # Copying the data
        redshift.run(f"COPY {self.table}                            \
                       FROM '{s3_path}'                             \
                       ACCESS_KEY_ID '{credentials.access_key}'     \
                       SECRET_ACCESS_KEY '{credentials.secret_key}' \
                       FORMAT AS JSON '{self.json_path}'"
                    )
        
        # Logging success
        self.log.info(f"Successfully copied the data from S3 into {self.table}")





