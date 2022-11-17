from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
        dq_checks=[],
        redshift_conn_id="",
        aws_credentials="",
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials=aws_credentials
        self.dq_checks=dq_checks

     
    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials)
        aws_credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        fail_list=[]
        
        self.log.info("Applying quality checks...")
       
        for check in self.dq_checks:
            query=check.get('check_query')
            expected_result=check.get('expected_result')
            result=redshift.get_records(query)[0]
            
            count_err=0
            
            if expected_result != result[0]:
                count_err = count_err+1
                fail_list.append(query)
            
        if count_err == 0:
            self.log.info('Data processed correctly')
        else:
            self.log.info('Data processing failure in the ETL')
            self.log.info(fail_list)