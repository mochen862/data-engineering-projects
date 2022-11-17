from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_insert="""
            insert into {}
            {}
            """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 source_tbl_query="",
                 aws_credentials="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table=fact_table
        self.source_tbl_query=source_tbl_query
        self.aws_credentials=aws_credentials

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials)
        aws_credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        
        sql_stmt=LoadFactOperator.sql_insert.format(
            self.fact_table,
            self.source_tbl_query
            )
        
        self.log.info(f"Loading fact table '{self.fact_table}' into Redshift")

        redshift.run(sql_stmt)

