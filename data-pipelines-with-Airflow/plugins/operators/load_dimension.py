from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_insert="""
            insert into {}
            {}
            """

    sql_truncate = """
        TRUNCATE TABLE {};
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table="",
                 source_tbl_query="",
                 truncate_table=False,
                 aws_credentials="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table=dim_table
        self.source_tbl_query=source_tbl_query
        self.aws_credentials=aws_credentials
        self.truncate_table=truncate_table

    def execute(self, context):

        aws_hook=AwsHook(self.aws_credentials)
        aws_credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)


        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.dim_table}")
            redshift.run(LoadDimensionOperator.sql_truncate.format(self.dim_table))

        self.log.info(f"Loading dimension table {self.dim_table}")
        sql_stmt_dim=LoadDimensionOperator.sql_insert.format(
            self.dim_table,
            self.source_tbl_query
            )
        
        redshift.run(sql_stmt_dim)
        