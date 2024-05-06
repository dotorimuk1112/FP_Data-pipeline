from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import table_name

def reset_table():
    # MySQL 연결 설정
    hook = MySqlHook(mysql_conn_id='ourcar_mysql')

    sql_query_truncate = f"""
    TRUNCATE TABLE {table_name.tmp_table_name}
    """

    # SQL 쿼리 실행
    hook.run(sql_query_truncate, autocommit=True)