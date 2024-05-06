from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import table_name

def delete_invalid_data():
    # MySQL 연결 설정
    hook = MySqlHook(mysql_conn_id='ourcar_mysql')

    sql_query_delete_0_price = f"""
    DELETE FROM {table_name.API_table_name} WHERE PRiCE = 0
    """

    sql_query_delete_nan = f"""
    DELETE FROM {table_name.API_table_name} WHERE L_NAME = 'nan'
    """

    sql_query_delete_null = f"""
    DELETE FROM {table_name.API_table_name} WHERE L_NAME IS NULL OR L_NAME = ''
    """

    hook.run(sql_query_delete_0_price, autocommit=True)

    hook.run(sql_query_delete_nan, autocommit=True)

    hook.run(sql_query_delete_null, autocommit=True)