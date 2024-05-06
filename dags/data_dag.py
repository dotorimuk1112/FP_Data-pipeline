from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_preprocessing import data_preprocessing
from union_table import union_tables
from reset_tmp_table import reset_table
from complete import complete
from invalid_data_processing import delete_invalid_data

default_args = {
   "start_date" : datetime(2023, 1, 1),
    "retry" : 3
}

# DAG 정의
with DAG(
    dag_id="CarAPI_Update_DAG",
    schedule_interval="@daily",
    default_args=default_args,
    tags=['Car', 'api', 'MySQL'],
    catchup=False,

) as dag:
    # 크롤링 및 DB 적재까지
    get_data_and_update_DB = PythonOperator(
    task_id="get_bobaedream_data",
    python_callable=data_preprocessing,
    dag=dag
    )

        # DAG에 추가할 task 정의
    union_and_update_seq = PythonOperator(
        task_id="union_and_update_seq",
        python_callable=union_tables,
        dag=dag
    )

    reset_table = PythonOperator(
        task_id="reset_tmp_table",
        python_callable=reset_table,
        dag=dag
    )

    invalid_data_processing = PythonOperator(
        task_id="delete_invalid_data",
        python_callable=delete_invalid_data,
        dag=dag
    )

    complete_dag = PythonOperator(
        task_id="complete_dag",
        python_callable=complete,
        dag=dag
    )

    get_data_and_update_DB >> union_and_update_seq >> reset_table >> invalid_data_processing >> complete_dag