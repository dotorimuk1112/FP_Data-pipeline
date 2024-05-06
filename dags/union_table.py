from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import table_name

def union_tables():
    # MySQL 연결 설정
    hook = MySqlHook(mysql_conn_id='ourcar_mysql')

    # SQL 쿼리 작성
    sql_query = f"""
    UPDATE {table_name.tmp_table_name}
    SET SEQ = SEQ + (SELECT MAX(SEQ) FROM common_carapi)
    """

    # SQL 쿼리 실행
    hook.run(sql_query, autocommit=True)

    # SQL 쿼리 작성
    sql_query = f"""
    UPDATE {table_name.tmp_table_name}
    SET TRANS = '오토'
    WHERE TRANS = '자동';
    """

    # SQL 쿼리 실행
    hook.run(sql_query, autocommit=True)

    # SQL 쿼리 작성
    sql_query = f"""
    UPDATE {table_name.tmp_table_name}
    SET L_NAME = '현대 펠리세이드'
    WHERE L_NAME = '현대 팰리세이드';
    """

    # SQL 쿼리 실행
    hook.run(sql_query, autocommit=True)

    # 두 테이블을 UNION
    sql_query = f"""
    INSERT INTO {table_name.API_table_name}
    SELECT * FROM {table_name.tmp_table_name}
    ON DUPLICATE KEY UPDATE
    SEQ = VALUES(SEQ),
    MNAME = VALUES(MNAME),
    PRICE = VALUES(PRICE),
    MYERAR = VALUES(MYERAR),
    MILEAGE = VALUES(MILEAGE),
    COLOR = VALUES(COLOR),
    TRANS = VALUES(TRANS),
    F_TYPE = VALUES(F_TYPE),
    DISP = VALUES(DISP),
    VTYPE = VALUES(VTYPE),
    CU_HIS = VALUES(CU_HIS),
    MVD_HIS = VALUES(MVD_HIS),
    AVD_HIS = VALUES(AVD_HIS),
    FD_HIS = VALUES(FD_HIS),
    VT_HIS = VALUES(VT_HIS),
    US_HIS = VALUES(US_HIS),
    L_NAME = VALUES(L_NAME)
    """

    # SQL 쿼리 실행
    hook.run(sql_query, autocommit=True)