from crawling import crawl_cars
import pandas as pd
from sqlalchemy import create_engine
import config
import car_model_names

def data_preprocessing():
    df = crawl_cars()

    # DataFrame에 SEQ 열 추가 및 초기화
    df['SEQ'] = 0

    df['PRICE'] = df['PRICE'].str.replace(',', '').str.replace('만원', '').str.replace('원', '').astype(int)
    df['MILEAGE'] = df['MILEAGE'].str.replace(',', '').str.replace('km', '').astype(int)
    df['MYERAR'] = df['MYERAR'].str.replace(',', '').str[:4]
    df['DISP'] = df['DISP'].str.replace(',', '').str[:4]
    df['VNUM'] = df['VNUM'].str.replace('차량번호 ', '')

    for key, value in car_model_names.models.items():
        # Check if the 'MNAME' column contains the key
        mask = df['MNAME'].str.contains(key) & df['MNAME'].str.contains(value)
        # Update the values in the 'L_NAME' column where the mask is True
        df.loc[mask, 'L_NAME'] = value + ' ' + key
    
    # MySQL 연결 설정
    engine = create_engine(f'mysql+mysqldb://{config.username}:{config.password}@{config.host}:{config.port}/{config.database_name}')

    try:
        last_seq_df = pd.read_sql(f"SELECT MAX(SEQ) as last_seq FROM {config.table_name}", engine)
        last_seq = last_seq_df['last_seq'].values[0]
    except Exception as e:
        print(f"Error occurred while fetching last SEQ: {e}")
        last_seq = -1

    # 현재 DataFrame에 새로운 SEQ 값 할당
    if last_seq is not None:
        df['SEQ'] = df.index + (last_seq + 1)
    else:
        df['SEQ'] = df.index

    # DataFrame을 MySQL 테이블에 삽입
    try:
        df.to_sql(config.table_name, con=engine, if_exists='append', index=False)
        print('DB 적재 완료')
    except Exception as e:
        print(f"Error occurred while inserting DataFrame to MySQL: {e}")

    print(df)
    print('DB 적재 완료')