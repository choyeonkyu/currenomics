from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from fredapi import Fred
import yfinance as yf
from io import StringIO
import pandas as pd
import requests

# airflow변수 설정 필요
# Airflow variable 저장했을 때
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
ECOS_API_KEY = Variable.get("ECOS_API_KEY")

# AWS s3 연결정보 (Airflow connection 저장했을 때)
def get_aws_s3_keys(conn_id):
    # Connection 객체 가져오기
    connection = BaseHook.get_connection(conn_id)

    # Connection 정보 확인
    aws_access_key_id = connection.login
    aws_secret_access_key = connection.password

    return aws_access_key_id, aws_secret_access_key


TABLE_NAME = 'consumer_price_kr'

# 데이터베이스, 스키마, 테이블 정보
SNOWFLAKE_DATABASE = "dev"
SNOWFLAKE_SCHEMA = "raw_data"
SNOWFLAKE_TABLE = TABLE_NAME

# 테이블 생성 쿼리 : 컬럼 별도 설정 필요
# Full Refresh
CREATE_TABLE_SQL = f"""
DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE};
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    date DATE,
    consumer_price FLOAT
);
"""

# 테이블 ZKVL 쿼리 : 컬럼 별도 설정 필요
# s3 파일을 snowflake에 복사
COPY_INTO_SQL=f"""
    USE WAREHOUSE COMPUTE_WH;
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    FROM 's3://pjt-currenomics/{SNOWFLAKE_TABLE}/{SNOWFLAKE_TABLE}.csv'
    CREDENTIALS=(
        AWS_KEY_ID='{AWS_ACCESS_KEY}'
        AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}'
    )
    FILE_FORMAT=(
        TYPE='CSV',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        NULL_IF = ('', 'NULL')
    );
"""

# 데이터프레임을 csv로 변환하여 S3로 업로드 (extract에서는 tmp에 저장)
# ds_nodash는 Airflow에서 제공하는 매크로 변수 중 하나로, DAG 실행 날짜 (dash '-' 없는 버전)
def upload_csv_to_S3(data, is_tmp, ds_nodash):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    if is_tmp:
        s3_hook.load_string(
            string_data=csv_data,
            key=f"tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv",
            bucket_name="pjt-currenomics",
            replace=True  
        )
    else:
        s3_hook.load_string(
            string_data=csv_data,
            key=f"{SNOWFLAKE_TABLE}/{SNOWFLAKE_TABLE}.csv",
            bucket_name="pjt-currenomics",
            replace=True  
        )


def extract(**kwargs):
    ds_nodash = kwargs['ds_nodash']

    # 한국은행 API 호출
    url = f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/1/50000/901Y009/M/201901/202410"
    response = requests.get(url)

    # 필요한 데이터 추출,'총지수'만 필터링
    data = response.json()
    rows = data['StatisticSearch']['row'] 
    filtered_data = [
        {'date': row['TIME'], 
        'consumer_price': row['DATA_VALUE']}
        for row in rows if row['ITEM_NAME1'] == '총지수'
    ]

    consumer_price_df = pd.DataFrame(filtered_data)
    consumer_price_df['date'] = pd.to_datetime(consumer_price_df['date'], format='%Y%m')
    
    upload_csv_to_S3(consumer_price_df, True, ds_nodash)


def transform(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    file_content = s3_hook.read_key(
        key=f"tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv",  # 임시로 저장
        bucket_name='pjt-currenomics'
    )

    # Pandas로 데이터 로드
    df = pd.read_csv(StringIO(file_content))  # S3 데이터를 pandas 데이터프레임으로 읽기

    # Pandas로 데이터 처리로직
    # TO-DO
    modified_df = df

    # S3로 CSV 저장
    upload_csv_to_S3(modified_df, False, ds_nodash)

# 임시 파일 삭제
def delete(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    s3_hook.delete_objects(bucket="pjt-currenomics", keys=[f'tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv'])


dag = DAG(
    dag_id = 'consumer_price_kr',
    start_date = datetime(2019,1,1), 
    schedule = '0 0 6 * *', # 매월 6일 오전 9시(KST)   
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
)

# 1. FRED API 호출 및 S3저장
extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        provide_context=True,
        dag = dag
    )

# 2. tmp에 저장된 CSV호출 후 데이터 변환 후 S3 본 테이블에 이관
transform = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        provide_context=True,
        dag = dag
    )

# 3. 테이블 생성 태스크 (IF NOT EXISTS)
create_table = SnowflakeOperator(
        task_id="create_table_if_not_exists",
        snowflake_conn_id='snowflake_conn_id',
        sql=CREATE_TABLE_SQL,
        dag = dag
    )

# 4. S3의 정제 데이터를 Snowflake로 COPY 태스크
load = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id='snowflake_conn_id',
        sql=COPY_INTO_SQL,
        dag = dag
    )

# 4. S3의 임시파일 삭제
delete_temp_data = PythonOperator(
        task_id = 'delete',
        python_callable = delete,
        provide_context=True,
        dag = dag
    )

extract >> transform >> create_table >> load >> delete_temp_data