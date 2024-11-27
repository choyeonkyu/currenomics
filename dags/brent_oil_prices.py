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

# airflow변수 설정 필요
# Airflow variable 저장했을 때
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

# AWS s3 연결정보 (Airflow connection 저장했을 때)
def get_aws_s3_keys(conn_id):
    # Connection 객체 가져오기
    connection = BaseHook.get_connection(conn_id)

    # Connection 정보 확인
    aws_access_key_id = connection.login
    aws_secret_access_key = connection.password

    return aws_access_key_id, aws_secret_access_key


# yfinance에서 일별 브렌트유 가격 데이터 ID
SERIES_ID = 'BZ=F' # Brent Crude Oil Last Day Financ
TABLE_NAME = 'brent_oil_prices'

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
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT
);
"""

# 테이블 ZKVL 쿼리 : 컬럼 별도 설정 필요
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

    brent_oil_ticker = yf.Ticker(SERIES_ID)
    brent_oil_data = brent_oil_ticker.history(start='2019-01-01')

# 인덱스인 Date를 dataframe의 컬럼으로 변환
    brent_oil_df = brent_oil_data[["Open", "High", "Low", "Close", "Volume"]].reset_index()


    # S3로 CSV저장 (tmp 경로에 저장)
    upload_csv_to_S3(brent_oil_df, True, ds_nodash)


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

    # Date 컬럼을 'yyyy-mm-dd' 형식으로 변환
    # Date 컬럼의 원본 데이터 예시 2019-01-02 00:00:00-05:00
    # 'Date' 컬럼을 datetime 형식으로 변환
    
    modified_df['Date'] = df['Date'].str.split(' ').str[0]
    # Float 형식의 컬럼은 소수점 셋째 자리에서 반올림.
    # yfinance 홈페이지에서도 소수점 둘째 자리까지 보여주는 것을 기준으로 함.
    modified_df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].round(3)

    # S3로 CSV저장
    upload_csv_to_S3(modified_df, False, ds_nodash)

# 임시 파일 삭제
def delete(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    s3_hook.delete_objects(bucket="pjt-currenomics", keys=[f'tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv'])


dag = DAG(
    dag_id = 'brent_oil_prices',
    start_date = datetime(2019,1,1), 
    schedule_interval = '30 2 * * *', # 한국 시간(KST) 기준으로 매일 오전 11시 30분 (미국 주식 개장 시간)  
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

# DAG 실행 순서 
extract >> transform >> create_table >> load >> delete_temp_data