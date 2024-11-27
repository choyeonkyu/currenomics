from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pytz
import yfinance as yf
import pandas as pd
from io import StringIO

# AWS S3 설정
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

# Snowflake 설정
SNOWFLAKE_DATABASE = "dev"
SNOWFLAKE_SCHEMA = "raw_data"
SNOWFLAKE_TABLE = "currency"


# 원천 데이터 수집 (Extract 단계)
def extract_currency_data(**kwargs):
    # 수집할 통화 쌍 리스트
    tickers = ["KRW=X", "JPY=X", "CNY=X", "EURUSD=X"]
    start_date = "2019-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    kst = pytz.timezone('Asia/Seoul')
    extract_datetime = datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S") # 추출일시(데이터 무결성 관리용)

    # 데이터를 저장할 딕셔너리
    raw_data = {}

    for ticker in tickers:
        # 환율 데이터 다운로드
        df = yf.download(ticker, start=start_date, end=end_date)
        df["extract_datetime"] = extract_datetime
        raw_data[ticker] = df

    # 원천 데이터를 S3에 저장
    for ticker, df in raw_data.items():
        csv_key = f"raw_currency/{ticker.replace('=', '_')}.csv"
        upload_csv_to_S3(df.reset_index(), key=csv_key)


# 데이터 변환 (Transform 단계)
def transform_currency_data(**kwargs):
    # 수집할 통화 쌍 리스트와 통화 이름 매핑
    tickers = {
        "KRW=X": "KRW",
        "JPY=X": "JPY",
        "CNY=X": "CNY",
        "EURUSD=X": "EUR"
    }

    # 데이터를 병합할 리스트
    all_data = []

    # S3에서 원천 데이터 로드 및 가공
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    for ticker, currency_name in tickers.items():
        # 원천 데이터 로드
        csv_key = f"raw_currency/{ticker.replace('=', '_')}.csv"
        raw_data = s3_hook.read_key(bucket_name="pjt-currenomics", key=csv_key)

        # Pandas로 읽어오기
        df = pd.read_csv(StringIO(raw_data))
        df.rename(
            columns={
                "Date": "date",
                "High": "high",
                "Low": "low",
                "Open": "open",
                "Close": "close",
                "Adj Close": "adj_close",
                "extract_datetime": "extract_datetime"
            },
            inplace=True
        )
        df["currency"] = currency_name  # 통화 이름 추가
        
        # 'date'와 'currency' 열 중 하나라도 비어있는 행 제거 - 기본키 무결성 보존
        df = df.dropna(subset=['date', 'currency'])
        
        df = df[["date", "currency", "high", "low", "open", "close", "adj_close", "extract_datetime"]]
        all_data.append(df)

    # 병합된 데이터프레임 생성
    final_df = pd.concat(all_data, ignore_index=True)

    # 최종 데이터를 S3에 업로드
    upload_csv_to_S3(final_df, key=f"{SNOWFLAKE_TABLE}/{SNOWFLAKE_TABLE}.csv")


# S3 업로드
def upload_csv_to_S3(data, key):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name="pjt-currenomics",
        replace=True
    )


# 테이블 삭제 후 생성 쿼리 (에러 방지를 위한 트랜잭션 처리)
DELETE_AND_CREATE_TABLE_SQL = f"""
BEGIN TRANSACTION;
DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE};
CREATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    date DATE,
    currency STRING,
    high FLOAT,
    low FLOAT,
    open FLOAT,
    close FLOAT,
    adj_close FLOAT,
    extract_datetime TIMESTAMP,
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 로드일시 컬럼 추가
    PRIMARY KEY (date, currency)
);
COMMIT;
"""

# Snowflake로 데이터 COPY
COPY_INTO_SQL = f"""
    ALTER SESSION SET TIMEZONE = 'Asia/Seoul';
    USE WAREHOUSE COMPUTE_WH;
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    (
    DATE,
    CURRENCY,
    HIGH,
    LOW,
    OPEN,
    CLOSE,
    ADJ_CLOSE,
    EXTRACT_DATETIME
    )  
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


# DAG 정의
dag = DAG(
    dag_id="currency",
    start_date=datetime(2023, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    max_active_runs=1
)

# 태스크 정의
# 1. yfinance api로 소스 데이터를 추출해 s3에 csv로 저장
extract = PythonOperator(
    task_id="extract_currency_data",
    python_callable=extract_currency_data,
    provide_context=True,
    dag=dag
)

# 2. 원하는 형태의 테이블로 변환 후 csv 파일로 s3에 저장
transform = PythonOperator(
    task_id="transform_currency_data",
    python_callable=transform_currency_data,
    provide_context=True,
    dag=dag
)

# 3. snowflake에 기존 테이블 삭제 후 생성 (트랜잭션 처리)
delete_and_create_table = SnowflakeOperator(
    task_id="delete_and_create_table",
    snowflake_conn_id="snowflake_conn_id",
    sql=DELETE_AND_CREATE_TABLE_SQL,
    dag=dag
)

# 4. snowflake에 테이블 적재
load_to_snowflake = SnowflakeOperator(
    task_id="load_to_snowflake",
    snowflake_conn_id="snowflake_conn_id",
    sql=COPY_INTO_SQL,
    dag=dag
)

# 태스크 의존성 설정
extract >> transform >> delete_and_create_table >> load_to_snowflake
