import re
from airflow import DAG
from datetime import datetime, timedelta
from sqlalchemy import false
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import boto3
import pyarrow.parquet as pq

default_args = {
    'owner': 'Airflow',
    "retries": 1,
    'start_date': datetime(2022, 5, 29),
    "retry_delay": timedelta(minutes=5),
}

dag = DAG('poc', default_args=default_args, schedule_interval='@daily')

def transform_func(ds, **kwargs):

    query = "SELECT * FROM data_mocks"

    fetch_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    fetch_conn = fetch_hook.get_conn()

    fetch_cursor = fetch_conn.cursor()

    fetch_cursor.execute(query)
    records = fetch_cursor.fetchall()

    print(type(records))
    # print(records)

    fetch_cursor.close()
    fetch_conn.close()

    df = pd.DataFrame(records, columns=["id", "first_name", "email"])
    # print(df)

    def clean_first_name(val):
        return re.sub(r'[^\w\s]', '', val).strip()

    df['first_name'] = df['first_name'].map(lambda x: clean_first_name(x))

    print(df)

    # df.to_csv('~/store_file_airflow/CLEAN_MOCK_DATA.csv', index=False)


    # table = pa.Table.from_pandas(df)
    # pq.write_table(table, 'example.parquet')
    df.to_parquet('~/store_file_airflow/CLEAN_MOCK_DATA.parquet', index=False)
      
def transfer_to_S3():
    # print("hello")
    client=boto3.client('s3',aws_access_key_id="", aws_secret_access_key="")

    client.create_bucket(Bucket='poc-airflow1')

    with open("/usr/local/airflow/store_file_airflow/CLEAN_MOCK_DATA.parquet","rb") as f:
        client.upload_fileobj(f,"poc-airflow1","CLEAN_MOCK_DATA.parquet")

t1 = BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_file_airflow/MOCK_DATA.csv', retries=2, retry_delay=timedelta(seconds=15), dag=dag)

t2 = PostgresOperator(
    task_id='create_table',
    postgres_conn_id= 'postgres_conn',
    sql= """
        CREATE TABLE IF NOT EXISTS data_mocks (
        id varchar(20), 
        first_name varchar(50), 
        email varchar(50))
        """, 
    provide_context=True, 
    dag=dag)

t3 = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id= 'postgres_conn',
    sql= """
        COPY data_mocks
        FROM '/store_file_psql/MOCK_DATA.csv' DELIMITER ',' HEADER CSV;
        """, 
    provide_context=True, 
    dag=dag)

t4 = PythonOperator(task_id='transformation', python_callable=transform_func, provide_context=True, dag=dag)

t5 = PythonOperator(task_id="load_to_s3", python_callable=transfer_to_S3 , dag=dag)

t1 >> t2 >> t3 >> t4 >> t5