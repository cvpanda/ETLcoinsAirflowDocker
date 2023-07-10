from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from dotenv import dotenv_values
from datetime import datetime, timedelta

config = dotenv_values('.env')

schema = 'nahuelcasagrande_coderhouse'  # config['REDSHIFT_SCHEMA']
table = 'coinrank'
columns = ["uuid", "symbol", "name", "color", "iconUrl",
           "marketCap", "price", "listedAt", "tier", "change", "rank", "transactionFee"]

QUERY_CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        timestamp TIMESTAMP,
        {', '.join([f"{column} VARCHAR" for column in columns])}
    ) DISTKEY(id) SORTKEY(symbol);
"""


default_args = {
    'owner': 'nahuelCasa',
    'start_date': datetime(2023, 7, 10),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="coin_api_dag",
    default_args=default_args,
    description="ETL crypto coins",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    spark_etl_users = SparkSubmitOperator(
        task_id="spark_etl_users",
        application=f'{Variable.get("spark_scripts_dir")}/apicalls.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    create_table >> spark_etl_users
