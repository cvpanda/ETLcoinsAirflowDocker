from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from dotenv import dotenv_values
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import json

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

def check_and_send_email(**context):
    ti = context['ti']
    api_response = ti.xcom_pull(task_ids='spark_etl_users', key='api_response')
    print(f'api_response')
    print(f'{api_response}')
    print('----------------')
    json_data = json.loads(api_response)
    print(f'{json_data}')

    #send_email_alert()
    for item in json_data:
        if item['rank'] == '1':
            send_email_alert()

def send_email_alert():
    email_content = "Se encontro una coin en el top 1!"
    email_operator = EmailOperator(
        task_id='send_email_alert',
        to='nahuelcasagrande@gmail.com',  #Poner email propio para probar
        subject='Coin Top 1 del momento',
        html_content=email_content,
    )
    return email_operator



default_args = {
    'owner': 'nahuelCasa',
    'start_date': datetime(2023, 7, 28),
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

    check_and_send_email_task = PythonOperator(
        task_id='check_and_send_email_task',
        python_callable=check_and_send_email,
        provide_context=True,
        dag=dag,
    )

    create_table >> spark_etl_users >> check_and_send_email_task
