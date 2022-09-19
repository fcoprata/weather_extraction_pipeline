import json 
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from pandas import json_normalize

def _process_weather(ti):
    weather = ti.xcom_pull(task_ids='extract_weather')
    processed_weather = json_normalize(
        {
            'temperature': weather['temperature'],
            'wind': weather['wind'],
            'description': weather['description']
        }
    )
    processed_weather.to_csv('/tmp/processed_user.csv', index=None, header=False)

def _store_weather():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY weather_maranguape FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv',
        )



with DAG(
    "weather_processing",
    start_date=datetime(2022,9,16),
    schedule_interval="*/1 * * * *",
    catchup=False) as dag:


    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres',
        sql=
        '''
            CREATE TABLE IF NOT EXISTS weather_maranguape (
            temperature TEXT NOT NULL,
            wind TEXT NOT NULL,
            description TEXT NOT NULL
        );
        '''
    )  

    is_api_avaliable = HttpSensor(
        task_id = 'is_api_avaliable',
        http_conn_id='api_weather',
        endpoint=""
    )

    extract_weather = SimpleHttpOperator(
        task_id='extract_weather',
        http_conn_id='api_weather',
        endpoint='',
        method='GET',
        response_filter= lambda response: json.loads(response.text),
        log_response=True
    )

    process_weather = PythonOperator(
        task_id='process_weather',
        python_callable=_process_weather
    )

    store_weather = PythonOperator(
        task_id='store_weather',
        python_callable=_store_weather
    )

    create_table>>is_api_avaliable>>extract_weather>>process_weather>>store_weather