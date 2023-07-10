from datetime import datetime, timedelta

from sqlalchemy import Table, MetaData

import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_to_db(ti):
    '''Загрузка полученных данных в базу данных'''

    # получаем информацию из Airflow для подключения к БД
    pg_hook = PostgresHook(postgres_conn_id='postgre_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    # Получаем информацию о таблице в базе данных
    meta_obj = MetaData(schema='public')
    canabis_table = Table('canabis_data',
                           meta_obj,
                           autoload_with=engine)
    
    # Выгружаем полученные данные из Xcom
    val_list = ti.xcom_pull(task_ids='getting_data')

    # открываем соединение 
    with engine.connect() as conn:

        # Формируем запрос
        db_req = (canabis_table
                  .insert()
                  .values(val_list)
                )
        # Выполняем
        conn.execute(db_req)


with DAG(
    'airflow_process',
    start_date = datetime(2023, 7, 10, 0, 0, 0), # изменить на необходимое время старта
    max_active_runs = 1,
    catchup = False,
    description = 'get canabis info',
    schedule_interval = timedelta(hours = 12)
) as dag:

    # Проверяем доступность API
    is_api_available = HttpSensor(
        task_id = 'api_test',
        http_conn_id = 'source_api',
        endpoint = '/cannabis/random_cannabis'
    )

    # Получение информации из источника 
    getting_data = SimpleHttpOperator(
        task_id = 'getting_data',
        http_conn_id= 'source_api',
        endpoint = '/cannabis/random_cannabis?size=10',
        method = 'GET',
        # преобразуем инфо из текста в json
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    # проверяем наличие таблицы если нет - создаем
    table_check = PostgresOperator(
        task_id = 'table_check_or_create',
        postgres_conn_id = 'postgre_conn',
        sql = '''
                CREATE TABLE IF NOT EXISTS public.canabis_data (
                    id INT,
                    uid TEXT,
                    strain TEXT,
                    cannabinoid_abbreviation TEXT,
                    cannabinoid TEXT,
                    terpene TEXT, 
                    medical_use TEXT,
                    health_benefit TEXT,
                    category TEXT,
                    type TEXT,
                    buzzword TEXT,
                    brand TEXT
                )
                '''
    )

    # Загрузка в БД
    load = PythonOperator(
        task_id = 'load_to_db',
        python_callable = load_to_db
    )

    is_api_available >> getting_data >> table_check >> load
