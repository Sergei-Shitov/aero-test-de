# раскомментить для получения параметров из переменных окружения
# import os

import requests
from requests.exceptions import ConnectionError

import pandas as pd
import json
from datetime import datetime

from sqlalchemy.engine import create_engine as eng
from sqlalchemy.exc import OperationalError

def elt_process(sourse_url:str, db_host:str, db_port:int, db_name:str, db_user:str, db_password:str, db_schema:str) -> None:
    '''Getting data from API and storing it to DB'''

    # получаем данные
    try:
        res = requests.get(sourse_url)

    except ConnectionError as e:
        # Ошибка если нет интернет соединения
        print(f'No internet connection\nerror:\n{e}')

    # проверяем ответ
    if res.status_code != 200:
        raise Exception('Check the API url address')

    # преобразуем в dict
    data = json.loads(res.text)

    # разделяем данные для загрузки в таблицу
    team = data['stats'][0]['splits'][0]['team']
    game_type = {'statsSingleSeason_'+ k : v for k, v in data['stats'][0]['type']['gameType'].items()}
    single_season = {'statsSingleSeason_'+ k : v for k, v in data['stats'][0]['splits'][0]['stat'].items()}
    regular_season = {'regularSeasonStatRankings_'+ k : v for k, v in data['stats'][1]['splits'][0]['stat'].items()}
    
    time_stamp = {'timestamp':datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    result = team | game_type | single_season | regular_season | time_stamp

    # перобразуем в pandas DataFrame
    df = pd.DataFrame(result, index=[0])

    # Задаем подключение к бд
    engine = eng(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # загружаем в бд
    try:
        df.to_sql('nhl_stats', con=engine, schema=db_schema, index=False, if_exists='append', method='multi')
    except OperationalError as e:
        print(f'No db connection\nerror:\n{e}')


if __name__ == '__main__':
    sourse_url = 'https://statsapi.web.nhl.com/api/v1/teams/21/stats'
    db_host = 'localhost'
    db_port = 5432
    db_name = 'storage'
    db_user = 'user'
    db_password = 'user'
    db_schema = 'public'

    # получение параметров из переменных окружения
    # sourse_url = os.getenv('API_URL')
    # db_host = os.getenv('DB_HOST')
    # db_port = os.getenv('DB_PORT', default=int)
    # db_name = os.getenv('DB_NAME')
    # db_user = os.getenv('DB_USER')
    # db_password = os.getenv('DB_PASSWORD')
    # db_schema = os.getenv('DB_SCHEMA')

    elt_process(sourse_url, db_host, db_port, db_name, db_user, db_password, db_schema)