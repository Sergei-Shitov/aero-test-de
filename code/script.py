# раскомментить для получения параметров из переменных окружения
# import os

import requests
from requests.exceptions import ConnectionError

import pandas as pd
import json
from datetime import datetime

from sqlalchemy.engine import create_engine as eng
from sqlalchemy.exc import OperationalError

def elt_process(db_host:str, db_port:int, db_name:str, db_user:str, db_password:str, db_schema:str) -> None:
    '''Getting data from API and storing it to DB'''

    sourse_url = 'https://statsapi.web.nhl.com/api/v1/teams/21/stats'

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
    # информация о команде
    team = data['stats'][0]['splits'][0]['team']
    
    # информация об игре statsSingleSeason = sss
    game = data['stats'][0]['type']['gameType']
    game_type = {'sss_'+ k : v for k, v in game.items()}

    # Информация с показателями по сезону statsSingleSeason = sss
    sin_seas_data = data['stats'][0]['splits'][0]['stat']
    single_season = {'sss_'+ k : v for k, v in sin_seas_data.items()}

    # Информация с местами regularSeasonStatRankings = rssr
    reg_seas_data = data['stats'][1]['splits'][0]['stat']
    regular_season = {'rssr_'+ k : v for k, v in reg_seas_data.items()}
    
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

    # задаем значения параметров для подключения БД
    db_host = 'localhost'
    db_port = 5432
    db_name = 'storage'
    db_user = 'user'
    db_password = 'user'
    db_schema = 'public'

    # получение параметров из переменных окружения
    #
    # db_host = os.getenv('DB_HOST')
    # db_port = os.getenv('DB_PORT', default=int)
    # db_name = os.getenv('DB_NAME')
    # db_user = os.getenv('DB_USER')
    # db_password = os.getenv('DB_PASSWORD')
    # db_schema = os.getenv('DB_SCHEMA')

    elt_process(db_host, db_port, db_name, db_user, db_password, db_schema)