from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import logging
logging.basicConfig()

import orjson

sqlCONN = 'sqlite_custom' # ссылаемся на коннектор

### Этот блок с флагами, что бы специально проускать медленные процессы скачивания, парсинга и поиска вакансий
flagDownloadEGRUL = False
flagParseEGRUL = False
flagGetHHVacancy = False
####

### Читаем конфиг
with open("/Users/alexe70/airflow_leanring/airflow/dags/config.json", "r") as configFile:
    config = orjson.loads(configFile.read())
###

default_args = {
    'owner': 'bokaty',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

#Функция создания нужных таблиц в БД
def create_tables():
    sqlite_hook = SqliteHook(sqlite_conn_id = sqlCONN) 

    telecom_companies = 'CREATE TABLE IF NOT EXISTS telecom_companies (code TEXT,	ogrn INTEGER, kpp INTEGER, name TEXT, inn INTEGER);'
    vacancies = 'CREATE TABLE IF NOT EXISTS vacancies (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT);'

    sqlite_hook.run(telecom_companies,autocommit=True)
    sqlite_hook.run(vacancies,autocommit=True)

#Функция скачивания архива с ЕГРУЛ
def download_egrul():
    logger = logging.getLogger('log')
    logger.setLevel(logging.INFO)

    if (flagParseEGRUL):
        import urllib.request
        try:
            urllib.request.urlretrieve(config["urlEGRUL"], config["archive"])
        except OSError as err:
            logger.error(f"Не удалось скачать файл. Ошибка: {err}")
    else:
        logger.info('Пропускаем этап скачивания архива')

#Функция поиска телеком компаний в архиве с ЕГРУЛ
def parse_egrul():
    
    if (flagParseEGRUL):
        from zipfile import ZipFile
    
        with ZipFile(config['archive'], 'r') as zipObj:
            for file in zipObj.namelist():
                listInsert = []
                for record in orjson.loads(zipObj.read(file)):  #Конвертируем json в список словарей и находив в нем компании с 'КодОКВЭД' = 61
                    if 'СвОКВЭД' in record['data'].keys():
                        if 'СвОКВЭДОсн' in record['data']['СвОКВЭД'].keys():
                            if record['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'].startswith('61.'):
                                insert = (record['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                                          record['inn'], 
                                          record['ogrn'], 
                                          record['kpp'],
                                        record['name'])
                                listInsert.append(insert)
        
        sqlite_hook = SqliteHook(sqlite_conn_id=sqlCONN)
        fields = ['code', 'inn','ogrn','kpp','name']
        sqlite_hook.insert_rows(
                    table='telecom_companies',
                    rows=listInsert,
                    target_fields=fields,
                    )
    else:
        print('Пропускаем этап распаковки архива')

#Функция поиска вакансий на HH
def apiHH():

    if (flagGetHHVacancy):
        import requests

        logger = logging.getLogger('log')
        logger.setLevel(logging.INFO)

        url = config['urlAPI']    
        list2DB = []     #список с данными для записи в БД
        vacancyCount = 0 #счетсик успешно обработаных вакансий   
        page = 0         #счетчик страниц со списком вакансий

        logger.info('Начинаем обрабатывать вакансии')
        while (page <= 10):                #В этом цикле передираем страницы со списком вакансий, пока не наберем 300 нужных нам вакансий с данными. На всякий случай не уходим дальше 10-й старницы, что бы не получить вечный цыкл.
            
            vacancyList = []
            url_params = {
                "text": "middle python",
                "search_field": "name",
                "per_page": "50",
                "page": str(page)
            }
            
            logger.info(f'ищем вакансии на странице {page}. {url}.')
            result = requests.get(url, params=url_params)
            if result.status_code == 200:
                for item in result.json().get('items'):
                    vacancyList.append(item['url'])
            else: 
                logger.info(f"Старница списка вакансий не загружена. Код ошибки: {result.status_code}. URL: {url}")

            logger.info(f'получили {len(vacancyList)} на странице {page}')
            if len(vacancyList) != 0:                      #Проверяем, что список не пустой. Если во время обработки страницы произойдет ошибка, функция вернет пустой список.
                for vacancyURL in vacancyList:             #Перебираем вакансии из списка
                    key_skillsList = []
                    result = requests.get(vacancyURL)
                    if result.status_code == 200:
                        try:
                            skills=result.json().get('key_skills')
                            for item in skills:
                                key_skillsList.append(item['name'])
                                key_skills = ', '.join(key_skillsList)

                            if key_skills:
                                logger.info(f"Найдены навыки  {key_skills}. URL: {vacancyURL}")
                                insert = ((result.json().get('employer'))['name'],
                                            result.json().get('name'), 
                                            result.json().get('description'), 
                                            key_skills)
                                list2DB.append(insert)
                                vacancyCount += 1              #Повышаем счетчик успешно собраных вакансий
                        except: 
                            logger.info('ошибка получения данных')
                    else: 
                        logger.info(f"Старница вакансии не загружена. Код ошибки: {result.status_code}. URL: {vacancyURL}")
            page += 1

        sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_custom')
        fields = ['company_name', 'position','job_description','key_skills']
        sqlite_hook.insert_rows(
                            table='vacancies',
                            rows=list2DB,
                            target_fields=fields,
                            )
    else:
        print('Пропускаем этап поиска вакансий')

#Функция поиска телеком компаний из списка ЕГРУЛ имеющих вакансии на HH
def find_telecom_vacancy():
    import pandas as pd
    
    sqlite_hook = SqliteHook(sqlite_conn_id=sqlCONN) 
    sql_telecom_companies = 'SELECT name FROM telecom_companies;'
    sql_vacancies = 'SELECT company_name FROM vacancies;'

    dfCompany = sqlite_hook.get_pandas_df(sql_telecom_companies)
    dfVacancies = sqlite_hook.get_pandas_df(sql_vacancies)
    
    dfVacancies = dfVacancies.assign(exst=dfVacancies['company_name'].isin(dfCompany['name']).astype(int))
    dfResult = dfVacancies.loc[dfVacancies['exst'] == 1]

    dfResult.to_csv(config['csvAllSkills'],index=False)
        
#Функция нахождения ТОП 10 скилов по вакансиям
def topSkills():
    import pandas as pd
    list = []
    
    sqlite_hook = SqliteHook(sqlite_conn_id=sqlCONN)
    sql = 'SELECT key_skills FROM vacancies;'
    skills = sqlite_hook.get_records(sql)

    for item in skills:
        for i in item[0].split(','):
            list.append(i)
    df = pd.DataFrame(list)
    topSkills = df.value_counts().nlargest(10)
    topSkills.to_csv(config['csvTOPSkills'])
    print(topSkills)
    
with DAG(
    dag_id='a_home_work_3',
    default_args=default_args,
    description='hw03',
    start_date=datetime(2023, 8, 12, 8),
    schedule="@once",
) as dag:
    
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,

    )
    
    download_egrul_task = PythonOperator(
        task_id='download_EGRUL',
        python_callable=download_egrul,

    )

    parse_egrul_task = PythonOperator(
        task_id='parse_EGRUL',
        python_callable=parse_egrul,

    )

    get_HH_vacancy_task = PythonOperator(
        task_id='get_HH_vacancy',
        python_callable=apiHH,

    )

    find_telecom_vacancy_task = PythonOperator(
        task_id='find_telecom_vacancy',
        python_callable=find_telecom_vacancy,

    )

    find_top_skill_task = PythonOperator(
        task_id='find_top_skill',
        python_callable=topSkills,

    )

    create_tables_task >> download_egrul_task
    create_tables_task >> get_HH_vacancy_task
    download_egrul_task >> parse_egrul_task
    get_HH_vacancy_task >> find_telecom_vacancy_task
    parse_egrul_task >> find_telecom_vacancy_task
    find_telecom_vacancy_task >> find_top_skill_task
    