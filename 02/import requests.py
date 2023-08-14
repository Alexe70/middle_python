import requests
import logging
import orjson
logging.basicConfig()


logger = logging.getLogger('log')
logger.setLevel(logging.INFO)

with open("/Users/alexe70/airflow_leanring/airflow/dags/config.json", "r") as configFile:
    config = orjson.loads(configFile.read())

url = config['urlAPI']    
list2DB = []     #список с данными для записи в БД
vacancyCount = 0 #счетсик успешно обработаных вакансий   
page = 0         #счетчик страниц со списком вакансий
Error = 0

logger.info('Начинаем обрабатывать вакансии')
while (page <=10):                #В этом цикле передираем страницы со списком вакансий, пока не наберем 300 нужных нам вакансий с данными. На всякий случай не уходим дальше 10-й старницы, что бы не получить вечный цыкл.
    logger.info(f'ищем вакансии на странице {page}. {url}')
        
    vacancyList = []
    url_params = {
        "text": "middle python",
        "search_field": "name",
        "per_page": "50",
        "page": str(page)
        }
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
                
                    skills=result.json().get('key_skills')
                    logger.info('1')
                    for item in skills:
                        key_skillsList.append(item['name'])
                        logger.info('2')
                        key_skills = ', '.join(key_skillsList)
                        logger.info('3')
                    logger.info('4')
                    if key_skills:
                        logger.info('5')
                        #logger.info(f"Найдены навыки  {key_skills}. URL: {vacancyURL}")
                        insert = ((result.json().get('employer'))['name'],
                                        result.json().get('name'), 
                                        result.json().get('description'), 
                                        key_skills)
                        list2DB.append(insert)
                        logger.info('6')
                        logger.info(len(list2DB))
                        vacancyCount += 1              #Повышаем счетчик успешно собраных вакансий
            
            else: 
                logger.info(f"Старница вакансии не загружена. Код ошибки: {result.status_code}. URL: {vacancyURL}")
    page += 1
l = len(list2DB)
print (l)


