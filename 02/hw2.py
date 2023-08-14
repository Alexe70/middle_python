import requests
from bs4 import BeautifulSoup
import lxml
import time
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
import json

with open("config.json", "r") as configFile:
    config = json.loads(configFile.read())


class Base(DeclarativeBase):
    pass

class vacancies(Base):
    __tablename__ = 'vacancies'
 
    key: Mapped[int] = mapped_column(primary_key=True)
    company_name: Mapped[str]
    position: Mapped[str]
    job_description: Mapped[str] 
    key_skills: Mapped[str]

engine = create_engine(config['dbConnect'])
Base.metadata.create_all(engine)

#Функция парсинга страницы со списком вакансий
def parsingVacancyList (url,page):

    url_params = {
    "text": "middle python",
    "search_field": "name",
    "per_page": "20",
    "page": page
    }

    result = requests.get(config['urlSearch'], headers=config['user_agent'], params=url_params)
    if result.status_code == 200:
        soup = BeautifulSoup(result.content.decode(), "html.parser")
        vacancyList = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
        return vacancyList
    else: 
        print(f"Старница списка вакансий не загружена. Код ошибки: {result.status_code}. URL: {url}")
        return None

#Функция парсинга страницы конкретной вакансии  
def parsingVacancy(url):
    tagVacancyList = []
    result = requests.get(url, headers=config['user_agent'])
    if result.status_code == 200:
        try:
            soup = BeautifulSoup(result.content.decode(), "html.parser")
            for tag in soup.find_all('div', attrs={'data-qa': 'bloko-tag bloko-tag_inline skills-element'}):
                tagVacancyList.append(tag.text)
            tagVacancy = ', '.join(tagVacancyList)

            if tagVacancy:
                insert = vacancies(company_name = soup.find('a', attrs={'data-qa': 'vacancy-company-name'}).text,
                         position = soup.find('h1').text, 
                         job_description = soup.find('div', attrs={'data-qa': 'vacancy-description'}).text, 
                         key_skills = tagVacancy)
                return insert
            else:
                return None
        except:
            print ('ошибка получения данных')
            return None
    else: 
        print(f"Старница вакансии не загружена. Код ошибки: {result.status_code}. URL: {url}")
        return None

#Функция получения списка URL вакансий через API    
def apiVacancyList (url,page):
    list = []
    url_params = {
        "text": "middle python",
        "search_field": "name",
        "per_page": "50",
        "page": str(page)
    }
    result = requests.get(url, params=url_params)
    if result.status_code == 200:
        for item in result.json().get('items'):
            list.append(item['url'])
    else: 
        print(f"Старница списка вакансий не загружена. Код ошибки: {result.status_code}. URL: {url}")
    return list 

#Функция получения данных со страницы вакакансии через API
def apiVacancy (url):
    key_skillsList = []
    result = requests.get(url)

    if result.status_code == 200:
        try:
            skills=result.json().get('key_skills')
            for item in skills:
                key_skillsList.append(item['name'])
            key_skills = ', '.join(key_skillsList)

            if key_skills:
                insert = vacancies(company_name = (result.json().get('employer'))['name'],
                                    position = result.json().get('name'), 
                                    job_description = result.json().get('description'), 
                                    key_skills=key_skills)
            return insert
        except: 
            print ('ошибка получения данных')
            return None
    else: 
        print(f"Старница вакансии не загружена. Код ошибки: {result.status_code}. URL: {url}")
        return None
    
#Функция записи данных в БД SQLite3
def writeDB(insert):
    try:
        with Session(engine) as session:
            session.add_all(insert)
            session.commit()
            print('Данные записаны в БД')
    except:
        print ('Ошибка записи данных в БД')

############################################################################
#Начинаем парсить HH в лоб
"""
list2DB = []    #Обнуляем список с данными для записи в БД
vacancyCount = 0 #Обнуляем счетсик успешно обработаных вакансий   
page = 0         #Обнуляем счетчик страниц со списком вакансий

while (vacancyCount <= 100): #В этом цикле передираем страницы со списком вакансий, пока не наберем 100 нужных нам вакансий с данными
    vacancyList = parsingVacancyList (config['urlSearch'],page)     # Через функцию получем список вакансий с одной станицы
    if vacancyList is not None:                                     # Проверяем, что функция вернула список, т.е. в результате обработки не было ошибок
        if len(vacancyList) == 0:                                   # Проверяем, что список не пустой. Если список пустой, мы дошли до последней страницы. Завершаем цыкл while
            break
        for vacancy in vacancyList:                         #Перебираем вакансии из списка
            insert = parsingVacancy(vacancy.attrs['href'])  #Через функцию получаем данные вакансии
            if insert is not None:                          #Проверка, что функция вернула данные
                list2DB.append(insert)                      #Довавляем данные в список, что бы потом записать в БД разом
                vacancyCount += 1                           #Повышаем счетчик успешно собраных вакансий
            time.sleep(5)                                   #Делаем паузу, чтобы не заблочили.
    page += 1                                   

writeDB(list2DB)    #Записываем полученные данные в БД
"""
#################################################################################
#Начинаем получать вакансии через API

list2DB = []    #Обнуляем список с данными для записи в БД
vacancyCount = 0 #Обнуляем счетсик успешно обработаных вакансий   
page = 0         #Обнуляем счетчик страниц со списком вакансий

while (vacancyCount <= 300 or page <=10):                #В этом цикле передираем страницы со списком вакансий, пока не наберем 100 нужных нам вакансий с данными. На всякий случай не уходим дальше 5 старницы, что бы не получить вечный цыкл.
    vacancyList = apiVacancyList(config['urlAPI'],page) #Через функцию получем список вакансий с одной станицы
    if len(vacancyList) != 0:                           #Проверяем, что список не пустой. Если во время обработки страницы произойдет ошибка, функция вернет пустой список.
        for vacancy in vacancyList:             #Перебираем вакансии из списка
            if vacancyCount >= 300:             #Если набрали 100 вакансий, заканчиваем перебор. 
                break
            insert = apiVacancy(vacancy)        #Через функцию получаем данные вакансии
            if insert is not None:              #Проверка, что функция вернула данные
                list2DB.append(insert)          #Довавляем данные в список, что бы потом записать в БД разом
                vacancyCount += 1               #Повышаем счетчик успешно собраных вакансий
    else: 
        break
    page += 1

writeDB(list2DB)    #Записываем полученные данные в БД