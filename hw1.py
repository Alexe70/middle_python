import sqlite3
import orjson
from zipfile import ZipFile
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped

from datetime import datetime

#Считываем переменные из конфиг-файла.
with open("config.json", "r") as configFile:
    config = orjson.loads(configFile.read())

#Объявляем классы работы с таблицами.
class Base(DeclarativeBase):
    pass
class okved(Base):
    __tablename__ = 'okved'
 
    key: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    parent_code: Mapped[str]
    section: Mapped[str] 
    name: Mapped[str] 
    comment: Mapped[str] 
class telecom_companies(Base):
    __tablename__ = 'telecom_companies'

    key: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] 
    inn: Mapped[int]
    ogrn: Mapped[int] 
    kpp: Mapped[int] 
    name: Mapped[str]

#Создаем таблицы в БД
engine = create_engine(config['dbConnect'])
Base.metadata.create_all(engine)

print(str(datetime.now().strftime("%H:%M:%S")))

#Перебираем файл с общероссийским классификатом видов экономической деятельности
listInsert = []
with open(config['okvedJson'], "r") as item:
    with Session(engine) as session:
        for record in orjson.loads(item.read()):
            insert = okved(code=record['code'],
                         parent_code=record['parent_code'], 
                         section=record['section'], 
                         name=record['name'],
                         comment=record['comment'])
            listInsert.append(insert)
        session.add_all(listInsert) #И записываем все строки в таблицу "okved"
        session.commit()            #Фиксируем изменения после внесения всех данных, т.к. объем вносимых строк небольшой. 

#Получаем из архива Единого государственного реестр юридических лиц список вложеных файлов
#Каждый файл по очереди читаем в RAM без распаковки на диск. 
with ZipFile(config['archive'], 'r') as zipObj:
    for file in zipObj.namelist():
        listInsert = []
        with Session(engine) as session:
            for record in orjson.loads(zipObj.read(file)):  #Конвертируем json в список словарей и находив в нем компании с 'КодОКВЭД' = 61
                if 'СвОКВЭД' in record['data'].keys():
                    if 'СвОКВЭДОсн' in record['data']['СвОКВЭД'].keys():
                        if record['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'].startswith('61.'):
                            insert = telecom_companies(code=record['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                                                    inn = record['inn'], 
                                                    ogrn = record['ogrn'], 
                                                    kpp = record['kpp'],
                                                    name = record['name'])
                            listInsert.append(insert)
            session.add_all(listInsert)     #И записываем в таблицу "telecom_companies"
            session.commit()                #Фиксируем изменения после обработки каждого файла. 

print(str(datetime.now().strftime("%H:%M:%S")))