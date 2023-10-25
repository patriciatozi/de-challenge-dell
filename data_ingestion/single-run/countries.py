from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import psycopg2 
import datetime
import zipfile
import json
import sys
import os


def connect_with_db_by_url(db_config):

    url_object = URL.create(
    "postgresql+psycopg2",
    username=db_config['username'],
    password=db_config['password'],  
    host=db_config['host'],
    database=db_config['database'],
    )   
  
    db_engine = create_engine(url_object) 
    
    return db_engine

def connect_with_db_by_psycopg2(db_config):
    conn = psycopg2.connect("host=" + db_config['host'] + " user=" + db_config['username'] + " password="+ db_config['password'] + " dbname=" + db_config['database']) 
    conn.autocommit = True

    return conn

def data_ingestion_countries_of_the_world(now):

    sys.stderr = open('logs/log_'+ str(os.path.basename(__file__)) + str(now) + '.txt', "w")
        
    file_name = os.getcwd() + '/settings/kaggle.json'

    with open(file_name, 'r', encoding='utf-8') as f:
        kaggle_config = json.load(f)

    os.environ['KAGGLE_USERNAME'] = kaggle_config['username']
    os.environ['KAGGLE_KEY'] = kaggle_config['key']

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files('fernandol/countries-of-the-world', path=".")

    with zipfile.ZipFile('countries-of-the-world.zip', 'r') as zip_ref:
        zip_ref.extractall('data_ingestion/temp')

    df_countries = pd.read_csv('data_ingestion/temp/countries of the world.csv')

    file_name = os.getcwd() + '/settings/credential_db.json'

    with open(file_name, 'r', encoding='utf-8') as f:
        db_config = json.load(f)

    db_engine = connect_with_db_by_url(db_config)

    conn = db_engine.connect() 
    
    df_countries.to_sql('countries_of_the_world', con=conn, if_exists='replace', 
            index=False) 
    
    conn = connect_with_db_by_psycopg2(db_config)
    cursor = conn.cursor() 
    
    sql1 = '''select * from countries_of_the_world;'''
    cursor.execute(sql1) 
    for i in cursor.fetchall(): 
        print(i) 
    
    conn.commit() 
    conn.close()


data_ingestion_countries_of_the_world(datetime.datetime.now())