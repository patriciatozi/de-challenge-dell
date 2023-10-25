from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import psycopg2 
import requests
import datetime
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


def data_ingestion_covid_19_rates(now):

    sys.stderr = open('logs/log_'+ str(os.path.basename(__file__)) + str(now) + '.txt', "w")

    url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/"

    response = requests.get(url)
    df = pd.read_json(response.text)

    df['date'] = ''
    df['updated_at'] = now

    for index, row in df.iterrows():
        df.loc[index, 'date'] = datetime.date(int(row['year_week'].split('-')[0]),1,1)+relativedelta(weeks=+int(row['year_week'].split('-')[1]))

    df = df.assign(
        weekly_count=df.weekly_count.fillna(0),
        cumulative_count=df.cumulative_count.fillna(0),
    )

    df['weekly_count'] = df['weekly_count'].astype(int)
    df['cumulative_count'] = df['cumulative_count'].astype(int)

    df.drop_duplicates(subset=['country_code', 'date'], inplace=True)

    file_name = os.getcwd() + '/settings/credential_db.json'

    with open(file_name, 'r', encoding='utf-8') as f:
        db_config = json.load(f)

    db_engine = connect_with_db_by_url(db_config)
     
    conn = db_engine.connect() 

    df.to_sql('covid_19_rates', con=conn, if_exists='replace', 
            index=False) 

    conn = connect_with_db_by_psycopg2(db_config)

    cursor = conn.cursor()
    
    sql1 = '''select * from covid_19_rates;'''
    cursor.execute(sql1) 
    for i in cursor.fetchall(): 
        print(i) 
    
    conn.commit() 
    conn.close()


data_ingestion_covid_19_rates(datetime.datetime.now())