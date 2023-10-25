from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
import pandas as pd
import requests
import datetime
import json
import sys
import os


def connect_with_db_by_url():

    file_name = os.getcwd() + '/settings/credential_db.json'

    with open(file_name, 'r', encoding='utf-8') as f:
        db_config = json.load(f)

    url_object = URL.create(
    "postgresql+psycopg2",
    username=db_config['username'],
    password=db_config['password'],  
    host=db_config['host'],
    database=db_config['database'],
    )   
  
    db_engine = create_engine(url_object) 
    
    return db_engine


def data_ingestion_covid_19_rates(now):

    sys.stderr = open('logs/log_'+ str(os.path.basename(__file__)) + str(now) + '.txt', "w")

    print('ok')

    url = 'https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/'

    response = requests.get(url)

    df = pd.read_json(response.text)

    df['date'] = ''
    df['updated_at'] = str(now)

    for index, row in df.iterrows():
        df.loc[index, 'date'] = datetime.date(int(row['year_week'].split('-')[0]),1,1)+relativedelta(weeks=+int(row['year_week'].split('-')[1]))

    df = df.assign(
        weekly_count=df.weekly_count.fillna(0),
        cumulative_count=df.cumulative_count.fillna(0),
        rate_14_day=df.rate_14_day.fillna(0),
        country=df.country.fillna(''),
        country_code=df.country_code.fillna(''),
        continent=df.continent.fillna(''),
        population=df.population.fillna(0),
        indicator=df.indicator.fillna(''),
        year_week=df.year_week.fillna(''),
        source=df.source.fillna(''),
    )

    df['weekly_count'] = df['weekly_count'].astype(int)
    df['cumulative_count'] = df['cumulative_count'].astype(int)
    df['rate_14_day'] = df['rate_14_day'].astype(float)
    df['date'] = df['date'].apply(lambda x: str(x))
    
    db_engine = connect_with_db_by_url()

    df.drop_duplicates(subset=['country_code', 'date'], inplace=True)

    query = text(f""" 
                    INSERT INTO public.covid_19_rates (country, country_code, continent, population, indicator, year_week, source, note, weekly_count, cumulative_count, rate_14_day, date, updated_at)
                    VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                    ON CONFLICT (country_code, date, indicator) 
                    DO  UPDATE SET country = excluded.country,
                                    country_code = excluded.country_code,
                                    continent = excluded.continent,
                                    population = excluded.population,
                                    indicator = excluded.indicator,
                                    year_week = excluded.year_week,
                                    source = excluded.source,
                                    note = excluded.note,
                                    weekly_count = excluded.weekly_count,
                                    cumulative_count = excluded.cumulative_count,
                                    rate_14_day = excluded.rate_14_day,
                                    date = excluded.date,
                                    updated_at = excluded.updated_at
            """)

    db_engine.execute(query)


data_ingestion_covid_19_rates(datetime.datetime.now())