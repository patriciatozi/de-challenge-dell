from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import country_converter as coco
import geopandas as gpd
import pandas as pd
import datetime
import psycopg2
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


def treatment_covid_19_vaccinations_data(df):

    cc = coco.CountryConverter()

    df = df[['date', 'location_key', 'new_persons_vaccinated', 'cumulative_persons_vaccinated', 'new_persons_fully_vaccinated', 'cumulative_persons_fully_vaccinated', 'new_vaccine_doses_administered', 'cumulative_vaccine_doses_administered']]


    df['country_code_2_chars'] = df['location_key'].apply(lambda x: str(x).split('_')[0])

    df_vaccinations = df.groupby(['date', 'country_code_2_chars']).sum()   
    df_vaccinations = df_vaccinations.reset_index() 

    df_vaccinations['country_code'] = cc.pandas_convert(series=df_vaccinations['country_code_2_chars'])

    df_vaccinations = df_vaccinations.drop(columns=['country_code_2_chars'])

    df_vaccinations['country_code'] = df_vaccinations['country_code'].str.strip()

    df_vaccinations.rename(columns={'date': 'date_str'}, inplace=True)

    return df_vaccinations


def data_transformation_create_global_table(now):

    sys.stderr = open('logs/log_'+ str(os.path.basename(__file__)) + str(now) + '.txt', "w")

    file_name = os.getcwd() + '/settings/credential_db.json'

    with open(file_name, 'r', encoding='utf-8') as f:
        db_config = json.load(f)

    db_engine = connect_with_db_by_url(db_config)
    conn = db_engine.connect() 


    df_countries = pd.read_sql_table('countries_of_the_world', conn)
    df_covid_19_rates = pd.read_sql_table('covid_19_rates', conn)
    df_covid_19_vaccinations = pd.read_csv(os.getcwd() + '/data_ingestion/temp/vaccinations.csv')


    df_countries.rename(columns={'Country': 'country', 
                                 'Region': 'region',
                                 'Population': 'population',
                                 'Area (sq. mi.)': 'area_in_sqr_mi',
                                 'Pop. Density (per sq. mi.)': 'populational_density_sqr_mi',
                                 'Coastline (coast/area ratio)': 'coastline',
                                 'Net migration': 'net_migration',
                                 'Infant mortality (per 1000 births)': 'infant_mortality_per_1000_births',
                                 'GDP ($ per capita)': 'gpd_per_capita',
                                 'Literacy (%)': 'literacy_percentage',
                                 'Phones (per 1000)': 'phones_per_1000',
                                 'Arable (%)': 'arable_percentage',
                                 'Crops (%)': 'crops_percentage',
                                 'Other (%)': 'other_percentage',
                                 'Climate': 'climate_classification',
                                 'Birthrate': 'birth_rate',
                                 'Deathrate': 'death_rate',
                                 'Agriculture': 'agriculture_rate',
                                 'Industry': 'industry_rate',
                                 'Service': 'service_rate'}, inplace=True)
    
    
    df_countries = df_countries.drop(columns=['population'])
    
    df_countries['country'] = df_countries['country'].str.strip()
    df_covid_19_rates['country'] = df_covid_19_rates['country'].str.strip()


    df_join_1 = pd.merge(df_covid_19_rates, df_countries, on="country", how='left')


    df_covid_19_vaccinations_treated = treatment_covid_19_vaccinations_data(df_covid_19_vaccinations)

    df_join_1['date_str'] = df_join_1['date'].astype('str')
    df_join_1['country_code'] = df_join_1['country_code'].str.strip()


    df_join_2 = pd.merge(df_join_1, df_covid_19_vaccinations_treated, on=["country_code", "date_str"], how="left")


    world_gpd = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    world_gpd["center_point"] = world_gpd['geometry'].centroid
    world_gpd["country_long"] = world_gpd.center_point.map(lambda p: p.x)
    world_gpd["country_lat"] = world_gpd.center_point.map(lambda p: p.y)
    world_gpd = world_gpd.rename(columns={'iso_a3':'country_code'})


    df_join_3 = pd.merge(df_join_2, world_gpd[['country_code', 'country_long', 'country_lat']], on=["country_code"], how="left")

    df_join_3['updated_at'] = now

    df_join_3.drop(columns=['date_str'], inplace=True)

    df_join_3.to_sql('cumulative_number_for_14_days_of_COVID_19_cases_per_100000', con=conn, if_exists='replace', 
            index=False) 
    
    conn = connect_with_db_by_psycopg2(db_config)
    
    cursor = conn.cursor()

    sql1 = '''select * from "cumulative_number_for_14_days_of_COVID_19_cases_per_100000";'''
    cursor.execute(sql1) 
    for i in cursor.fetchall(): 
        print(i) 
    
    conn.commit() 
    conn.close()


data_transformation_create_global_table(datetime.datetime.now())