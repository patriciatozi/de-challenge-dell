from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import country_converter as coco
from sqlalchemy import text
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

    df_join_3 = df_join_3.assign(
        country = df_join_3.country.fillna(''),
        country_code = df_join_3.country_code.fillna(''),
        continent = df_join_3.continent.fillna(''),
        population = df_join_3.population.fillna(0),
        indicator = df_join_3.indicator.fillna(''),
        year_week = df_join_3.year_week.fillna(''),
        source = df_join_3.source.fillna(''),
        note = df_join_3.note.fillna(''),
        weekly_count = df_join_3.weekly_count.fillna(0),
        cumulative_count = df_join_3.cumulative_count.fillna(0),
        rate_14_day = df_join_3.rate_14_day.fillna(0),
        date = df_join_3.date.fillna(''),
        updated_at = df_join_3.updated_at.fillna(''),
        region = df_join_3.region.fillna(''),
        area_in_sqr_mi = df_join_3.area_in_sqr_mi.fillna(0),
        populational_density_sqr_mi = df_join_3.populational_density_sqr_mi.fillna(''),
        coastline = df_join_3.coastline.fillna(''),
        net_migration = df_join_3.net_migration.fillna(''),
        infant_mortality_per_1000_births = df_join_3.infant_mortality_per_1000_births.fillna(''),
        gpd_per_capita = df_join_3.gpd_per_capita.fillna(0),
        literacy_percentage = df_join_3.literacy_percentage.fillna(''),
        phones_per_1000 = df_join_3.phones_per_1000.fillna(''),
        arable_percentage = df_join_3.arable_percentage.fillna(''),
        crops_percentage = df_join_3.crops_percentage.fillna(''),
        other_percentage = df_join_3.other_percentage.fillna(''),
        climate_classification = df_join_3.climate_classification.fillna(''),
        birth_rate = df_join_3.birth_rate.fillna(''),
        death_rate = df_join_3.death_rate.fillna(''),
        agriculture_rate = df_join_3.agriculture_rate.fillna(''),
        industry_rate = df_join_3.industry_rate.fillna(''),
        service_rate = df_join_3.service_rate.fillna(''),
        new_persons_vaccinated = df_join_3.new_persons_vaccinated.fillna(0),
        cumulative_persons_vaccinated = df_join_3.cumulative_persons_vaccinated.fillna(0),
        new_persons_fully_vaccinated = df_join_3.new_persons_fully_vaccinated.fillna(0),
        cumulative_persons_fully_vaccinated = df_join_3.cumulative_persons_fully_vaccinated.fillna(0),
        new_vaccine_doses_administered = df_join_3.new_vaccine_doses_administered.fillna(0),
        cumulative_vaccine_doses_administered = df_join_3.cumulative_vaccine_doses_administered.fillna(0),
        country_long = df_join_3.country_long.fillna(0),
        country_lat = df_join_3.country_lat.fillna(0),
    )
    
    df_join_3.drop_duplicates(subset=['country_code', 'date', 'indicator'], inplace=True)

    query = text(f""" 
                    INSERT INTO public."cumulative_number_for_14_days_of_COVID_19_cases_per_100000" (country,country_code,continent,population,indicator,year_week,source,note,weekly_count,cumulative_count,rate_14_day,date,updated_at,region,area_in_sqr_mi,populational_density_sqr_mi,coastline,net_migration,infant_mortality_per_1000_births,gpd_per_capita,literacy_percentage,phones_per_1000,arable_percentage,crops_percentage,other_percentage,climate_classification,birth_rate,death_rate,agriculture_rate,industry_rate,service_rate,new_persons_vaccinated,cumulative_persons_vaccinated,new_persons_fully_vaccinated,cumulative_persons_fully_vaccinated,new_vaccine_doses_administered,cumulative_vaccine_doses_administered,country_long,country_lat)
                    VALUES {','.join([str(i) for i in list(df_join_3.to_records(index=False))])}
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
                                    updated_at = excluded.updated_at,
                                    region = excluded.region,
                                    area_in_sqr_mi = excluded.area_in_sqr_mi,
                                    populational_density_sqr_mi = excluded.populational_density_sqr_mi,
                                    coastline = excluded.coastline,
                                    net_migration = excluded.net_migration,
                                    infant_mortality_per_1000_births = excluded.infant_mortality_per_1000_births,
                                    gpd_per_capita = excluded.gpd_per_capita,
                                    literacy_percentage = excluded.literacy_percentage,
                                    phones_per_1000 = excluded.phones_per_1000,
                                    arable_percentage = excluded.arable_percentage,
                                    crops_percentage = excluded.crops_percentage,
                                    other_percentage = excluded.other_percentage,
                                    climate_classification = excluded.climate_classification,
                                    birth_rate = excluded.birth_rate,
                                    death_rate = excluded.death_rate,
                                    agriculture_rate = excluded.agriculture_rate,
                                    industry_rate = excluded.industry_rate,
                                    service_rate = excluded.service_rate,
                                    new_persons_vaccinated = excluded.new_persons_vaccinated,
                                    cumulative_persons_vaccinated = excluded.cumulative_persons_vaccinated,
                                    new_persons_fully_vaccinated = excluded.new_persons_fully_vaccinated,
                                    cumulative_persons_fully_vaccinated = excluded.cumulative_persons_fully_vaccinated,
                                    new_vaccine_doses_administered = excluded.new_vaccine_doses_administered,
                                    cumulative_vaccine_doses_administered = excluded.cumulative_vaccine_doses_administered,
                                    country_long = excluded.country_long,
                                    country_lat = excluded.country_lat

            """)

    db_engine.execute(query)


data_transformation_create_global_table(datetime.datetime.now())