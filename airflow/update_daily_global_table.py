from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 10, 24),
        'email': ['ptrc.tozi@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

dag = DAG('update_covid_19_rates_table-dag', schedule_interval='@daily', default_args=default_args, catchup=False)

t1 = BashOperator(
    task_id='update_covid_19_rates_table',
    bash_command='python /home/patricia/Documents/GitHub/de-challenge-dell/data_ingestion/daily-run/covid_19_rates.py',
    dag=dag)

t2 = BashOperator(
    task_id='update_covid_19_vaccinations_file',
    bash_command='python /home/patricia/Documents/GitHub/de-challenge-dell/data_ingestion/daily-run/covid_19_vaccinations.py',
    dag=dag)

t3 = BashOperator(
    task_id='update_global_table',
    bash_command='python /home/patricia/Documents/GitHub/de-challenge-dell/data_transformation/daily_run/update_global_table.py',
    dag=dag)


t1 >> t2 >> t3