from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime

with DAG(
    dag_id='create_view_global_table-dag',
    start_date=datetime.datetime(2023, 10, 24),
    schedule="@daily",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_view_global_table",
        sql="""
            CREATE OR REPLACE VIEW cumulative_number_for_14_days_of_COVID_19_cases_per_100000_view AS
                SELECT *
                FROM cumulative_number_for_14_days_of_COVID_19_cases_per_100000;
          """,
    )