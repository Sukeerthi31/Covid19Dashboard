import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "airflow", "start_date": airflow.utils.dates.days_ago(0)}

dag = DAG(
    dag_id="CREATE_INDIA_TABLES", default_args=args, schedule_interval=None
)

snowflake_query_one = [
    """CREATE OR REPLACE TABLE INDIA_CASES_TEMPORARY AS( SELECT * FROM GLOBAL_COVID.GLOBAL_DAILY_CASES.GLOBAL_CASES WHERE COUNTRY_REGION='India');""",
    
    ]
snowflake_query_two= [
     """CREATE OR REPLACE TABLE INDIA_LATEST_INFO_TEMPORARY AS

    select t.province_state, t.last_update, t.deaths,t.recovered,t.active,t.confirmed
    
    from india_cases_temporary t
    
    inner join (

    select province_state, max(last_update) as MaxDate

    from india_cases_temporary

    group by province_state

    ) tm on t.province_state = tm.province_state and t.last_update = tm.MaxDate order by province_state;"""
     ]
snowflake_query_three=[
        
    """CREATE OR REPLACE TABLE INDIA_SUPERSET_TEMPORARY AS

    (Select india_latest_info_temporary.province_state,india_latest_info_temporary.last_update,india_latest_info_temporary.deaths,india_latest_info_temporary.recovered,india_latest_info_temporary.active,india_latest_info_temporary.confirmed, 

    india_codes_population.code,india_codes_population.population

    FROM india_latest_info_temporary

    INNER JOIN india_codes_population ON india_latest_info_temporary.province_state=india_codes_population.province_state);"""
    ]


with dag:
    extract_india_from_global = SnowflakeOperator(
        task_id="EXTRACT_INIDA_CASES",
        sql=snowflake_query_one ,
        snowflake_conn_id="snowflake_india_conn",
    )
    extract_latest_india_cases = SnowflakeOperator(
        task_id="EXTRACT_LATEST_INIDA_CASES",
        sql=snowflake_query_two ,
        snowflake_conn_id="snowflake_india_conn",
    )
    merge_with_codes = SnowflakeOperator(
        task_id="MERGE_CODE_POPULATION",
        sql=snowflake_query_three ,
        snowflake_conn_id="snowflake_india_conn",
    )
    


extract_india_from_global >> extract_latest_india_cases >> merge_with_codes
