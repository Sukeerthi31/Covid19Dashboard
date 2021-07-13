import pandas as pd  
import logging
import airflow
from airflow.models import Variable
from airflow import models
from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 
import pandas as pd
import snowflake.connector as sf
import time
import random
import os

default_arguments = {   'owner': 'airflow',
                        'start_date': airflow.utils.dates.days_ago(0),
                        'retries':1 ,
                        'retry_delay':timedelta(minutes=5)}

etl_dag = DAG( 'US_VACCINE_CSV_UPLOAD',
                default_args=default_arguments,
                schedule_interval =None,
                 )

conn=sf.connect(
    user=Variable.get("USER"),
    password=Variable.get("PSWD"),
    account=Variable.get("ACCOUNT"),
    warehouse=Variable.get("WRH"),
    database=Variable.get("US_DATABASE"),
    schema=Variable.get("US_VACCINATIONS_SCHEMA")
    )
def preprocess_func():
    data = pd.read_csv('/mnt/c/users/sm139/CSV/USA_Vaccination/us_state_vaccinations.csv')
    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')
    print(data['date'])
    data.to_csv("/mnt/c/users/sm139/PROCESSED/USA_Vaccination/processed_us_covid_vaccination.csv", index=None)

def validation_connection(conn, **kwargs):

    
    
    print('This is my Database : ' +str(conn.database))
    print('\n This my Schema : ' +str(conn.schema))
    
    print('\n Conection SUCCESS !!!! ' )

def carga_stage(conn, **kwargs):

    csv_file='/mnt/c/users/sm139/PROCESSED/USA_Vaccination/processed_us_covid_vaccination.csv'
    sql = 'put file://{0} @{1} auto_compress=true'.format(csv_file,Variable.get("US_VACCINE_STAGE"))
    curs=conn.cursor()
    curs.execute(sql)
    print('UPLOAD TO STAGE SUCCESS')

def carga_table(conn, **kwargs):
    sql1=" truncate table if exists us_vaccines_auto"
    curs=conn.cursor()
    curs.execute(sql1)
    sql2 = "copy into {0} from @{1}/processed_us_covid_vaccination.csv.gz FILE_FORMAT=(TYPE=csv field_delimIter=',' skip_header=1 field_optionally_enclosed_by = '\042') ON_ERROR = 'CONTINUE' ".format(Variable.get("US_VACCINE_TABLE"),Variable.get("US_VACCINE_STAGE"))
    curs=conn.cursor()
    curs.execute(sql2)
    print('UPLOAD TABLE SUCCESS')
    
def create_superset_table(conn, **kwargs):

    sql3= "CREATE OR REPLACE TABLE US_VACCINE_SUPERSET_AUTO AS(SELECT us_vaccines_auto.*, us_vaccinations.iso_codes.code,us_vaccinations.iso_codes.population FROM us_vaccines_auto JOIN us_vaccinations.iso_codes ON us_vaccines_auto.location=iso_codes.state)"
    curs=conn.cursor()
    curs.execute(sql3)
    print('UPLOAD TO SUPERSET TABLE SUCCESS')
    
preprocess = PythonOperator(task_id='PREPROCESS_FILE', 
                            provide_context=True,    
                            python_callable=preprocess_func,
                            op_kwargs={"conn":conn},     
                            dag=etl_dag )
                            
validation = PythonOperator(task_id='CONNECTION_SUCCESS', 
                            provide_context=True,    
                            python_callable=validation_connection,
                            op_kwargs={"conn":conn},     
                            dag=etl_dag )


# Data Upload  Task 
upload_stage = PythonOperator(task_id='UPLOAD_STAGE',     
                             python_callable=carga_stage,
                             op_kwargs={"conn":conn},     
                             dag=etl_dag )


# Data Upload  Task 
upload_table = PythonOperator(task_id='UPLOAD_TABLE_SNOWFLAKE',     
                             python_callable=carga_table,
                             op_kwargs={"conn":conn},     
                             dag=etl_dag )
                             
upload_superset = PythonOperator(task_id='CREATE_SUPERSET_TABLE',     
                             python_callable=create_superset_table,
                             op_kwargs={"conn":conn},     
                             dag=etl_dag )




preprocess >> validation >> upload_stage >> upload_table >> upload_superset
