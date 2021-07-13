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

etl_dag = DAG( 'INDIA_VACCINE_CSV_UPLOAD',
                default_args=default_arguments,
                schedule_interval =None,
                 )

conn=sf.connect(
    user=Variable.get("USER"),
    password=Variable.get("PSWD"),
    account=Variable.get("ACCOUNT"),
    warehouse=Variable.get("WRH"),
    database=Variable.get("INDIA_DATABASE"),
    schema=Variable.get("INDIA_VACCINATIONS_SCHEMA")
    )
def preprocess_func():
    data = pd.read_csv('/mnt/c/users/sm139/CSV/India_Vaccination/covid_vaccine_statewise.csv')
    data.drop('Transgender(Individuals Vaccinated)', inplace=True, axis=1)
    data.drop('Total Sputnik V Administered', inplace=True, axis=1)
    data.drop('AEFI', inplace=True, axis=1)
    data.drop('18-45 years (Age)', inplace=True, axis=1)
    data.drop('45-60 years (Age)', inplace=True, axis=1)
    data.drop('60+ years (Age)', inplace=True, axis=1)
    data.rename(columns={"Updated On":"Updated_On",
                    "Total Doses Administered":"Total_Doses_Administered",
                    "Total Sessions Conducted":"Total_Sessions_Conducted",
                    "Total Sites":"Total_Sites",
                    "First Dose Administered":"First_Dose_Administered",
                    "Second Dose Administered":"Second_Dose_Administered",
                    "Male(Individuals Vaccinated)":"Male_Individuals_Vaccinated",
                    "Female(Individuals Vaccinated)":"Female_Individuals_Vaccinated",
                    "Total Covaxin Administered":"Total_Covaxin_Administered",
                    "Total CoviShield Administered":"Total_CoviShield_Administered",
                    "Total Individuals Vaccinated":"Total_Individuals_Vaccinated"
                    },inplace=True)
    data['Updated_On'] = pd.to_datetime(data['Updated_On'])
    data['Updated_On'] = data['Updated_On'].dt.strftime('%Y-%m-%d')
    print(data['Updated_On'])
    data.to_csv("/mnt/c/users/sm139/PROCESSED/India_Vaccination/processed_india_covid_vaccination.csv", index=None)

def validation_connection(conn, **kwargs):

    
    
    print('This is my Database : ' +str(conn.database))
    print('\n This my Schema : ' +str(conn.schema))
    
    print('\n Conection SUCCESS !!!! ' )

def carga_stage(conn, **kwargs):

    csv_file='/mnt/c/users/sm139/PROCESSED/India_Vaccination/processed_india_covid_vaccination.csv'
    sql = 'put file://{0} @{1} auto_compress=true'.format(csv_file,Variable.get("INDIA_STAGE"))
    curs=conn.cursor()
    curs.execute(sql)
    print('UPLOAD TO STAGE SUCCESS')

def carga_table(conn, **kwargs):
    sql1=" truncate table if exists india_vaccine_auto"
    curs=conn.cursor()
    curs.execute(sql1)
    sql2 = "copy into {0} from @{1}/processed_india_covid_vaccination.csv.gz FILE_FORMAT=(TYPE=csv field_delimIter=',' skip_header=1) ON_ERROR = 'CONTINUE' ".format(Variable.get("INDIA_VACCINE_TABLE"),Variable.get("INDIA_STAGE"))
    curs=conn.cursor()
    curs.execute(sql2)
    print('UPLOAD TABLE SUCCESS')
    
def create_superset_table(conn, **kwargs):

    sql3= "CREATE OR REPLACE TABLE INDIA_VACCINE_SUPERSET AS(SELECT india_vaccine_auto.*, india_daily_cases.india_codes_population.code,india_daily_cases.india_codes_population.population FROM india_vaccine_auto JOIN india_daily_cases.india_codes_population ON india_vaccine_auto.state=india_codes_population.province_state)"
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
