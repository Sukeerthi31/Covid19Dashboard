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
import glob

default_arguments = {   'owner': 'airflow',
                        'start_date': airflow.utils.dates.days_ago(0),
                        'retries':1 ,
                        'retry_delay':timedelta(minutes=5)}

etl_dag = DAG( 'GLOBAL_COUNTRIES_CSV_UPLOAD',
                default_args=default_arguments,
                schedule_interval =None,
                 )

conn=sf.connect(
    user=Variable.get("USER"),
    password=Variable.get("PSWD"),
    account=Variable.get("ACCOUNT"),
    warehouse=Variable.get("WRH"),
    database=Variable.get("GLOBAL_DATABASE"),
    schema=Variable.get("GLOBAL_VACCINATIONS_SCHEMA")
    )
    
def merge_files():
    
    os.chdir("/mnt/c/users/sm139/CSV/Global_Vaccination/country_data")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    combined_csv.to_csv( "/mnt/c/users/sm139/CSV/Global_Vaccination/country_data/combined_countries.csv", index=False, encoding='utf-8-sig')
 
def preprocess_func():
    data = pd.read_csv('/mnt/c/users/sm139/CSV/Global_Vaccination/country_data/combined_countries.csv')
 
    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')
    print(data['date'])
    data.to_csv("/mnt/c/users/sm139/PROCESSED/Global_Vaccination/processed_countries.csv", index=None)


def validation_connection(conn, **kwargs):

    
    
    print('This is my Database : ' +str(conn.database))
    print('\n This my Schema : ' +str(conn.schema))
    
    print('\n Conection SUCCESS !!!! ' )
    
    


def carga_stage(conn, **kwargs):

    csv_file='/mnt/c/users/sm139/PROCESSED/Global_Vaccination/processed_countries.csv'
    sql = 'put file://{0} @{1} auto_compress=true'.format(csv_file,Variable.get("GLOBAL_VSTAGE"))
    curs=conn.cursor()
    curs.execute(sql)
    print('UPLOAD TO STAGE SUCCESS')

def carga_table(conn, **kwargs):
    sql2 = "copy into {0} from @{1}/processed_locations.csv.gz FILE_FORMAT=(TYPE=csv field_delimIter=',' skip_header=1 field_optionally_enclosed_by = '\042') ON_ERROR = 'CONTINUE' ".format(Variable.get("GLOBAL_COUNTRIES_TABLE"),Variable.get("GLOBAL_VSTAGE"))
    curs=conn.cursor()
    curs.execute(sql2)
    print('UPLOAD TABLE SUCCESS')

merge_files= PythonOperator(task_id='MERGE_FILES', 
                            provide_context=True,    
                            python_callable=merge_files,
                            op_kwargs={"conn":conn},     
                            dag=etl_dag )

preprocess = PythonOperator(task_id='PREPROCESS_FILE', 
                            provide_context=True,    
                            python_callable=preprocess_func,
                            op_kwargs={"conn":conn},     
                            dag=etl_dag )
# Conection Task
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



merge_files >> preprocess >> validation >> upload_stage >> upload_table



  
