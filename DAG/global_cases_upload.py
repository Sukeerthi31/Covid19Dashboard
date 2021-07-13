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


etl_dag = DAG( 'GLOBAL_CASES_UPLOAD',
                default_args=default_arguments,
                schedule_interval =None,
                 )


conn=sf.connect(
    user=Variable.get("USER"),
    password=Variable.get("PSWD"),
    account=Variable.get("ACCOUNT"),
    warehouse=Variable.get("WRH"),
    database=Variable.get("GLOBAL_DATABASE"),
    schema=Variable.get("GLOBAL_DAILY_CASES_SCHEMA")
    )

def validation_connection(conn, **kwargs):

    
    
    print('This is my Database : ' +str(conn.database))
    print('\n This my Schema : ' +str(conn.schema))
    
    print('\n Conection SUCCESS !!!! ' )
    
    


def carga_stage(conn, **kwargs):

    csv_file="/mnt/c/users/sm139/CSV/Global_Cases/*.csv"
    sql = 'put file://{0} @{1} auto_compress=true'.format(csv_file,Variable.get("GLOBAL_STAGE"))
    curs=conn.cursor()
    curs.execute(sql)
    print('UPLOAD TO STAGE SUCCESS')

def carga_tabla(conn, **kwargs):
    sql2 = "copy into {0} from @{1}/ FILE_FORMAT=(format_name=test) ON_ERROR = 'CONTINUE' ".format(Variable.get("GLOBAL_DAILY_CASES_TABLE"),Variable.get("GLOBAL_STAGE"))
    curs=conn.cursor()
    curs.execute(sql2)
    print('UPLOAD TABLE SUCCESS')  
    
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
upload_tabla = PythonOperator(task_id='UPLOAD_TABLE_SNOWFLAKE',     
                             python_callable=carga_tabla,
                             op_kwargs={"conn":conn},     
                             dag=etl_dag )



validation >> upload_stage >> upload_tabla

