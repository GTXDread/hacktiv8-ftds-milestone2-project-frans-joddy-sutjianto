'''
---------------------------------------------------------------------
Milestone 3

Nama: Frans Joddy Sutjianto
Batch: HCK-040
Objektif: File ini digunakan untuk mengatur task yang akan dijadwalkan dan dilakukan oleh Airflow berdasarkan urutan dan waktu yang telah ditentukan
---------------------------------------------------------------------

'''

import pandas as pd
import psycopg2 as pg
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def load_from_postgre():
    '''
    Load data from PostgreSQL using psycopg2 and pandas to convert to csv
    
    * **Input**: None
    * **Output**: File saved in Airflow's dags folder with .csv format
    '''
    # Connect to Postgres
    conn = pg.connect(dbname='airflow', user='airflow', password='airflow', host='postgres', port='5432')
    
    # Execute query
    df = pd.read_sql_query('SELECT * FROM table_m3', conn)
    
    # Save as csv
    df.to_csv('./dags:/opt/airflow/dags/P2M3_frans_joddy_sutjianto_data_raw.csv', index=False)
    
    # Close connection
    conn.close()
    
def clean_data_from_postgre():
    '''
    Clean data from PostgreSQL fetched data with pandas library
    
    * **Input**: None
    * **Output**: File saved with cleaned data in Airflow's dags folder with .csv format
    '''
    # Read from csv
    df = pd.read_csv('./dags:/opt/airflow/dags/P2M3_frans_joddy_sutjianto_data_raw.csv')
    
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    
    # Change column names
    updated_columns = []
    for x in df.columns.to_list():
        x = x.lower()
        x = x.replace(' ', '_')
        updated_columns.append(x)
    df.columns = updated_columns
 
    # Fill NaN values
    df.ffill(inplace=True)
    
    # File saving
    df.to_csv('./dags:/opt/airflow/dags/P2M3_frans_joddy_sutjianto_data_clean.csv', index=False)
    
def load_to_elasticsearch():
    '''
    Load cleaned data to Elasticsearch using bulk helper
    
    * **Input**: None
    * **Output**: None
    '''
    # Initiate Elasticsearch
    es = Elasticsearch("http://elasticsearch:9200")
    
    # Load data from csv
    df = pd.read_csv('./dags:/opt/airflow/dags/P2M3_frans_joddy_sutjianto_data_clean.csv')
    
    # Insert data to Elasticsearch
    actions = [
       {
           "_index": "milestone3",
           "_source": r.to_json(orient='index')
       }
       for i, r in df.iterrows() 
    ]
    helpers.bulk(es, actions)

# Set default argument for Airflow Scheduler
default_args = {
    'owner': 'joddy',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=30),
}
        
# Initiate DAG task with name 'pipeline_data'
with DAG('pipeline_data',
    default_args=default_args,
    schedule_interval='10-30/10 9 * * 6',
) as dag:
    
    # Task definiton and function used
    fetch_postgre = PythonOperator(task_id = 'fetch_postgre',
                              python_callable = load_from_postgre)
    clean_data = PythonOperator(task_id = 'clean_data',
                                  python_callable = clean_data_from_postgre)
    load_elasticsearch = PythonOperator(task_id = 'load_elasticsearch',
                                        python_callable = load_to_elasticsearch)
    
# DAG Task order

fetch_postgre >> clean_data >> load_elasticsearch
        