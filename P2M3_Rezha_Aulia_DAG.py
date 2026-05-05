'''
Milestone 3

Nama : Rezha Aulia
Batch : FTDS-037-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke Elasticsearch.
Dataset yang digunakan adalah Electric Vehicle Population Data dari Washington State Department of Licensing.

'''
import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch

# Function 1 - Fetch dari PostgreSQL
def fetch_from_postgresql():
    '''
    Fungsi ini untuk mengambil data dari PostgreSQL
    Parameters : -
    Return : - , karena data disimpan ke CSV di folder dags
    ex : 
    fetch_from_postgresql()
    '''
    conn = psycopg2.connect(
    dbname='airflow',
    user='airflow',
    password='airflow',
    host='postgres',  # Menggunakan docker
    port='5432'       # port docker
    )

    df = pd.read_sql('SELECT * FROM table_m3', conn)
    df.to_csv('/opt/airflow/dags/P2M3_Rezha_Aulia_data_raw.csv', index=False)

    conn.close()
    print(f'Data berhasil diambil: {df.shape}')

# Function 2 - Data Cleaning
def data_cleaning():
    '''
    Fungsi ini untuk melakukan data cleaning pada dataset
    Proses cleaning :
    - Hapus data duplikat
    - Filter hanya untuk Washington State
    - Meratakan nama kolom (lowercase + underscore + hapus simbol)
    - Handling missing values
    Parameters : -
    Return : - , data disimpan ke CSV
    ex:
    data_cleaning()
    '''
    df = pd.read_csv('/opt/airflow/dags/P2M3_Rezha_Aulia_data_raw.csv')

    # 1. Hapus data duplikat
    print(f'Sebelum hapus duplikat: {df.shape}')
    df.drop_duplicates(inplace=True)
    print(f'Setelah hapus duplikat: {df.shape}')

    # 2. Meratakan nama kolom
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
    print(f'Kolom setelah perataan: {df.columns.tolist()}')

    # 3. Rename kolom
    df.rename(columns={
        'vin_110': 'vin',
        'clean_alternative_fuel_vehicle_cafv_eligibility': 'cafv_eligibility',
        '2020_census_tract': 'census_tract'
    }, inplace=True)

    # 4. Filter untuk data Washington State 
    print(f'Sebelum filter WA: {df.shape}')
    df = df[df['state'] == 'WA']
    print(f'Setelah filter WA: {df.shape}')

    # 5. Handling missing values
    print(f'Missing values sebelum:\n{df.isnull().sum()}')
    # Kolom Model isi 'Unknown'
    df['model'].fillna('Unknown', inplace=True)

    # Legislative District isi dengan Median
    df['legislative_district'].fillna(df['legislative_district'].median(), inplace=True)

    # Vehicle Location isi 'Unknown' karena format koordinat
    df['vehicle_location'].fillna('Unknown', inplace=True)

    print(f'Missing values sesudah:\n{df.isnull().sum()}')
    print(f'Shape final: {df.shape}')

    # Simpan data cleaning
    df.to_csv('/opt/airflow/dags/P2M3_Rezha_Aulia_data_clean.csv', index=False)
    print('Data clean berhasil disimpan')

# Function 3 - Post ke Elasticsearch
def post_to_elasticsearch():
    '''
    Fungsi ini untuk memasukkan data clean ke Elasticsearch
    Parameters : -
    Return : - , data diindex ke Elasticsearch
    ex :
    post_to_elasticsearch()
    '''
    es = Elasticsearch('http://elasticsearch:9200') # service docker
    df = pd.read_csv('/opt/airflow/dags/P2M3_Rezha_Aulia_data_clean.csv')

    for i, row in df.iterrows():
        doc = row.to_dict()
        es.index(index='electric_vehicle', body=doc)

    print(f'Berhasil index {len(df)} dokumen ke Elasticsearch')

# DAG Definition
default_args = {
    'owner' : 'Rezha',
    'start_date' : dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'P2M3_Rezha_Aulia_DAG',
    default_args=default_args,
    schedule_interval='10,20,30 9 * * 6',
    catchup=False
) as dag:
    
    fetch_task = PythonOperator(
        task_id = 'fetch_from_postgresql',
        python_callable=fetch_from_postgresql
    )

    cleaning_task = PythonOperator(
        task_id = 'data_cleaning',
        python_callable=data_cleaning
    )

    post_task = PythonOperator(
        task_id = 'post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    fetch_task >> cleaning_task >> post_task