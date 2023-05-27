from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import datetime as dt
import psycopg2 as db
import json


href = '/home/ya/Документы/Projects/vkproject/support/'
VALUE = 40

def clean_table():
    
    with open(href + 'config.json', 'r') as file: conf = json.load(file)
    
    co_data = conf['connect']
    
    conn_string = f'dbname={co_data["base"]} host={co_data["host"]} \
                                user={co_data["user"]} password={co_data["password"]}'
    

    conn = db.connect(conn_string)
    cur = conn.cursor()

    query = f'DELETE FROM motorsport \
        WHERE date in (select date from motorsport order by date asc limit {VALUE});'
    
    cur.execute(query)

    conn.commit()
    cur.close()
    conn.close()

deafult_args = {
    'owner': 'ya',
    'start_date':days_ago(0),
    'retries':1,
    'retry_delay': dt.timedelta(minutes=20)
}

dag_clean = DAG(
    "clean_dag",
    default_args=deafult_args,
    schedule_interval='@weekly'
    )

cleaning = PythonOperator(
    task_id = 'query for delete',
    dag=dag_clean,
    python_callable=clean_table
)

#launch
cleaning