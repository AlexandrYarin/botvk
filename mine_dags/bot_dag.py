import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys

sys.path.append('/home/ya/Документы/Projects/vkproject/')

from parsing import parsing
from query import query
from bot3 import send_content


deafult_args = {
    'owner': 'ya',
    'start_date':days_ago(0),
    'retries':1,
    'retry_delay': dt.timedelta(minutes=20)
}

worker = DAG('botDAG',
          default_args=deafult_args,
          schedule_interval='@hourly')



pars = PythonOperator(task_id='parsing',
                      dag=worker,
                      python_callable=parsing)
    
qu = PythonOperator(task_id='query',
                    dag=worker,
                    python_callable=query)
    
bot_launch = PythonOperator(task_id='bot',
                            dag=worker,
                            python_callable=send_content)
    

pars >> qu >> bot_launch

