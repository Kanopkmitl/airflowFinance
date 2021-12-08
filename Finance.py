import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def get_Finance_report_today():
    url = 'https://tradestie.com/api/v1/apps/reddit'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_Finance_report_today()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='3.90.131.157',user='root',passwd='password',db='Finance')
        
        cursor = db.cursor()
        no_of_comments = data['no_of_comments']
        sentiment = data['sentiment']
        sentiment_score = data['sentiment_score']
        ticker = data['ticker']
 

        cursor.execute('INSERT INTO Finance_TB (no_of_comments,sentiment,sentiment_score,ticker)'
                  'VALUES("%s","%s","%s","%s")',
                  (no_of_comments,sentiment,sentiment_score,ticker))
        
        db.commit()
        print("Record inserted successfully into table")
        cursor.close()
                                       
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021,1,1),
    
}
with DAG('Finance_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for exchange_rate report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_Finance_report_today',
        python_callable= get_Finance_report_today
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t1 >> t2 

