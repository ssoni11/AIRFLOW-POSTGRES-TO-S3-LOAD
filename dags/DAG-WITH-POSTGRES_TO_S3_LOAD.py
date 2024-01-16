from datetime import datetime,timedelta
import logging, csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args={
    'owner':'sagar',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def postgres_to_s3(ds_nodash):
     hook=PostgresHook(postgres_conn_id='postgres_localhost')
     conn=hook.get_conn()
     cursor=conn.cursor()
     cursor.execute("select * from product where restockingleadtime > 4")
     with open("./config/product_{0}.txt".format(ds_nodash), "w") as f:
        csv_writer=csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
     cursor.close()
     conn.close()
     logging.info(f"Saved product in prdouct_{ds_nodash}.txt")
    #step2
     s3_hook = S3Hook(aws_conn_id="minio_conn")
     s3_hook.load_file(
         filename="./config/product_{0}.txt".format(ds_nodash),
         key="products/{0}.txt".format(ds_nodash),
         bucket_name="airflow",
         replace=True
     )

with DAG(
    dag_id='DAG-WITH-POSTGRES_TO_S3_LOAD',
    default_args=default_args,
    start_date=datetime(2024,1,14),
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1