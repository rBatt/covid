import os
import datetime
import time
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql.types import *

from covid.data.data import DailyHistNYT


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['battrd@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=600),
}
dag = DAG(
    'covid_data_nyt_county',
    default_args=default_args,
    description='Pull Daily County History from NYT GitHub',
    schedule_interval=timedelta(minutes=1440),
)

def get_spark():
    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'
    spark = SparkSession\
        .builder\
        .appName('covid_data_nyt')\
        .config('spark.driver.host', 'localhost')\
        .config('spark.cores.max', '20')\
        .getOrCreate()
    return spark

def update_data():
    nyt = DailyHistNYT(spark=get_spark())
    nyt.get_df_new()
    nyt.update_table()

# Operators
t1 = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    dag=dag,
)

dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
**Test example documentation**
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

t1