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

from covid.data.data import BaseData, DailyHistCT


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
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=600),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'covid_data_dhct',
    default_args=default_args,
    description='Pull Daily History from CovidTracker',
    schedule_interval=timedelta(minutes=60),
)

def get_spark():
    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'
    spark = SparkSession\
        .builder\
        .appName('covid_data')\
        .config('spark.driver.host', 'localhost')\
        .config('spark.cores.max', '20')\
        .getOrCreate()
    return spark

def check_api_new_data():
    dhct = DailyHistCT(spark=get_spark())
    return dhct.api_needs_updating

def branch_data(**kwargs):
    ti = kwargs['ti']
    xcom_value = bool(ti.xcom_pull(task_ids='check_api_new_data'))
    if xcom_value:
        return "update_data"
    else:
        return "dummy_done"

def update_data():
    dhct = DailyHistCT(spark=get_spark())
    dhct.get_df_new()
    dhct.update_table()
    dhct.update_api()

# Operators
t1 = PythonOperator(
    task_id='check_api_new_data',
    python_callable=check_api_new_data,
    xcom_push=True,
    dag=dag,
)

t2 = BranchPythonOperator(
    task_id='data_branch'
    , python_callable=branch_data
    , provide_context=True
    , dag=dag
)

t3a = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    dag=dag,
)

t3b = DummyOperator(
    task_id="dummy_done"
    , dag=dag
    , trigger_rule='none_failed_or_skipped'
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



t1 >> t2 >> [t3a, t3b]
t3a >> t3b