from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
import requests
import random
import string
import json

CMLHOST = "https://ml-xxxxx.se-sandb.a465-9q4k.cloudera.site"
USERNAME = "sunilemanjee"
CML_LEGACY_API_KEY = "xxxx"
CML_PROJECT_NAME = "workflow-orchestration"


TARGET_FILE = "".join([random.choice(string.ascii_uppercase) for _ in range(10)]) + ".txt"

cml_url = "/".join([CMLHOST, "api/v1/projects", USERNAME, CML_PROJECT_NAME, "files", "resources", TARGET_FILE])

default_args = {
    'owner': 'sunilemanjee',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021, 1, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'run_cml_job_dag',
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False
)


def write_to_cml():
    payload = {'username': 'bob', 'email': 'bob@bob.com'}

    res = requests.put(cml_url, auth=(CML_LEGACY_API_KEY, ""), files={"upload": json.dumps(payload)}, verify=False)

    print(res)


run_this = PythonOperator(
    task_id='write_cml_file',
    python_callable=write_to_cml,
    dag=dag,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> run_this >> end
