from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
import random

default_args = {
    'owner': 'sunilemanjee',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021, 1, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'retail_box_dag',
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False
)


def deterministic():
    var1 = random.randint(1, 2)
    if (var1 % 2) == 0:
        branch_name = 'prep_sale_data'
    else:
        branch_name = 'prep_return_data'
    return branch_name


dataFork = BranchPythonOperator(
    dag=dag,
    task_id='transaction_type',
    python_callable=deterministic
)

prep_sale_data = CDEJobRunOperator(
    task_id='prep_sale_data',
    dag=dag,
    job_name='prepData'
)

prep_return_data = CDEJobRunOperator(
    task_id='prep_return_data',
    dag=dag,
    job_name='prepData'
)

stage_data = CDEJobRunOperator(
    task_id='stage_data',
    dag=dag,
    job_name='stageData'
)

enrich_data = CDEJobRunOperator(
    task_id='enrich_data',
    dag=dag,
    job_name='enrichData'
)

rma_process = CDEJobRunOperator(
    task_id='rma_process',
    dag=dag,
    job_name='loadData'
)

load_data = CDEJobRunOperator(
    task_id='load_data',
    dag=dag,
    job_name='loadData'
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> stage_data >> dataFork
dataFork >> prep_sale_data >> enrich_data >> load_data >> end
dataFork >> prep_return_data >> enrich_data >> rma_process >> load_data >> end
