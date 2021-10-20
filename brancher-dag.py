from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
import requests

HOST = "https://xxxxxx.se-sandb.a465-9q4k.cloudera.site"
USERNAME = "sunilemanjee"
API_KEY = "xxxxxxx"
PROJECT_NAME = "workflow-orchestration"
jdict = "resources/dic.json"

url = "/".join([HOST, "api/v1/projects", USERNAME, PROJECT_NAME, "files", jdict])

################## CML OPERATOR CODE START #######################
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook
import time


class CMLJobRunOperator(BaseOperator):

    def __init__(
            self,
            project: str,
            job: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.project = project
        self.job = job

    def execute(self, context):
        job_label = '({}/{})'.format(self.project, self.job)

        get_hook = HttpHook(http_conn_id='cml_rest_api', method='GET')
        post_hook = HttpHook(http_conn_id='cml_rest_api', method='POST')
        projects_url = 'api/v2/projects'
        r = get_hook.run(endpoint=projects_url)
        projects = {p['name']: p['id'] for p in r.json()['projects']} if r.ok else None

        if projects and self.project in projects.keys():
            jobs_url = '{}/{}/jobs'.format(projects_url, projects[self.project])
            r = get_hook.run(endpoint=jobs_url)
            jobs = {j['name']: j['id'] for j in r.json()['jobs']} if r.ok else None

            if jobs and self.job in jobs.keys():
                runs_url = '{}/{}/runs'.format(jobs_url, jobs[self.job])
                r = post_hook.run(endpoint=runs_url)
                run = r.json() if r.ok else None

                if run:
                    status = run['status']
                    RUNNING_STATES = ['ENGINE_SCHEDULING', 'ENGINE_STARTING', 'ENGINE_RUNNING']
                    SUCCESS_STATES = ['ENGINE_SUCCEEDED']
                    POLL_INTERVAL = 10
                    while status and status in RUNNING_STATES:
                        run_id_url = '{}/{}'.format(runs_url, run['id'])
                        r = get_hook.run(endpoint=run_id_url)
                        status = r.json()['status'] if r.ok else None
                        time.sleep(POLL_INTERVAL)
                    if status not in SUCCESS_STATES:
                        raise AirflowException('Error while waiting for CML job ({}) to complete'
                                               .format(job_label))
                else:
                    raise AirflowException('Problem triggering CML job ({})'.format(job_label))
            else:
                raise AirflowException('Problem finding the CML job ID ({})'.format(self.job))
        else:
            raise AirflowException('Problem finding the CML project ID ({})'.format(self.project))


################## CML OPERATOR CODE END #######################


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


def deterministic():
    res = requests.get(url, headers={"Content-Type": "application/json"}, auth=(API_KEY, ""))

    if res.json()['dataSet'] > 5:
        branch_name = 'train'
    else:
        branch_name = 'test'
    return branch_name


dataFork = BranchPythonOperator(
    dag=dag,
    task_id='branching',
    python_callable=deterministic
)

fetchSources = CMLJobRunOperator(
    task_id='fetchSources',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

featureEngineering = CMLJobRunOperator(
    task_id='featureEngineering',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

testDataSetTuning = CMLJobRunOperator(
    task_id='testDataSetTuning',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

hyperParamTuning = CMLJobRunOperator(
    task_id='hyperParamTuning',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

fitModel = CMLJobRunOperator(
    task_id='fitModel',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

modelSelection = CMLJobRunOperator(
    task_id='modelSelection',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

evalModel = CMLJobRunOperator(
    task_id='evalModel',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

deployModel = CMLJobRunOperator(
    task_id='deployModel',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

Scoring = CMLJobRunOperator(
    task_id='Scoring',
    project=PROJECT_NAME,
    job='compute-something',
    dag=dag)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> fetchSources >> featureEngineering >> dataFork
dataFork >> testDataSetTuning >> evalModel >> deployModel >> Scoring >> end
dataFork >> hyperParamTuning >> fitModel >> modelSelection >> evalModel >> deployModel >> Scoring >> end
