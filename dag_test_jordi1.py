from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s

YESTERDAY = datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': YESTERDAY,
    'email': ['j.crespoguzman@pharmmacess.org,m.mozena@Pharmaccess.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'jordi_test1', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='run_this_first', dag=dag)

quay_k8s = KubernetesPodOperator(
        namespace='default',
        image='marcelotest',
        image_pull_secrets=[k8s.V1LocalObjectReference('azure-registry')],
        cmds=["python", "main.py"],
        arguments=["echo", "10", "echo pwd"],
        labels={"foo": "bar"},
        name="marcelotest",
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="task-two",
        get_logs=True,
    )

quay_k8s.set_upstream(start)

