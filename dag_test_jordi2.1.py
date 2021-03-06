from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
import logging
import os
import sys
import traceback 

try:

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.utcnow(),
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }

    dag = DAG(
        'jordi_test23', default_args=default_args, schedule_interval=timedelta(minutes=10))


    start = DummyOperator(task_id='run_this_first', dag=dag)

    quay_k8s = KubernetesPodOperator(
            namespace='default',
            name="passing-test",
            image_pull_secrets='azure-registry',
            image='acrmcfdev1.azurecr.io/testingairlfowdags',
            cmds=["python","jordi_test.py"],
            arguments=["print('hello world')"],
            task_id="passing-task",
            get_logs=True,
            dag=dag
        )


    start >> quay_k8s

except Exception as e:

    error_message = {
            "message": "An internal error ocurred"
            ,"error": str(e)
            , "error information" : str(sys.exc_info())
            , "traceback": str(traceback.format_exc())
        }
    logging.info(error_message)