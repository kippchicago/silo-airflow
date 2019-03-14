from __future__ import division, unicode_literals

from datetime import datetime, timedelta, date

import os
from os.path import expanduser

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

from airflow.operators.idea_plugin import BigQueryTableModifiedSensor

home = expanduser("~")

STATE_PATH = '{0}/gcs/data/nwea_assessment_results_last_modified.text'.format(home)
IDEA2_API_KEY = Variable.get('idea2_api_key')

"""
DAG for updating Illuminate grade review data
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 3, 12),
    "email": ["soliva@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 6, 2014),
}

dag = DAG(
    "idea_update_grade_review_2019-03-13",
    default_args=default_args,
    schedule_interval='0 21 * * *', #can we get FiveTran to update right before this? Currently set to 13
    catchup = False)


with dag:
   
    t_update_idea = SimpleHttpOperator(
                task_id = "run_grade_review_prep",
                http_conn_id = "idea2_http_api",
                endpoint = "run_idea_grade_review",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(IDEA2_API_KEY)}
    )

    t_update_idea