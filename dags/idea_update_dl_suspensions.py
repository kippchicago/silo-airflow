from __future__ import division, unicode_literals

from datetime import datetime, timedelta, date

import os
from os.path import expanduser

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator




home = expanduser("~")

#STATE_PATH = '{0}/gcs/data/nwea_assessment_results_last_modified.text'.format(home)
IDEA2_API_KEY = Variable.get('idea2_api_key')


"""
DAG for checking NWEA  and then updating IDEA2 NWEA data
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 23),
    "email": ["chaid@kippchicago.org", "soliva@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 6, 2014),
}


dag = DAG(
    "idea_update_dl_suspensions_2018-12-19",
    default_args=default_args,
    schedule_interval='7 15 * * *',
    catchup = False)

with dag:
    t_start = DummyOperator(task_id = "start_idea_get_suspensions")


    t_update_idea = SimpleHttpOperator(
                task_id = "run_dl_suspensions_prep",
                http_conn_id = "idea2_http_api",
                endpoint = "run_idea_dl_suspensions",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(IDEA2_API_KEY)}
    )

    t_start >> t_update_idea
