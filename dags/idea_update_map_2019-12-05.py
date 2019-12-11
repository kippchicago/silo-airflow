#from __future__ import division, unicode_literals

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
DAG for checking NWEA  and then updating IDEA2 NWEA data
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 5),
    "email": ["chaid@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 6, 2014),
}


dag = DAG(
    "idea_update_map_2019-12-05",
    default_args=default_args,
    schedule_interval='0 10 * * *',
    catchup = False)

with dag:
    t_start = DummyOperator(task_id = "start_idea_map_check")

    t_sensor = BigQueryTableModifiedSensor(
                task_id = "check_map_last_modified",
                table_name = "assessmentresults",
                dataset = "nwea_map",
                project_id = "kipp-chicago-silo-2",
                state_file = STATE_PATH,
                soft_fail = True,
                poke_interval = 60 * 10, #(seconds) check every 15 minutes
                timeout =  60 * 60 * 3 # timout after three hours
                )

    t_update_idea = SimpleHttpOperator(
                task_id = "run_map_prep",
                http_conn_id = "idea2_http_api",
                endpoint = "run_idea_map",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(IDEA2_API_KEY)}
    )

    t_start >> t_sensor >> t_update_idea
