# The first step os to follow Charlie Bini's lead [here](https://github.com/TEAMSchools/sync_deanslist).
#
# However, we will combine this into one file rather than a module and a scipt file since we will be using Airflow, it will be easier to maintain in a single file.
#
# The first chunk comes from the config file. You'll need the following for the config:
# * `save_path`: where the data pulled from the DL API will be stored locally
# * `api_keys`: dict of DL API keys for each campus
# * `base_url`: string of DeansList base ULR for your schools
# * `end_points`: dict of DL endpoints to hit
import base64
import ast


from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz

import os
from os.path import expanduser

import json
import requests
import logging
import zipfile

from deanslist_api.dl import get_endpoint_with_dates, row_count_branch


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.models import Variable



home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/deanslist/'.format(home)

"""
DeansList variables
"""

BASE_URL = 'https://kippchicago.deanslistsoftware.com'


API_KEYS = Variable.get("dl_api_keys")
API_KEYS = ast.literal_eval(API_KEYS)


"""
Airflow specific DAG set up #
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 8, 20),
    #"email": ["chaid@kippchicago.org"],
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

ep_template = {'sdt' : '{{ ds }}'}

dag = DAG(
    "silo_dl_daily_attendance",
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup = True)

with dag:

    t1 = DummyOperator(task_id = "start_dl")


    endpoint_name = 'daily_attendance'
    get_enpdpoints_task_id = "get_{0}_dl_endpoint".format(endpoint_name)
    branch_task_id = "branch_row_count_{0}_dl".format(endpoint_name)
    file_to_gcs_task_id = "{0}_to_gcs".format(endpoint_name)
    zero_branch_task_id = "{0}_zero_row".format(endpoint_name)


    t2 = PythonOperator(
            task_id = get_enpdpoints_task_id,
            python_callable = get_endpoint_with_dates,
            op_args = [SAVE_PATH, BASE_URL, API_KEYS],
            templates_dict = ep_template
            )

    t_branch = BranchPythonOperator(
        task_id = branch_task_id,
        python_callable = row_count_branch,
        op_args = [get_enpdpoints_task_id, file_to_gcs_task_id, zero_branch_task_id],
        trigger_rule = "all_done"
        )

    t_gcs = FileToGoogleCloudStorageOperator(
         task_id = file_to_gcs_task_id,
         google_cloud_storage_conn_id = 'gcs_silo',
         bucket = "deanslist",
         src =  "{{ task_instance.xcom_pull(task_ids='" + get_enpdpoints_task_id + "', key='dl_file_path' )}}",
         dst = endpoint_name + "/{{ task_instance.xcom_pull(task_ids='" + get_enpdpoints_task_id + "', key='dl_file_name') }}",
         dag = dag
    )

    t_zero_row = DummyOperator(
        task_id =zero_branch_task_id
        )

    t2.set_upstream(t1)
    t2.set_downstream(t_branch)
    t_branch.set_downstream(t_gcs)
    t_branch.set_downstream(t_zero_row)
