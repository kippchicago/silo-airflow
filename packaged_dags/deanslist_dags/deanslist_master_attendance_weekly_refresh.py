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

from deanslist_api.dl import get_master_attendance, row_count_branch


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

def datetime_range(start=None, end=None):
    span = end - start
    for i in range(span.days + 1):
        yield start + timedelta(days=i)

end_date = datetime.today()
start_date = datetime(2019, 8, 19)

execution_dates = list((datetime_range(start=start_date, end=end_date)))

"""
Airflow specific DAG set up #
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 19),
    "email": ["chaid@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 6, 2014),
}


dag = DAG(
    "silo_dl_weekly_master_attendance_2018-08-19",
    default_args=default_args,
    schedule_interval='0 6 * * 0',
    catchup = False)

with dag:

    t1 = DummyOperator(task_id = "start_dl")

    for i, ex_date in enumerate(execution_dates):

        ed = ex_date.strftime('%Y-%m-%d')

        ep_template = {'sdt' : ed}

        endpoint_name = 'master_attendance'

        get_enpdpoints_task_id = "get_{0}_dl_endpoint_{1}".format(endpoint_name, ed)
        branch_task_id = "branch_row_count_{0}_{1}_dl".format(endpoint_name, ed)
        file_to_gcs_task_id = "{0}_{1}_to_gcs".format(endpoint_name, ed)
        zero_branch_task_id = "{0}_{1}_zero_row".format(endpoint_name, ed)


        t2 = PythonOperator(
                task_id = get_enpdpoints_task_id,
                python_callable = get_master_attendance,
                op_args = [SAVE_PATH, BASE_URL, API_KEYS],
                templates_dict = ep_template,
                pool = 'deanslist_pool'
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
