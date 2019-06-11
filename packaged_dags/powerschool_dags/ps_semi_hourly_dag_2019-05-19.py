from __future__ import division
import base64

from powerschool_api.ps import get_state, build_auth_headers, get_endpoints

import os
from os.path import expanduser

import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import logging

home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/powerschool/'.format(home)
BASE_URL = 'https://kippchicago.powerschool.com'
MAXPAGESIZE = 1000
STATE_FILEPATH = '{0}/gcs/data/'.format(home) + 'state.json'


client_id = Variable.get("ps_client_id")
client_secret = Variable.get("ps_client_secret")
credentials_concat = '{0}:{1}'.format(client_id, client_secret)
CREDENTIALS_ENCODED = base64.b64encode(credentials_concat.encode('utf-8'))

endpoints = [{"table_name":"attendance","query_expression":"yearid==28","projection":"dcid,id,attendance_codeid,calendar_dayid,schoolid,yearid,studentid,ccid,periodid,parent_attendanceid,att_mode_code,att_comment,att_interval,prog_crse_type,lock_teacher_yn,lock_reporting_yn,transaction_type,total_minutes,att_date,ada_value_code,ada_value_time,adm_value,programid,att_flags,whomodifiedid,whomodifiedtype,ip_address"}]


#################################
# Airflow specific DAG set up ##
###############################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 19),
    "email": ["chaid@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
    "provide_context": True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    "silo_ps_semi_hourly_endpoints_2019-05-20",
    default_args=default_args,
    schedule_interval='0 0/4 * * *'
)

t1 = PythonOperator(task_id = "get_state",
                    python_callable = get_state,
                    op_args = [STATE_FILEPATH],
                    dag = dag)

t2 = PythonOperator(task_id = "build_auth_headers",
                    python_callable = build_auth_headers,
                    op_args = [CREDENTIALS_ENCODED, STATE_FILEPATH, BASE_URL],
                    dag = dag)

#with open(ENDPOINT_PATH) as file:
#    endpoints_json = json.load(file)


logging.info("Loop through endpoints . . . ")
for i, e in enumerate(endpoints):

    # get endpoint name
    endpoint_name = e['table_name']
    # get dict
    get_enpdpoints_task_id = "get_{0}_endpoint".format(endpoint_name)
    file_to_gcs_task_id = "{0}_to_gcs".format(endpoint_name)
    logging.info("Getting endoint: {0}".format(endpoint_name))
    t3 = PythonOperator(
            task_id = get_enpdpoints_task_id,
            python_callable = get_endpoints,
            op_args = [e, SAVE_PATH, BASE_URL, MAXPAGESIZE],
            dag = dag,
            execution_timeout=timedelta(hours=3)
            )

    logging.info("Sending endoint {0} to GCS".format(endpoint_name))
    t4 = FileToGoogleCloudStorageOperator(
            task_id = file_to_gcs_task_id,
            google_cloud_storage_conn_id = 'gcs_silo',
            bucket = "ps_mirror", #"{{var.value.gcs_ps_mirror}}",
            src =  "{{ task_instance.xcom_pull(task_ids='" + get_enpdpoints_task_id + "', key='file_path' )}}",
            dst = "powerschool/" + endpoint_name + "/{{ task_instance.xcom_pull(task_ids='" + get_enpdpoints_task_id + "', key='file_name') }}",
            dag = dag
            )

    t3.set_upstream(t2)
    t3.set_downstream(t4)

t1 >> t2
