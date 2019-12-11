# NWEA MAP CDF integration to Silo
#
#
#
# The first chunk comes from the config file. You'll need the following for the config:
# * `SAVE_PATH`: where the data pulled from the DL API will be stored locally
# * `NWEA_API_ENDPOINT`: NWEA endpoint for cdf
# * `NWEA_UID`: uid for nwea user that hosts the CDF (updates daily)
# * `NWEA_PWD`: password for nwea user that hosts the CDF (updates daily)
#from __future__ import division, unicode_literals
import requests
import pandas as pd
import zipfile
import logging
import os
from os.path import expanduser
import io
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from airflow.models import Variable


home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/nwea_map/'.format(home)

"""
NWEA variables
"""

NWEA_API_ENDPOINT = 'https://api.mapnwea.org/services/reporting/dex'
NWEA_UID = Variable.get("nwea_uid")
NWEA_PWD = Variable.get("nwea_password")


"""
Function to grab NWEA CDF zip file, unzip it, inspect term name and rename
files to include term names
"""
def get_nwea_map(endpoint, uid, pwd, **context):
    logging.info('Requesitng CDF from DEX endpoint . . .')
    req = requests.get(endpoint, auth=(uid, pwd))

    if req.status_code == 200:
        logging.info('Status code is 200')
        zip = zipfile.ZipFile(io.BytesIO(req.content))
        zip.extractall(SAVE_PATH)

        students_path = "{0}StudentsBySchool.csv".format(SAVE_PATH)
        term_name = pd.read_csv(students_path, usecols = ['TermName'], nrows=3)

        term_name = term_name['TermName'][0].replace(" ", "_").replace("-", "_")

        for filename in os.listdir(SAVE_PATH):
            src = filename
            split_filename = filename.split(".")
            dst =  "{0}_{1}.{2}".format(split_filename[0], term_name, split_filename[1])
            os.rename("{0}{1}".format(SAVE_PATH, src), "{0}{1}".format(SAVE_PATH, dst))

        logging.info('Pushing xcom . . .')
        task_instance = context['task_instance']
        task_instance.xcom_push('nwea_term_name', term_name)
    else:
        logging.error('Status code is not 200, but rather {}'.format(req.status_code))




"""
Airflow specific DAG set up #
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 5),
    "email": ["chaid@kippchicago.org"],
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
    "silo_nwea_map_daily_2019-12-05",
    default_args=default_args,
    schedule_interval='*/15 9-12 * * *',
    catchup = False)

with dag:

    t_start = DummyOperator(task_id = "start_nwea_map")


    endpoint_name = 'master_attendance'
    get_enpdpoints_task_id = "get_nwea_map_cdf"
    branch_task_id = "branch_row_count_{0}_dl".format(endpoint_name)
    file_to_gcs_task_id = "{0}_to_gcs".format(endpoint_name)
    zero_branch_task_id = "{0}_zero_row".format(endpoint_name)


    t_get_map = PythonOperator(
            task_id = "get_nwea_map_cdf",
            python_callable = get_nwea_map,
            op_args = [NWEA_API_ENDPOINT, NWEA_UID, NWEA_PWD]
            )

    rm_command = "rm {0}/*.csv".format(SAVE_PATH)

    t_clean_up = BashOperator(
                    task_id = "clean_up_files",
                    bash_command = rm_command
     )

    cdf_table_names = ["AssessmentResults",
                  "StudentsBySchool",
                  "ProgramAssignments",
                  "ClassAssignments",
                  "AccommodationAssignment"]

    for table in cdf_table_names:
        file_to_gcs_task_id = "{0}_to_gcs".format(table)

        t_gcs = FileToGoogleCloudStorageOperator(
             task_id = file_to_gcs_task_id,
             google_cloud_storage_conn_id = 'gcs_silo',
             bucket = "nwea_map",
             src = SAVE_PATH + table +"_{{ task_instance.xcom_pull(task_ids='get_nwea_map_cdf', key='nwea_term_name' )}}.csv",
             dst =  table + "/" + table + "_{{ task_instance.xcom_pull(task_ids='get_nwea_map_cdf', key='nwea_term_name') }}" + ".csv",
             dag = dag
        )

        t_gcs.set_upstream(t_get_map)
        t_gcs.set_downstream(t_clean_up)



    t_start >> t_get_map
