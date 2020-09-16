# The first step os to follow Charlie Bini's lead [here](https://github.com/TEAMSchools/sync_deanslist).
#
# However, we will combine this into one file rather than a module and a scipt file since we will be
# using Airflow, it will be easier to maintain in a single file.
#
# The first chunk comes from the config file. You'll need the following for the config:
# * `save_path`: where the data pulled from the DL API will be stored locally
# * `api_keys`: dict of DL API keys for each campus
# * `base_url`: string of DeansList base ULR for your schools
# * `end_points`: dict of DL endpoints to hit

import base64
import ast


from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pytz

import os
from os.path import expanduser

import json
import requests
import logging
import zipfile

#from deanslist_api.dl import get_endpoint_with_dates, row_count_branch


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.models import Variable


home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/deanslist/'.format(home)

"""
Functions
"""

def get_table_data(endpoint_url, endpoint_params, api_keys, **context):
    """
    gets data at endpoint
    """
    table_data = []
    for i, k in enumerate(api_keys):

        api_key = k['apikey']
        school_name = k['school']

        payload = {'apikey':api_key}
        payload.update(endpoint_params)


        r = requests.get(endpoint_url, params=payload)

        r_json = r.json()
        r_data = r_json['data']

        r_data_school_added = []
        for j, r in enumerate(r_data):
            r.update({u'school_name' : school_name})
            r_data_school_added.append(r)

        if 'deleted_data' in r_json.keys():
            for d in r_data:
                d['is_deleted'] = 0
            table_data.extend(r_data_school_added)

            r_deleted = r_json['deleted_data']
            for d in r_deleted:
                d['is_deleted'] = 1
            table_data.extend(r_deleted)
        else:
            table_data.extend(r_data_school_added)

    return table_data



#table_data = get_table_data(endpoint_url, endpoint_params, api_keys = API_KEYS)

def save_file(save_dir, filename, data):
    """
    check if save folder exists (create if not) and save data to specified filepath
        - filepath
        - data
    """
    logging.info('\tSaving to... {}'.format(save_dir))
    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)

    filepath = '{0}/{1}'.format(save_dir, filename)
    with open(filepath, 'w+') as outfile:
        json.dump(data, outfile)

    zipfilepath = filepath.replace('.json','.zip')
    with zipfile.ZipFile(zipfilepath, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(filepath)


def get_endpoint(endpoint, save_path, base_url, api_keys, **context):

    task_instance = context['task_instance']

    if not os.path.isdir(save_path):
        os.mkdir(save_path)

    logging.info("Processing endpoint . . . ")
    ep_start = datetime.now()

    ep_name = endpoint['name']
    ep_url = '{0}{1}'.format(base_url, endpoint['endpoint'])

    save_dir = '{0}{1}'.format(save_path, ep_name)

    filename = '{0}.json'.format(ep_name)

    ## if there's a query included...
    ep_params = {}
    if 'params' in endpoint.keys():
        ## format query expression and rename file to include query string
        ep_params = endpoint['params']
        query = str(ep_params)
        query_filename = ''.join(endpoint for endpoint in query if endpoint.isalnum())
        filename = '{0}_{1}.json'.format(ep_name, query_filename)


     ## download data
    logging.info('GET {0} {1}'.format(ep_name, ep_params))
    table_data = get_table_data(ep_url, ep_params, api_keys)
    row_count = len(table_data) or 0
    logging.info('\t{} rows'.format(row_count))

    if row_count is None:
        row_count = 0

    if row_count > 0:
        ## save data as JSON file
        save_file(save_dir, filename, table_data)
        filename = filename.replace('.json','.zip')
        file_path = os.path.join(save_dir, filename)
    else:
        logging.info("0 rows returned so exiting")
        return


    logging.info('Pushing xcom . . .')
    task_instance = context['task_instance']
    task_instance.xcom_push('dl_file_path', file_path)
    task_instance.xcom_push('dl_file_name', filename)
    task_instance.xcom_push('dl_row_count', row_count)


def get_endpoint_with_dates(save_path, base_url, api_keys, templates_dict, **context):

    start_date = templates_dict['sdt']
    end_date = templates_dict['sdt']


    ep = {'endpoint':'/api/v1/daily-attendance',
           'name':'daily-attendance',
           'params':{'sdt': start_date, 'edt': end_date, 'include_iac':'Y'}}


    logging.info('Endpoint is: {}'.format(ep))


    get_endpoint(ep , save_path, base_url, api_keys, **context)


def row_count_branch(pushing_task_id, gcs_task_id, zero_branch_task_id, **context):
    task_instance = context['task_instance']
    row_count = task_instance.xcom_pull(task_ids=pushing_task_id, key = 'dl_row_count') or 0

    logging.info("row count is: {}".format(row_count))
    if row_count > 0:
        return gcs_task_id
    else:
        return zero_branch_task_id


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
    "start_date": datetime(2020, 9, 15),
    "email": ["mberrien@kippchicago.org"],
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

ep_template2 = {'sdt': (date.today() - timedelta(days=2)).strftime('%Y-%m-%d')}


dag = DAG(
    "silo_dl_daily_attendance_seven-day-window2",
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
                templates_dict = ep_template2
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
