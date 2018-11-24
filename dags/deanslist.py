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

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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


tz = pytz.timezone('US/Central')
today = datetime.now(tz).date()
today_str = today.strftime('%Y-%m-%d')
yesterday_str = today - timedelta(days=1)
yesterday_str = yesterday_str.strftime('%Y-%m-%d')


endpoints = [
        ## FLAT JSON - PARAMETERS ##
        #{'endpoint':'/api/beta/export/get-behavior-data.php',
        # 'name':'behavior',
        # 'params':{'sdt':'2018-08-20', 'edt':yesterday_str, 'UpdatedSince':yesterday_str, 'IncludeDeleted':'Y'}},
        ## FLAT JSON - NO PARAMETERS ##
        {'endpoint':'/api/v1/referrals', 'name':'referrals'},
        #{'endpoint':'/api/beta/export/get-comm-data.php', 'name':'communication'},
        #{'endpoint':'/api/v1/followups', 'name':'followups'},
        #{'endpoint':'/api/beta/export/get-roster-assignments.php', 'name':'roster_assignments'},
        #{'endpoint':'/api/v1/lists', 'name':'lists'},
        #{'endpoint':'/api/v1/rosters', 'name':'rosters_all'},
        {'endpoint':'/api/beta/export/get-users.php', 'name':'users'},
        ## CONTAIN NESTED JSON ##
        {'endpoint':'/api/v1/incidents', 'name':'incidents'},
        {'endpoint':'/api/v1/suspensions', 'name':'suspensions'},
        # ## UNUSED ##
        # {'endpoint':'/api/beta/export/get-homework-data.php', 'name':'homework', 'array_cols':[]},
        # {'endpoint':'/api/v1/students', 'name':'students', 'nested':0},
        # {'endpoint':'/api/v1/daily-attendance',
        #  'name':'daily_attendance',
        #  'params':{'sdt':'2018-08-20', 'edt':yesterday_str, 'include_iac':'Y'}},
        # {'endpoint':'/api/v1/class-attendance', 'name':'class_attendance', 'nested':0},
        # {'endpoint':'/api/v1/terms', 'name':'terms', 'nested':1},
        # {'endpoint':'/api/beta/bank/get-bank-book.php', 'name':'points_bank', 'nested':1},
        # {'endpoint':'/api/v1/lists/{ListID}', 'name':'list_sessions_all', 'nested':1},
        # {'endpoint':'/api/v1/lists/{ListID}/{SessionID}', 'name':'list_sessions_id', 'nested':1},
        # {'endpoint':'/api/v1/lists/{ListID}/{SessionDate}', 'name':'list_sessions_date', 'nested':1},
        # {'endpoint':'/api/v1/rosters/(RosterID)', 'name':'rosters_single', 'nested':1},
    ]



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
    row_count = len(table_data)
    logging.info('\t{} rows'.format(row_count))

    if row_count > 0:
        ## save data as JSON file
        save_file(save_dir, filename, table_data)
        filename = filename.replace('.json','.zip')
        file_path = os.path.join(save_dir, filename)


    logging.info('Pushing xcom . . .')
    task_instance = context['task_instance']
    task_instance.xcom_push('dl_file_path', file_path)
    task_instance.xcom_push('dl_file_name', filename)



"""
Airflow specific DAG set up #
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 9, 12),
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
    "dl_daily_endpoints",
    default_args=default_args,
    schedule_interval='51 12 * * *')

with dag:
    t1 = DummyOperator(task_id = "start_dl", dag=dag)
    #Loop through endpoints
    for i, e in enumerate(endpoints):
        # get endpoint name
        endpoint_name = e['name']
        # get dict
        get_enpdpoints_task_id = "get_{0}_dl_endpoint".format(endpoint_name)
        file_to_gcs_task_id = "{0}_to_gcs".format(endpoint_name)

        if 'params' in e.keys():
            if 'sdt' in e['params']:
                sdt = '{{ execution_date }}'
                e['params']


        t2 = PythonOperator(
                task_id = get_enpdpoints_task_id,
                python_callable = get_endpoint,
                op_args = [e, SAVE_PATH, BASE_URL, API_KEYS],
                )

        t3 = FileToGoogleCloudStorageOperator(
             task_id = file_to_gcs_task_id,
             google_cloud_storage_conn_id = 'gcs_silo',
             bucket = "deanslist",
             src =  "{{ task_instance.xcom_pull(task_ids='" + get_enpdpoints_task_id + "', key='dl_file_path' )}}",
             dst = "TEST/" + endpoint_name + "/{{ task_instance.xcom_pull(task_ids='" + get_enpdpoints_task_id + "', key='dl_file_name') }}",
             dag = dag
        )

        t2.set_upstream(t1)
        t2.set_downstream(t3)
