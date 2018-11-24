from __future__ import division
import base64

import os
from os.path import expanduser

import json
import requests

from datetime import datetime
import re
import math
import zipfile
import sys
import logging

from datetime import datetime, timedelta


"""
Access Token generation and validation
    - get_access_token()
    - build_auth_headers()
"""
def get_access_token(base_url, credentials_encoded):
    """
    retrieve a new access_token from PowerSchool
        - base_url                  full URL of PS instance (default from CONFIG)
        - credentials_encoded       API credentials (default from CONFIG)
    """
    logging.info('Retrieving new access token')
    access_token_timestamp = datetime.now()
    access_headers = {
            'Authorization': b'Basic ' + credentials_encoded,
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
        }
    access_payload = {'grant_type':'client_credentials'}
    r_access = requests.post('{0}/oauth/access_token/'.format(base_url), headers=access_headers, params=access_payload)

    access_json = r_access.json()
    access_json['timestamp'] = str(access_token_timestamp)
    return access_json


def build_auth_headers(credentials_encoded, state_filepath, base_url, **context):
    """
    check if `access_token` is still valid (obtain a new one if not) then build auth_headers for further API calls and return
        - state                     dict containing saved-state info
        - credentials_encoded       API credentials (default from CONFIG)
    """
    ## parse access_token variables
    task_instance = context['task_instance']
    state = task_instance.xcom_pull('get_state', key='state')

    access_token_saved = state['access_token']
    access_token_timestamp = datetime.strptime(state['timestamp'], '%Y-%m-%d %H:%M:%S.%f') ## TODO: make this better
    access_token_expires = int(state['expires_in'])

    current_timestamp = datetime.now()
    timestamp_diff = current_timestamp - access_token_timestamp
    if timestamp_diff.total_seconds() > access_token_expires:
        logging.info('Access token has expired, refreshing...')
        logging.info('Using encoded credentials: {}'.format(credentials_encoded))
        access_json = get_access_token(base_url, credentials_encoded)
        logging.info(access_json)
        access_token_new = access_json['access_token']

        with open(state_filepath, 'w') as file:
            json.dump(access_json, file)

        access_token = access_token_new
    else:
        logging.info('Access token still valid\n')
        access_token = access_token_saved

    auth_headers = {'Authorization': 'Bearer {0}'.format(access_token),}

    task_instance = context['task_instance']
    task_instance.xcom_push('auth_headers', auth_headers)

    #return auth_headers


def get_state(state_filepath, **context):
    if os.path.isfile(state_filepath):
        with open(state_filepath) as file:
            state = json.load(file)

    else:
        state = get_access_token()
        with open(state_filepath, 'w+') as file:
            json.dump(state, file)

    task_instance = context['task_instance']
    task_instance.xcom_push('state', state)

    #return state


'''
Check/get state and build Auth headers
'''
#state = get_state(STATE_FILEPATH)
#AUTH_HEADERS = build_auth_headers(state, CREDENTIALS_ENCODED)


def try_print(stmnt):
    """
    try print and if it failes raise an exception
    """
    try:
        print(stmnt)
    except ValueError:
        print("Could not convert data to an integer in print statement.")

def get_table_count(table_name, query, headers, base_url, maxpagesize):
    """
    get row and page count from endpoint
        - table_name
        - query
        - headers
        - maxpagesize
    """
    logging.info("Running get_table_count() . . . ")

    #task_instance = context['task_instance']
    #headers = task_instance.xcom_pull('build_auth_headers', key='auth_headers')

    r_count = requests.get('{0}/ws/schema/table/{1}/count?{2}'.format(base_url, table_name, query), headers=headers)
    r_status = r_count.status_code
    if r_status != 200:
         logging.info('Response NOT successful. I got code {} '.format(r_status))
         raise ValueError('Response NOT successful. I got code {} '.format(r_status))
    else:
         logging.info('Response  successful! I got code {} '.format(r_status))

    count_json = r_count.json()
    row_count = count_json['count']

    pages = int(math.ceil(row_count / maxpagesize))

    return row_count, pages


def find_previous_partitions(parameter, stopping_criteria, decrement, identifier, table_name, table_columns, headers):
    """
    find all valid historic partitions on PS using count endpoint
    """
    logging.info("Running find_previous_partitions() . . . ")
    historic_queries = []
    while parameter >= stopping_criteria:
        historic_query = {}
        parameter_new = parameter - decrement
        if parameter < 0:
            probing_query = '{0}=gt={1};{0}=le={2}'.format(identifier, parameter_new, parameter)
        else:
            probing_query = '{0}=ge={1};{0}=lt={2}'.format(identifier, parameter_new, parameter)
        probing_query_formatted = 'q={}&'.format(probing_query)

        row_count, pages = get_table_count(table_name, probing_query_formatted, headers)
        if row_count > 0:
            historic_query = {
                    'table_name': table_name,
                    'query_expression': probing_query,
                    'projection': table_columns
                }
            historic_queries.append(historic_query)
        parameter = parameter_new

    return historic_queries


def get_table_data(table_name, query, pages, table_columns, headers, base_url, maxpagesize):
    """
    get data at specified endpoint
        - table_name
        - query
        - pages
        - table_columns
        - headers
        - base_url
        - maxpagesize
        - headers
    """


    logging.info("Running get_table_data() . . . ")
    table_data = []
    for p in range(pages):
        page_number = p + 1

        #print('\tGetting page number {}'.format(page_number))
        #print("Running TEST MESSAGE . . . ")

        endpoint = '{0}/ws/schema/table/{1}?{2}page={3}&pagesize={4}&projection={5}'.format(base_url, table_name, query, page_number, maxpagesize, table_columns)
        r_data = requests.get(endpoint, headers=headers)

        if r_data.ok:
            data_json = r_data.json()
            records = data_json['record']
            for r in records:
                table_data.append(r['tables'][table_name])
        else:
            logging.info(r_data.text)
            raise Exception(r_data.text)

    return table_data


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

    #os.remove(filepath)

def get_endpoints(endpoint, save_path, base_url, maxpagesize, **context):

    task_instance = context['task_instance']
    auth_headers = task_instance.xcom_pull('build_auth_headers', key='auth_headers')

    if not os.path.isdir(save_path):
        os.mkdir(save_path)

    ## parse endpoint JSON file
    #print("Parse endpoints . . . ")
    #with open(endpoints_path) as file:
    #    endpoints_json = json.load(file)

    ## for each endpoint...
    logging.info("Processing endpoint . . . ")
    #ENDPOINTS = endpoints_json['endpoints']
    #print("Loop through endpoints . . . ")
    #for i, e in enumerate(ENDPOINTS):
    table_start = datetime.now()

    ## parse variables
    table_name = endpoint['table_name']
    table_columns = endpoint['projection']

    save_dir = '{0}{1}'.format(save_path, table_name)

    filename = '{0}.json'.format(table_name)
    query = ''

    ## if there's a query included...
    if 'query_expression' in endpoint.keys():
        ## format query expression and rename file to include query string
        query = 'q={}&'.format(endpoint['query_expression'])
        query_filename = ''.join(endpoint for endpoint in query if (endpoint.isalnum() or endpoint == '-'))
        filename = '{0}_{1}.json'.format(table_name, query_filename)

        ## check if there's already a directory of historical data, and if not...
        if not os.path.isdir(save_dir):
            ## create the directory
            os.mkdir(save_dir)

            ## extract identifiers, operators, and parameters from query (assumes only one identifier used)
            logging.info('{}:  No data found on disk, searching for historical data to backfill...'.format(table_name))
            pattern = r'([\w_]*)(==|=gt=|=ge=|=lt=|=le=)([\d]{4}-[\d]{2}-[\d]{2}|[\d]+);?'
            match = re.search(pattern, query)
            identifier, operator, parameter = match.group(1), match.group(2), match.group(3)

            ## build list of queries that return valid historical data
            ## TODO: there's got to be a better way to do this
            if identifier == 'termid':
                parameter = int(parameter)
                stopping_criteria = (parameter * -1)    ## termids can be negative
                decrement = 100
            if identifier == 'yearid':
                parameter = int(parameter)
                stopping_criteria = 0
                decrement = 1
            if identifier == 'assignmentcategoryassocid':
                parameter = int(parameter)
                stopping_criteria = 0
                decrement = 100000
            if 'date' in identifier:
                parameter = datetime.strptime(parameter, '%Y-%m-%d').date()
                stopping_criteria = datetime.strptime('2000-07-01', '%Y-%m-%d').date()
                decrement = relativedelta(years=1)

            historic_queries = find_previous_partitions(parameter, stopping_criteria, decrement, identifier, table_name, table_columns)

            ## extend ENPOINTS list to include `historic_queries`
            ENDPOINTS[i+1:i+1] = historic_queries

    ## get a row count and number of pages
    logging.info("Getting row count. . . ")
    row_count, pages = get_table_count(table_name, query, auth_headers, base_url, maxpagesize)
    logging.info('GET {0}:  {1} rows, {2} pages {3}'.format(table_name, row_count, pages, query))

    ## download data
    logging.info("download data. . . ")

    table_data = get_table_data(table_name, query, pages, table_columns, auth_headers, base_url, maxpagesize)

    ## save data as JSON file
    logging.info("Saving data. . . ")
    save_file(save_dir, filename, table_data)
    filename = filename.replace('.json','.zip')
    file_path = os.path.join(save_dir, filename)
    #file_path = file_path.strip()

    ## push JSON file to GCS
    #print("Pushing to GCS. . . ")
    #gcs.upload_to_gcs('powerschool', table_name, save_dir, filename)

    table_end = datetime.now()
    table_elapsed = table_end - table_start

    logging.info('Pushing XCom . . .')

    task_instance = context['task_instance']
    task_instance.xcom_push('file_path', file_path)
    task_instance.xcom_push('file_name', filename)

    logging.info('\t{0} sync complete!\tElapsed time = {1}'.format(table_name, str(table_elapsed)))
