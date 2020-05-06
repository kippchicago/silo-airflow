#from __future__ import division, unicode_literals

from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator

WEBCRAWLER_PLUMBER_API_KEY = Variable.get("webcrawler_plumber_api_key")

"""
DAG for scraping Aspen Attendance from https://aspen.cps.edu/
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 6),
    "email": ["mberrien@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
}

dag = DAG(
    "aspen_attendance_2020-05-06",
    default_args=default_args,
    schedule_interval='0 15 * * *',
    catchup = False)

with dag:
    t_start = DummyOperator(task_id = "start_aspen_attendance_pull")

    t_update_aspen_attendance_ascend = SimpleHttpOperator(
                task_id = "pull_ascend_attendance",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_attendance_ascend",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_update_aspen_attendance_academy = SimpleHttpOperator(
                task_id = "pull_academy_attendance",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_attendance_academy",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_update_aspen_attendance_bloom = SimpleHttpOperator(
                task_id = "pull_bloom_attendance",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_attendance_bloom",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_update_aspen_attendance_one = SimpleHttpOperator(
                task_id = "pull_one_attendance",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_attendance_one",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_end = DummyOperator(task_id = "end_aspen_attendance_pull")

    t_start >> t_update_aspen_attendance_ascend >> t_update_aspen_attendance_academy >> t_update_aspen_attendance_bloom >> t_update_aspen_attendance_one >> t_end
