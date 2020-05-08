from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator

WEBCRAWLER_PLUMBER_API_KEY = Variable.get("webcrawler_plumber_api_key")

"""
DAG for scraping Aspen Student Information from https://aspen.cps.edu/
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 2),
    "email": ["mberrien@kippchicago.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
}

dag = DAG(
    "aspen_student_info_2020-05-08",
    default_args=default_args,
    schedule_interval='0 * * * 0',
    catchup = False)

with dag:
    t_start = DummyOperator(task_id = "start_aspen_student_info_pull")

    t_pull_aspen_student_info_ascend = SimpleHttpOperator(
                task_id = "pull_ascend_student_info",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_student_info_ascend",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_pull_aspen_student_info_academy = SimpleHttpOperator(
                task_id = "pull_academy_student_info",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_student_info_academy",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_pull_aspen_student_info_bloom = SimpleHttpOperator(
                task_id = "pull_bloom_student_info",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_student_info_bloom",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_pull_aspen_student_info_one = SimpleHttpOperator(
                task_id = "pull_one_student_info",
                http_conn_id = "webcrawler_plumber_http_api",
                endpoint = "aspen_student_info_one",
                method = 'GET',
                headers = {'Authorization': 'Key {0}'.format(WEBCRAWLER_PLUMBER_API_KEY)}
    )

    t_end = DummyOperator(task_id = "end_aspen_student_info_pull")

    t_start >> t_pull_aspen_student_info_ascend >> t_end
    t_start >> t_pull_aspen_student_info_academy >> t_end
    t_start >> t_pull_aspen_student_info_bloom >> t_end
    t_start >> t_pull_aspen_student_info_one >> t_end
