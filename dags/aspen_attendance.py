import ast
import base64
import json
import logging
import os
import pandas as pd
import pytz
import requests
import time
import zipfile

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from os.path import expanduser
from pprint import pprint
from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.support.ui import Select

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/aspen/'.format(home)

###################
# Aspen Variables #
###################

BASE_URL = "https://aspen.cps.edu/aspen/logon.do"

ASPEN_USERNAME = Variable.get("aspen_username")
ASPEN_PASSWORD = Variable.get("aspen_password")

VIEW = "school"
SCHOOL = "ascend"
date = '{{ ds_nodash }}'
# file_name = '{}_attendance_{}.mp3'.format(date, SCHOOL)
cwd = os.getcwd()
local_downloads = os.path.join(cwd, '..', 'data', 'aspen')

def _cleanAttendance(attendance_file):
    """

    :param attendance_file:
    :return:
    """

    # Drop empty rows
    attendance_file.dropna(subset=['Student.1'], inplace=True)

    # Chose subset of columns to keep
    attendance_file = attendance_file[
        ['Student', 'Grade', 'Homeroom', 'Member', 'Present', 'Absent', 'Tardy', 'Dismiss']]

    # filter out renments of excel pagination
    attendance_file = attendance_file.loc[attendance_file['Student'] != "CHARTER"]
    attendance_file = attendance_file.loc[attendance_file['Student'] != "Student"]
    attendance_file = attendance_file.loc[~attendance_file['Student'].str.contains("Page [0-9]", regex=True)]

    return (attendance_file)

def my_sleeping_function(minutes):
    """This is a function that will run within the DAG execution"""
    time.sleep(minutes)

# def download_attendance(url, aspen_username, aspen_password, school, download_dir, date):
#     """
#     This function downloads attendance info for one school
#     """
#     table_data = []
#     # ---------HEADLESS BROWSER---------------------------------

    # options = webdriver.ChromeOptions()
    #
    # options.add_argument('--disable-extensions')
    # options.add_argument('--headless')
    # options.add_argument('--disable-gpu')
    # options.add_argument('--no-sandbox')
    #
    # driver = webdriver.Chrome(chrome_options=options)
    #
    # # ---------NAVIGATE TO WEBSITE---------------------------------
    #
    # driver.get(url)
    #
    # time.sleep(3)
    #
    # # fill in username and Password and click submit
    # username = driver.find_element_by_name("username")
    # username.clear()
    # username.send_keys(aspen_username)
    #
    # password = driver.find_element_by_name("password")
    # password.clear()
    # password.send_keys(aspen_password)
    #
    # driver.find_element_by_id("logonButton").click()
    #
    # time.sleep(3)
    #
    # # select view
    # driver.find_element_by_id("contextSchool").click()
    #
    # # select school
    # window_before = driver.window_handles[0]
    #
    # driver.find_element_by_xpath(
    #     '//*[@id="header"]/table[1]/tbody/tr/td[3]/div/table/tbody/tr/td[2]/div/a').click()
    #
    # window_after = driver.window_handles[1]
    # driver.switch_to.window(window_after)
    #
    # time.sleep(2)
    #
    # if school.lower() == "academy":
    #     driver.find_element_by_id("skl01000000769").click()
    #
    # elif school.lower() == "ascend":
    #     driver.find_element_by_id("skl01000000088").click()
    #
    # elif school.lower() == "bloom":
    #     driver.find_element_by_id("skl01000000817").click()
    # elif school.lower() == "one":
    #     driver.find_element_by_id("skl01000000853").click()
    # else:
    #     raise Exception("Incorrect Selection: must choose academy, ascend, bloom, or one")
    #
    # driver.find_element_by_id("okButton").click()
    #
    # driver.switch_to.window(window_before)
    #
    # # select students tab
    # time.sleep(3)
    #
    # driver.find_element_by_id("topTabs")
    #
    # # 'Student'
    # driver.find_element_by_link_text(top_tab_selection.capitalize()).click()
    #
    # time.sleep(3)
    #
    #  # click report menu
    # driver.find_element_by_xpath('//*[@id="reportsMenu"]').click()
    #
    # # switch windows (quick report opens up new window)
    # window_before = driver.window_handles[0]
    #
    # # click Student Membership under report menu
    # driver.find_element_by_xpath('//*[@id="reportsMenu_Option11"]/td[2]').click()
    # window_after = driver.window_handles[1]
    # driver.switch_to.window(window_after)
    #
    # # Format: Select CSV
    # driver.find_element_by_id('format').click()
    # driver.find_element_by_xpath('//*[@id="format"]/option[2]').click()
    #
    # # Students to include: Select all students
    # driver.find_element_by_xpath('// *[ @ id = "tab_0"] / td[2] / select').click()
    # driver.find_element_by_xpath('//*[@id="tab_0"]/td[2]/select/option[2]').click()
    #
    #
    # # click run
    # driver.find_element_by_xpath(
    #     '/html/body/table/tbody/tr[2]/td/form/table/tbody/tr[5]/td/button[1]').click()
    #
    # window_final = driver.window_handles[1]
    # driver.switch_to.window(window_final)
    #
    # time.sleep(3)
    #
    # content = driver.page_source
    #
    # # read in html table to pd.dataframe
    # custom_report = pd.read_html(content,
    #                              flavor='html5lib',
    #                              header=13)[0]
    #
    # custom_report_clean = _cleanAttendance(custom_report)
    #
    # custom_report_clean.to_csv(SAVE_PATH + 'aspen_attendance{}_{}.csv'.format(date, school))

#################################
# Airflow specific DAG set up ##
###############################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 25),
    "email": ["mberrien@kippchicago.org"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=1),
    "provide_context": True
}

dag = DAG(
    "pull_aspen_attendance",
    default_args=default_args,
    schedule_interval=('30 12 * * *'))


start = DummyOperator(task_id = "start_task", dag=dag)

# get_attendance = PythonOperator(task_id = "attendance_pull",
#                                 python_callable=download_attendance,
#                                 op_args = [BASE_URL, ASPEN_USERNAME,
#                                            ASPEN_PASSWORD, SCHOOL,
#                                            local_downloads, date])

t2 = PythonOperator(task_id = "sleepy",
                    python_callable=my_sleeping_function,
                    op_kwargs={'minutes': 1},
                    dag=dag)

# end = DummyOperator(task_id="end_task", dag=dag)

# start >> get_attendance >> end
t2.set_upstream(start)
