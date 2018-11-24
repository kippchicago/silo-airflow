from __future__ import division, unicode_literals

from datetime import datetime, timedelta, date

import os
from os.path import expanduser

import feather
import pandas as pd

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.models import Variable
from airflow.operators.idea_plugin import BigQueryToFeatherOperator, CreateAttendStudentOperator
from airflow.operators.idea_plugin import CreateGroupedAttendance, GroupedADAToGroupedYTDOperator


home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/staging/'.format(home)

todays_date = date.today()

def calc_school_year(date, format="long"):

    date = str(date)
    parsed_date = datetime.strptime(date, '%Y-%m-%d')
    parsed_year = parsed_date.year
    parsed_month = parsed_date.month
    output = parsed_year - (parsed_month<7)
    if format=="long":
        output = u"{0}-{1}".format(output, output+1)

    if format=="short":
        year_1 = re.findall('\d{2}$', "{}".format(output))[0]
        year_2 = str(int(year_1)+1)
        output = "{0}-{1}".format(year_1, year_2)

    if format=="first_year":
       output = output

    if format=="second_year":
       output = output + 1


    return output

def calc_ps_termid(sy, quarter=0):
    termid = (sy-1990)*100+quarter
    return termid

sy = calc_school_year(todays_date, "first_year")
ps_termid=int(calc_ps_termid(sy)/100)


"""
Test Dag for IDEA plugin
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 31),
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


students_qry = """
    SELECT id,
            student_number,
            lastfirst,
            home_room,
            enroll_status
    FROM `kipp-chicago-silo-2.powerschool.students`
    """


att_qry = """
    SELECT schoolid, studentid, att_date AS date, attendance_codeid, yearid
    FROM `kipp-chicago-silo-2.powerschool.attendance`
    WHERE yearid >= {} AND att_mode_code = "ATT_ModeDaily"
    """.format(ps_termid)

att_code_qry = """
    SELECT id,
          yearid,
          schoolid,
          att_code,
          presence_status_cd
    FROM `kipp-chicago-silo-2.powerschool.attendance_code`
    WHERE yearid >= {}
    """.format(ps_termid)


membership_qry = """
    SELECT studentid,
           schoolid,
           calendardate AS date,
           studentmembership AS enrolled,
           grade_level,
          ATT_CalcCntPresentAbsent AS attendance
    FROM `kipp-chicago-silo-2.powerschool.ps_membership_reg`
    WHERE yearid >= {}
    """.format(ps_termid)

terms_qry = """
    SELECT abbreviation,
           yearid,
           name,
           firstday,
           lastday
   FROM `kipp-chicago-silo-2.powerschool.terms`
   WHERE yearid >= {}
""".format(ps_termid)


def get_current_stus_ada(save_path, **context):

    students = feather.read_dataframe("{0}/students.feather".format(save_path))
    attend_student = feather.read_dataframe("{0}/attend_student.feather".format(save_path))

    current_students = students[students.enroll_status == 0].copy()
    current_students = pd.DataFrame(current_students['student_number'])

    attend_student_current = pd.merge(attend_student, current_students, on="student_number", how="inner")

    attend_student_current_grouped = attend_student_current.groupby(['student_number', 'lastfirst', 'grade_level', 'school_abbrev'], as_index=False)

    attend_student_ytd = (attend_student_current_grouped.aggregate({'enrolled':'sum',
                                                                    'present':'sum',
                                                                    'absent': 'sum'
                                                                   })

                         )

    attend_student_ytd = attend_student_ytd.assign(ada = attend_student_ytd.present/attend_student_ytd.enrolled*100)

    attend_student_ytd['ada_rank'] = (attend_student_ytd.groupby(['school_abbrev', 'grade_level'], group_keys=False)['ada']
                                                        .rank("dense", ascending=True))

    attend_student_ytd = attend_student_ytd.sort_values(['school_abbrev', 'grade_level', 'ada_rank'])

    write_path =  "{0}/attend_student_ytd.feather".format(save_path)

    feather.write_dataframe(attend_student_ytd, write_path)

    return write_path



dag = DAG(
    "idea_ops_attendance_dashboard_2018-11-05",
    default_args=default_args,
    schedule_interval='0 6/3 * * *',
    catchup = False)

with dag:

    start_dag = DummyOperator(task_id = "start_idea_attendance")

    get_students = BigQueryToFeatherOperator(task_id= "get_students",
        sql = students_qry,
        destination_file="{0}/students.feather".format(SAVE_PATH))

    get_attendance = BigQueryToFeatherOperator(task_id= "get_attendance",
        sql = att_qry,
        destination_file="{0}/attendance.feather".format(SAVE_PATH))

    get_att_code = BigQueryToFeatherOperator(task_id= "get_att_code",
        sql = att_code_qry,
        destination_file="{0}/att_code.feather".format(SAVE_PATH))

    get_membership = BigQueryToFeatherOperator(task_id= "get_membership",
        sql = membership_qry,
        destination_file="{0}/membership.feather".format(SAVE_PATH))

    get_terms = BigQueryToFeatherOperator(task_id= "get_terms",
        sql = terms_qry,
        destination_file="{0}/terms.feather".format(SAVE_PATH))

    create_att_student = CreateAttendStudentOperator(
                                task_id = "create_att_student",
                                dirpath = SAVE_PATH
                                )

    attend_student_to_gcs = FileToGoogleCloudStorageOperator(
                            task_id = "attend_student_to_gcs",
                            google_cloud_storage_conn_id = 'gcs_silo',
                            bucket = "idea_attendance",
                             src =  "{0}/attend_student.feather".format(SAVE_PATH),
                             dst = "attend_student.feather")
    # by school by grade
    create_group_by_school_grade = CreateGroupedAttendance(
                            task_id = "ada_by_date_school_grade",
                            dirpath = SAVE_PATH,
                            file_name = "attend_date_school_grade.feather",
                            grouping_vars = ['date', 'school_abbrev', 'grade_level']
                            )

    create_ytd_by_school_by_grade = GroupedADAToGroupedYTDOperator(
                            task_id = "create_ytd_by_school_grade",
                            dirpath = SAVE_PATH,
                            in_file_name = "attend_date_school_grade.feather",
                            out_file_name = "ada_weekly_school_grade.feather",
                            grouping_vars = ['date', 'school_abbrev', 'grade_level']
                            )
    attend_school_grade_to_gcs = FileToGoogleCloudStorageOperator(
                            task_id = "attend_school_grade_to_gcs",
                            google_cloud_storage_conn_id = 'gcs_silo',
                            bucket = "idea_attendance",
                             src =  "{0}/attend_date_school_grade.feather".format(SAVE_PATH),
                             dst = "attend_date_school_grade.feather")

    ytd_school_grade_to_gcs = FileToGoogleCloudStorageOperator(
                        task_id = "ytd_school_grade_to_gcs",
                        google_cloud_storage_conn_id = 'gcs_silo',
                        bucket = "idea_attendance",
                         src =  "{0}/ada_weekly_school_grade.feather".format(SAVE_PATH),
                         dst = "ada_weekly_school_grade.feather")

    #by_date_school = ['date', 'school_abbrev']
    create_group_by_school = CreateGroupedAttendance(
                            task_id = "ada_by_date_school",
                            dirpath = SAVE_PATH,
                            file_name = "attend_date_school.feather",
                            grouping_vars = ['date', 'school_abbrev']
                            )

    create_ytd_by_school = GroupedADAToGroupedYTDOperator(
                            task_id = "create_ytd_by_school",
                            dirpath = SAVE_PATH,
                            in_file_name = "attend_date_school.feather",
                            out_file_name = "ada_weekly_school.feather",
                            grouping_vars = ['date', 'school_abbrev']
                            )
    attend_school_to_gcs = FileToGoogleCloudStorageOperator(
                            task_id = "attend_school_to_gcs",
                            google_cloud_storage_conn_id = 'gcs_silo',
                            bucket = "idea_attendance",
                             src =  "{0}/attend_date_school.feather".format(SAVE_PATH),
                             dst = "attend_date_school.feather")

    ytd_school_to_gcs = FileToGoogleCloudStorageOperator(
                        task_id = "ytd_school_to_gcs",
                        google_cloud_storage_conn_id = 'gcs_silo',
                        bucket = "idea_attendance",
                         src =  "{0}/ada_weekly_school.feather".format(SAVE_PATH),
                         dst = "ada_weekly_school.feather")

    #by_date_grade_homeroom = ['date', 'school_abbrev', 'grade_level', 'home_room']
    create_group_by_school_by_hr = CreateGroupedAttendance(
                            task_id = "attend_date_grade_hr",
                            dirpath = SAVE_PATH,
                            file_name = "attend_date_grade_hr.feather",
                            grouping_vars = ['date', 'school_abbrev', 'grade_level', 'home_room']
                            )

    create_ytd_by_school_hr = GroupedADAToGroupedYTDOperator(
                            task_id = "create_ytd_by_school_hr",
                            dirpath = SAVE_PATH,
                            in_file_name = "attend_date_grade_hr.feather",
                            out_file_name = "ada_weekly_grade_hr.feather",
                            grouping_vars = ['date', 'school_abbrev', 'grade_level', 'home_room']
                            )
    attend_school_hr_to_gcs = FileToGoogleCloudStorageOperator(
                            task_id = "attend_school_hr_to_gcs",
                            google_cloud_storage_conn_id = 'gcs_silo',
                            bucket = "idea_attendance",
                             src =  "{0}/attend_date_grade_hr.feather".format(SAVE_PATH),
                             dst = "attend_date_grade_hr.feather")

    ytd_school_hr_to_gcs = FileToGoogleCloudStorageOperator(
                        task_id = "ytd_school_hr_to_gcs",
                        google_cloud_storage_conn_id = 'gcs_silo',
                        bucket = "idea_attendance",
                         src =  "{0}/ada_weekly_grade_hr.feather".format(SAVE_PATH),
                         dst = "ada_weekly_grade_hr.feather")

    # student ada
    create_student_ada = PythonOperator(
            task_id = "create_student_ada",
            python_callable = get_current_stus_ada,
            op_args = [SAVE_PATH]
            )

    student_ada_to_gcs = FileToGoogleCloudStorageOperator(
                        task_id = "student_ada_to_gcs",
                        google_cloud_storage_conn_id = 'gcs_silo',
                        bucket = "idea_attendance",
                         src =  "{0}/attend_student_ytd.feather".format(SAVE_PATH),
                         dst = "attend_student_ytd.feather")

    rm_command = "rm {0}/*.feather".format(SAVE_PATH)

    clean_up = BashOperator(
                    task_id = "clean_up_files",
                    bash_command = rm_command
     )




start_dag.set_downstream([get_students,
                         get_attendance,
                         get_att_code,
                         get_membership,
                         get_terms
                         ])

create_att_student.set_upstream([get_students,
                         get_attendance,
                         get_att_code,
                         get_membership,
                         get_terms])

create_att_student.set_downstream([create_group_by_school_grade,
                                   create_group_by_school,
                                   create_group_by_school_by_hr,
                                   create_student_ada,
                                   attend_student_to_gcs])

# by school by grade
create_group_by_school_grade.set_downstream([attend_school_grade_to_gcs, create_ytd_by_school_by_grade])

create_ytd_by_school_by_grade.set_downstream([ytd_school_grade_to_gcs])

# by school
create_group_by_school.set_downstream([attend_school_to_gcs, create_ytd_by_school])

create_ytd_by_school.set_downstream([ytd_school_to_gcs])

# by school by grade by homeroom
create_group_by_school_by_hr.set_downstream([attend_school_hr_to_gcs, create_ytd_by_school_hr])

create_ytd_by_school_hr.set_downstream([ytd_school_hr_to_gcs])

# Student ada
create_student_ada.set_downstream([student_ada_to_gcs])

clean_up.set_upstream([student_ada_to_gcs,
                       ytd_school_hr_to_gcs,
                       ytd_school_grade_to_gcs,
                       ytd_school_to_gcs,
                       attend_student_to_gcs])
