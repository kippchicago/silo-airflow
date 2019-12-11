from __future__ import division, unicode_literals #you need the unicode_literals to deal with string type problems with Feather

import pandas as pd
from pandas_gbq import read_gbq
import numpy as np
import os
import datetime
import pytz
import json


import feather
import logging

import re
import ast

from airflow.models import BaseOperator, Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
#from airflow.operators.sensors import BaseSensorOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class BigQueryToFeatherOperator(BaseOperator):
    """
    BigQuery query output to local feather file

    :param sql:                 SQL to execute
    :type sql:                  string
    :param bigquery_conn_id:    BigQuery connection id
    :type bigquery_conn_id:     string
    :param use_legacy_sql:      Whether to use legacy SQL (true) or standard SQL (false)
    :type use_legacy_sql:       bool
    :param destination_file:    Path to save the feather file
    :type destination_file:     string

    """

    template_fields = ('sql',)
    ui_color = '#60A2D7'


    @apply_defaults
    def __init__(self,
                 sql,
                 destination_file,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 use_legacy_sql=False,
                 *args,
                 **kwargs):
                 super(BigQueryToFeatherOperator, self).__init__(*args, **kwargs)

                 #self.task_id = task_id
                 self.sql = sql
                 self.bigquery_conn_id = bigquery_conn_id
                 self.delegate_to = delegate_to
                 self.destination_file = destination_file
                 self.use_legacy_sql = use_legacy_sql
                 self.http_error = None

    def execute(self, context):
        dest = self.destination_file
        sql = self.sql
        logging.info("Connecting to Big Query")
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, delegate_to=self.delegate_to)
        bq_conn = bq_hook.get_connection(self.bigquery_conn_id)
        bq_conn_extra_json = bq_conn.extra
        bq_conn_extra = json.loads(bq_conn_extra_json)
        service_dict = bq_conn_extra['extra__google_cloud_platform__keyfile_dict']

        logging.info("Getting table from BQ with SQL:/n{0}".format(sql))
        df = read_gbq(sql, dialect='standard', private_key = service_dict)
        logging.info("Got table!")

        #logging.info('\tSaving to... {}'.format(save_dir))
        #if not os.path.isdir(save_dir):
        #    os.mkdir(save_dir)
        logging.info("Writing table to disk in feather format")
        feather.write_dataframe(df, dest)

        logging.info("Table written to {0}".format(dest))
        return df.info()

class CreateAttendStudentOperator(BaseOperator):
    """
    Use local PS staging files to create an Student attendance file

    :param dirpath: Path to PS staging files
    :type filepath: string
    """

    template_fields = ('dirpath',)
    ui_color = '#FEBC11'

    @apply_defaults
    def __init__(self,
                 dirpath,
                 *args,
                 **kwargs):
                 super(CreateAttendStudentOperator, self).__init__(*args, **kwargs)

                 #self.task_id = task_id
                 self.dirpath = dirpath

    def execute(self, context):
        path = self.dirpath

        logging.info("Reading attendance.feather")
        attendance = feather.read_dataframe("{0}/attendance.feather".format(path))

        logging.info("Reading att_code.feather")
        att_code = feather.read_dataframe("{0}/att_code.feather".format(path))





        logging.info("Reading membership.feather")
        membership = feather.read_dataframe("{0}/membership.feather".format(path))


        logging.info("Getting dates less then or equal to today from membership")
        today = datetime.datetime.now(pytz.utc)


        membership = membership[membership.date <= today]

        logging.info("Reading students.feather")
        students = feather.read_dataframe("{0}/students.feather".format(path))

        logging.info("All tables read into data.frames")

        logging.info("Getting schools schools_dict airflow variable")
        schools_dict = Variable.get("schools_dict")
        schools_dict = ast.literal_eval(schools_dict)
        schools = pd.DataFrame(schools_dict)
        logging.info("Schools cast as data.frame")



        logging.info("Merging attendance and att_code")
        attendance_2 = (pd.merge(attendance,
                         att_code,
                         left_on=['attendance_codeid', 'yearid', 'schoolid'],
                         right_on = ['id', 'yearid', 'schoolid'],
                         how = 'inner')
                .drop(columns = "id")
               )

        logging.info("Joining membership to attendance_2")
        member_att = pd.merge(membership,
                              attendance_2[['studentid', 'date', 'att_code', 'presence_status_cd']],
                              on=['studentid', 'date'],
                              how='left')


        logging.info("Processing member_att")

        # fix the fivetran turning T's into True everywhere
        member_att['att_code'] = np.where(member_att.att_code == 'true', 'T', member_att.att_code)

        member_att['enrolled'] = 1

        member_att['enrolled'] = np.where(np.logical_and(member_att.att_code == 'D',
                                                        ~pd.isna(member_att.att_code)),
                                          0,
                                          member_att.enrolled)

        member_att['present'] = np.where(np.logical_or(pd.isna(member_att.att_code), member_att.att_code=="") , 1, 0)
        member_att['present'] = np.where(member_att.att_code.isin(['A', 'S']), 0, member_att.present)
        member_att['present'] = np.where(member_att.att_code=='H', 0.5, member_att.present)
        member_att['present'] = np.where(member_att.att_code.isin(["T", "E","I", "L"]), 1, member_att.present)
        member_att['present'] = np.where(pd.isna(member_att.present), 1, member_att.present)
        member_att['absent'] = (1-member_att.present)*member_att.enrolled

        students_2 = (students[['id', 'student_number', 'lastfirst', 'home_room']]
                        .rename({'id': 'studentid'}, axis = 'columns'))

        member_att_stu = pd.merge(member_att, students_2, on='studentid', how = 'left')

        logging.info("Peparing output file")
        attend_student = pd.merge(member_att_stu, schools, on='schoolid', how = 'inner')

        attend_student = attend_student[['studentid',
                                         'student_number',
                                         'lastfirst',
                                         'grade_level',
                                         'schoolid',
                                         'school_name',
                                         'school_abbrev',
                                         'home_room',
                                         'date',
                                         'att_code',
                                         'enrolled',
                                         'present',
                                         'absent']]

        write_path = "{0}/attend_student.feather".format(path)
        logging.info("Writing to '{0}'".format(write_path))
        feather.write_dataframe(attend_student, write_path)

        return attend_student.info()


class CreateGroupedAttendance(BaseOperator):
    """
    Use student attendance file to greate grouped attendance files (slices)

    :param dirpath: Path to PS staging files
    :type dirpath: string
    :param file_name: File name to save to
    :type file_name: string
    :param grouping_vars: variables to group by
    :type grouping_vars: dict
    """

    template_fields = ('dirpath',
                      'file_name',)
    ui_color = '#FEBC11'

    @apply_defaults
    def __init__(self,
                 dirpath,
                 file_name,
                 grouping_vars,
                 *args,
                 **kwargs):
                 super(CreateGroupedAttendance, self).__init__(*args, **kwargs)

                 self.dirpath = dirpath
                 self.file_name = file_name
                 self.grouping_vars = grouping_vars


    def get_quarter(self, candidate_date):
        terms = feather.read_dataframe("{0}/terms.feather".format(self.dirpath))

        terms_dedupe = terms.drop_duplicates().copy()

        terms_dedupe_q = terms_dedupe[terms_dedupe['abbreviation'].str.contains("Q")]

        candidate_date = pd.Timestamp(candidate_date)
        quarter = terms_dedupe_q[((terms_dedupe_q.lastday >= candidate_date) & (terms_dedupe_q.firstday <= candidate_date))]

        return quarter.iloc[0]["abbreviation"]

    def prep_att_tables(self, df):
        output = (df.assign(pct_absent = df.absent/df.enrolled,
                            pct_present = df.present/df.enrolled)
                    .assign(pct_present_gte95 = lambda x: x.pct_present >= .95,
                            pct_present_gte96 = lambda x: x.pct_present >= .96,
                            pct_present_gte97 = lambda x: x.pct_present >= .97,
                            pct_present_gte98 = lambda x: x.pct_present >= .98,
                           week_in_year = lambda x: x.date.dt.week
                           )
                 )

        output['week_of_date'] = output.date.dt.to_period("W").apply(lambda x: x.start_time)

        output['week_in_sy'] = (output.groupby('school_abbrev', group_keys = False)
                                      .apply(lambda x: ((x.week_of_date - min(x.week_of_date)).dt.days/7)+1)
                                      .astype(int)
                               )
        output["week_of_date_short_label"] = output.week_of_date.dt.strftime('%b %d')

        unique_weeks = output.sort_values('week_in_sy').week_of_date_short_label.unique()

        output["week_of_date_short_label"] = (output.week_of_date_short_label
                                                    .astype(pd.api.types.CategoricalDtype(unique_weeks,
                                                                                          ordered = True
                                                                                         )
                                                           )
                                             )

        output['quarter'] = output.date.apply(lambda x: self.get_quarter(candidate_date=x))


        return output


    def grouped_attendance(self, grouping, attend_student):
        output = (attend_student
                   .groupby(grouping,
                            as_index=False)
                   .aggregate({'enrolled':'sum',
                               'present':'sum',
                               'absent': 'sum'
                              })
                  )

        output = self.prep_att_tables(output)

        return output




    def execute(self, context):
        logging.info("Reading in attend_student.feather")
        attend_student = feather.read_dataframe("{0}/attend_student.feather".format(self.dirpath))

        grouping_vars = self.grouping_vars
        logging.info("Grouping with variables: {0}".format(grouping_vars))
        output = self.grouped_attendance(grouping=grouping_vars, attend_student=attend_student)

        file_path = "{0}/{1}".format(self.dirpath, self.file_name)
        feather.write_dataframe(output, file_path)

        return file_path

class GroupedADAToGroupedYTDOperator(BaseOperator):
        """
        Take Grouped ADA file and calculate Grouped YTD

        :param dirpath: Path to PS staging files
        :type dirpath: string
        :param in_file_name: Grouped ADA file ep_name
        :type in_file_name: string
        :param out_file_name: Grouped ADA file ep_name
        :type out_file_name: string
        :param grouping_vars: variables to group by
        :type grouping_vars: dict
        """

        template_fields = ('dirpath',
                          'in_file_name',
                          'out_file_name',)
        ui_color = '#FEBC11'

        @apply_defaults
        def __init__(self,
                     dirpath,
                     in_file_name,
                     out_file_name,
                     grouping_vars,
                     *args,
                     **kwargs):
                     super(GroupedADAToGroupedYTDOperator, self).__init__(*args, **kwargs)

                     self.dirpath = dirpath
                     self.in_file_name = in_file_name
                     self.out_file_name = out_file_name
                     self.grouping_vars = grouping_vars

        def execute(self, context):
            in_file_path = "{0}/{1}".format(self.dirpath, self.in_file_name)

            data = feather.read_dataframe(in_file_path)

            grouping = list(self.grouping_vars)
            if 'date' in grouping:
                grouping.remove('date')

            output = pd.DataFrame(data, copy=False)
            # ytd ada
            grouped_df = data.groupby(grouping, group_keys = False)
            output['ytd_present'] = grouped_df.apply(lambda x: x.present.cumsum())
            output['ytd_enrolled'] = grouped_df.apply(lambda x: x.enrolled.cumsum())
            output['ytd_ada'] = output.ytd_present/output.ytd_enrolled*100

            # quarterly ada
            grouping.append('quarter')
            grouped_df = data.groupby(grouping, group_keys = False)
            output['cum_quarterly_present'] = grouped_df.apply(lambda x: x.present.cumsum())
            output['cum_quarterly_enrolled'] = grouped_df.apply(lambda x: x.enrolled.cumsum())
            output['quarterly_ada'] = output.ytd_present/output.ytd_enrolled*100

            # quarterly weekly
            grouping.remove('quarter')
            grouping.extend(['week_of_date', 'week_of_date_short_label'])
            grouped_df = data.groupby(grouping, group_keys = False)
            output['cum_weekly_present'] = grouped_df.apply(lambda x: x.present.cumsum())
            output['cum_weekly_enrolled'] = grouped_df.apply(lambda x: x.enrolled.cumsum())
            output['weekly_ada'] = output.ytd_present/output.ytd_enrolled*100

            grouped_df = output.groupby(grouping, group_keys = False)
            output = grouped_df.filter(lambda x: (x['date'] == x['date'].max()).any())

            write_path = "{0}/{1}".format(self.dirpath, self.out_file_name)
            feather.write_dataframe(output, write_path)

            return write_path

class BigQueryTableModifiedSensor(BaseSensorOperator):
    """
    Sensor to check if a give BQ tables has been modifiedself.

    :param table_name:          name of table to check LastModified metadata
    :type table_name:           string
    :param dataset:             BQ dataset that table is located in
    :type dataset: string
    :param project_id:          Project ID of dataset table is located in
    :type project_id:           string
    :param bigquery_conn_id:    BigQuery connection id
    :type bigquery_conn_id:     string
    :param use_legacy_sql:      Whether to use legacy SQL (true) or standard SQL (false)
    :type use_legacy_sql:       bool
    :param state_file:          location of text file used to save state of table being checked
    :type state_file:           string
    """
    template_fields = ('table_name',
                       'dataset',
                       'project_id',
                       'state_file',)
    ui_color = '#BCD631'


    @apply_defaults
    def __init__(self,
                 table_name,
                 dataset,
                 project_id,
                 state_file,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 use_legacy_sql=False,
                 *args,
                 **kwargs):
                 super(BigQueryTableModifiedSensor, self).__init__(*args, **kwargs)

                 self.table_name = table_name
                 self.dataset = dataset
                 self.project_id = project_id
                 self.state_file = state_file
                 self.bigquery_conn_id = bigquery_conn_id
                 self.delegate_to = delegate_to
                 self.use_legacy_sql = use_legacy_sql


    def bq_get_last_modified(self):
        logging.info("Connecting to Big Query")
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, delegate_to=self.delegate_to)
        bq_conn = bq_hook.get_connection(self.bigquery_conn_id)
        bq_conn_extra_json = bq_conn.extra
        bq_conn_extra = json.loads(bq_conn_extra_json)
        service_dict = bq_conn_extra['extra__google_cloud_platform__keyfile_dict']

        sql = """
            #standardSQL
            SELECT last_modified_time AS TS
            FROM `{0}.{1}.__TABLES__`
            WHERE table_id = '{2}'
            """.format(self.project_id, self.dataset, self.table_name)

        logging.info("Getting table last_modified_time from BQ with SQL:/n{0}".format(sql))
        df = read_gbq(sql, dialect='standard', project_id=self.project_id, private_key = service_dict)
        logging.info("Got table!")

        ts = str(df['TS'][0])
        return ts

    def check_timestamp_change(self, timestamp):
        ts = str(timestamp)
        state_file = self.state_file

        try:
            f = open(state_file, "r")
            ts_old = f.read()
            logging.info("Able to read old state file (ts_old = {})".format(ts_old))
            f.close()
        except IOError:
            logging.info("No existing state file.  Creating new state file.")
            f = open(state_file, "w+")
            f.close()
            ts_old = "0"

        #Status if table has been modified
        table_changed = ts_old != ts

        if table_changed:
            logging.info("Table modifed. Old and current timestamp don't match: {0} != {1}".format(ts_old, ts))
            f = open(state_file, "w")
            f.write(str(ts))
            f.close()
        else:
            logging.info("Table not modified. Old and current timestamps are the same: {0} = {1}".format(ts_old, ts))

        return table_changed

    def poke(self, context):
        ts_new = self.bq_get_last_modified()
        table_modified = self.check_timestamp_change(ts_new)

        return table_modified



class IdeaPlugin(AirflowPlugin):
    name = "idea_plugin"
    operators = [BigQueryToFeatherOperator,
                CreateAttendStudentOperator,
                CreateGroupedAttendance,
                GroupedADAToGroupedYTDOperator]
    sensors = [BigQueryTableModifiedSensor]
