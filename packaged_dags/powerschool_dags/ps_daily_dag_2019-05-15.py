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

home = expanduser("~")

SAVE_PATH = '{0}/gcs/data/powerschool/'.format(home)
BASE_URL = 'https://kippchicago.powerschool.com'
MAXPAGESIZE = 1000
STATE_FILEPATH = '{0}/gcs/data/'.format(home) + 'state.json'


client_id = Variable.get("ps_client_id")
client_secret = Variable.get("ps_client_secret")

credentials_concat = '{0}:{1}'.format(client_id, client_secret)
CREDENTIALS_ENCODED = base64.b64encode(credentials_concat.encode('utf-8'))


endpoints = [
    {"table_name":"districtteachercategory", "projection":"defaultdaysbeforedue,displayposition,defaultscoreentrypoints,defaultextracreditpoints,defaultweight,defaulttotalvalue,defaultpublishstate,defaultpublishoption,isdefaultpublishscores,defaultscoretype,districtteachercategoryid,name,description,color,isinfinalgrades,isactive,isusermodifiable"},
    {"table_name":"bell_schedule", "projection":"dcid,id,schoolid,year_id,attendance_conversion_id,name"},
    {"table_name":"schools", "projection":"address,district_number,school_number,pscomm_path,low_grade,high_grade,sortorder,abbreviation,schoolgroup,activecrslist,bulletinemail,sysemailfrom,hist_low_grade,hist_high_grade,tchrlogentrto,dfltnextschool,portalid,view_in_portal,state_excludefromreporting,alternate_school_number,schooladdress,schoolcity,schoolstate,schoolzip,schoolphone,schoolfax,schoolcountry,principal,principalphone,principalemail,asstprincipalphone,asstprincipalemail,countyname,countynbr,asstprincipal,schedulewhichschool,fee_exemption_status,schoolinfo_guid,sif_stateprid,issummerschool,dcid,id,name"},
    {"table_name":"fte", "projection":"dcid,id,schoolid,yearid,fte_value,name,description,dflt_att_mode_code,dflt_conversion_mode_code"},
    {"table_name":"cycle_day", "projection":"dcid,id,schoolid,year_id,letter,day_number,abbreviation,day_name,sortorder"},
    {"table_name":"period", "projection":"dcid,id,schoolid,year_id,period_number,name,abbreviation,sort_order"},
    {"table_name":"spenrollments", "projection":"dcid,id,programid,enter_date,exit_date,code1,code2,exitcode,sp_comment,schoolid,gradelevel,studentid"},
    {"table_name":"terms", "projection":"dcid,id,name,firstday,lastday,yearid,abbreviation,noofdays,schoolid,yearlycredithrs,termsinyear,portion,importmap,isyearrec,periods_per_day,days_per_cycle,attendance_calculation_code,sterms,terminfo_guid"},
    {"table_name":"attendance_code", "projection":"dcid,id,schoolid,yearid,att_code,alternate_code,description,presence_status_cd,unused1,course_credit_points,assignment_filter_yn,calculate_ada_yn,calculate_adm_yn,sortorder,attendancecodeinfo_guid"},
    {"table_name":"relationship", "projection":"dcid,id,person_id,relatedperson_id,reciprocalrelationship_id"},
    {"table_name":"gradecalculationtype", "projection":"gradecalculationtypeid,gradeformulasetid,abbreviation,storecode,yearid,type,isnograde,isdroplowstudentfavor,isalternatepointsused,droplowscoreoption,iscalcformulaeditable,isdropscoreeditable"},
    {"table_name":"teachercategory", "projection":"defaultdaysbeforedue,teachercategoryid,districtteachercategoryid,name,usersdcid,description,categorytype,color,isinfinalgrades,isactive,isusermodifiable,teachermodified,displayposition,defaultscoreentrypoints,defaultextracreditpoints,defaultweight,defaulttotalvalue,defaultpublishstate,defaultpublishoption,isdefaultpublishscores,defaultscoretype"},
    {"table_name":"courses", "projection":"credittype,crhrweight,sched_year,sched_department,sched_coursesubjectareacode,sched_fullcatalogdescription,sched_coursepackage,sched_coursepkgcontents,sched_scheduled,sched_scheduletypecode,sched_sectionsoffered,sched_teachercount,sched_periodspermeeting,sched_frequency,sched_maximumperiodsperday,sched_minimumperiodsperday,sched_maximumdayspercycle,sched_minimumdayspercycle,sched_consecutiveperiods,sched_blockstart,sched_validstartperiods,sched_validdaycombinations,validextradaycombinations,sched_extradayscheduletypecode,sched_lengthinnumberofterms,sched_consecutiveterms,sched_balanceterms,sched_maximumenrollment,sched_concurrentflag,sched_facilities,sched_multiplerooms,sched_labflag,sched_labfrequency,sched_labperiodspermeeting,sched_repeatsallowed,sched_loadpriority,sched_loadtype,sched_substitutionallowed,sched_globalsubstitution1,sched_globalsubstitution2,sched_globalsubstitution3,sched_usepreestablishedteams,sched_closesectionaftermax,sched_usesectiontypes,sched_balancepriority,sched_periodspercycle,gradescaleid,gpa_addedvalue,excludefromgpa,excludefromclassrank,excludefromhonorroll,sched_lunchcourse,sched_do_not_print,exclude_ada,programid,excludefromstoredgrades,maxcredit,dcid,id,course_number,course_name,credit_hours,add_to_gpa,code,prerequisitesvalue,schoolid,corequisites,powerlink,powerlinkspan,regavailable,reggradelevels,regteachers,targetclasssize,maxclasssize,regcoursegroup,multiterm,termsoffered,sectionstooffer,schoolgroup,vocational,status"},
    {"table_name":"schoolstaff", "projection":"dcid,id,schoolid,users_dcid,balance1,balance2,balance3,balance4,classpua,noofcurclasses,log,staffstatus,status,sched_gender,sched_classroom,sched_homeroom,sched_department,sched_maximumcourses,sched_maximumduty,sched_maximumfree,sched_totalcourses,sched_maximumconsecutive,sched_isteacherfree,sched_housecode,sched_buildingcode,sched_activitystatuscode,sched_primaryschoolcode,sched_teachermoreoneschool,sched_substitute,sched_scheduled,sched_usebuilding,sched_usehouse,sched_team,sched_lunch,sched_maxpers,sched_maxpreps,notes"},
    {"table_name":"users", "projection":"dcid,homeschoolid,lastfirst,first_name,middle_name,last_name,photo,title,homeroom,email_addr,password,numlogins,ipaddrrestrict,allowloginstart,allowloginend,psaccess,accessvalue,homepage,loginid,defaultstudscrn,groupvalue,teachernumber,lunch_id,ssn,home_phone,school_phone,street,city,state,zip,periodsavail,powergradepw,canchangeschool,teacherloginpw,nameasimported,teacherloginid,teacherloginip,supportcontact,wm_status,wm_statusdate,wm_tier,wm_address,wm_password,wm_createdate,wm_createtime,wm_ta_flag,wm_ta_date,wm_exclude,wm_alias,ethnicity,preferredname,lastmeal,staffpers_guid,adminldapenabled,teacherldapenabled,sif_stateprid,maximum_load,gradebooktype,fedethnicity,fedracedecline,ptaccess"},
    {"table_name":"prefs", "projection":"dcid,name,value,id,schoolid,yearid,userid"},
    {"table_name":"reenrollments", "projection":"exitcode,exitcomment,schoolid,grade_level,type,track,districtofresidence,enrollmenttype,enrollmentcode,fulltimeequiv_obsolete,membershipshare,tuitionpayer,lunchstatus,fteid,withdrawal_reason_code,studentschlenrl_guid,dcid,id,studentid,entrydate,entrycode,entrycomment,exitdate"},
    {"table_name":"sections", "projection":"gradebooktype,blockperiods_obsolete,dependent_secs,grade_level,campusid,exclude_ada,expression,gradescaleid,excludefromgpa,buildid,bitmap,schedulesectionid,wheretaughtdistrict,excludefromclassrank,excludefromhonorroll,parent_section_id,original_expression,comment_value,attendance_type_code,section_type,team,house,maxcut,exclude_state_rpt_yn,instruction_lang,sectioninfo_guid,att_mode_code,sortorder,programid,excludefromstoredgrades,max_load_status,dcid,id,section_number,course_number,teacher,termid,period_obsolete,no_of_students,room,fastperlist,ccrnarray,attendance,lastattupdate,schoolid,noofterms,trackteacheratt,maxenrollment,distuniqueid,wheretaught,pgflags,days_obsolete,gradeprofile,log,rostermodser,teacherdescr,pgversion"},
    {"table_name":"students", "projection":"sched_nextyearhomeroom,sched_nextyeargrade,sched_nextyearbus,sched_scheduled,sched_lockstudentschedule,wm_ta_flag,wm_ta_date,sched_priority,districtentrydate,districtentrygradelevel,schoolentrydate,schoolentrygradelevel,graduated_schoolname,graduated_schoolid,graduated_rank,alert_discipline,alert_disciplineexpires,alert_guardian,alert_guardianexpires,alert_medical,alert_medicalexpires,alert_other,alert_otherexpires,state_studentnumber,state_excludefromreporting,state_enrollflag,districtofresidence,enrollmenttype,enrollmentcode,fulltimeequiv_obsolete,membershipshare,tuitionpayer,enrollment_transfer_date_pend,enrollment_transfer_info,exitcomment,fee_exemption_status,team,house,building,fteid,withdrawal_reason_code,guardian_studentcont_guid,father_studentcont_guid,mother_studentcont_guid,studentpers_guid,studentpict_guid,studentschlenrl_guid,sched_loadlock,person_id,ldapenabled,summerschoolid,summerschoolnote,fedethnicity,fedracedecline,geocode,mailing_geocode,gpentryyear,enrollmentid,dcid,id,lastfirst,first_name,middle_name,last_name,student_number,enroll_status,grade_level,balance1,balance2,phone_id,lunch_id,photoflag,gender,entrydate,exitdate,web_id,web_password,sdatarn,schoolid,dob,street,city,state,zip,guardianemail,allowwebaccess,transfercomment,guardianfax,ssn,entrycode,exitcode,lunchstatus,ethnicity,cumulative_gpa,simple_gpa,cumulative_pct,lastmeal,pl_language,simple_pct,classof,family_ident,next_school,log,track,exclude_fr_rank,gradreqset,teachergroupid,campusid,balance3,balance4,enrollment_schoolid,gradreqsetid,applic_submitted_date,applic_response_recvd_date,student_web_id,student_web_password,student_allowwebaccess,bus_route,bus_stop,doctor_name,doctor_phone,emerg_contact_1,emerg_contact_2,emerg_phone_1,emerg_phone_2,father,home_phone,home_room,locker_combination,locker_number,mailing_city,mailing_street,mailing_state,mailing_zip,mother,wm_status,wm_statusdate,wm_tier,wm_address,wm_password,wm_createdate,wm_createtime,sched_yearofgraduation,sched_nextyearhouse,sched_nextyearbuilding,sched_nextyearteam"},
    {"table_name":"calendar_day", "projection":"dcid,id,schoolid,date_value,scheduleid,a,b,c,d,e,f,insession,membershipvalue,note,type,cycle_day_id,bell_schedule_id,week_num,ip_address"},
    {"table_name":"cc", "query_expression":"termid=ge=2800;termid=lt=2900",
      "projection":"dateleft,schoolid,termid,period_obsolete,attendance_type_code,unused2,currentabsences,currenttardies,attendance,teacherid,lastgradeupdate,section_number,course_number,origsectionid,unused3,teachercomment,lastattmod,asmtscores,firstattdate,finalgrades,studyear,log,expression,studentsectenrl_guid,teacherprivatenote,ab_course_cmp_fun_flg,ab_course_cmp_ext_crd,ab_course_cmp_met_cd,ab_course_eva_pro_cd,ab_course_cmp_sta_cd,dcid,id,studentid,sectionid,dateenrolled"},
    {"table_name":"cc", "query_expression":"termid=gt=-2900;termid=le=-2800",
     "projection":"dateleft,schoolid,termid,period_obsolete,attendance_type_code,unused2,currentabsences,currenttardies,attendance,teacherid,lastgradeupdate,section_number,course_number,origsectionid,unused3,teachercomment,lastattmod,asmtscores,firstattdate,finalgrades,studyear,log,expression,studentsectenrl_guid,teacherprivatenote,ab_course_cmp_fun_flg,ab_course_cmp_ext_crd,ab_course_cmp_met_cd,ab_course_eva_pro_cd,ab_course_cmp_sta_cd,dcid,id,studentid,sectionid,dateenrolled"},
    {"table_name":"storedgrades", "query_expression":"termid=ge=2700;termid=lt=2800", "projection":"gpa_addedvalue,gpa_custom2,excludefromclassrank,excludefromhonorroll,gpa_custom1,ab_course_cmp_fun_flg,ab_course_cmp_ext_crd,ab_course_cmp_fun_sch,ab_course_cmp_met_cd,ab_course_eva_pro_cd,ab_course_cmp_sta_cd,ab_pri_del_met_cd,ab_lng_cd,ab_dipl_exam_mark,ab_final_mark,isearnedcrhrsfromgb,ispotentialcrhrsfromgb,termbinsname,replaced_grade,excludefromtranscripts,replaced_dcid,excludefromgraduation,excludefromgradesuppression,replaced_equivalent_course,gradereplacementpolicy_id,dcid,studentid,sectionid,termid,storecode,datestored,grade,percent,absences,tardies,behavior,potentialcrhrs,earnedcrhrs,comment_value,course_name,course_number,credit_type,grade_level,schoolid,log,course_equiv,schoolname,gradescale_name,teacher_name,excludefromgpa,gpa_points"}
]


#################################
# Airflow specific DAG set up ##
###############################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 16),
    "email": ["chrishiad@kippchicago.com"],
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
    "silo_ps_daily_endpoints_2019-05-15",
    default_args=default_args,
    schedule_interval='30 12 * * *')

t1 = PythonOperator(task_id = "get_state",
                    python_callable = get_state,
                    op_args = [STATE_FILEPATH],
                    dag = dag)

t2 = PythonOperator(task_id = "build_auth_headers",
                    python_callable = build_auth_headers,
                    op_args = [CREDENTIALS_ENCODED, STATE_FILEPATH, BASE_URL],
                    dag = dag)



#Loop through endpoints
for i, e in enumerate(endpoints):

    # get endpoint name
    endpoint_name = e['table_name']
    # get dict
    get_enpdpoints_task_id = "get_{0}_endpoint".format(endpoint_name)
    file_to_gcs_task_id = "{0}_to_gcs".format(endpoint_name)

    #cc exceptions
    if endpoint_name == 'cc':
        get_enpdpoints_task_id = "get_{0}_{1}_endpoint".format(endpoint_name, i)
        file_to_gcs_task_id = "{0}_{1}_to_gcs".format(endpoint_name, i)

    t3 = PythonOperator(
            task_id = get_enpdpoints_task_id,
            python_callable = get_endpoints,
            op_args = [e, SAVE_PATH, BASE_URL, MAXPAGESIZE],
            dag = dag
            )

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
