# -*- coding: utf8-*-
##############################
# v20200622 for python3.7
# 힐당 관련 정합성 체크 및 sync; RD-59
##############################

import logging
import logging.handlers
import subprocess
import os
import sys
import pytz
from pytz import timezone, utc
from datetime import datetime,timedelta
import operator
import math
import json
import time
import redis
from imp import reload

reload(sys)

##############################
# cli 옵션 처리; project_id
##############################
if len(sys.argv) != 2:
    print('Usage: python {} {{task ID}}'.format(os.path.basename(__file__)))
    sys.exit(1)

task_id = sys.argv[1]

try:
    task_id = int(task_id)
except:
    print('Invalid task ID: {}'.format(task_id))
    sys.exit(1)

##############################
# 기본 변수
##############################
dry_run = False  # True 인 경우, DB 업데이트 실행은 안하고 명령어 로깅만 함
batch_size = 3 

db_host = '127.0.0.1' #'db-svmk.pub-cdb.ntruss.com' #'db-1ea6m.cdb.ntruss.com'
mysql_exe = None
try:
    mysql_exe = subprocess.check_output('which mysql', shell=True)
    mysql_exe = mysql_exe.strip() # 줄바꿈 (if any) 제거
finally:
    if mysql_exe is None:
        print('ERROR: mysql not found')
        sys.exit(1)

# 어제 날짜; 한국시간 기준
today = datetime.now(timezone('Asia/Seoul'))
yesterday = today - timedelta(days=1)
yesterday_ymd = yesterday.strftime('%Y%m%d')
yesterday_ymd_sql = yesterday.strftime('%Y-%m-%d')

time_now = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))

# 환경 변수에서 DB, 계정정보 가져오기
db_host_env = os.environ.get('ASSIGN_SYNC_DB_HOST')
if db_host_env is not None:
    db_host = db_host_env

db_user = os.environ.get('ASSIGN_SYNC_DB_USER')
dbu = None
dbp = None
if db_user is None:
    print('ERROR: ASSIGN_SYNC_DB_USER is None')
    sys.exit(1)
else:
    tmp_pos = db_user.find(':')
    if tmp_pos > 0:
        dbu = db_user[0:tmp_pos]
        dbp = db_user[tmp_pos + 1:]
if not dbu or not dbp:
    print('ERROR: ASSIGN_SYNC_DB_USER is invalid')
    sys.exit(1)


##############################
# dir , ps
##############################
current_dir = os.path.dirname(os.path.realpath(__file__))
current_file = os.path.basename(__file__)
current_file_name = current_file[:-3] #xxxx.py
current_pid = os.getpid()

##############################
# 시간 관련 처리
# timezone to local
##############################
local_tz = pytz.timezone('Asia/Seoul')
def localTime(*args):
        utc_dt = utc.localize(datetime.utcnow())
        converted = utc_dt.astimezone(local_tz)
        return converted.timetuple()

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

##############################
# init mkdirs
##############################
log_dir = '{}/logs'.format(current_dir)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

##############################
# logging
##############################  
#logging.basicConfig(level=logging.DEBUG)
LOG_FILENAME = '{}/log_{}'.format(log_dir, current_file_name)
logger = logging.getLogger(current_file_name)
logger.setLevel(logging.DEBUG)
file_handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when='midnight', interval=1, backupCount=14)
file_handler.suffix = 'log-%Y%m%d'
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(process)d - [%(filename)s:%(lineno)d] %(message)s')
formatter.converter = localTime
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

###############################
# 중복 실행 방지
###############################
result = subprocess.check_output('ps -ef | grep {} | wc -l'.format(os.path.basename(__file__)), shell=True)
#logger.info('PS: {}'.format(result))
if int(result.strip()) > 3:
    logger.info('There is a previous run; I \'m exiting')
    sys.exit(0)

###############################
# 실행 함수들
###############################
# DB 쿼리 실행 커맨드 구성
def get_db_cmd(qry):
    cmd = '{} -h {} -u{} -p"{}" cwaidata -N -e "{}"'
    cmd = cmd.format(mysql_exe.decode('utf-8'), db_host, dbu, dbp, qry.replace('"','\\"'))
    return cmd

# DB 쿼리 결과를 이중 배열로 구성해서 리턴
def results_to_array(result):
    rev = []
    for row in result.split(b'\n'):
        if row:
            rev.append(row.split(b'\t'))
    return rev

# 단일 할당 정합성 체크
# DB 데이터 수와 pointer가 일치하면 이상 없음.
def single_assign_check():
    logger.info('Starting single assign check')
    qry = "SELECT count(1) FROM TB_PRJ_DATA WHERE prj_idx = {}".format(task_id)
    cmd = get_db_cmd(qry)
    logger.info(cmd.replace(dbp, 'xxxxx'))
    result = subprocess.check_output(cmd, shell=True)
    if result:
        results_list = results_to_array(result)
        logger.info('fetched: {}'.format(len(results_list)))
        db_cnt  = int(results_list[0][0])
        k = 'sasp_{}'.format(task_id)
        v = redis_client.get(k)
        if v:
            redis_sasp = int(v)
            print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_sasp, task_id, db_cnt))
            if redis_sasp != db_cnt:
                return (redis_sasp, db_cnt)
            else:
                print('The DB count and REDIS count are same for prj_idx {} - no sync needed'.format(task_id))
        else:
            print("No Redis key: {}".format(k))

    logger.info('Single assign check end')
    return -1

# 단일 할당 정합성 맞추기 처리
# Redis > DB 인 경우, 누락된 src_idx를 찾아서 DB에 INSERT (DATA, LABEL 테이블)
# DB > Redis 인 경우, Redis pointer를 맞춰 준다 
def single_assign_sync(sync_needed_info, project_id, prj_idx):
    logger.info('Starting single assign synchronization')
    print('Single assign prj_idx synchronization process : {}'.format(prj_idx))
    redis_sasp = sync_needed_info[0]
    db_cnt = sync_needed_info[1]
    print('REDIS : {} DB_CNT : {}'.format(redis_sasp, db_cnt))

    if redis_sasp < db_cnt:
        k = 'sasp_{}'.format(prj_idx)
        v = redis_client.get(k)

        if v:
            redis_client.set(k, db_cnt)
            v = redis_client.get(k)
            redis_new_sasp = int(v)
            print('Set REDIS counter for {} to {}'.format(k, db_cnt))
            print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_new_sasp, prj_idx, db_cnt))
        else:
            print("No Redis key: {}".format(k))

    elif redis_sasp > db_cnt:
        qry = "SELECT src_idx, max(src_idx) FROM TB_PRJ_DATA WHERE prj_idx = {} GROUP BY src_idx".format(task_id)
        cmd = get_db_cmd(qry)
        logger.info(cmd.replace(dbp, 'xxxxx'))
        result = subprocess.check_output(cmd, shell=True)

        if result:
            result_list = results_to_array(result)
            logger.info('fetched: {}'.format(len(result_list)))
            mx = int(result_list[-1][1].decode('utf-8'))
            add_list = []

            for i in range(1, int(result_list[0][0])):
                add_list.append(i)

            for i in range(len(result_list) - 1):
                val = int(result_list[i][0])
                next_val = int(result_list[i+1][0])
                if val + 1 != next_val:
                    while val + 1 < next_val:
                        val += 1
                        add_list.append(val)
                        print('Added to the sync list : {}'.format(val))

            if mx < redis_sasp:

                for i in range(mx + 1, redis_sasp + 1):
                    add_list.append(i)
                    print('Added to the sync list : {}'.format(i))

            print('List of src_idx to be synchronized : {}'.format(add_list))

            for add in add_list:
                # Insert scr_idx into TB_PRJ_DATA
                input_qry = "INSERT INTO TB_PRJ_DATA (prj_idx, project_id, prog_state_cd, src_idx, work_user, work_sdate, old_reset) VALUES ({}, {}, \"WORK_ING\", {}, {}, NOW(), {})".format(prj_idx, project_id, add, -1, 1)
                input_cmd = get_db_cmd(input_qry)
                logger.info(input_cmd.replace(dbp, 'xxxxx'))
                subprocess.call(input_cmd, shell=True)
                print('Added {} to the DataBase - TB_PRJ_DATA'.format(add))

                # Get data_id from recent insert
                get_dataid_qry = "SELECT data_idx FROM TB_PRJ_DATA WHERE prj_idx={} AND project_id={} AND src_idx={}".format(prj_idx, project_id, add)
                get_dataid_cmd = get_db_cmd(get_dataid_qry)
                logger.info(get_dataid_cmd.replace(dbp, 'xxxxx'))
                get_dataid_result = subprocess.check_output(get_dataid_cmd, shell=True)

                if get_dataid_result:
                    dataid_result_list = results_to_array(get_dataid_result)
                    logger.info('fetched: {}'.format(len(dataid_result_list)))
                    data_id = int(dataid_result_list[0][0].decode('utf-8'))
                    print('The data_idx is : {}'.format(data_id))

                else:
                    print('INSERT was unsuccessful and could not find the data_idx')
                    sys.exit(1)

                # Insert scr_idx into TB_TMPL_IMG_LABEL_DATA
                input_qry2 = "INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx) VALUES ({}, {}, {})".format(data_id, prj_idx, add)
                input_cmd2 = get_db_cmd(input_qry2)
                logger.info(input_cmd2.replace(dbp, 'xxxxx'))
                subprocess.call(input_cmd2, shell=True)

                print('Added {} to the DataBase - TB_TMPL_IMG_LABEL_DATA'.format(add))
                logger.info('Single assign synchronization end')

    else:
        print('ERROR : REDIS counter and DB counter are the same! Something must be wrong...')
        logger.info('Single assign synchronization end')



def multi_assign_check_cnt():
    logger.info('Starting multi assign count check')
    qry = "SELECT src_idx, count(1) AS cnt, max(src_idx) AS mx FROM TB_PRJ_DATA WHERE prj_idx = {} GROUP BY src_idx".format(task_id)
    cmd = get_db_cmd(qry)
    logger.info(cmd.replace(dbp, 'xxxxx'))
    result = subprocess.check_output(cmd, shell=True)
    src_idx_sync_need = {}
    
    count = 0

    if result:
        results_list = results_to_array(result)
        logger.info('fetched: {}'.format(len(results_list)))
        src_idx_cnt_map = {}
        mx = -1
        for src_idx, db_cnt, mx in results_list:
            src_idx_cnt_map[int(src_idx)] = int(db_cnt)
            # 각 소스 ID 별 카운트 + 최종 max 소스 아이디

        print('List of src_idx and the count for each : {} \n'.format(src_idx_cnt_map))

        for src_idx in range(1, int(mx) + 1):
            k = 'maic_{}_{}'.format(task_id, src_idx)
            v = redis_client.get(k)
            if v:
                redis_maic = int(v)
                db_cnt = src_idx_cnt_map.get(src_idx)
                print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_maic, task_id, db_cnt))
                if redis_maic != db_cnt:
                    src_idx_sync_need[src_idx] = (redis_maic, db_cnt)
                    print('Added to the sync needed list : {}'.format(src_idx))
                    count += 1
            else:
                print("No Redis key: {}".format(k))

    if len(src_idx_sync_need) == 0:
        print('\n')
        print('No need to synchronize prj_idx {}'.format(task_id))
        logger.info('Multi assign counter check end')
        return -1
    else:
        print('\n')
        print('List of src_idx that need to be synchronized : {}'.format(src_idx_sync_need))
        print('There are {} src_idx in the list for cnt'.format(count))
        logger.info('Multi assign counter check end')
        return src_idx_sync_need

def multi_assign_check_mx():
    logger.info('Starting multi assign max check')
    qry = "SELECT work_user, max(src_idx) AS mx FROM TB_PRJ_DATA WHERE prj_idx={} AND work_user != -1 GROUP BY work_user".format(task_id)
    cmd = get_db_cmd(qry)
    logger.info(cmd.replace(dbp, 'xxxxx'))
    result = subprocess.check_output(cmd, shell=True)
    src_idx_sync_need = {}

    count = 0

    if result:
        results_list = results_to_array(result)
        logger.info('fetched: {}'.format(len(results_list)))
        src_idx_mx_map = {}

        for user, mx in results_list:
            user = user.decode('utf-8')
            mx = mx.decode('utf-8')
            src_idx_mx_map[int(user)] = int(mx)

        print('List of users and the assigned max src_idx : {} \n'.format(src_idx_mx_map))

        users = list(int(u[0].decode('utf-8')) for u in results_list)
        
        for user in users: # Each key is a user ID (work_user)
            k = 'mapc_{}_{}'.format(task_id, user)
            v = redis_client.get(k)
            if v:
                redis_mapc = int(v)
                db_mx = src_idx_mx_map.get(user)
                print('Redis({}):\t{}\nDB({}):\t{}'.format(k, redis_mapc, task_id, db_mx))
                if redis_mapc != db_mx:
                    src_idx_sync_need[user] = (redis_mapc, db_mx)
                    print('Added to the sync needed list : {} \n'.format(user))
                    count += 1
            else:
                print("No Redis key: {} \n".format(k))

    if len(src_idx_sync_need) == 0:
        print('\n')
        print('No need to synchronize \n')
        logger.info('Multi assign max check end')
        return -1
    else:
        print('\n')
        print('List of work_users and the src_idx that need to be synchronized : {} \n'.format(src_idx_sync_need))
        print('There are {} src_idx in the list for mx'.format(count))
        logger.info('Multi assign max check end')
        return src_idx_sync_need

# 다중 할당 정합성 맞추기 처리
def multi_assign_sync_cnt(sync_needed_dict, project_id, prj_idx):
    logger.info('Starting multi assign count synchronization')
    keys = list(sync_needed_dict.keys())

    for key in keys: # key = source_id (src_idx)
        print('Count synchronizing process for src_idx : {}'.format(key))
        redis_maic = sync_needed_dict[key][0]
        db_cnt = sync_needed_dict[key][1]
        print('REDIS : {} DB_CNT : {}'.format(redis_maic, db_cnt))

        if redis_maic < db_cnt:
            k = 'maic_{}_{}'.format(prj_idx, key)
            v = redis_client.get(k)

            if v:
                redis_client.set(k, db_cnt)
                v = redis_client.get(k)
                redis_new_sasp = int(v)
                print('Set REDIS counter for src_idx {} to {}'.format(key, db_cnt))
                print('Redis({}):\0t{}\nDB({}):\t{}\n'.format(k, redis_new_sasp, prj_idx, db_cnt))
            else:
                print("No Redis key: {}".format(k))

        elif redis_maic > db_cnt:

            # Get json_index needed
            json_qry = "SELECT json_index, max(json_index) FROM TB_TMPL_IMG_LABEL_DATA WHERE prj_idx={} AND src_idx={} GROUP BY json_index".format(prj_idx, key)
            json_cmd = get_db_cmd(json_qry)
            logger.info(cmd.replace(dbp, 'xxxxx'))
            json_result = subprocess.check_output(json_cmd, shell=True)
            json_list = []

            if json_result:
                json_result_list = results_to_array(json_result)
                logger.info('fetched: {}'.format(len(json_result_list)))

                for j in range(1, int(json_result_list[0][0])):
                    if j + 1 != int(json_result_list[0][0]):
                        json_list.append(j)

                for j in range(len(json_result_list) - 1):
                    v = int(json_result_list[j][0])
                    nv = int(json_result_list[j + 1][0])

                    if v + 1 != nv:
                        while v + 1 < nv:
                            v += 1
                            json_list.append(v)

                json_cur_mx = int(json_result_list[-1][1])

                if len(json_list) < redis_maic - db_cnt:
                    for i in range(1, redis_maic - db_cnt - len(json_list) + 1):
                        json_list.append(json_cur_mx + i)
            
            print('List of json indexes to be added : {}'.format(json_list))

            for i in range(db_cnt + 1, redis_maic + 1):
                # Insert scr_idx into TB_PRJ_DATA
                input_qry = "INSERT INTO TB_PRJ_DATA (prj_idx, project_id, prog_state_cd, src_idx, work_user, work_sdate, old_reset) VALUES ({}, {}, \"WORK_ING\", {}, {}, NOW(), {})".format(prj_idx, project_id, key, -1, 1)
                input_cmd = get_db_cmd(input_qry)
                logger.info(input_cmd.replace(dbp, 'xxxxx'))
                subprocess.call(input_cmd, shell=True)

                print('Added src_idx {} to the DataBase - TB_PRJ_DATA'.format(key))

                # Get data_id from recent insert
                get_dataid_qry = "SELECT data_idx FROM TB_PRJ_DATA WHERE prj_idx={} AND project_id={} AND src_idx={} AND work_user={}".format(prj_idx, project_id, key, -1)
                get_dataid_cmd = get_db_cmd(get_dataid_qry)
                logger.info(get_dataid_cmd.replace(dbp, 'xxxxx'))
                get_dataid_result = subprocess.check_output(get_dataid_cmd, shell=True)
                dataid_result_list = results_to_array(get_dataid_result)
                logger.info('fetched: {}'.format(len(dataid_result_list)))
                data_id = int(dataid_result_list[i - (db_cnt + 1)][0].decode('utf-8'))
                print('The data_idx is : {}'.format(data_id))


                # Insert scr_idx into TB_TMPL_IMG_LABEL_DATA
                if len(json_list) == 0:
                    input_qry2 = "INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx) VALUES ({}, {}, {})".format(data_id, prj_idx, key)
                    input_cmd2 = get_db_cmd(input_qry2)
                    logger.info(input_cmd2.replace(dbp, 'xxxxx'))
                    subprocess.call(input_cmd2, shell=True)
                else:
                    input_qry2 = "INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx, json_index) VALUES ({}, {}, {}, {})".format(data_id, prj_idx, key, json_list[i - (db_cnt + 1)])
                    input_cmd2 = get_db_cmd(input_qry2)
                    logger.info(input_cmd2.replace(dbp, 'xxxxx'))
                    subprocess.call(input_cmd2, shell=True)

                print('Added src_idx {} to the DataBase - TB_TMPL_IMG_LABEL_DATA'.format(key))

                logger.info('Multi assign count synchronization end')

        else:
            logger.info('Multi assign count synchronization end')
            print('ERROR : REDIS counter and DB counter are the same! Something must be wrong...')

def multi_assign_sync_mx(sync_needed_dict, project_id, prj_idx):
    logger.info('Starting multi assign max synchronization')
    keys = list(sync_needed_dict.keys())

    for key in keys: # key = user_id (work_user) -> value = max src_idx
        print('Count synchronizing process for work_user : {}'.format(key))
        redis_mapc = sync_needed_dict[key][0]
        db_mx = sync_needed_dict[key][1]
        print('REDIS : {} DB_CNT : {}'.format(redis_mapc, db_mx))

        if redis_mapc > db_mx:

            # Check if updated redis value is valid

            check_qry = "SELECT count(1) AS cnt FROM TB_PRJ_DATA WHERE prj_idx = {} AND src_idx = {}".format(task_id, redis_mapc)
            check_cmd = get_db_cmd(check_qry)
            logger.info(check_cmd.replace(dbp, 'xxxxx'))
            check_result = subprocess.check_output(check_cmd, shell=True)

            if check_result:
                check_result = results_to_array(check_result)
                logger.info('fetched: {}'.format(len(check_result)))
                check_count = int(check_result[0][0])

            k = 'maic_{}_{}'.format(task_id, redis_mapc)
            v = redis_client.get(k)

            if v:
                compare_count = int(v)

            if compare_count <= check_count:
                # k = 'mapc_{}_{}'.format(task_id, key)
                # redis_client.set(k, db_mx)

                # print('Since former REDIS pointer value was an update process the pointer was set back to the DB max - {} -> {}'.format(redis_mapc, db_mx))
                print('{} : REDIS - {} DB - {}'.format(redis_mapc, compare_count, check_count))
                print('Skip this')
            else:
                # Insert scr_idx into TB_PRJ_DATA
                input_qry = "INSERT INTO TB_PRJ_DATA (prj_idx, project_id, prog_state_cd, src_idx, work_user, work_sdate, old_reset) VALUES ({}, {}, \"WORK_ING\", {}, {}, NOW(), {})".format(prj_idx, project_id, redis_mapc, key, 1)
                input_cmd = get_db_cmd(input_qry)
                logger.info(input_cmd.replace(dbp, 'xxxxx'))
                subprocess.call(input_cmd, shell=True)

                print('Added src_idx {} to the DataBase - TB_PRJ_DATA'.format(redis_mapc))

                # Get data_id from recent insert
                get_dataid_qry = "SELECT data_idx FROM TB_PRJ_DATA WHERE prj_idx={} AND project_id={} AND src_idx={} AND work_user={}".format(prj_idx, project_id, redis_mapc, key)
                get_dataid_cmd = get_db_cmd(get_dataid_qry)
                get_dataid_result = subprocess.check_output(get_dataid_cmd, shell=True)
                dataid_result_list = results_to_array(get_dataid_result)
                logger.info('fetched: {}'.format(len(dataid_result_list)))
                data_id = int(dataid_result_list[0][0].decode('utf-8'))
                print('The data_idx is : {}'.format(data_id))

                # Get json_index needed
                json_qry = "SELECT json_index, max(json_index) FROM TB_TMPL_IMG_LABEL_DATA WHERE prj_idx={} AND src_idx={} GROUP BY json_index".format(prj_idx, redis_mapc)
                json_cmd = get_db_cmd(json_qry)
                logger.info(json_cmd.replace(dbp, 'xxxxx'))
                json_result = subprocess.check_output(json_cmd, shell=True)

                if json_result:
                    json_result_list = results_to_array(json_result)
                    logger.info('fetched: {}'.format(len(json_result_list)))

                    json_index = -1

                    for j in range(len(json_result_list) - 1):
                        v = int(json_result_list[j][0])
                        nv = int(json_result_list[j + 1][0])

                        if v + 1 != nv:
                            json_index = v + 1


                    if json_index == -1:
                        json_index = int(json_result_list[-1][1]) + 1
                    
                    print('Json index to be added : {}'.format(json_index))

                    # Insert scr_idx into TB_TMPL_IMG_LABEL_DATA
                    input_qry2 = "INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx, json_index) VALUES ({}, {}, {}, {})".format(data_id, prj_idx, redis_mapc, json_index)
                    input_cmd2 = get_db_cmd(input_qry2)
                    logger.info(input_cmd2.replace(dbp, 'xxxxx'))
                    subprocess.call(input_cmd2, shell=True)

                else:
                    # Insert scr_idx into TB_TMPL_IMG_LABEL_DATA
                    input_qry2 = "INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx) VALUES ({}, {}, {})".format(data_id, prj_idx, redis_mapc)
                    input_cmd2 = get_db_cmd(input_qry2)
                    logger.info(input_cmd2.replace(dbp, 'xxxxx'))
                    subprocess.call(input_cmd2, shell=True)

                print('Added src_idx {} to the DataBase - TB_TMPL_IMG_LABEL_DATA'.format(redis_mapc))

                logger.info('Multi assign max synchronization end')


        elif redis_mapc < db_mx:
            k = 'mapc_{}_{}'.format(task_id, key)
            v = redis_client.get(k)

            if v:
                redis_client.set(k, db_mx)
                v = redis_client.get(k)
                redis_new_sasp = int(v)
                print('Set REDIS counter for work_user {} to {}'.format(key, db_mx))
                print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_new_sasp, task_id, db_mx))
            else:
                print("No Redis key: {}".format(k))


        else:
            print('ERROR : REDIS counter and DB counter are the same! Something must be wrong...')
            logger.info('Multi assign max synchronization end')



def dry_run_single(sync_needed_info, project_id, prj_idx):
    print('Go Dry Run - Single : prj_idx -> {} \n'.format(prj_idx))

    redis_sasp = sync_needed_info[0]
    db_cnt = sync_needed_info[1]

    print('REDIS : {} DB_CNT : {} \n'.format(redis_sasp, db_cnt))

    if redis_sasp < db_cnt:
        k = 'sasp_{}'.format(prj_idx)
        v = redis_client.get(k)

        print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_sasp, prj_idx, db_cnt))
        print('REDIS actions for prj_idx {} : \n'.format(prj_idx))

        if v:
            print('Need to sync prj_idx value : {}'.format(k))
            print('REDIS : set {} {}'.format(k, db_cnt))
        else:
            print("ERROR - No Redis key: {}".format(k))

    elif redis_sasp > db_cnt:
        print("SELECT src_idx, max(src_idx) FROM TB_PRJ_DATA WHERE prj_idx = {} GROUP BY src_idx \n".format(prj_idx))
        qry = "SELECT src_idx, max(src_idx) FROM TB_PRJ_DATA WHERE prj_idx = {} GROUP BY src_idx".format(prj_idx)
        cmd = get_db_cmd(qry)
        result = subprocess.check_output(cmd, shell=True)
        if result:
            result_list = results_to_array(result)
            mx = int(result_list[-1][1])
            add_list = []

            print('Current list of src_idx : {} \n'.format(list(l[0].decode('utf-8') for l in result_list)))

            for i in range(1, int(result_list[0][0])):
                add_list.append(i)

            for i in range(len(result_list) - 1):
                val = int(result_list[i][0])
                next_val = int(result_list[i+1][0])
                if val + 1 != next_val:
                    while val + 1 < next_val:
                        val += 1
                        add_list.append(val)
                        print('Added to the sync list : {}'.format(val))

            if mx < redis_sasp:

                for i in range(mx + 1, redis_sasp + 1):
                    add_list.append(i)
                    print('Added to the sync list : {}'.format(i))

            print('\n')
            print('List of src_idx to be synchronized : {}'.format(add_list))
            
            for add in add_list:

                print('Actions for src_idx {} : \n'.format(add))

                # Insert scr_idx into TB_PRJ_DATA
                print("INSERT INTO TB_PRJ_DATA (prj_idx, project_id, prog_state_cd, src_idx, work_user, work_sdate, old_reset) VALUES ({}, {}, {}, {}, {}, {}, {})".format(prj_idx, project_id, 'WORK_ING', add, -1, time_now, 1))
                print('Added {} to the DataBase - TB_PRJ_DATA \n'.format(add))

                # Data_ID
                print('To get the data_id from the new src_idx')
                print("SELECT data_idx FROM TB_PRJ_DATA WHERE prj_idx={} AND project_id={} AND src_idx={} \n".format(prj_idx, project_id, add))

                # Insert scr_idx into TB_TMPL_IMG_LABEL_DATA
                print("INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx) VALUES ([data_idx from last insert], {}, {})".format(prj_idx, add))
                print('Added {} to the DataBase - TB_TMPL_IMG_LABEL_DATA \n'.format(add))


    else:
        print('Notification : REDIS counter and DB counter are the same! No need to synchronize')

def dry_run_multi_cnt(sync_needed_dict, project_id, prj_idx):
    keys = list(sync_needed_dict.keys())

    print('List of src_idx that need to be synchronized : {} \n'.format(keys))

    for key in keys: # key = source_id (src_idx)
        print('Count synchronizing process for src_idx : {} \n'.format(key))
        redis_maic = sync_needed_dict[key][0]
        db_cnt = sync_needed_dict[key][1]
        print('REDIS : {} DB_CNT : {} \n'.format(redis_maic, db_cnt))

        if redis_maic < db_cnt:
            k = 'maic_{}_{}'.format(task_id, key)
            v = redis_client.get(k)

            print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_maic, task_id, db_cnt))
            print('REDIS actions for src_idx {} : \n'.format(key))

            if v:
                print('Needed src_idx value : {}'.format(k))
                print('REDIS : set {} {} \n'.format(k, db_cnt))
            else:
                print("No Redis key: {} \n".format(k))

        elif redis_maic > db_cnt:
            print('DB Actions for src_idx {} : \n'.format(key))

            # Json_index
            print('To get the new json_index needed')
            print("SELECT json_index, max(json_index) FROM TB_TMPL_IMG_LABEL_DATA WHERE prj_idx={} AND src_idx={} GROUP BY json_index \n".format(prj_idx, key))

            for i in range(db_cnt + 1, redis_maic + 1):
                # Insert scr_idx into TB_PRJ_DATA
                print("INSERT INTO TB_PRJ_DATA (prj_idx, project_id, prog_state_cd, src_idx, work_user, work_sdate, old_reset) VALUES ({}, {}, {}, {}, {}, {}, {})".format(prj_idx, project_id, 'WORK_ING', key, -1, time_now, 1))
                print('Added src_idx {} to the DataBase - TB_PRJ_DATA \n'.format(key))

                # Data_ID
                print('To get the data_id from the new src_idx')
                print("SELECT data_idx FROM TB_PRJ_DATA WHERE prj_idx={} AND project_id={} AND src_idx={} \n".format(prj_idx, project_id, key))

                # Insert scr_idx into TB_TMPL_IMG_LABEL_DATA
                print("INSERT INTO TB_TMPL_IMG_LABEL_DATA (data_idx, prj_idx, src_idx, json_index) VALUES ([data_idx from last insert], {}, {}, [json_index])".format(prj_idx, key))
                print('Added src_idx {} to the DataBase - TB_TMPL_IMG_LABEL_DATA \n'.format(key))

        else:
            print('Notification : REDIS counter and DB counter are the same! \n')

def dry_run_multi_mx(sync_needed_dict, project_id, prj_idx):
    keys = list(sync_needed_dict.keys())

    actual_sync_list = []

    print('List of work_user that need to be synchronized : {} \n'.format(keys))

    for key in keys: # key = user_id (work_user) -> value = max src_idx
        print('Count synchronizing process for work_user : {} \n'.format(key))
        redis_mapc = sync_needed_dict[key][0]
        db_mx = sync_needed_dict[key][1]
        print('REDIS : {} DB_CNT : {} \n'.format(redis_mapc, db_mx))

        if db_mx == None or redis_mapc > db_mx:
            print('DB Actions for work_user {} : \n'.format(key))

            # Check if updated redis value is valid

            check_qry = "SELECT count(1) AS cnt FROM TB_PRJ_DATA WHERE prj_idx = {} AND src_idx = {}".format(task_id, redis_mapc)
            check_cmd = get_db_cmd(check_qry)
            check_result = subprocess.check_output(check_cmd, shell=True)

            if check_result:
                check_result = results_to_array(check_result)
                check_count = int(check_result[0][0])

            k = 'maic_{}_{}'.format(task_id, redis_mapc)
            v = redis_client.get(k)

            if v:
                compare_count = int(v)

            if compare_count <= check_count:
                # print('Since former REDIS pointer value was an update process the pointer was set back to the DB max - {} -> {}'.format(redis_mapc, db_mx))
                print('{} : REDIS - {} DB - {}'.format(redis_mapc, compare_count, check_count))
                print('Skip')
            else:
                actual_sync_list.append(redis_mapc)
                print("INSERT INTO TB_PRJ_DATA (prj_idx, project_id, prog_state_cd, src_idx, work_user, work_sdate, old_reset) VALUES ({}, {}, {}, {}, {}, {}, {})".format(prj_idx, project_id, 'WORK_ING', redis_mapc, key, time_now, 1))
                print('Added src_idx {} to the DataBase - TB_PRJ_DATA \n'.format(redis_mapc))

                # Data_ID
                print('To get the data_id from the new src_idx')
                print("SELECT data_idx FROM TB_PRJ_DATA WHERE prj_idx={} AND project_id={} AND src_idx={} \n".format(prj_idx, project_id, key))

                # Json_index
                print('To get the json_index needed')
                print("SELECT json_index FROM TB_TMPL_IMG_LABEL_DATA WHERE prj_idx={} AND src_idx={} \n".format(prj_idx, redis_mapc))

                print("INSERT INTO TB_TMPL_IMG_LABEL_DATA ([data_idx from the last insert], prj_idx, src_idx, [json_index]) VALUES ({}, {})".format(prj_idx, redis_mapc))
                print('Added src_idx {} to the DataBase - TB_TMPL_IMG_LABEL_DATA \n'.format(redis_mapc))

        elif redis_mapc < db_mx:
                k = 'mapc_{}_{}'.format(prj_idx, key)
                v = redis_client.get(k)
                actual_sync_list.append(redis_mapc)

                print('Redis({}):\t{}\nDB({}):\t{}\n'.format(k, redis_mapc, prj_idx, db_mx))
                print('REDIS actions for work_user {} :  \n'.format(key))

                if v:
                    print('Needed src_idx value : {}'.format(k))
                    print('REDIS : set {} {} \n'.format(k, db_mx))
                else:
                    print("No Redis key: {} \n".format(k))


        else:
            print('Notification : REDIS counter and DB counter are the same! \n')

    print(actual_sync_list)

def dry_run_multi(sync_needed_dict_cnt, sync_needed_dict_mx, project_id, prj_idx):
    print('Go Dry Run - Multi \n')

    print('Dry Run for src_idx assign count check process \n')

    if sync_needed_info_cnt != -1:
        dry_run_multi_cnt(sync_needed_dict_cnt, project_id, prj_idx)
    else:
        print('No sync needed for cnt')

    print('Dry Run for work_user assign max check process \n')

    if sync_needed_info_mx != -1:
        dry_run_multi_mx(sync_needed_dict_mx, project_id, prj_idx)
    else:
        print('No synce needed for mx')

    

logger.info('======================== START ==========================')
##############################
# 메인 비즈 로직; 조회
#   - 프로젝트 할당 속성 조회
#   - 단일 할당/다중 할당 정합성 체크
#   - 사용자 입력에 따라 정합 처리 수행
##############################

redis_client = redis.Redis(host='redis-t4iu.cdb.ntruss.com', port=6379, db=0)

# Get project_id from task_id (prj_idx)
get_projid_qry = "SELECT project_id FROM TB_PRJ_MST WHERE prj_idx={}".format(task_id)
get_projid_cmd = get_db_cmd(get_projid_qry)
logger.info(get_projid_cmd.replace(dbp, 'xxxxx'))
   
get_projid_result = subprocess.check_output(get_projid_cmd, shell=True)

if get_projid_result:
    get_projid_result_list = results_to_array(get_projid_result)
    logger.info('fetched: {}'.format(len(get_projid_result_list)))
    project_id = get_projid_result_list[0][0].decode('utf-8')

else:
    print('Can not find the project for this task - something is wrong!')
    sys.exit(1)

# Continue process
qry = "SELECT use_multi_assign FROM CW_PROJECT WHERE project_id={}".format(project_id)
cmd = get_db_cmd(qry)
logger.info(cmd.replace(dbp, 'xxxxx')) # logging에 비밀번호 노출 방지 -> xxxx로 임시 변환
result = subprocess.check_output(cmd, shell=True)
sync_needed_info = -1 # unknown
sync_needed_info_cnt = -1
sync_needed_info_mx = -1

if result:
   
    # 다중 할당 속성 정보
    results_list = results_to_array(result)
    len_list = len(results_list)
    logger.info('fetched: {}'.format(len_list))
    is_single_assign = (results_list[0][0] == b'0')


    ##################
    # 단일 할당 정합성 체크
    ##################
    if is_single_assign: #단일 할당
        sync_needed_info = single_assign_check()
        # 단순 tuple 형태로 return

    ##################
    # 다중 할당 정합성 체크
    ##################
    else:
        sync_needed_info_cnt = multi_assign_check_cnt()
        # 'source id : tuple' 형태의 dictionary return

        print('\n')

        sync_needed_info_mx = multi_assign_check_mx()
        # 'work_user : tuple' 형태의 dictionary return

    ##################
    # 정합성 처리
    ##################
    is_sync_needed = -1

    while (sync_needed_info != -1 or (sync_needed_info_cnt != -1 or sync_needed_info_mx != -1)) and is_sync_needed == -1: # dry run 실행 후 다시 확인해야해서 while로 처리
        input_command = input("sync? [Y/N/D]: ").strip().lower()
        # sync 진행
        if input_command == 'y':
            if is_single_assign:
                single_assign_sync(sync_needed_info, project_id, task_id)
            else:
                if sync_needed_info_cnt != -1:
                    multi_input_cmd_cnt = input("sync cnt? [Y/N/D]: ").strip().lower()
                    if multi_input_cmd_cnt == 'y':
                        multi_assign_sync_cnt(sync_needed_info_cnt, project_id, task_id)
                    elif multi_input_cmd_cnt == 'n':
                        print('Skipping sync process for multi - cnt')

                if sync_needed_info_mx != -1:
                    multi_input_cmd_mx = input("sync mx? [Y/N/D]: ").strip().lower()
                    if multi_input_cmd_mx == 'y':
                        multi_assign_sync_mx(sync_needed_info_mx, project_id, task_id)
                    elif multi_input_cmd_mx == 'n':
                        print('Skipping sync process for multi - mx')

            is_sync_needed = 0
        elif input_command == 'd':
            if is_single_assign:
                dry_run_single(sync_needed_info, project_id, task_id)
            else:
                dry_run_multi(sync_needed_info_cnt, sync_needed_info_mx, project_id, task_id)
        elif input_command == 'n':
            print('Ending the process - Bye!')
            sys.exit(0)
        else:
            print('I do not understand that command')
            
    if is_sync_needed == -1: #unknown
        if is_single_assign:
            if sync_needed_info == -1:
                print('No need to synchronize and ending - Bye!')
                sys.exit(0)
            else:
                print("something wrong")
                sys.exit(1)

        else:
            if sync_needed_info_cnt == -1 and sync_needed_info_mx == -1:
                print('No need to synchronize and ending - Bye!')
                sys.exit(0)
            else:
                print("something wrong")
                sys.exit(1)

    else:
        print('Synchronization done! - Bye!')
    

logger.info('======================== END ==========================')