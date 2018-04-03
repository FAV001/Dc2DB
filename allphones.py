import firebirdsql as fdb
from configobj import ConfigObj
import datetime
import os
import logging
import time
import sys
import csv

cur_date = datetime.datetime.now() - datetime.timedelta(days=1)
s_cur_date = cur_date.strftime("%d.%m.%Y")
startTime = datetime.datetime.now()

#"""читаем конфиг"""
config = ConfigObj('call.cfg')
server = config['ServerDB']['ServerName']
db_stat = config['ServerDB']['DB_Stat']
db_ib = config['ServerDB']['DB_IB']
user = config['ServerDB']['user']
userpass = config['ServerDB']['pass']
LogFile = os.getcwd() + "\\Log\\allphones" + cur_date.strftime("%d.%m.%Y") + ".log"
#logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.DEBUG, filename = LogFile)
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)
logging.info(u'Запустили скрипт' )

def create_time_struct(hex1,hex2,hex3):
    return time.strptime(("%s:%s:%s") % (str(hex1),str(hex2),str(hex3)), "%H:%M:%S")

def checktaksofonotzvon(phone_id):
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, s_cur_date, s_cur_date)
    with cur_stat.execute(sql):
        cur_stat.fetchone()
        #logging.debug('phone_id - %4s, count - %s' % (phone_id, cur_stat.rowcount))
        if cur_stat.rowcount > 0:
            return 1
        else:
            return 0

def countallfais(phone_id):
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, s_cur_date, s_cur_date)
    with cur_stat.execute(sql):
        cur_stat.fetchone()
        return cur_stat.rowcount

def codotzvona(phone_id):
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, s_cur_date, s_cur_date)       
    
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[2])
            return r[2]
        else:
            return 1009

logging.info(u'Соединяемся с базой...' )
con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_stat = con_stat.cursor()
cur_ib = con_ib.cursor()

Select = "Select * from PHONES where LOT_NUMB in (%s) order by phone_id" % s_Lot
#    PH.LINE_NUMB,

sql = f"""SELECT PH.PHONE_ID,
    PH.STATUS as STATUS_TEXT,
    0 as STATUS,
    255 as ERRORCODE, 
    0 as COUNTTIMEPERIOD,
    0 as COUNTALLFAILS,
    PH.RMS_TIME,
    PH.RMS_NUM,
    'Не отзвонился' as ERRORTEXT,
    PH.ADDRESS,
    PH.TIMECALL,
    PH.SLOT_ID,
    PH.WORK_SAM,
    PH.VERSIONPROG,
    PH.LOT_NUMB,
    PH.COMPETITION_NUMB,
    (SELECT TEXT_DISTRICT from district d where PH.DISTRICT_ID = d.DISTRICT_ID),
    (SELECT NAME_SERVICEMAN from SERVICEMAN s where PH.SERVICEMAN_ID = s.SERVICEMAN_ID)
    FROM PHONES PH
    WHERE LOT_NUMB in (%s)
    order by phone_id""" % s_Lot
cur_ib.execute(sql)
desc = cur_ib.description
column_names = [col[0] for col in desc]
data = [dict(zip(column_names, row))  
        for row in cur_ib.fetchall()]
#with cur_ib.execute(Select):
#    #прочитаем все записи в Dict
for key in data:
    #print(  "{}".format(key))
    time_hex = key['RMS_TIME']
    timeperiod = 0
    if (time_hex[0] != time_hex[3]) or (time_hex[1] != time_hex[4]) or (time_hex[2] != time_hex[5]):
        timeperiod += 1
    if (time_hex[6] != time_hex[9]) or (time_hex[7] != time_hex[10]) or (time_hex[8] != time_hex[11]):
        timeperiod += 1
    if (time_hex[12] != time_hex[15]) or (time_hex[13] != time_hex[16]) or (time_hex[14] != time_hex[17]):
        timeperiod += 1
    if (time_hex[18] != time_hex[21]) or (time_hex[19] != time_hex[22]) or (time_hex[20] != time_hex[23]):
        timeperiod += 1
    key['COUNTTIMEPERIOD'] = timeperiod
    key['ERRORCODE'] = codotzvona(key['PHONE_ID'])
    key['COUNTALLFAILS'] = countallfais(key['PHONE_ID'])
    key['STATUS'] = checktaksofonotzvon(key['PHONE_ID'])
    if (key['COUNTALLFAILS'] >= key['COUNTTIMEPERIOD']) or (key['COUNTALLFAILS'] > 1):
        key['1TRUECALL'] = 1
        pass
    else:
        key['1TRUECALL'] = 0
        pass        
    print(key['PHONE_ID'], key['COUNTTIMEPERIOD'], key['COUNTALLFAILS'], key['1TRUECALL'])

csv_file = os.getcwd() + "\\Names" + cur_date.strftime("%d.%m.%Y") + ".csv"
# ERRORCODE
#     COUNTTIMEPERIOD,
#     0 as COUNTALLFAILS,
#     PH.RMS_TIME,
#     PH.RMS_NUM,
#     'Не отзвонился' as ERRORTEXT,
#     PH.ADDRESS,
#     PH.TIMECALL,
#     PH.SLOT_ID,
#     PH.WORK_SAM,
#     PH.VERSIONPROG,
#     PH.LOT_NUMB,
#     PH.COMPETITION_NUMB,
#     (SELECT TEXT_DISTRICT from district d where PH.DISTRICT_ID = d.DISTRICT_ID),
#     (SELECT NAME_SERVICEMAN from SERVICEMAN s where PH.SERVICEMAN_ID = s.SERVICEMAN_ID)
#     1TRUECALL
csv_columns = ['PHONE_ID','STATUS_TEXT','STATUS','ERRORCODE','COUNTTIMEPERIOD','COUNTALLFAILS','ERRORTEXT','ADDRESS','TIMECALL','1TRUECALL']
with open(csv_file, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in key:
        writer.writerow(data)
logging.info(u'Закрыли соединения с базой' )
con_ib.close()
con_stat.close()
logging.info("время работы скрипта - %s" % (datetime.datetime.now() - startTime))
logging.info("~" * 80)

