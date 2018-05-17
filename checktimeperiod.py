#import firebirdsql as fdb
import fdb
from configobj import ConfigObj
import datetime
import os
import logging
import time
import sys
import csv

cur_date = datetime.datetime.now()
s_cur_date = cur_date.strftime("%d.%m.%Y")
startTime = datetime.datetime.now()

#"""читаем конфиг"""
config = ConfigObj('call.cfg')
server = config['ServerDB']['ServerName']
db_stat = config['ServerDB']['DB_Stat']
db_ib = config['ServerDB']['DB_IB']
user = config['ServerDB']['user']
userpass = config['ServerDB']['pass']
list_lot = config['WorkConfig']['Lot']
#procent = float(config['WorkConfig']['Procent'])
LogFile = os.getcwd() + "\\Log\\checktime" + cur_date.strftime("%d.%m.%Y") + ".log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.DEBUG, filename = LogFile)
#logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)
logging.info(u'Запустили скрипт' )

def create_time_struct(hex1,hex2,hex3):
    return time.strptime(("%s:%s:%s") % (str(hex1),str(hex2),str(hex3)), "%H:%M:%S")

s_Lot = ""
i = 1
len_lot = len(list_lot)
for single_lot in list_lot:
    if i !=len_lot:
        s_Lot = s_Lot + "'" + str(single_lot) + "',"
        i = i + 1
        continue
    else:
        s_Lot = s_Lot + "'" + str(single_lot) + "'"
        break
    
lot = ', '.join(map(str,config['WorkConfig']['Lot']))

logging.info(u'Соединяемся с базой...' )
con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_ib = con_ib.cursor()
cur_ib_update = con_ib.cursor()

Select = "Select * from PHONES where LOT_NUMB in (%s) order by phone_id" % s_Lot
#    PH.LINE_NUMB,

sql = "SELECT PH.PHONE_ID,PH.RMS_TIME FROM PHONES PH WHERE LOT_NUMB in (%s) order by phone_id" % s_Lot
#sql = "SELECT PH.PHONE_ID,PH.RMS_TIME FROM PHONES PH order by phone_id"
cur_ib.execute(sql)
desc = cur_ib.description
column_names = [col[0] for col in desc]
data = [dict(zip(column_names, row))  
        for row in cur_ib.fetchall()]
y = 0
for key in data:
    #print(  "{}".format(key))
    temp_time = [0]*24
    time_hex = key['RMS_TIME']
    for i in range (len(time_hex)):
        temp_time[i] = time_hex[i]
    #temp_time = time_hex
    var_time = []
    if (time_hex[0] != time_hex[3]) or (time_hex[1] != time_hex[4]) or (time_hex[2] != time_hex[5]):
        var_time.append(time_hex[0])
    if (time_hex[6] != time_hex[9]) or (time_hex[7] != time_hex[10]) or (time_hex[8] != time_hex[11]):
        var_time.append(time_hex[6])
    if (time_hex[12] != time_hex[15]) or (time_hex[13] != time_hex[16]) or (time_hex[14] != time_hex[17]):
        var_time.append(time_hex[12])
    if (time_hex[18] != time_hex[21]) or (time_hex[19] != time_hex[22]) or (time_hex[20] != time_hex[23]):
        var_time.append(time_hex[18])
    n = 1
    check = False
    while n < len(var_time):
        for i in range (len(var_time)-n):
            if var_time[i] > var_time[i+1]:
                check = True
                print('не верно настроено расписание %s' % key['PHONE_ID'])
                var_time[i],var_time[i+1] = var_time[i+1],var_time[i]
                a1 = i * 6
                b1 = (i + 1) * 6
                print(temp_time[a1])
                print(temp_time[b1])
                temp_time[a1],temp_time[b1] = temp_time[b1],temp_time[a1]
                temp_time[a1+1],temp_time[b1+1] = temp_time[b1+1],temp_time[a1+1]
                temp_time[a1+2],temp_time[b1+2] = temp_time[b1+2],temp_time[a1+2]
                temp_time[a1+3],temp_time[b1+3] = temp_time[b1+3],temp_time[a1+3]
                temp_time[a1+4],temp_time[b1+4] = temp_time[b1+4],temp_time[a1+4]
                temp_time[a1+5],temp_time[b1+5] = temp_time[b1+5],temp_time[a1+5]
        n += 1
    if (check):
        y += 1
        print('Произвели упорядочивание, надо записать строку')
        hex_string = bytes(temp_time)
        logging.info("=" * 80)
        logging.info("Phone_id -> %s" % key['PHONE_ID'])
        logging.info("old RMS_TIME [%s]" % time_hex)
        logging.info("new RMS_TIME [%s]" % hex_string)
        logging.info("-" * 80)
        sql_update = """
            UPDATE PHONES SET 
            RMS_TIME = %s
            WHERE (PHONE_ID = %s);""" % (hex_string, key['PHONE_ID'])
        logging.warning("sql для PHONES")
        logging.warning(sql_update)
#        cur_ib_update.execute(sql_update)
        cur_ib_update.execute("UPDATE PHONES SET RMS_TIME = ? WHERE (PHONE_ID = ?)", (hex_string, key['PHONE_ID']))
        con_ib.commit()


logging.info("Обработали %s таксофонов" % y)
# csv_file = os.getcwd() + "\\Names" + cur_date.strftime("%d.%m.%Y") + ".csv"
# csv_columns = data[0].keys()
# for key1 in key.keys():
#     csv_columns.append(key1)
#     print(key1)
# with open(csv_file, 'w', newline='') as csvfile:
#     writer = csv.DictWriter(csvfile, fieldnames=csv_columns,delimiter=';')
#     writer.writeheader()
#     for row in data:
#         writer.writerow(row)
logging.info(u'Закрыли соединения с базой' )
con_ib.close()
logging.info("время работы скрипта - %s" % (datetime.datetime.now() - startTime))
logging.info("~" * 80)

