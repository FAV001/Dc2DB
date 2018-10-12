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

logging.info(u'Соединяемся с базой...' )
con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_stat = con_stat.cursor()
cur_ib = con_ib.cursor()

sql = f"""SELECT PH.PHONE_ID,
  ph.address,
  ph.status,
  '' as lastcall,
  PH.LOT_NUMB,
  PH.COMPETITION_NUMB,
  (SELECT NAME_SERVICEMAN from SERVICEMAN s where PH.SERVICEMAN_ID = s.SERVICEMAN_ID)
FROM PHONES PH order by phone_id"""

cur_ib.execute(sql)
desc = cur_ib.description
column_names = [col[0] for col in desc]
data = [dict(zip(column_names, row))  
        for row in cur_ib.fetchall()]
y = 0
statjurnal = "select phone_id, MAX(datetime) as CALL from STATCONNECTJOURNAL group by phone_id"
stat = {}
cur_stat.execute(statjurnal)
row = cur_stat.fetchone()
while row:
    stat[row[0]] = row[1]
    row = cur_stat.fetchone()




for key in data:
#    sql_stat = "select * from STATCONNECTJOURNAL where phone_id = %s order by datetime desc" % key['PHONE_ID']
    print(key['PHONE_ID'])
    var = key['PHONE_ID']
    if key['PHONE_ID'] in stat.keys():
        key['LASTCALL'] = (stat[key['PHONE_ID']]).strftime("%Y-%m-%d %H:%M:%S")



csv_file = os.getcwd() + "\\allphones" + cur_date.strftime("%d.%m.%Y") + ".csv"
csv_columns = data[0].keys()
with open(csv_file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns,delimiter=';')
    writer.writeheader()
    for row in data:
        writer.writerow(row)
logging.info(u'Закрыли соединения с базой' )
con_ib.close()
con_stat.close()
logging.info("время работы скрипта - %s" % (datetime.datetime.now() - startTime))
logging.info("~" * 80)

