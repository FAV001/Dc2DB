#import firebirdsql as fdb
import fdb
from configobj import ConfigObj
import datetime
import logging
import os
cur_date = datetime.datetime.now()
cur_date1 = datetime.datetime.now() - datetime.timedelta(days=2)
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
LogFile = os.getcwd() + "\\Log\\newcall" + cur_date.strftime("%d.%m.%Y") + ".log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.DEBUG, filename = LogFile)
#logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)
logging.info(u'Запустили скрипт' )

s_Lot = ""
i = 1
len_lot = len(list_lot)
for single_lot in list_lot:
    if i != len_lot:
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

Select = "Select * from PHONES where LOT_NUMB not in (%s) order by timecall" % s_Lot

cur_ib.execute(Select)
desc = cur_ib.description
column_names = [col[0] for col in desc]
data = [dict(zip(column_names, row))  
        for row in cur_ib.fetchall()]
y = 0
for key in data:
    if ((key['TIMECALL'] != None) and (key['TIMECALL'] > cur_date1)) :
        y += 1
        logging.info("=" * 80)
        logging.info("Phone_id -> %s" % key['PHONE_ID'])
        logging.info("time last call -> %s" % key['TIMECALL'])
        logging.info("адрес установки -> %s" % key['ADDRESS'])
        logging.info("-" * 80)
if (y != 0):
    logging.info(u"Отзваниваются %s таксофонов, и они не включены в контракт" % y)
logging.info(u'Закрыли соединения с базой' )
con_ib.close()
logging.info(u"время работы скрипта - %s" % (datetime.datetime.now() - startTime))
logging.info("~" * 80)

