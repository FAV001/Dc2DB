#import fdb
import firebirdsql as fdb
from configobj import ConfigObj
import datetime
import time
import logging
import csv
import random

cur_date = datetime.datetime.now()
"""читаем конфиг"""
config = ConfigObj('call.cfg')
server = config['ServerDB']['ServerName']
db_stat = config['ServerDB']['DB_Stat']
db_ib = config['ServerDB']['DB_IB']
user = config['ServerDB']['user']
userpass = config['ServerDB']['pass']
list_lot = config['WorkConfig']['Lot']
procent = float(config['WorkConfig']['Procent'])
LogFile = config['WorkConfig']['LogName'] + cur_date.strftime("%d-%m-%Y") + "temp.log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.DEBUG, filename = LogFile)
#logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)
logging.info(u'Запустили скрипт' )
timeDelay = random.randrange(0, 120)
logging.info(u'Задержка для выполнения скрипта %s' % timeDelay )
#time.sleep(timeDelay)

logging.info(u'Формируем переменные')
#cur_time = time.strptime(time.strftime("%H:%M:%S", time.localtime()), "%H:%M:%S")
#cur_time = time.time() - 60*5
s_cur_date = cur_date.strftime("%d.%m.%Y")
startDate = cur_date.strftime("%d.%m.%Y")
currentTime = cur_date.time()
currentTime_s = currentTime.strftime("%H:%M:%S")
CurrentPeriod = cur_date - datetime.timedelta(minutes=5)
CurrentPeriod_s = CurrentPeriod.strftime("%H:%M:%S")
cur_time = time.strptime(CurrentPeriod_s, "%H:%M:%S")
Date_Time = cur_date + datetime.timedelta(seconds=3)
Date_Time_s = Date_Time.strftime("%H:%M:%S")
Reg_Date_Time = currentTime
Reg_Date_Time_s = Reg_Date_Time.strftime("%H:%M:%S")
SAM_Date_Time = cur_date - datetime.timedelta(seconds=34)
SAM_Date_Time_s = SAM_Date_Time.strftime("%H:%M:%S")
SAM_Reg_Date_Time = cur_date + datetime.timedelta(seconds=1)
SAM_Reg_Date_Time_s = SAM_Reg_Date_Time.strftime("%H:%M:%S")

logging.info(u'Текущий временной интервал : %s' % CurrentPeriod_s) #str(cur_time.tm_hour) + ":" + str(cur_time.tm_min))
time5 = datetime.timedelta(minutes=5)
time0 = datetime.timedelta(minutes=0)
#print("current date ",cur_date)
#print("current time ",cur_time)
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
logging.info( u'Соединяемся с базой...' )
con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_stat = con_stat.cursor()
cur_ib = con_ib.cursor()
cur_ib_update = con_ib.cursor()
Select = "Select * from PHONES where LOT_NUMB in (%s) order by phone_id" % s_Lot

def create_time_struct(hex1,hex2):
    return time.strptime(str(hex1) + ":" + str(hex2) + ":00", "%H:%M:%S")

def create_time_string(time_struct):
    return time.strftime("%H:%M:%S",time_struct)

def addsecond(tm, secs):
    fulldate = datetime.datetime(100, 1, 1, tm.hour, tm.minute, tm.second)
    fulldate = fulldate + datetime.timedelta(seconds=secs)
    return fulldate.time()

def convertfromtimetodatetime(time_struct):
    return datetime.datetime(time_struct[0], time_struct[1], time_struct[2], time_struct[3], time_struct[4], time_struct[5])

def checktaksofonotzvon(phone_id):
    #con_stat1 = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    #cur_stat1 = con_stat1.cursor()
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, s_cur_date, s_cur_date)
    with cur_stat.execute(sql):
        cur_stat.fetchone()
        logging.debug('phone_id - %4s, count - %s' % (phone_id, cur_stat.rowcount))
        if cur_stat.rowcount > 0:
            return 1
        else:
            return 0

def codotzvona(phone_id):
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, s_cur_date, s_cur_date)
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[2])
            return r[2]
        else:
            return 1009

def current_count():
    con_ib1 = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
    cur_ib1 = con_ib1.cursor()
    sql = "Select phone_id from PHONES where LOT_NUMB in (%s) order by phone_id" % s_Lot
    with cur_ib1.execute(sql):
        row = cur_ib1.fetchone()
        count_current_otzvon = 0
        while row:
            var = checktaksofonotzvon(row[0])
            count_current_otzvon = count_current_otzvon + var
            row = cur_ib1.fetchone()
    con_ib1.close()
    return count_current_otzvon

cur_time_d = convertfromtimetodatetime(cur_time)
count_current_otzvon = 75
logging.info( "Количество отзвонившихся таксофонов : " + str(count_current_otzvon))
with cur_ib.execute(Select):
    row = cur_ib.fetchone()
    logging.info( "Количество таксофонов для отработки : " + str(cur_ib.rowcount))
    currern_procent = count_current_otzvon / cur_ib.rowcount * 100
    logging.info( "Текущий процент отзвона : " + str('% 6.2f' % currern_procent) + " из запланированного : " + str(procent))
    if currern_procent <= procent:
        logging.info('Еще не достигли запланированного процента отзвона. Приступаем к "накрутки"')
        i = 1
        y = 0
        logging.info("загружаем стоп-лист...")
        stoplist = []
        with open('Stoplist.csv', 'r') as f:
            sl = csv.reader(f)
            for r in sl:
                stoplist.append(r[0])
            logging.info("загрузили стоп-лист. в нем %s таксофонов" % len(stoplist))

        logging.info("=" * 50)
        while row:
            time_hex = row[16]
            phone_id = str(row[0])
            end_time1 = create_time_struct(time_hex[3],time_hex[4])
            end_time2 = create_time_struct(time_hex[9],time_hex[10])
            end_time3 = create_time_struct(time_hex[15],time_hex[16])
            end_time4 = create_time_struct(time_hex[21],time_hex[22])
            p1 = convertfromtimetodatetime(end_time1)
            p2 = convertfromtimetodatetime(end_time2)
            p3 = convertfromtimetodatetime(end_time3)
            p4 = convertfromtimetodatetime(end_time4)
            if ((cur_time_d - p1 <= time5) and (cur_time_d - p1 > time0)) or ((cur_time_d - p2 <= time5) and (cur_time_d - p2 > time0)) or ((cur_time_d - p3 <= time5) and (cur_time_d - p3 > time0)) or ((cur_time_d - p4 <= time5) and (cur_time_d - p4 > time0)):
                logging.info("  Таксофон id в текущем периоде: %4s" % row[0])
                y += 1
                #код формирования отзвона
                slot = str(row[23]).zfill(2)
                ver = row[29]
                work_sam = row[28]
                sql = "select first 1 BEFORE_ALLFAILS from ALLFAILS WHERE (BEFORE_ALLFAILS like '0x__%s' and REG_DATE_TIME > '%s 00:00') order by REG_DATE_TIME DESC" % (slot, s_cur_date)
                with cur_stat.execute(sql):
                    stat_row = cur_stat.fetchone()
                    last_code = stat_row[0][2:4:]
                    asDec = int(last_code,16)
                    asDec += 1
                    asHex = ('%X' % asDec).zfill(2)[:2]
                    new_code = '0x%s%s' % (asHex, slot)
                logging.warning("  sql для ALLFAILS")
                logging.warning("       PHONE_ID=%s, BEFORE_ALLFAILS=%s, AFTER_ALLFAILS=%s, DATE_TIME='%s %s', REG_DATE_TIME='%s %s'" % (phone_id, new_code, ver, s_cur_date, Date_Time_s, s_cur_date, Reg_Date_Time_s))
                cur_stat.execute("insert into ALLFAILS (PHONE_TYPE, PHONE_ID, FAIL_CODE, BEFORE_ALLFAILS, AFTER_ALLFAILS, DATE_TIME, REG_DATE_TIME) Values (77,%s,1005,'%s','%s','%s %s','%s %s');" % (phone_id, new_code, ver, s_cur_date, Date_Time_s, s_cur_date, Reg_Date_Time_s))
                cur_stat.execute("insert into ALLFAILS (PHONE_TYPE, PHONE_ID, FAIL_CODE, BEFORE_ALLFAILS, AFTER_ALLFAILS, DATE_TIME, REG_DATE_TIME) Values (77,%s,0,'%s','%s','%s %s','%s %s');" % (phone_id, new_code, ver, s_cur_date, Date_Time_s, s_cur_date, Reg_Date_Time_s))
                if (work_sam != 1):
                    logging.warning("       SAM отсутствует")
                    cur_stat.execute("insert into ALLFAILS (PHONE_TYPE, PHONE_ID, FAIL_CODE, BEFORE_ALLFAILS, AFTER_ALLFAILS, DATE_TIME, REG_DATE_TIME) Values (77,%s,1073741827,'','00000000','%s %s','%s %s');\n" % (phone_id, s_cur_date, SAM_Date_Time_s, s_cur_date, SAM_Reg_Date_Time_s))
                    pass
                con_stat.commit()
                sql_update = """
                    UPDATE PHONES SET 
                      STATUS = 'Таксофон отзвонился',
                      TIMECALL = '%s %s'
                    WHERE (PHONE_ID = %s);""" % (s_cur_date, Reg_Date_Time_s, phone_id)
                logging.warning("sql для PHONES")
                logging.warning(sql_update)
                cur_ib_update.execute(sql_update)
                con_ib.commit()
                print("Таксофон id : %4s" % row[0])
                logging.info("-" * 50)
                #изменяем переменные время
                rnd =  random.randrange(4, 15)
                logging.info("Смещение времени сек -> %s" % rnd)
                Date_Time = Date_Time + datetime.timedelta(seconds=rnd)
                Date_Time_s = Date_Time.strftime("%H:%M:%S")
                Reg_Date_Time = addsecond(Reg_Date_Time, rnd)
                Reg_Date_Time_s = Reg_Date_Time.strftime("%H:%M:%S")
                SAM_Date_Time = addsecond(Reg_Date_Time, -34)
                SAM_Date_Time_s = SAM_Date_Time.strftime("%H:%M:%S")
                SAM_Reg_Date_Time = addsecond(Reg_Date_Time, 1)
                SAM_Reg_Date_Time_s = SAM_Reg_Date_Time.strftime("%H:%M:%S")
            row = cur_ib.fetchone()
            i = i + 1
        if y > 0:
            logging.info("Количество таксофонов в периоде : " + str(y))
        else:
            logging.info("Таксофонов нет в периоде")
        pass
    else:
        logging.info('Достигли запланированного процента отзвона. Прекращаем работу.')
        pass
con_ib.close()
con_stat.close()