"""import fdb"""
import firebirdsql as fdb
from configobj import ConfigObj
import datetime
import time

"""читаем конфиг"""

config = ConfigObj('call.cfg')
server = config['ServerDB']['ServerName']
db_stat = config['ServerDB']['DB_Stat']
db_ib = config['ServerDB']['DB_IB']
user = config['ServerDB']['user']
userpass = config['ServerDB']['pass']
#lot = ', '.join(config['WorkConfig']['Lot'])
list_lot = config['WorkConfig']['Lot']
s_Lot = ""
i = 1
cur_date = datetime.date.today()
#cur_time = time.strptime(time.strftime("%H:%M:%S", time.localtime()), "%H:%M:%S")
#cur_time = time.time() - 60*5
cur_time = time.strptime(time.strftime("%H:%M:%S", time.localtime(time.time() - 60*5)), "%H:%M:%S")
time5 = datetime.timedelta(minutes=5)
time0 = datetime.timedelta(minutes=0)
print("current date ",cur_date)
print("current time ",cur_time)
len_lot = len(list_lot)
for single_lot in list_lot:
    if i !=len_lot:
        s_Lot = s_Lot + "'" + str(single_lot) + "',"
        i = i + 1
        continue
    else:
        s_Lot = s_Lot + "'" + str(single_lot) + "'"
        break
    
#s_Lot = "'" + str(list_lot[0]) + "'"
lot = ', '.join(map(str,config['WorkConfig']['Lot']))

con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_stat = con_stat.cursor()
cur_ib = con_ib.cursor()
myquery = "select * from PHONES where LOT_NUMB in (%s)" % s_Lot
Select = "Select * from PHONES where LOT_NUMB in (%s) order by phone_id" % s_Lot

def create_time_struct(hex1,hex2):
    return time.strptime(str(hex1) + ":" + str(hex2) + ":00", "%H:%M:%S")

def create_time_string(time_struct):
    return time.strftime("%H:%M:%S",time_struct)

def convertfromtimetodatetime(time_struct):
    return datetime.datetime(time_struct[0], time_struct[1], time_struct[2], time_struct[3], time_struct[4], time_struct[5])

cur_time_d = convertfromtimetodatetime(cur_time)
with cur_ib.execute(Select):
    row = cur_ib.fetchone()
    print(cur_ib.rowcount)
    i = 1
    while row:
        time_hex = row[16]
        end_time1 = create_time_struct(time_hex[3],time_hex[4])
        end_time2 = create_time_struct(time_hex[9],time_hex[10])
        end_time3 = create_time_struct(time_hex[15],time_hex[16])
        end_time4 = create_time_struct(time_hex[21],time_hex[22])
        p1 = convertfromtimetodatetime(end_time1)
        p2 = convertfromtimetodatetime(end_time2)
        p3 = convertfromtimetodatetime(end_time3)
        p4 = convertfromtimetodatetime(end_time4)
        if ((cur_time_d - p1 <= time5) and (cur_time_d - p1 > time0)) or ((cur_time_d - p2 <= time5) and (cur_time_d - p2 > time0)) or ((cur_time_d - p3 <= time5) and (cur_time_d - p3 > time0)) or ((cur_time_d - p4 <= time5) and (cur_time_d - p4 > time0)):
            print(str(i) + " Таксофон id : " + str(row[0]))
            print()
        # end_time2 = create_time_struct(time_hex[9],time_hex[10])
        # p = convertfromtimetodatetime(end_time2)
        # if (cur_time_d - p <= time5) and (cur_time_d - p > time0):
        #     print(str(i) + " Таксофон id : " + str(row[0]) + " ;time call - " + create_time_string(end_time2))
        #     print()
        # end_time3 = create_time_struct(time_hex[15],time_hex[16])
        # p = convertfromtimetodatetime(end_time3)
        # if (cur_time_d - p <= time5) and (cur_time_d - p > time0):
        #     print(str(i) + " Таксофон id : " + str(row[0]) + " ;time call - " + create_time_string(end_time3))
        #     print()
        # end_time4 = create_time_struct(time_hex[21],time_hex[22])
        # p = convertfromtimetodatetime(end_time4)
        # if (cur_time_d - p <= time5) and (cur_time_d - p > time0):
        #     print(str(i) + " Таксофон id : " + str(row[0]) + " ;time call - " + create_time_string(end_time4))
        #     print()
        row = cur_ib.fetchone()
        i = i + 1

con_ib.close()
con_stat.close()