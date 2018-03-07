''' import configparser

config = configparser.RawConfigParser()

config.add_section('ServerDB')
config.add_section('WorkConfig')
config.set('ServerDB', 'ServerName', 'wdvam0020146')
config.set('ServerDB', 'DB_Stat', r'c:\Supervis\DB_IB\DC2_STAT.GDB')
config.set('ServerDB', 'DB_IB', r'c:\Supervis\DB_IB\DC2_IB1.GDB')
config.set('ServerDB', 'user', 'sysdba')
config.set('ServerDB', 'pass', 'tariffib')
config.set('WorkConfig', 'Lot', ['1','2','3'])
with open('call.cfg', 'w') as configfile:
    config.write(configfile) '''

from configobj import ConfigObj
config = ConfigObj()
config.filename = 'call.cfg'
#
config['ServerDB'] = {}
config['ServerDB']['ServerName'] = 'wdvam0020146'
config['ServerDB']['DB_Stat'] = r'c:\Supervis\DB_IB\DC2_STAT.GDB'
config['ServerDB']['DB_IB'] = r'c:\Supervis\DB_IB\DC2_IB1.GDB'
config['ServerDB']['user'] = 'sysdba'
config['ServerDB']['pass'] = 'tariffib'
#
config['WorkConfig'] = {}
config['WorkConfig']['Lot'] = ['1', '2', '3']
#
config.write()

