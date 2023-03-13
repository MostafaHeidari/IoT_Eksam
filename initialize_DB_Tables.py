#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# website that we will look
#1-https://www.digi.com/resources/documentation/Digidocs/90001541/reference/r_example_subscribe_mqtt.htm
#2- https://cedalo.com/blog/configuring-paho-mqtt-python-client-with-examples/
#3- https://techoverflow.net/2021/12/27/how-to-set-username-password-in-paho-mqtt/
#https://flespi.com/mqtt-broker
# https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# #
#------------------------------------------

import sqlite3

# SQLite DB Name
DB_Name =  "BlindDb.db"

# SQLite DB Table Schema
TableSchema="""
drop table if exists DHT22_Temperature_Data ;
create table blind_mqtt_data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  warningddee text
);
"""

#Connect or Create DB File
conn = sqlite3.connect(DB_Name)
curs = conn.cursor()

#Create Tables
sqlite3.complete_statement(TableSchema)
curs.executescript(TableSchema)

#Close DB
curs.close()
conn.close()
