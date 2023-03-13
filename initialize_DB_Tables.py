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
