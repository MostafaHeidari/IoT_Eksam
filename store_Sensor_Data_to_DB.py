import json
import sqlite3

# SQLite DB Name
DB_Name =  "BlindDb.db"

#===============================================================
# Database Manager Class

class DatabaseManager():
	def __init__(self):
		self.conn = sqlite3.connect(DB_Name)
		self.conn.commit()
		self.cur = self.conn.cursor()

	def add_del_update_db_record(self, sql_query, args=()):
		self.cur.execute(sql_query, args)
		self.conn.commit()
		return

	def __del__(self):
		self.cur.close()
		self.conn.close()

#===============================================================
# Functions to push Sensor Data into Database

# Function to save BlindData to DB Table
def DHT22_Blind_Data_Handler(jsonData):
	#Parse Data
	json_Dict = json.loads(jsonData)
	SensorID = json_Dict['Sensor_ID']
	Data_and_Time = json_Dict['Date']
	Distance = json_Dict['Distance']

	#Push into DB Table
	dbObj = DatabaseManager()
	dbObj.add_del_update_db_record("insert into blind_mqtt_data (SensorID, Date_n_Time, Distance) values (?,?,?)",[SensorID, Data_and_Time, Distance])
	del dbObj
	print("Inserted Distance Data into Database.")
	print("")

#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	if Topic == "BlindData/warningddee":
		DHT22_Blind_Data_Handler(jsonData)
	else: return None,

