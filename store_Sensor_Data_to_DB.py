#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------


import json
import sqlite3

# SQLite DB Name
DB_Name =  "IoT.db"

#===============================================================
# Database Manager Class

class DatabaseManager():
	def __init__(self):
		self.conn = sqlite3.connect(DB_Name)
		self.conn.execute('pragma foreign_keys = on')
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

# Function to save Temperature to DB Table
def Eletricity_Data_Handler(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	SensorID = json_Dict['Sensor_ID']
	Data_and_Time = json_Dict['Date']
	KWh = json_Dict['KWh']
	
	#Push into DB Table
	dbObj = DatabaseManager()
	dbObj.add_del_update_db_record("insert into Eletricity_Data (SensorID, Date_n_Time, KWh) values (?,?,?)",[SensorID, Data_and_Time, KWh])
	del dbObj
	print "Inserted Eletricity Data into Database."
	print ""

# Function to save Humidity to DB Table
def Water_Data_Handler(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	SensorID = json_Dict['Sensor_ID']
	Data_and_Time = json_Dict['Date']
	m3 = json_Dict['m3']
	
	#Push into DB Table
	dbObj = DatabaseManager()
	dbObj.add_del_update_db_record("insert into Water_Data (SensorID, Date_n_Time, m3) values (?,?,?)",[SensorID, Data_and_Time, m3])
	del dbObj
	print "Inserted Water Data into Database."
	print ""


#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	if Topic == "Realty/00a-4erT-wTy/Eletricity":
		Eletricity_Data_Handler(jsonData)
	elif Topic == "Realty/00a-4erT-wTy/Water":
		Water_Data_Handler(jsonData)	

#===============================================================
