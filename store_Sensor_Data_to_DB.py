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

# Function to save Eletricity to DB Table
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

# Function to update Eletrical Consumption in DB Table
def Eletrical_Consumption_Data_Handler(jsonData):
	#Start Data Handling
	print "Reading json data"
	json_Dict = json.loads(jsonData)
	print json_Dict
	wattmeterId = json_Dict['wattmeterId']
	oldRecord = 0
	oldConsumption = 0
	

	#Pull eletrical data from DB
	dbObj = DatabaseManager()
	dbObj.cur.execute('select count(*) from Eletricity_Consumption_Data')
	dt = dbObj.cur.fetchall()

	count = dt[0][0]
	#if data exists
	if count > 0:
		dbObj.cur.execute('select * from Eletricity_Consumption_Data where wattmeterId = "%s"' % wattmeterId)
		row = dbObj.cur.fetchone()
		print "Fetched Last Eletrical Consumption Data in Database."
		print ""
		oldRecord = float(row['lastRecord'])
		oldConsumption = float(row['consumption'])
		#update record with new values formatting
		newRecord= "{0:.4f}".format(json_Dict['lastRecord'] + oldRecord)
		newConsumption = "{0:.4f}".format(json_Dict['consumption'] + oldConsumption)
		#Push data tp eletrycal consumption data
		data = (newRecord, newConsumption, wattmeterId)
		dbObj.cur.execute("update Eletricity_Consumption_Data set lastRecord = ? consumption = ? where wattmeterId = ?", data)
		dbObj.conn.commit()
		del dbObj
		print "Updated Eletrical Consumption Data"
		print ""

	else:
		#create new Data
		print 'Creating first data'
		print ''
		firstRecord = json_Dict['lastRecord'] + oldRecord
		firstConsumption = json_Dict['consumption'] + oldConsumption
		
		firstRecord = "{0:.4f}".format(firstRecord)
		firstConsumption = "{0:.4f}".format(firstConsumption)
		#Push data to eletrical consumption data
		dbObj.add_del_update_db_record("insert into Eletricity_Consumption_Data (wattmeterId, lastRecord, consumption) values (?,?,?)",[wattmeterId, firstRecord, firstConsumption])
		del dbObj
		print "Inserted First Eletrical Consumption Data into Database."
		print ""

# Function to update Water Consumption in DB Table
def Water_Consumption_Data_Handler(jsonData):
	#Start Data Handling
	print "Reading json data"
	json_Dict = json.loads(jsonData)
	print json_Dict
	hydrometerId = json_Dict['hydrometerId']
	oldRecord = 0
	oldConsumption = 0
	
	#Pull water data from DB
	dbObj = DatabaseManager()
	dbObj.cur.execute('select count(*) from Water_Consumption_Data')
	dt = dbObj.cur.fetchall()

	count = dt[0][0]
	#if data exists
	if count > 0:
		dbObj.cur.execute('select * from Water_Consumption_Data where hydrometerId = "%s"' % hydrometerId)
		row = dbObj.cur.fetchone()
		print "Fetched Last Water Consumption Data in Database."
		print ""
		oldRecord = float(row['lastRecord'])
		oldConsumption = float(row['consumption'])
		#update record with new values formatting
		newRecord = "{0:.4f}".format(json_Dict['lastRecord'] + oldRecord)
		newConsumption = "{0:.4f}".format(json_Dict['consumption'] + oldConsumption)
		#Push data to water consumption data
		data = (newRecord, newConsumption, hydrometerId)
		dbObj.cur.execute("update Water_Consumption_Data set lastRecord = ? consumption = ? where hydrometerId = ?", data)
		dbObj.conn.commit()
		del dbObj
		print "Updated Water Consuption Data"
		print ""

	#creates first record
	else:
		#create record
		print 'Creating first data'
		print ''
		firstRecord = json_Dict['lastRecord'] + oldRecord
		firstConsumption = json_Dict['consumption'] + oldConsumption

		firstRecord = "{0:.4f}".format(firstRecord)
		firstConsumption = "{0:.4f}".format(firstConsumption)
		#Push data to water consumption data
		dbObj.add_del_update_db_record("insert into Water_Consumption_Data (hydrometerId, lastRecord, consumption) values (?,?,?)", [hydrometerId, firstRecord, firstConsumption])
		del dbObj
		print "Inserted First Water Consumption Data into Database."
		print ""

# Function to save Water to DB Table
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
	print "Handling MQTT Data insertion to Database..."

	if Topic == "Realty/00a-4erT-wTy/Eletricity":
		print "Eletricity Data"
		print ""
		Eletricity_Data_Handler(jsonData)
	elif Topic == "Realty/00a-4erT-wTy/Water":
		print "Water Data"
		print ""
		Water_Data_Handler(jsonData)
	elif Topic == "Realty/00a-4erT-wTy/Eletrical_Consumption":
		print "Eletrical Consumption Data"
		print ""
		Eletrical_Consumption_Data_Handler(jsonData)
	elif Topic == "Realty/00a-4erT-wTy/Water_Consumption":
		print "Water Consumption Data"
		print ""
		Water_Consumption_Data_Handler(jsonData)

#===============================================================
