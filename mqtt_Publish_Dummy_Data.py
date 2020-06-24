#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------


import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime

#====================================================
# MQTT Settings 
MQTT_Broker = "mqtt.eclipse.org"
MQTT_Port = 1883
Keep_Alive_Interval = 60
MQTT_Topic_Eletricity = "Realty/00a-4erT-wTy/Eletricity"
MQTT_Topic_Water = "Realty/00a-4erT-wTy/Water"

MQTT_Topic_Eletrical_Consumption = "Realty/00a-4erT-wTy/Eletrical_Consumption"
MQTT_Topic_Water_Consumption = "Realty/00a-4erT-wTy/Water_Consumption"

#====================================================

def on_connect(client, userdata, flags, rc):
	if rc != 0:
		pass
		print "Unable to connect to MQTT Broker..."
	else:
		print "Connected with MQTT Broker: " + str(MQTT_Broker)

def on_publish(client, userdata, mid):
	pass
		
def on_disconnect(client, userdata, rc):
	if rc !=0:
		pass
		
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))		

		
def publish_To_Topic(topic, message):
	mqttc.publish(topic,message)
	print ("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
	print ""


#====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker

toggle = 0
wattElapsed = 0
hydroElapsed = 0
wattmeterConsumption = 0
hydrometerConsumption = 0

def publish_Fake_Sensor_Values_to_MQTT():
	threading.Timer(5.0, publish_Fake_Sensor_Values_to_MQTT).start()
	global toggle
	global wattElapsed
	global hydroElapsed
	global wattmeterConsumption
	global hydrometerConsumption

	if toggle == 0:
		wattElapsed += 5
		Wattmeter_Fake_Value = float("{0:.4f}".format(random.uniform(0.0091, 0.0118)))

		Wattmeter_Data = {}
		Wattmeter_Data['Sensor_ID'] = "Wattmeter@00a-4erT-wTy"
		Wattmeter_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
		Wattmeter_Data['KWh'] = Wattmeter_Fake_Value
		wattmeter_json_data = json.dumps(Wattmeter_Data)


		print "Publishing Wattmeter@00a-4erT-wTy Value: " + str(Wattmeter_Fake_Value) + "..."
		publish_To_Topic (MQTT_Topic_Eletricity, wattmeter_json_data)
		
		wattmeterConsumption += Wattmeter_Fake_Value

		if ((wattElapsed % 15) == 0):
			wattElapsed = 0

			watt_consumption = {}
			watt_consumption['wattmeterId'] = Wattmeter_Data['Sensor_ID']
			watt_consumption['lastRecord'] = Wattmeter_Data['Date']
			watt_consumption['consumption'] = wattmeterConsumption
			watt_consumption_json = json.dumps(watt_consumption)

			print "Publishing 00a-4erT-wTy Eletrical Consumption: " + str(wattmeterConsumption) + "..."
			publish_To_Topic (MQTT_Topic_Eletrical_Consumption, watt_consumption_json)
			wattmeterConsumption = 0

		toggle = 1

	else:
		hydroElapsed += 5
		Hydrometer_Fake_Value = float("{0:.4f}".format(random.uniform(0.0004, 0.0006)))

		Hydrometer_Data = {}
		Hydrometer_Data['Sensor_ID'] = "Hydrometer@00a-4erT-wTy"
		Hydrometer_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
		Hydrometer_Data['m3'] = Hydrometer_Fake_Value
		hydrometer_json_data = json.dumps(Hydrometer_Data)

		print "Publishing Hydrometer@00a-4erT-wTy Value: " + str(Hydrometer_Fake_Value) + "..."
		publish_To_Topic (MQTT_Topic_Water, hydrometer_json_data)

		hydrometerConsumption += Hydrometer_Fake_Value

		if ((hydroElapsed % 15) == 0):
			hydroElapsed = 0
			
			hydro_consumption = {}
			hydro_consumption['hydrometerId'] = Hydrometer_Data['Sensor_ID']
			hydro_consumption['lastRecord'] = Hydrometer_Data['Date']
			hydro_consumption['consumption'] = hydrometerConsumption
			hydro_consumption_json = json.dumps(hydro_consumption)

			print "Publishing 00a-4erT-wTy Hydro Consumption: " + str(hydrometerConsumption) + "..."
			publish_To_Topic (MQTT_Topic_Water_Consumption, hydro_consumption_json)
			hydrometerConsumption = 0

		toggle = 0


publish_Fake_Sensor_Values_to_MQTT()

#====================================================
