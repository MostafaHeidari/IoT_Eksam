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
MQTT_Broker = "mqtt.flespi.io"
MQTT_Port = 1883
Keep_Alive_Interval = 45
mqttToken = 'K4UTKf296rzPkzUMicca57aeqijYBwgbo1TZ4LW01jD9GnJOpMNiHLmXTtJjNQk2'
MQTT_Topic_warning = "BlindData/warning"

#====================================================

def on_connect(client, userdata, rc):
	if rc != 0:
		pass
		print("Unable to connect to MQTT Broker...")
	else:
		print("Connected with MQTT Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
	pass
		
def on_disconnect(client, userdata, rc):
	if rc !=0:
		pass
		
mqttc = mqtt.Client("Cocololo")
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.username_pw_set(mqttToken, "")
mqttc.connect(MQTT_Broker, MQTT_Port)
mqttc.loop_forever()  # Start networking daemon

		
def publish_To_Topic(topic, message):
	mqttc.publish(topic,message)
	print ("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
	print("")


#====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker



#====================================================
