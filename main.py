import paho.mqtt.client as mqtt
import sqlite3
from time import time

DB_File_Name = 'BlindDb.db'


# This happens when connecting
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))


# On subscribing to messages
def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


# Connect to MQTT
def on_message(client, user_data, msg):
    # Parse message and extract data
    data = msg.payload.decode('utf-8')
    print(data)
    print(msg.topic)
    danger = data.split(", ")[0]
    locations = data.split(", ")[1]
    print(danger)
    print(locations)

    db_conn = user_data['db_conn']
    if data != "{\"Low\"}" and data != "{\"high\"}":
        sql = 'INSERT INTO blind_data (topic, dangerlevel, location, created_at) VALUES (?, ?, ?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (msg.topic, danger, locations, int(time())))
        db_conn.commit()
        cursor.close()


# SQLite DB Table Schema
def main():
    db_conn = sqlite3.connect(DB_File_Name)
    sql = """
    CREATE TABLE IF NOT EXISTS blind_data (    
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        dangerlevel TEXT NOT NULL,
        location TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    """
    cursor = db_conn.cursor()
    cursor.execute(sql)
    # Close DB
    cursor.close()

    myhost = "mqtt.flespi.io"
    client = mqtt.Client()

    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_connect = on_connect

    # MQTT Settings and Connection
    client.username_pw_set("T0jLbGxLz6LQVQPXDKFJNPIs17LM1DUKt3lvzG4ZBFDmmi9NQDkriSJ9PlJGOsh5", "")
    client.connect(myhost, 1883)
    # to retrieve the database connection object stored in the user data.
    client.user_data_set({'db_conn': db_conn})
    # the client will receive each message published to the subscribed topic at least once
    client.subscribe("BlindData/#", 1)

    client.loop_forever()

main()
