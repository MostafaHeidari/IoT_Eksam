import paho.mqtt.client as mqtt
import sqlite3
from time import time

DB_File_Name = 'BlindDb.db'


# This happens when connecting and prints a message to tell if it failed or works
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))


# On subscribing to messages to tell if it failed or works
def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


# Connect to MQTT
def on_message(client, user_data, msg):
    # Parse message and extract data
    data = msg.payload.decode('utf-8')
    # Print the data to see mistakes
    print(data)
    print(msg.topic)
    # Split the information, so it can be saved in default columns and add it to a method name to be called
    danger = data.split(", ")[0]
    locations = data.split(", ")[1]
    # Print the data to see mistakes
    print(danger)
    print(locations)


    # Connects to the database and commit the data that is sendt
    db_conn = user_data['db_conn']
    if data != "{\"Low\"}" and data != "{\"high\"}":
        sql = 'INSERT INTO blind_data (topic, dangerlevel, location, created_at) VALUES (?, ?, ?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (msg.topic, danger, locations, int(time())))
        db_conn.commit()
        cursor.close()


# SQLite DB Table Schema
def main():
    # Calls the data bace connect method and creates a new database if one isn't there yer
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
    # Execute the sql code and close the connection to the database
    cursor.execute(sql)
    # Close DB
    cursor.close()


    # Set the host and the client for all the data is coming from
    myhost="mqtt.flespi.io"
    client = mqtt.Client()

    # calls the method that handles the information that is communing in
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_connect = on_connect


    # thies 3 lines connects and logs in to our muqtt, so we can recvie inmation form the device
    client.username_pw_set("T0jLbGxLz6LQVQPXDKFJNPIs17LM1DUKt3lvzG4ZBFDmmi9NQDkriSJ9PlJGOsh5","")
    client.connect(myhost, 1883)

    # to retrieve the database connection object stored in the user data.
    client.user_data_set({'db_conn': db_conn})


    # Here we subscribe to the publisher we use a # becomes we want alle information send from this broker
    # the client will receive each message published to the subscribed topic at least once
    client.subscribe("BlindData/#", 1)

    # Makes the code run FOREVER until we force stop it
    client.loop_forever()

# Calls the main method
main()
