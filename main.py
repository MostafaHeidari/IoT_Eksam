import sqlite3
from time import time
import paho.mqtt.client as mqtt
DB_File_Name = 'BlindDb.db'


# This happens when connecting and prints a message to tell if it failed or not
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))


# On subscribing to messages to tell if it failed or not
def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


# This function (on_message) is executed whenever a message is received from the MQTT broker.
# When the function is executed, it extracts the payload of the message and decodes it to a UTF-8 string and It then prints the topic of the message to the console
def on_message(client, user_data, msg):
    # Parse message and extract data
    data = msg.payload.decode('utf-8')
    # Print the topic to see mistakes
    print(msg.topic)
    # Split the information, so it can be saved in default columns and add it to a method name to be called
    Danger = data.split(", ")[0]
    Locations = data.split(", ")[1]
    DateTime = data.split(", ")[2]
    # Print the data to see mistakes
    print(Danger)
    print(Locations)
    print(DateTime)


    # Connects to the database and performs a database insert operation using the extracted data from the MQTT message.
    db_conn = user_data['db_conn']
    if data != "{\"Low\"}" and data != "{\"high\"}":
        sql = 'INSERT INTO blind_data (Topic, DangerLevel, Location, DateTime) VALUES (?, ?, ?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (msg.topic, Danger, Locations, DateTime))
        db_conn.commit()
        cursor.close()


# SQLite DB Table Schema
def main():
    # Calls the data bace connect method and creates a new database if one isn't there yer
    db_conn = sqlite3.connect(DB_File_Name)
    sql = """
    CREATE TABLE IF NOT EXISTS blind_data (    
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Topic TEXT NOT NULL,
        DangerLevel TEXT NOT NULL,
        Location TEXT NOT NULL,
        DateTime INTEGER NOT NULL
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

    # Connect to MQTT
    # these 3 lines connects and logs in to our MQTT, so we can recvie information form the device
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
