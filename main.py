
import paho.mqtt.client as mqtt
import sqlite3
from time import time
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.future import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DATABASE_FILE = 'BlindDb.db'

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

    db_conn = user_data['db_conn']
    if data != "{Low}":
        sql = 'INSERT INTO blind_data (topic, dangerlevel, location, created_at) VALUES (?, ?, ?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (msg.topic, data, data, int(time())))
        db_conn.commit()
        cursor.close()


    # Connect to the database
    #engine = create_engine('DATABASE_URL')
    #Session = sessionmaker(bind=engine)
    #session = Session()
    # Insert data into the database
    #record = Data(value=data)
    #session.add(record)
    #session.commit()
    #session.close()

def main():
    db_conn = sqlite3.connect(DATABASE_FILE)
    sql = """
    CREATE TABLE IF NOT EXISTS blind_data (    
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        dangerlevel TEXT NOT NULL,
        location TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """
    cursor = db_conn.cursor()
    cursor.execute(sql)
    cursor.close()

    myhost="mqtt.flespi.io"
    client = mqtt.Client()


    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_connect = on_connect

    client.username_pw_set("T0jLbGxLz6LQVQPXDKFJNPIs17LM1DUKt3lvzG4ZBFDmmi9NQDkriSJ9PlJGOsh5","")
    client.connect(myhost, 1883)
    client.user_data_set({'db_conn': db_conn})

    client.subscribe("BlindData/#", 1)

    client.loop_forever()


# Define the database model
#Base = declarative_base()


#class Data(Base):
    #__tablename__ = 'data'
    #id = Column(Integer, primary_key=True)
    #value = Column(String)


#Base.metadata.create_all(engine)

main()
