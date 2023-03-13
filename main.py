
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.future import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# This happens when connecting
def on_connect(mqttc, obj, flags, rc):
 print("rc: " + str(rc))

# On subscribing to messages
def on_subscribe(mqttc, obj, mid, granted_qos):
 print("Subscribed: " + str(mid) + " " + str(granted_qos))

# Connect to MQTT
def on_message(client, userdata, msg):
    print(str(msg.topic) + " " + str(msg.qos) + " " + str(msg.payload))
    # Parse message and extract data
    data = msg.payload.decode()
    print(data)
    # Connect to the database
    engine = create_engine('DATABASE_URL')
    Session = sessionmaker(bind=engine)
    session = Session()
    # Insert data into the database
    record = Data(value=data)
    session.add(record)
    session.commit()
    session.close()

myhost="mqtt.flespi.io"
client = mqtt.Client()
client.on_message = on_message
client.on_subscribe = on_subscribe
client.on_connect = on_connect

client.username_pw_set("T0jLbGxLz6LQVQPXDKFJNPIs17LM1DUKt3lvzG4ZBFDmmi9NQDkriSJ9PlJGOsh5","")
client.connect(myhost, 1883)
client.subscribe("BlindData/#", 1)



# Define the database model
Base = declarative_base()


class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    value = Column(String)


Base.metadata.create_all(engine)


client.loop_forever()