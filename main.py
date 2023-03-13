import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.future import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Connect to MQTT
def on_message(client, userdata, msg):
    # Parse message and extract data
    data = msg.payload.decode()
    # Connect to the database
    engine = create_engine('DATABASE_URL')
    Session = sessionmaker(bind=engine)
    session = Session()
    # Insert data into the database
    record = Data(value=data)
    session.add(record)
    session.commit()
    session.close()

client = mqtt.Client()
client.on_message = on_message
client.connect("T0jLbGxLz6LQVQPXDKFJNPIs17LM1DUKt3lvzG4ZBFDmmi9NQDkriSJ9PlJGOsh5", 1883)
client.subscribe("BlindData/warning")

# Define the database model
Base = declarative_base()

class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    value = Column(String)

Base.metadata.create_all(engine)

client.loop_forever()
