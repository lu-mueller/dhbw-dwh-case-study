""" Subscribe to the topic "DataMgmt" and insert the received messages into the database. """

import os
from datetime import datetime

import paho.mqtt.client as mqtt
import psycopg2

db_password = os.getenv("DB_PASSWORD")

# Verbindung zur Datenbank herstellen
conn = psycopg2.connect(database="postgres", user="postgres", password=db_password)
print("Connected to database")

# Cursor erstellen
cur = conn.cursor()


def on_message(message):
    """callback function that is called when a message is received"""

    try:
        timestamp = datetime.now()

        sql = (
            "INSERT INTO staging.messung(messung_id, payload, empfangen) "
            "VALUES (DEFAULT, %s, %s);"
        )
        cur.execute(sql, (message.payload.decode("utf-8"), timestamp))
        conn.commit()
        print("Nachricht erfolgreich in die Datenbank eingefügt.")
    except psycopg2.DatabaseError as error:
        print("Fehler beim Einfügen der Nachricht in die Datenbank:", error)


mqttc = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="subscriberID-jlkiolososssiHSOFLi",
    clean_session=False,
)
mqttc.on_message = on_message
mqttc.connect("broker.hivemq.com", 1883, 60)
mqttc.subscribe("DataMgmt", qos=1)
mqttc.loop_forever()
cur.close()
conn.close()
