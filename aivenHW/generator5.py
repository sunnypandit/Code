import time
import uuid
import random
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

HOST = 'mykafkaservice-sunnyproject.a.aivencloud.com'
SSL_PORT = 26294
TOPIC_NAME = 'topic5'



producer = KafkaProducer(
    bootstrap_servers=f"{HOST}:{SSL_PORT}",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    #value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_json_data():
    # Generate random UUID
    random_uuid = str(uuid.uuid4())

    # Generate random temperature (assuming it's in Celsius)
    random_temperature = round(random.uniform(-10, 30), 2)

    # Generate ISO 8601 timestamp in UTC
    iso_timestamp = datetime.now(timezone.utc).isoformat()

    # Create a dictionary with the data
  


    data = {
        "uuid": random_uuid,
        "temperature": random_temperature,
        "ts": iso_timestamp
    }
    #KEY = data["uuid"]



    return data

if __name__ == "__main__":
    try:
        count = 0
        while count < 10:
            count += 1
            json_data = generate_json_data()
            KEY = {
                    "uuid": json_data["uuid"]
                    }


            try:
                #producer.send(TOPIC_NAME,key=json.dumps(KEY).encode('utf-8'),value=json.dumps(json_data).encode('utf-8'))
                producer.send(TOPIC_NAME,key=json.dumps(KEY).encode('ascii'),value=json.dumps(json_data).encode('ascii'))
            except Exception as e:
                print(f"An error occurred: {str(e)}")
            #producer.send(TOPIC_NAME, str(json_data))
            #print("Message Sent: ", json.)
            print("Key: ",KEY)
            print("Message Sent: ",json_data)
            time.sleep(1)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        producer.close()

