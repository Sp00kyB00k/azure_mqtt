import os
import time
import ssl
import random
import json
import paho.mqtt.client as mqtt
from pathlib import Path
from dotenv import load_dotenv
from helpers.sastoken import SasToken

# load the environment variables needed for instanting the MQTT client
env_path = os.path.join(Path(__file__).resolve().parent, '.env')
load_dotenv(dotenv_path=env_path)

# Set up for creating a secure connection
path_to_root_cert = os.getenv('ROOT_CERT')
device_id = os.getenv('DEVICE_ID')
iot_hub_name = os.getenv('IOT_HUB')
key = os.getenv('UNIQ_KEY')

# configuration variables
username = f"{iot_hub_name}.azure-devices.net/{device_id}/?api-version=2018-06-30"
topic = f"devices/{device_id}/messages/events/"

def create_token():
    """
    Building the sas_object with the SasToken helper lib
    params:
        uri: string
        key: string
        ttl: int
    """
    sas_object = SasToken(uri=iot_hub_name, key=key, ttl=3600)
    return sas_object._build_token()

# Setting up the callback methods for the MQTT client
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connection OK")
    elif rc == 5:
        client.username_pw_set(username=username, password=create_token())
    else:
        print(f'Bad connection, returned {rc}')        

def on_disconnect(client, userdata, flags, rc = 0):
    print (f"Disconnected with result code {rc}")

def on_log(client, userdata, level, buf):
    print(f"log: {buf}")

def on_publish(client, userdata, mid):
    print("sent the message")

def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed with a granted QOS level of {}".format(granted_qos))

def on_message(client, userdata, msg):
    print("Message received" + msg.topic + " " + str(msg.payload))

def run_client():
    # Instantiate the MQTT client
    client = mqtt.Client(client_id=device_id, clean_session=False)

    # Binding the callback methods to the client
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log
    client.on_publish = on_publish
    client.on_message = on_message

    # Binding attributes for setting up TLS encryption 
    client.username_pw_set(username=username, password=create_token())
    client.tls_set(ca_certs=path_to_root_cert, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
    client.tls_insecure_set(False)

    # Connecting to the Azure IoT hub
    client.connect(host=iot_hub_name, port=8883, keepalive=300)
    client.subscribe(topic=topic, qos=1)
    client.loop_forever()


if __name__ == '__main__':
    run_client()
