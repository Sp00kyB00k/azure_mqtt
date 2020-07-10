import time, ssl, random, json, os
import paho.mqtt.client as mqtt
from mqtt_bridge.helpers.sastoken import SasToken
from dotenv import load_dotenv

# load the environment variables needed for instanting the MQTT client
env_path = '/home/jd/Desktop/programming/work_related/mqtt_bridge/mqtt_bridge/publisher/.env'
load_dotenv(dotenv_path=env_path)

path_to_root_cert = os.getenv('ROOT_CERT')
device_id = os.getenv('DEVICE_ID')
iot_hub_name = os.getenv('IOT_HUB')
key = os.getenv('UNIQ_KEY')
print(iot_hub_name)

def create_token():
    sas_object =  SasToken(uri=iot_hub_name, key=key, ttl=3600)
    return sas_object._build_token()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connection OK")
    elif rc == 5:
        client.username_pw_set(username=iot_hub_name+".azure-devices.net/" +
                        device_id + "/?api-version=2018-06-30", password=create_token())
    else:
        print(f'Bad connection, Returned {rc}')        

def on_disconnect(client, userdata, flags, rc = 0):
    print (f"Disconnected with result code {rc}")

def on_publish(client, userdata, mid):
    print("sent the message")

def on_log(client, userdata, level, buf):
    print(f"log: {buf}")

def run_client():
    client = mqtt.Client(client_id=device_id, clean_session=False)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log
    client.on_publish = on_publish

    client.username_pw_set(username=iot_hub_name+"/" +
                       device_id + "/?api-version=2018-06-30", password=create_token())
    
    client.tls_set(ca_certs=path_to_root_cert, certfile=None, keyfile=None,
               cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
    client.tls_insecure_set(False)

    client.connect(host=iot_hub_name, port=8883, keepalive=300)
    
    mid = 1
    msg = {"mid" : mid, "temp" : random.randint(20, 25)}
    payload = json.dumps(msg)
    
    while True:
        client.loop_start()
        client.publish(topic=f"devices/{device_id}/messages/events/", payload=payload, qos=1, retain=False)
        mid += 1
        time.sleep(5)

if __name__ == '__main__':
    run_client()
