import os
import asyncio
import time
import pandas as pd
import re
import json
from paho.mqtt import client as mqtt
from urllib.parse import urlparse, parse_qs
import random

global text_index

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_log(client, userdata, level, buf):
    print("log: ", buf)


# Load data from JSON file
datafile = pd.read_json("publishednodes.json")
device_id = str(datafile["id"].iloc[0])
mqtt_conn_string = str(datafile["Connection_string"].iloc[0])
topic = str(datafile["Topic"].iloc[0])

# Extract IoT Hub name and Shared Access Signature from the connection string
parsed_url = urlparse(mqtt_conn_string)
query_params = parse_qs(parsed_url.query)
shared_access_signature = query_params.get("SharedAccessSignature", [None])[0]
iot_hub_name = parsed_url.hostname.split('.')[0]

start_index = mqtt_conn_string.find("SharedAccessSignature")
end_index = mqtt_conn_string.find('@', start_index)
extracted_part = mqtt_conn_string[start_index:end_index]
new_sas_token = str(extracted_part)

with open('publishednodes.json', 'r') as file:
    data = json.load(file)
    delay= data[0]["Publish_interval"]["Sec"]
    # print(type(delay))
   
INTERVAL = 1

def create_client():
    client = mqtt.Client(client_id=device_id, protocol=mqtt.MQTTv311, clean_session=False)
    client.on_log = on_log
    client.tls_set_context(context=None)

    # Set up client credentials
    username = "{}.azure-devices.net/{}/api-version=2018-06-30".format(iot_hub_name, device_id)
    client.username_pw_set(username=username, password=new_sas_token)

    # Connect to the Azure IoT Hub
    client.on_connect = on_connect
    client.connect(iot_hub_name + ".azure-devices.net", port=8883)

    return client

async def run_telemetry_sample(client):

    print("IoT Hub device sending periodic messages")
    msg_txt_formatted = {}
    text_index=1
    while True:
        # Build the message with simulated telemetry values.
        
        TimeStamp=time.time()
        msg_txt_formatted["ID"] = int(device_id)
        msg_txt_formatted["TimeStamp"] = TimeStamp
        for key, value in data[0].items():
            if key in ["Connection_string", "Topic", "id","Publish_interval"]:
                continue # Skip these keys
            if isinstance (value,dict) and "set" in value:
                if value["set"] == "UTC":
                    msg_txt_formatted[key] = {"Val":time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
                elif value["set"] == "Local":
                    msg_txt_formatted[key] = {"Val": time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime())}

            elif isinstance(value, dict) and all (k in value for k in ("Text1", "Text2", "Text3","Text4")):
                #Handle multiple string
                msg_txt_formatted[key] = {"Val": value[f"Text{text_index}"]}
                text_index +=1
                if text_index > 4:
                    text_index = 1
            elif isinstance(value, dict) and "set" in value:
                if "start" in value and "end" in value and "ramp" in value:
                    start_value = value["start"]
                    end_value = value["end"]
                    jump = value["ramp"]
                    if key not in msg_txt_formatted:
                        msg_txt_formatted[key] = {"Val": start_value}
                    else:
                        msg_txt_formatted[key]["Val"] += jump
                        if msg_txt_formatted[key]["Val"] > end_value:
                            msg_txt_formatted[key]["Val"] = start_value
            elif isinstance(value,dict) and "Text" in value:
                msg_txt_formatted[key] = {"Val":value["Text"]}
            elif "Bool" in value:
                if value["Bool"] == True or value["Bool"] == "TRUE":
                    msg_txt_formatted[key] = {"Val": True}
                elif value["Bool"] == False or value["Bool"] == "FALSE":
                    msg_txt_formatted[key] = {"Val": False}
                elif value["Bool"] == "random" or "Random" or "RANDOM":
                    msg_txt_formatted[key] = {"Val":random.choice([True,False])}
                else:
                    msg_txt_formatted[key] = {"Val": value["Bool"]}
                
                # msg_txt_formatted[key] ={"Val": value["Bool"]}
            elif isinstance (value,dict) and "set" in value:
                if value["set"] == "UTC":
                    msg_txt_formatted[key] = {"Val":time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
                elif value["set"] == "Local":
                    msg_txt_formatted[key] = {"Val": time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime())}
                        
            else:
                # Handle numerical telemetry data
                start_value = value["start"]
                end_value = value["end"]
                jump = value["ramp"]

                if key not in msg_txt_formatted:
                    msg_txt_formatted[key] = {"Val": start_value}
                else:
                    msg_txt_formatted[key]["Val"] += jump
                    if msg_txt_formatted[key]["Val"] > end_value:
                        msg_txt_formatted[key]["Val"] = start_value               

        message = json.dumps(msg_txt_formatted)
        print("Sending message: {}".format(message))
        client.publish(topic, payload=message, qos=1, retain=False)
        print("Message sent")
        time.sleep(delay)
        # await asyncio.sleep(INTERVAL)
        client=create_client()
def main():
    print("IoT Hub Quickstart #1 - Simulated device")
    print("Press Ctrl-C to exit")

    client = create_client()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_telemetry_sample(client))
    except KeyboardInterrupt:
        print("IoTHubClient sample stopped by user")
    finally:
        print("Shutting down IoTHubClient")
        loop.close()

if __name__ == '__main__':
    main()
