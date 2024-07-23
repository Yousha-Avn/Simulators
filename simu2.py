import asyncio
import json
import re
import time
from datetime import datetime
import pandas as pd
from azure.iot.device import Message, MethodResponse
from azure.iot.device.aio import IoTHubDeviceClient
import random

# Read configuration data from JSON file
datafile = pd.read_json("data.json")
connection_string = str(datafile["Connection_string"].iloc[0])
device_id = re.search(r'DeviceId=([^;]+)', connection_string).group(1)
CONNECTION_STRING = str(connection_string)

with open('data.json') as file:
    data = json.load(file)
    delay = data[0]["Publish_interval"]["Sec"]
    print(delay)

# Define the interval for sending telemetry data
INTERVAL = 1

def create_client():
    # Create an IoT Hub client
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

    # Define a method request handler
    async def method_request_handler(method_request):
        global INTERVAL
        if method_request.name == "SetTelemetryInterval":
            try:
                INTERVAL = int(method_request.payload)
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404

        method_response = MethodResponse.create_from_method_request(method_request, response_status, response_payload)
        await client.send_method_response(method_response)

    try:
        # Attach the method request handler
        client.on_method_request_received = method_request_handler
    except:
        # Clean up in the event of failure
        client.shutdown()
        raise

    return client

async def run_telemetry_sample(client):
    print("IoT Hub device sending periodic messages")
    await client.connect()
    msg_txt_formatted = {}
    text_index = 1  # Initialize text_index to 1

    while True:
        TimeStamp = time.time()
        # Build the message with simulated telemetry values.
        for key, value in data[0].items():
            if key in ["Connection_string", "Publish_interval"]:
                continue  # Skip the Connection_string
            elif isinstance(value, dict) and "set" in value:
                # Handle timestamp tags based on the "set" value
                if value["set"]== "utc" or "UTC":
                    msg_txt_formatted[key] = {"Val": datetime.utcnow().isoformat()}
                elif value["set"] == "local" or "Local" :
                    msg_txt_formatted[key] = {"Val": datetime.now().isoformat()}
            elif isinstance(value, dict) and all(k in value for k in ["Text1", "Text2", "Text3", "Text4"]):
                # Handle sequential text data for String
                msg_txt_formatted[key] = {"Val": value[f"Text{text_index}"]}
                text_index += 1
                if text_index > 4:
                    text_index = 1
            elif isinstance(value, dict) and "Text" in value:
                # Handle static text data
                msg_txt_formatted[key] = {"Val": value["Text"]}
            elif isinstance(value, dict) and "Bool" in value:
                if value["Bool"] == True:
                    msg_txt_formatted[key] = {"Val": True}
                elif value["Bool"] == False:
                    msg_txt_formatted[key] = {"Val": False}
                elif value["Bool"] == "random":
                    msg_txt_formatted[key] = {"Val": random.choice([True, False])}
                else:
                    msg_txt_formatted[key] = {"Val": value["Bool"]}
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

        msg_txt_formatted["ID"] = int(device_id)
        msg_txt_formatted["TimeStamp"] = TimeStamp  # Add the timestamp to the message
        message = json.dumps(msg_txt_formatted)
        message = Message(message)
        print("Sending message: {}".format(message))
        await client.send_message(message)
        print("Message sent")
        # await asyncio.sleep(delay)
        time.sleep(delay)

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
        loop.run_until_complete(client.shutdown())
        loop.close()

if __name__ == '__main__':
    main()
