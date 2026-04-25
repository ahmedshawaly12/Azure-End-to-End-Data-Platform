import os

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import json
load_dotenv()  

# Connection string and Event Hub name
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

def send_events(ride_data=None, batch_size=1):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )
    try:
        
        ride_json = json.dumps(ride_data)

        event_data_batch = producer.create_batch()
        event = EventData(ride_json)
            
        event_data_batch.add(event)

        # Send the batch
        producer.send_batch(event_data_batch)
        print("Events sent successfully.")
    except Exception as e:
        print(f"Error sending events: {e}")
    finally:
        producer.close()