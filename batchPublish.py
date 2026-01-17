import json
from google.cloud import pubsub_v1
from pathlib import Path

PROJECT_ID = "helpful-data-483403-d6"
TOPIC_ID = "retail-data-test"

def publish_json(f_path):

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    with open(f_path, 'r') as file:
        data = json.load(file)

    BATCH_SIZE = 500
    batched = []

    for row in data:
        
        batched.append(row)

        if len(batched) == BATCH_SIZE: 
            message_bytes = json.dumps(batched).encode("utf-8")
            future = publisher.publish(topic_path, message_bytes, table = f_path.stem)
            message_id = future.result()
            print(f"Published message with ID: {message_id}")

            batched.clear()
    
    if batched:
        message_bytes = json.dumps(batched).encode("utf-8")
        future = publisher.publish(topic_path, message_bytes, table = f_path.stem)
        message_id = future.result()
        print(f"Published FINAL message with ID: {message_id}") 

jsonDir = Path("json_data")

for file_path in jsonDir.iterdir():
    if file_path.is_file():
        publish_json(file_path)

            