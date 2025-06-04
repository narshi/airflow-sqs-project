import boto3
import json
import numpy as np
import logging
import os
from datetime import datetime
import yaml

# Load configuration from YAML file
with open(os.path.join(os.path.dirname(__file__), '../config/sqs_config.yaml'), 'r') as config_file:
    config = yaml.safe_load(config_file)

sqs_queue = config['sqs_queue']
region_name = config['region_name']
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize SQS client
sqs_client = boto3.client(
    'sqs',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def create_messages():
    run_types = ["regular", "delta", "full_refresh"]
    message_list = []
    
    num_messages = np.random.randint(1, 6)
    logging.info(f"Generating {num_messages} messages")
    
    for i in range(num_messages):
        message = {
            "config_id": int(np.random.randint(1, 10)),
            "run_type": np.random.choice(run_types),
            "batch_id": f"batch_{i+1}",
            "timestamp": datetime.now().isoformat()
        }
        message_list.append(json.dumps(message))
        
    logging.info(f"Messages to be sent: {message_list}")
    return message_list

def send_messages_to_sqs(messages):
    for message in messages:
        response = sqs_client.send_message(
            QueueUrl=sqs_queue,
            MessageBody=message
        )
        logging.info(f"Message sent to SQS: {response['MessageId']}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    messages_to_send = create_messages()
    send_messages_to_sqs(messages_to_send)