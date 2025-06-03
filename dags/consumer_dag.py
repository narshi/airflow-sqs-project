from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from datetime import datetime, timedelta
import logging
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sqs_message_consumer',
    default_args=default_args,
    description='DAG that reads and processes messages from SQS queue',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['sqs', 'consumer', 'event-driven'],
) as dag:

    # Poll SQS queue for messages
    read_sqs_queue = SqsSensor(
        task_id='poll_sqs_queue',
        sqs_queue='data-processing-queue',  # Your SQS queue name
        max_messages=5,  # Maximum number of messages to retrieve at once
        region_name='eu-north-1',  # Change to your AWS region
        aws_conn_id='aws_default',
        mode='reschedule',  # Free up worker slots while waiting
        poke_interval=30,  # Check every 30 seconds
        timeout=300,  # Timeout after 5 minutes
    )

    @task
    def extract_and_parse_messages(**context):
        """
        Extract messages from SQS sensor and parse JSON content
        """
        # Get messages from the sensor task
        retrieved_messages = context['task_instance'].xcom_pull(task_ids='poll_sqs_queue')
        
        if not retrieved_messages:
            logging.info("No messages retrieved from queue")
            return []
        
        logging.info(f"Retrieved {len(retrieved_messages)} messages from SQS")
        
        # Parse JSON content from message bodies
        parsed_messages = []
        for message in retrieved_messages:
            try:
                message_body = json.loads(message['Body'])
                parsed_messages.append(message_body)
                logging.info(f"Parsed message: {message_body}")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse message: {message['Body']}, Error: {e}")
                
        return parsed_messages

    extract_messages = extract_and_parse_messages()

    @task
    def process_individual_message(message):
        """
        Process each individual message
        This is where you'd implement your business logic
        """
        logging.info(f"Processing message: {message}")
        
        config_id = message.get('config_id')
        run_type = message.get('run_type')
        batch_id = message.get('batch_id')
        
        # Simulate some processing based on message content
        if run_type == 'regular':
            logging.info(f"Executing regular processing for config {config_id}, batch {batch_id}")
        elif run_type == 'delta':
            logging.info(f"Executing delta processing for config {config_id}, batch {batch_id}")
        elif run_type == 'full_refresh':
            logging.info(f"Executing full refresh for config {config_id}, batch {batch_id}")
        
        # Here you could call other functions, execute SQL, trigger other DAGs, etc.
        return f"Processed message for config {config_id} with {run_type} run type"

    # Create dynamic tasks to process each message individually
    process_messages = process_individual_message.expand(message=extract_messages)

    # Define task dependencies
    read_sqs_queue >> extract_messages >> process_messages