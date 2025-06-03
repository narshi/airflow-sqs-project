from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from datetime import datetime, timedelta
import numpy as np
import logging
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sqs_message_producer',
    default_args=default_args,
    description='DAG that sends messages to SQS queue',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['sqs', 'producer', 'event-driven'],
) as dag:

    @task
    def create_messages():
        """
        Generate random messages to send to SQS queue
        Returns a list of message dictionaries
        """
        run_types = ["regular", "delta", "full_refresh"]
        message_list = []
        
        # Generate 1-5 different messages randomly
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

    # Generate messages
    messages_to_send = create_messages()

    # Publish messages to SQS queue using dynamic tasks
    publish_messages = SqsPublishOperator.partial(
        task_id='publish_to_sqs_queue',
        sqs_queue='data-processing-queue',  # Your SQS queue name
        region_name='eu-north-1',  # Change to your AWS region
        aws_conn_id='aws_default',
        delay_seconds=0,
    ).expand(message_content=messages_to_send)

    # Define task dependencies
    messages_to_send >> publish_messages