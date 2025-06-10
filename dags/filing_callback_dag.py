from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

default_args = {
    'owner': 'filing_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1)
}

def process_webhook_callback(**context):
    '''Process incoming webhook callback from filing system'''
    
    # Get callback data from DAG run configuration
    callback_data = context['dag_run'].conf or {}
    
    filing_id = callback_data.get('filing_id')
    status = callback_data.get('status')
    result_data = callback_data.get('data', {})
    
    if not filing_id:
        raise ValueError("No filing_id provided in callback")
    
    print(f"📞 Received webhook callback for filing: {filing_id}")
    print(f"Status: {status}")
    print(f"Data: {json.dumps(result_data, indent=2)}")
    
    # Update database with callback information
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Update filing tracker
    pg_hook.run(
        '''
        UPDATE filing_tracker 
        SET status = %s, result_data = %s, completed_at = CURRENT_TIMESTAMP
        WHERE filing_id = %s
        ''',
        parameters=(status, json.dumps(result_data), filing_id)
    )
    
    # Add to status history
    pg_hook.run(
        '''
        INSERT INTO filing_status_history (filing_id, status, message)
        VALUES (%s, %s, %s)
        ''',
        parameters=(filing_id, status, f"Webhook callback received: {status}")
    )
    
    print(f"✅ Database updated for filing: {filing_id}")
    
    return {
        'filing_id': filing_id,
        'status': status,
        'processed_at': datetime.now().isoformat(),
        'data': result_data
    }

# Create callback DAG
with DAG(
    'filing_callback_dag',
    default_args=default_args,
    description='Handle webhook callbacks from filing system',
    schedule=None,
    max_active_runs=10,
    catchup=False,
    tags=['filing', 'webhook', 'callback']
) as callback_dag:
    
    process_callback = PythonOperator(
        task_id='process_webhook_callback',
        python_callable=process_webhook_callback,
        doc_md="Process webhook callback and update database"
    )