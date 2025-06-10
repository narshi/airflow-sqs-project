from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from operators.smart_filing_sensor import SmartFilingSensor, WebhookTriggerSensor
from hooks.filing_api_hook import FilingAPIHook

import json
import uuid

# Default arguments
default_args = {
    'owner': 'filing_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

def validation_task(**context):
    '''Validate input data'''
    filing_data = context['params'].get('filing_data', {})
    
    # Validation logic
    required_fields = ['company_name', 'filing_type', 'documents']
    for field in required_fields:
        if field not in filing_data:
            raise ValueError(f"Missing required field: {field}")
    
    print(f"✅ Validation completed for {filing_data.get('company_name')}")
    return {"status": "validated", "data": filing_data}

def transformation_task(**context):
    '''Transform data for filing'''
    validated_data = context['task_instance'].xcom_pull(task_ids='validation')
    
    # Transformation logic
    transformed_data = {
        **validated_data['data'],
        'transformed_at': datetime.now().isoformat(),
        'processing_id': str(uuid.uuid4())
    }
    
    print(f"🔄 Transformation completed")
    return {"status": "transformed", "data": transformed_data}

def initiate_filing(**context):
    '''Start the filing process with webhook registration'''
    
    transformed_data = context['task_instance'].xcom_pull(task_ids='transformation')
    filing_id = f"filing_{context['dag_run'].run_id}_{int(datetime.now().timestamp())}"
    
    # Webhook URL for callbacks
    callback_url = "http://airflow-webserver:8080/api/v1/dags/filing_callback_dag/dagRuns"
    
    # Use custom hook to submit filing
    filing_hook = FilingAPIHook()
    result = filing_hook.submit_filing(
        filing_data=transformed_data['data'],
        callback_url=callback_url,
        filing_id=filing_id
    )
    
    print(f"📤 Filing submitted: {result['filing_id']}")
    print(f"🔔 Webhook registered: {callback_url}")
    
    return result

def decide_monitoring_strategy(**context):
    '''Decide whether to use webhook or polling based on filing type'''
    
    filing_result = context['task_instance'].xcom_pull(task_ids='initiate_filing')
    filing_data = context['params'].get('filing_data', {})
    
    # Decision logic - for demo, alternate between strategies
    filing_type = filing_data.get('filing_type', 'standard')
    
    if filing_type in ['urgent', 'priority']:
        print("🚀 Using webhook monitoring for priority filing")
        return 'webhook_monitor'
    else:
        print("📊 Using smart polling for standard filing")
        return 'polling_monitor'

def process_filing_result(**context):
    '''Process the final filing result'''
    
    # Try to get result from either monitoring strategy
    webhook_result = context['task_instance'].xcom_pull(task_ids='webhook_monitor', key='webhook_result')
    polling_result = context['task_instance'].xcom_pull(task_ids='polling_monitor', key='filing_result')
    
    result = webhook_result or polling_result
    
    if not result:
        raise ValueError("No filing result received from monitoring")
    
    print(f"📋 Processing filing result: {result['status']}")
    
    if result['status'] == 'SUCCESS':
        return 'success_handler'
    else:
        return 'failure_handler'

def success_handler(**context):
    '''Handle successful filing'''
    result = context['task_instance'].xcom_pull(task_ids='process_result')
    print("🎉 Filing completed successfully!")
    print(f"Result data: {json.dumps(result, indent=2)}")
    
    # Trigger document merger DAG
    return TriggerDagRunOperator(
        task_id='trigger_document_merger',
        dag_id='filing_continuation_dag',
        conf={'filing_result': result, 'action': 'merge_documents'},
        wait_for_completion=False
    ).execute(context)

def failure_handler(**context):
    '''Handle filing failure'''
    result = context['task_instance'].xcom_pull(task_ids='process_result')
    print("❌ Filing failed!")
    print(f"Error details: {json.dumps(result, indent=2)}")
    
    # Could trigger error handling DAG or send notifications
    return {"status": "error_handled", "details": result}

# Create the DAG
with DAG(
    'intelligent_filing_workflow',
    default_args=default_args,
    description='Intelligent filing workflow with webhook and polling strategies',
    schedule=None,
    max_active_runs=3,
    catchup=False,
    tags=['filing', 'poc', 'intelligent']
) as dag:
    
    # Step 1: Validation
    validation = PythonOperator(
        task_id='validation',
        python_callable=validation_task,
        doc_md="Validate input filing data"
    )
    
    # Step 2: Transformation
    transformation = PythonOperator(
        task_id='transformation',
        python_callable=transformation_task,
        doc_md="Transform data for filing submission"
    )
    
    # Step 3: Initiate Filing
    filing_init = PythonOperator(
        task_id='initiate_filing',
        python_callable=initiate_filing,
        doc_md="Submit filing request and register webhook"
    )
    
    # Step 4: Decide monitoring strategy
    monitoring_decision = BranchPythonOperator(
        task_id='decide_monitoring_strategy',
        python_callable=decide_monitoring_strategy,
        doc_md="Choose between webhook or polling monitoring"
    )
    
    # Step 5a: Webhook monitoring (for priority filings)
    webhook_monitor = WebhookTriggerSensor(
        task_id='webhook_monitor',
        filing_id="{{ task_instance.xcom_pull(task_ids='initiate_filing')['filing_id'] }}",
        timeout=timedelta(hours=72).total_seconds(),
        poke_interval=30,
        mode='reschedule',
        doc_md="Wait for webhook callback from filing system"
    )
    
    # Step 5b: Smart polling monitoring (for standard filings)
    polling_monitor = SmartFilingSensor(
        task_id='polling_monitor',
        filing_id="{{ task_instance.xcom_pull(task_ids='initiate_filing')['filing_id'] }}",
        timeout=timedelta(hours=72).total_seconds(),
        initial_poke_interval=60,
        max_poke_interval=600,
        mode='reschedule',
        doc_md="Smart polling with exponential backoff"
    )
    
    # Step 6: Process result
    process_result = BranchPythonOperator(
        task_id='process_result',
        python_callable=process_filing_result,
        trigger_rule='none_skipped',
        doc_md="Process filing result and decide next action"
    )
    
    # Step 7a: Success handling
    success_task = PythonOperator(
        task_id='success_handler',
        python_callable=success_handler,
        doc_md="Handle successful filing completion"
    )
    
    # Step 7b: Failure handling
    failure_task = PythonOperator(
        task_id='failure_handler',
        python_callable=failure_handler,
        doc_md="Handle filing failure"
    )
    
    # Step 8: End
    end_task = EmptyOperator(
        task_id='end_workflow',
        trigger_rule='none_skipped',
        doc_md="End of workflow"
    )
    
    # Define task dependencies
    validation >> transformation >> filing_init >> monitoring_decision
    monitoring_decision >> [webhook_monitor, polling_monitor]
    [webhook_monitor, polling_monitor] >> process_result
    process_result >> [success_task, failure_task]
    [success_task, failure_task] >> end_task