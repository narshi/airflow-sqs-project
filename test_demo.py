#!/usr/bin/env python3
"""
Testing script for Filing Workflow POC
This script demonstrates various scenarios and tests the workflow
"""

import requests
import json
import time
from datetime import datetime

# Configuration
AIRFLOW_URL = "http://localhost:8080"
FILING_SERVICE_URL = "http://localhost:5000"
AUTH = ('airflow', 'airflow')

def test_filing_service():
    """Test the mock filing service"""
    print("🧪 Testing Filing Service...")
    
    # Health check
    response = requests.get(f"{FILING_SERVICE_URL}/health")
    print(f"Health check: {response.status_code}")
    
    # Submit test filing
    filing_data = {
        "filing_data": {
            "company_name": "Test Corporation",
            "filing_type": "urgent",
            "documents": ["test1.pdf", "test2.pdf"]
        },
        "callback_url": f"{AIRFLOW_URL}/api/v1/dags/filing_callback_dag/dagRuns"
    }
    
    response = requests.post(f"{FILING_SERVICE_URL}/api/filing/submit", json=filing_data)
    print(f"Filing submission: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        filing_id = result['filing_id']
        print(f"Filing ID: {filing_id}")
        
        # Check status
        time.sleep(2)
        status_response = requests.get(f"{FILING_SERVICE_URL}/api/filing/status/{filing_id}")
        print(f"Status check: {status_response.json()}")
        
        return filing_id
    
    return None

def trigger_airflow_dag(filing_data):
    """Trigger Airflow DAG with test data"""
    print("🚀 Triggering Airflow DAG...")
    
    dag_run_data = {
        "conf": {
            "filing_data": filing_data
        }
    }
    
    response = requests.post(
        f"{AIRFLOW_URL}/api/v1/dags/intelligent_filing_workflow/dagRuns",
        json=dag_run_data,
        auth=AUTH,
        headers={'Content-Type': 'application/json'}
    )
    
    print(f"DAG trigger: {response.status_code}")
    if response.status_code in [200, 201]:
        result = response.json()
        print(f"DAG Run ID: {result.get('dag_run_id')}")
        return result.get('dag_run_id')
    else:
        print(f"Error: {response.text}")
    
    return None

def monitor_dag_run(dag_run_id):
    """Monitor DAG run progress"""
    print(f"📊 Monitoring DAG run: {dag_run_id}")
    
    for i in range(30):  # Monitor for 5 minutes
        response = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/intelligent_filing_workflow/dagRuns/{dag_run_id}",
            auth=AUTH
        )
        
        if response.status_code == 200:
            result = response.json()
            state = result.get('state')
            print(f"DAG state: {state}")
            
            if state in ['success', 'failed']:
                return state
        
        time.sleep(10)
    
    return 'timeout'

def run_demo_scenarios():
    """Run various demo scenarios"""
    
    scenarios = [
        {
            "name": "Standard Filing (Polling)",
            "data": {
                "company_name": "Standard Corp",
                "filing_type": "standard",
                "documents": ["standard1.pdf", "standard2.pdf"]
            }
        },
        {
            "name": "Priority Filing (Webhook)",
            "data": {
                "company_name": "Priority Corp",
                "filing_type": "urgent",
                "documents": ["priority1.pdf", "priority2.pdf"]
            }
        },
        {
            "name": "Large Filing (Extended Processing)",
            "data": {
                "company_name": "Large Corp",
                "filing_type": "complex",
                "documents": ["large1.pdf", "large2.pdf", "large3.pdf", "large4.pdf"]
            }
        }
    ]
    
    results = []
    
    for scenario in scenarios:
        print(f"\n{'='*50}")
        print(f"🎯 Running scenario: {scenario['name']}")
        print(f"{'='*50}")
        
        # Trigger DAG
        dag_run_id = trigger_airflow_dag(scenario['data'])
        
        if dag_run_id:
            # Monitor progress
            final_state = monitor_dag_run(dag_run_id)
            results.append({
                'scenario': scenario['name'],
                'dag_run_id': dag_run_id,
                'final_state': final_state
            })
        
        print(f"✅ Scenario completed: {scenario['name']}")
        time.sleep(5)  # Brief pause between scenarios
    
    return results

if __name__ == "__main__":
    print("🎭 Filing Workflow POC - Demo Test Script")
    print("=" * 60)
    
    # Test filing service
    test_filing_id = test_filing_service()
    
    if test_filing_id:
        print("✅ Filing service is working")
        
        # Run demo scenarios
        results = run_demo_scenarios()
        
        print(f"\n{'='*60}")
        print("📊 DEMO RESULTS SUMMARY")
        print(f"{'='*60}")
        
        for result in results:
            print(f"Scenario: {result['scenario']}")
            print(f"DAG Run ID: {result['dag_run_id']}")
            print(f"Final State: {result['final_state']}")
            print("-" * 40)
        
        print("🎉 Demo completed!")
        
    else:
        print("❌ Filing service test failed")