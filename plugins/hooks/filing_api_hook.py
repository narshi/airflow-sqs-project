from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
from typing import Dict, Any, Optional

class FilingAPIHook(BaseHook):
    '''Custom hook for filing API interactions'''
    
    def __init__(
        self,
        filing_api_conn_id: str = 'filing_api_default',
        postgres_conn_id: str = 'postgres_default'
    ):
        super().__init__()
        self.filing_api_conn_id = filing_api_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.base_url = 'http://filing-service:5000/api'
        
    def submit_filing(
        self,
        filing_data: Dict[str, Any],
        callback_url: Optional[str] = None,
        filing_id: Optional[str] = None
    ) -> Dict[str, Any]:
        '''Submit filing request with callback registration'''
        
        payload = {
            'filing_data': filing_data,
            'filing_id': filing_id,
            'callback_url': callback_url
        }
        
        response = requests.post(
            f"{self.base_url}/filing/submit",
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Store in database
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            pg_hook.run(
                '''
                INSERT INTO filing_tracker 
                (filing_id, status, filing_data, callback_url, dag_run_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (filing_id) 
                DO UPDATE SET 
                    status = EXCLUDED.status,
                    filing_data = EXCLUDED.filing_data,
                    callback_url = EXCLUDED.callback_url,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                parameters=(
                    result['filing_id'],
                    'SUBMITTED',
                    json.dumps(filing_data),
                    callback_url,
                    filing_id
                )
            )
            
            return result
        else:
            raise Exception(f"Filing submission failed: {response.status_code} - {response.text}")
    
    def get_filing_status(self, filing_id: str) -> Dict[str, Any]:
        '''Get current filing status'''
        
        response = requests.get(
            f"{self.base_url}/filing/status/{filing_id}",
            timeout=10
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Status check failed: {response.status_code}")
    
    def register_webhook(self, filing_id: str, webhook_url: str) -> bool:
        '''Register webhook for filing updates'''
        
        payload = {
            'filing_id': filing_id,
            'webhook_url': webhook_url
        }
        
        response = requests.post(
            f"{self.base_url}/filing/webhook/register",
            json=payload,
            timeout=10
        )
        
        return response.status_code == 200