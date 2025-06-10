from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
import requests
import json
import time
from typing import Any, Dict, Optional

class SmartFilingSensor(BaseSensorOperator):
    '''
    Smart sensor that adapts polling frequency based on filing duration
    and integrates with database tracking
    '''
    
    template_fields = ['filing_id']
    
    def __init__(
        self,
        filing_id: str,
        postgres_conn_id: str = 'postgres_default',
        filing_api_endpoint: str = 'http://filing-service:5000/api/filing/status',
        initial_poke_interval: int = 30,
        max_poke_interval: int = 300,
        exponential_backoff: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.filing_id = filing_id
        self.postgres_conn_id = postgres_conn_id
        self.filing_api_endpoint = filing_api_endpoint
        self.initial_poke_interval = initial_poke_interval
        self.max_poke_interval = max_poke_interval
        self.exponential_backoff = exponential_backoff
        self.current_interval = initial_poke_interval
        
    def poke(self, context: Context) -> bool:
        '''Check filing status with intelligent polling'''
        
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        try:
            # First check database
            db_result = pg_hook.get_first(
                "SELECT status, result_data, updated_at FROM filing_tracker WHERE filing_id = %s",
                parameters=(self.filing_id,)
            )
            
            if db_result and db_result[0] in ['SUCCESS', 'FAILED', 'COMPLETED']:
                self.log.info(f"Filing {self.filing_id} completed with status: {db_result[0]}")
                
                # Store result in XCom
                result_data = {
                    'status': db_result[0],
                    'data': json.loads(db_result[1]) if db_result[1] else {},
                    'completed_at': db_result[2].isoformat() if db_result[2] else None
                }
                
                context['task_instance'].xcom_push(key='filing_result', value=result_data)
                return True
            
            # If not in final state, check external API
            try:
                response = requests.get(f"{self.filing_api_endpoint}/{self.filing_id}", timeout=10)
                if response.status_code == 200:
                    api_result = response.json()
                    
                    # Update database with latest status
                    pg_hook.run(
                        '''
                        UPDATE filing_tracker 
                        SET status = %s, result_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE filing_id = %s
                        ''',
                        parameters=(
                            api_result.get('status', 'UNKNOWN'),
                            json.dumps(api_result.get('data', {})),
                            self.filing_id
                        )
                    )
                    
                    # Insert status history
                    pg_hook.run(
                        '''
                        INSERT INTO filing_status_history (filing_id, status, message)
                        VALUES (%s, %s, %s)
                        ''',
                        parameters=(
                            self.filing_id,
                            api_result.get('status'),
                            api_result.get('message', 'Status check via API')
                        )
                    )
                    
                    if api_result.get('status') in ['SUCCESS', 'FAILED', 'COMPLETED']:
                        context['task_instance'].xcom_push(key='filing_result', value=api_result)
                        return True
                        
            except requests.RequestException as e:
                self.log.warning(f"API call failed, relying on database: {e}")
            
            # Update polling interval with exponential backoff
            if self.exponential_backoff:
                self.current_interval = min(
                    self.current_interval * 1.5,
                    self.max_poke_interval
                )
                self.poke_interval = int(self.current_interval)
                
            self.log.info(f"Filing {self.filing_id} still in progress. Next check in {self.poke_interval} seconds")
            return False
            
        except Exception as e:
            self.log.error(f"Error checking filing status: {e}")
            return False

class WebhookTriggerSensor(BaseSensorOperator):
    '''
    Sensor that waits for webhook callbacks stored in database
    '''
    
    def __init__(
        self,
        filing_id: str,
        postgres_conn_id: str = 'postgres_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.filing_id = filing_id
        self.postgres_conn_id = postgres_conn_id
        
    def poke(self, context: Context) -> bool:
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # Check if webhook has been received
        result = pg_hook.get_first(
            '''
            SELECT status, result_data, completed_at 
            FROM filing_tracker 
            WHERE filing_id = %s AND status IN ('SUCCESS', 'FAILED', 'COMPLETED')
            ''',
            parameters=(self.filing_id,)
        )
        
        if result:
            webhook_data = {
                'status': result[0],
                'data': json.loads(result[1]) if result[1] else {},
                'completed_at': result[2].isoformat() if result[2] else None
            }
            
            context['task_instance'].xcom_push(key='webhook_result', value=webhook_data)
            return True
            
        return False