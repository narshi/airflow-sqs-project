from flask import Flask, request, jsonify
import requests
import json
import logging
from datetime import datetime
import threading
import time
from queue import Queue
import sqlite3

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Webhook queue for processing
webhook_queue = Queue()

# Configuration
AIRFLOW_BASE_URL = "http://airflow-webserver:8080"
AIRFLOW_AUTH = ('airflow', 'airflow')
DB_PATH = '/tmp/webhook_listener.db'

def init_webhook_db():
    '''Initialize webhook tracking database'''
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id TEXT,
            event_type TEXT,
            payload TEXT,
            status TEXT,
            received_at TEXT,
            processed_at TEXT,
            airflow_response TEXT,
            retry_count INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS webhook_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id TEXT UNIQUE,
            callback_url TEXT,
            dag_id TEXT,
            active BOOLEAN DEFAULT 1,
            created_at TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Webhook database initialized")

def store_webhook_event(filing_id, event_type, payload, status='RECEIVED'):
    '''Store webhook event in database'''
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO webhook_events 
        (filing_id, event_type, payload, status, received_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (filing_id, event_type, json.dumps(payload), status, datetime.now().isoformat()))
    
    event_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return event_id

def update_webhook_event(event_id, status, airflow_response=None, retry_count=None):
    '''Update webhook event status'''
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    update_fields = ['status = ?', 'processed_at = ?']
    params = [status, datetime.now().isoformat()]
    
    if airflow_response:
        update_fields.append('airflow_response = ?')
        params.append(json.dumps(airflow_response))
    
    if retry_count is not None:
        update_fields.append('retry_count = ?')
        params.append(retry_count)
    
    params.append(event_id)
    
    cursor.execute(f'''
        UPDATE webhook_events 
        SET {', '.join(update_fields)}
        WHERE id = ?
    ''', params)
    
    conn.commit()
    conn.close()

def forward_to_airflow(filing_id, status, data, dag_id='filing_callback_dag', max_retries=3):
    '''Forward webhook to Airflow with retry logic'''
    
    payload = {
        'conf': {
            'filing_id': filing_id,
            'status': status,
            'data': data,
            'timestamp': datetime.now().isoformat(),
            'source': 'webhook_listener'
        }
    }
    
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns"
    headers = {
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Forwarding to Airflow (attempt {attempt + 1}/{max_retries}): {filing_id}")
            
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                auth=AIRFLOW_AUTH,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ Successfully forwarded webhook for {filing_id}")
                return {
                    'success': True,
                    'status_code': response.status_code,
                    'response': response.json(),
                    'attempt': attempt + 1
                }
            else:
                logger.warning(f"⚠️ Airflow responded with {response.status_code}: {response.text}")
                
                if attempt == max_retries - 1:
                    return {
                        'success': False,
                        'status_code': response.status_code,
                        'error': response.text,
                        'attempts': max_retries
                    }
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed (attempt {attempt + 1}): {e}")
            
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'error': str(e),
                    'attempts': max_retries
                }
            
            # Wait before retry
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return {'success': False, 'error': 'Max retries exceeded'}

def process_webhook_queue():
    '''Background worker to process webhook queue'''
    logger.info("🚀 Webhook queue processor started")
    
    while True:
        try:
            # Get webhook from queue (blocking)
            webhook_data = webhook_queue.get(timeout=1)
            
            event_id = webhook_data['event_id']
            filing_id = webhook_data['filing_id']
            status = webhook_data['status']
            data = webhook_data['data']
            
            logger.info(f"📤 Processing webhook for filing {filing_id}")
            
            # Forward to Airflow
            result = forward_to_airflow(filing_id, status, data)
            
            # Update database
            if result['success']:
                update_webhook_event(event_id, 'FORWARDED', result)
                logger.info(f"✅ Webhook processed successfully: {filing_id}")
            else:
                update_webhook_event(event_id, 'FAILED', result)
                logger.error(f"❌ Webhook processing failed: {filing_id}")
            
            # Mark task as done
            webhook_queue.task_done()
            
        except Exception as e:
            if "timed out" not in str(e).lower():
                logger.error(f"Queue processing error: {e}")
            continue

@app.route('/webhook/filing/<filing_id>', methods=['POST'])
def receive_filing_webhook(filing_id):
    '''Receive webhook from filing service'''
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        status = data.get('status', 'UNKNOWN')
        result_data = data.get('data', {})
        
        logger.info(f"📥 Received webhook for filing {filing_id}: {status}")
        
        # Store in database
        event_id = store_webhook_event(filing_id, 'STATUS_UPDATE', {
            'status': status,
            'data': result_data,
            'headers': dict(request.headers)
        })
        
        # Add to processing queue
        webhook_queue.put({
            'event_id': event_id,
            'filing_id': filing_id,
            'status': status,
            'data': result_data
        })
        
        return jsonify({
            'message': 'Webhook received successfully',
            'filing_id': filing_id,
            'event_id': event_id,
            'status': status,
            'queued_at': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing webhook for {filing_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/webhook/generic', methods=['POST'])
def receive_generic_webhook():
    '''Receive generic webhook (auto-extract filing_id)'''
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Try to extract filing_id from various locations
        filing_id = (
            data.get('filing_id') or 
            data.get('id') or 
            data.get('reference_id') or
            request.headers.get('X-Filing-ID')
        )
        
        if not filing_id:
            return jsonify({'error': 'filing_id not found in payload or headers'}), 400
        
        status = data.get('status', 'UNKNOWN')
        result_data = data.get('data', data)  # Use entire payload as data if no 'data' field
        
        logger.info(f"📥 Received generic webhook for filing {filing_id}: {status}")
        
        # Store and queue
        event_id = store_webhook_event(filing_id, 'GENERIC_UPDATE', {
            'status': status,
            'data': result_data,
            'raw_payload': data
        })
        
        webhook_queue.put({
            'event_id': event_id,
            'filing_id': filing_id,
            'status': status,
            'data': result_data
        })
        
        return jsonify({
            'message': 'Generic webhook received successfully',
            'filing_id': filing_id,
            'event_id': event_id,
            'status': status
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing generic webhook: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/webhook/register', methods=['POST'])
def register_webhook_subscription():
    '''Register webhook subscription for a filing'''
    
    try:
        data = request.get_json()
        
        filing_id = data.get('filing_id')
        callback_url = data.get('callback_url')
        dag_id = data.get('dag_id', 'filing_callback_dag')
        
        if not filing_id or not callback_url:
            return jsonify({'error': 'filing_id and callback_url are required'}), 400
        
        # Store subscription
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO webhook_subscriptions 
            (filing_id, callback_url, dag_id, created_at)
            VALUES (?, ?, ?, ?)
        ''', (filing_id, callback_url, dag_id, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        logger.info(f"📝 Registered webhook subscription for {filing_id}")
        
        return jsonify({
            'message': 'Webhook subscription registered',
            'filing_id': filing_id,
            'callback_url': callback_url,
            'dag_id': dag_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error registering webhook subscription: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/webhook/events/<filing_id>', methods=['GET'])
def get_webhook_events(filing_id):
    '''Get webhook events for a filing'''
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, event_type, payload, status, received_at, processed_at, retry_count
        FROM webhook_events 
        WHERE filing_id = ? 
        ORDER BY received_at DESC
    ''', (filing_id,))
    
    events = []
    for row in cursor.fetchall():
        events.append({
            'id': row[0],
            'event_type': row[1],
            'payload': json.loads(row[2]) if row[2] else {},
            'status': row[3],
            'received_at': row[4],
            'processed_at': row[5],
            'retry_count': row[6]
        })
    
    conn.close()
    
    return jsonify({
        'filing_id': filing_id,
        'events': events,
        'total_events': len(events)
    })

@app.route('/webhook/status', methods=['GET'])
def webhook_status():
    '''Get webhook listener status'''
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get statistics
    cursor.execute('SELECT COUNT(*) FROM webhook_events')
    total_events = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM webhook_events WHERE status = "FORWARDED"')
    forwarded_events = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM webhook_events WHERE status = "FAILED"')
    failed_events = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM webhook_subscriptions WHERE active = 1')
    active_subscriptions = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        'service': 'Webhook Listener',
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'queue_size': webhook_queue.qsize(),
        'statistics': {
            'total_events': total_events,
            'forwarded_events': forwarded_events,
            'failed_events': failed_events,
            'active_subscriptions': active_subscriptions
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    '''Health check endpoint'''
    return jsonify({
        'status': 'healthy',
        'service': 'Webhook Listener',
        'timestamp': datetime.now().isoformat(),
        'queue_size': webhook_queue.qsize()
    })

if __name__ == '__main__':
    # Initialize database
    init_webhook_db()
    
    # Start background queue processor
    queue_thread = threading.Thread(target=process_webhook_queue, daemon=True)
    queue_thread.start()
    
    logger.info("🚀 Webhook Listener starting...")
    logger.info("📡 Available endpoints:")
    logger.info("  POST /webhook/filing/<filing_id> - Receive filing-specific webhook")
    logger.info("  POST /webhook/generic - Receive generic webhook")
    logger.info("  POST /webhook/register - Register webhook subscription")
    logger.info("  GET  /webhook/events/<filing_id> - Get events for filing")
    logger.info("  GET  /webhook/status - Service status")
    logger.info("  GET  /health - Health check")
    
    app.run(host='0.0.0.0', port=5001, debug=True)