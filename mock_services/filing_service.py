from flask import Flask, request, jsonify
import json
import time
import threading
import requests
import uuid
from datetime import datetime, timedelta
import sqlite3
import os

app = Flask(__name__)

# Configuration
WEBHOOK_LISTENER_URL = os.getenv('WEBHOOK_LISTENER_URL', 'http://webhook-listener:5001')
AIRFLOW_WEBHOOK_URL = os.getenv('AIRFLOW_WEBHOOK_URL', 'http://airflow-webserver:8080/api/v1/dags/filing_callback_dag/dagRuns')

# SQLite database for persistence
DB_PATH = '/tmp/filing_service.db'

def init_db():
    '''Initialize SQLite database'''
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS filings (
            filing_id TEXT PRIMARY KEY,
            status TEXT,
            filing_data TEXT,
            result_data TEXT,
            created_at TEXT,
            updated_at TEXT,
            callback_url TEXT,
            webhook_url TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def update_filing_status(filing_id, status, result_data=None):
    '''Update filing status in database'''
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE filings 
        SET status = ?, result_data = ?, updated_at = ?
        WHERE filing_id = ?
    ''', (status, json.dumps(result_data) if result_data else None, 
          datetime.now().isoformat(), filing_id))
    
    conn.commit()
    conn.close()

def get_filing_status(filing_id):
    '''Get filing status from database'''
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM filings WHERE filing_id = ?', (filing_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'filing_id': result[0],
            'status': result[1],
            'filing_data': json.loads(result[2]) if result[2] else {},
            'result_data': json.loads(result[3]) if result[3] else {},
            'created_at': result[4],
            'updated_at': result[5],
            'callback_url': result[6],
            'webhook_url': result[7]
        }
    return None

def send_webhook_notification(filing_id, status, data):
    '''Send webhook notification via webhook listener'''
    
    try:
        # Send to webhook listener first
        webhook_payload = {
            'filing_id': filing_id,
            'status': status,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        
        response = requests.post(
            f"{WEBHOOK_LISTENER_URL}/webhook/filing/{filing_id}",
            json=webhook_payload,
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✅ Webhook sent to listener for {filing_id}: {status}")
        else:
            print(f"⚠️ Webhook listener failed for {filing_id}: {response.status_code}")
            
            # Fallback: send directly to Airflow
            fallback_payload = {
                'conf': webhook_payload
            }
            
            airflow_response = requests.post(
                AIRFLOW_WEBHOOK_URL,
                json=fallback_payload,
                auth=('airflow', 'airflow'),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if airflow_response.status_code in [200, 201]:
                print(f"✅ Fallback webhook sent directly to Airflow for {filing_id}")
            else:
                print(f"❌ All webhook attempts failed for {filing_id}")
                
    except Exception as e:
        print(f"❌ Webhook error for {filing_id}: {e}")

def simulate_filing_process(filing_id, filing_data, callback_url=None, webhook_url=None):
    '''Simulate long-running filing process with webhook notifications'''
    
    print(f"🚀 Starting filing simulation for {filing_id}")
    
    # Simulate different processing stages
    stages = [
        ('RECEIVED', 'Filing received and queued', 2),
        ('PROCESSING', 'Document validation in progress', 10),
        ('REVIEWING', 'Under regulatory review', 15),
        ('FINALIZING', 'Generating final documents', 8)
    ]
    
    try:
        for stage, message, duration in stages:
            print(f"📋 {filing_id}: {stage} - {message}")
            
            stage_data = {
                'message': message,
                'timestamp': datetime.now().isoformat(),
                'stage': stage
            }
            
            # Update status
            update_filing_status(filing_id, stage, stage_data)
            
            # Send webhook notification
            send_webhook_notification(filing_id, stage, stage_data)
            
            # Simulate processing time (reduced for demo)
            time.sleep(duration)
        
        # Determine final result (90% success rate for demo)
        import random
        if random.random() < 0.9:
            final_status = 'SUCCESS'
            result_data = {
                'filing_reference': f"REF-{filing_id[-8:]}",
                'approval_date': datetime.now().isoformat(),
                'documents_generated': [
                    f"certificate_{filing_id}.pdf",
                    f"approval_letter_{filing_id}.pdf"
                ],
                'next_renewal_date': (datetime.now() + timedelta(days=365)).isoformat(),
                'completion_time': datetime.now().isoformat()
            }
        else:
            final_status = 'FAILED'
            result_data = {
                'error_code': 'VALIDATION_ERROR',
                'error_message': 'Document format validation failed',
                'required_corrections': [
                    'Update signature format',
                    'Correct date formatting in section 3'
                ],
                'failure_time': datetime.now().isoformat()
            }
        
        print(f"✅ {filing_id}: Filing completed with status {final_status}")
        
        # Update final status
        update_filing_status(filing_id, final_status, result_data)
        
        # Send final webhook
        send_webhook_notification(filing_id, final_status, result_data)
            
    except Exception as e:
        print(f"❌ {filing_id}: Filing failed with error: {e}")
        error_data = {
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'stage': 'ERROR'
        }
        update_filing_status(filing_id, 'ERROR', error_data)
        send_webhook_notification(filing_id, 'ERROR', error_data)

@app.route('/api/filing/submit', methods=['POST'])
def submit_filing():
    '''Submit new filing request'''
    
    data = request.get_json()
    filing_data = data.get('filing_data', {})
    callback_url = data.get('callback_url')
    filing_id = data.get('filing_id') or f"filing_{uuid.uuid4().hex[:8]}"
    
    # Webhook URL (prefer webhook listener)
    webhook_url = f"{WEBHOOK_LISTENER_URL}/webhook/filing/{filing_id}"
    
    # Store filing in database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO filings 
        (filing_id, status, filing_data, created_at, updated_at, callback_url, webhook_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (filing_id, 'SUBMITTED', json.dumps(filing_data), 
          datetime.now().isoformat(), datetime.now().isoformat(), 
          callback_url, webhook_url))
    
    conn.commit()
    conn.close()
    
    print(f"📥 New filing submitted: {filing_id}")
    print(f"Webhook URL: {webhook_url}")
    print(f"Callback URL: {callback_url}")
    
    # Register webhook with listener
    try:
        registration_payload = {
            'filing_id': filing_id,
            'callback_url': callback_url or AIRFLOW_WEBHOOK_URL,
            'dag_id': 'filing_callback_dag'
        }
        
        requests.post(
            f"{WEBHOOK_LISTENER_URL}/webhook/register",
            json=registration_payload,
            timeout=5
        )
        print(f"✅ Webhook registered with listener for {filing_id}")
        
    except Exception as e:
        print(f"⚠️ Failed to register webhook with listener: {e}")
    
    # Start background processing
    process_thread = threading.Thread(
        target=simulate_filing_process,
        args=(filing_id, filing_data, callback_url, webhook_url),
        daemon=True
    )
    process_thread.start()
    
    return jsonify({
        'message': 'Filing submitted successfully',
        'filing_id': filing_id,
        'status': 'SUBMITTED',
        'webhook_url': webhook_url,
        'callback_url': callback_url,
        'estimated_completion': (datetime.now() + timedelta(minutes=35)).isoformat(),
        'tracking_url': f'/api/filing/status/{filing_id}'
    }), 201

@app.route('/api/filing/status/<filing_id>', methods=['GET'])
def get_filing_status_endpoint(filing_id):
    '''Get current status of a filing'''
    
    filing = get_filing_status(filing_id)
    
    if not filing:
        return jsonify({'error': 'Filing not found'}), 404
    
    return jsonify({
        'filing_id': filing_id,
        'status': filing['status'],
        'filing_data': filing['filing_data'],
        'result_data': filing['result_data'],
        'created_at': filing['created_at'],
        'updated_at': filing['updated_at'],
        'callback_url': filing['callback_url'],
        'webhook_url': filing['webhook_url']
    })

@app.route('/api/filing/list', methods=['GET'])
def list_filings():
    '''List all filings with optional status filter'''
    
    status_filter = request.args.get('status')
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if status_filter:
        cursor.execute('SELECT * FROM filings WHERE status = ? ORDER BY created_at DESC', (status_filter,))
    else:
        cursor.execute('SELECT * FROM filings ORDER BY created_at DESC')
    
    results = cursor.fetchall()
    conn.close()
    
    filings = []
    for row in results:
        filings.append({
            'filing_id': row[0],
            'status': row[1],
            'filing_data': json.loads(row[2]) if row[2] else {},
            'result_data': json.loads(row[3]) if row[3] else {},
            'created_at': row[4],
            'updated_at': row[5]
        })
    
    return jsonify({
        'filings': filings,
        'total_count': len(filings),
        'status_filter': status_filter
    })

@app.route('/api/filing/cancel/<filing_id>', methods=['POST'])
def cancel_filing(filing_id):
    '''Cancel a filing in progress'''
    
    filing = get_filing_status(filing_id)
    
    if not filing:
        return jsonify({'error': 'Filing not found'}), 404
    
    if filing['status'] in ['SUCCESS', 'FAILED', 'CANCELLED']:
        return jsonify({'error': f'Cannot cancel filing with status: {filing["status"]}'}), 400
    
    # Update status to cancelled
    cancellation_data = {
        'cancelled_at': datetime.now().isoformat(),
        'reason': request.get_json().get('reason', 'User requested cancellation') if request.get_json() else 'User requested cancellation'
    }
    
    update_filing_status(filing_id, 'CANCELLED', cancellation_data)
    
    # Send webhook notification
    send_webhook_notification(filing_id, 'CANCELLED', cancellation_data)
    
    print(f"🚫 Filing cancelled: {filing_id}")
    
    return jsonify({
        'message': 'Filing cancelled successfully',
        'filing_id': filing_id,
        'status': 'CANCELLED',
        'cancelled_at': cancellation_data['cancelled_at']
    })

@app.route('/api/filing/retry/<filing_id>', methods=['POST'])
def retry_filing(filing_id):
    '''Retry a failed filing'''
    
    filing = get_filing_status(filing_id)
    
    if not filing:
        return jsonify({'error': 'Filing not found'}), 404
    
    if filing['status'] != 'FAILED':
        return jsonify({'error': f'Can only retry failed filings. Current status: {filing["status"]}'}), 400
    
    print(f"🔄 Retrying filing: {filing_id}")
    
    # Reset status to submitted
    update_filing_status(filing_id, 'SUBMITTED', {'retried_at': datetime.now().isoformat()})
    
    # Send webhook notification
    send_webhook_notification(filing_id, 'RETRY_STARTED', {'retried_at': datetime.now().isoformat()})
    
    # Start background processing again
    process_thread = threading.Thread(
        target=simulate_filing_process,
        args=(filing_id, filing['filing_data'], filing['callback_url'], filing['webhook_url']),
        daemon=True
    )
    process_thread.start()
    
    return jsonify({
        'message': 'Filing retry initiated',
        'filing_id': filing_id,
        'status': 'SUBMITTED',
        'retried_at': datetime.now().isoformat()
    })

@app.route('/api/filing/webhook/test/<filing_id>', methods=['POST'])
def test_webhook(filing_id):
    '''Send test webhook for a filing'''
    
    filing = get_filing_status(filing_id)
    
    if not filing:
        return jsonify({'error': 'Filing not found'}), 404
    
    test_data = {
        'test': True,
        'message': 'This is a test webhook notification',
        'timestamp': datetime.now().isoformat(),
        'filing_status': filing['status']
    }
    
    send_webhook_notification(filing_id, 'TEST', test_data)
    
    return jsonify({
        'message': 'Test webhook sent',
        'filing_id': filing_id,
        'test_data': test_data
    })

@app.route('/api/stats', methods=['GET'])
def get_statistics():
    '''Get filing service statistics'''
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get counts by status
    cursor.execute('''
        SELECT status, COUNT(*) as count 
        FROM filings 
        GROUP BY status
    ''')
    
    status_counts = {}
    for row in cursor.fetchall():
        status_counts[row[0]] = row[1]
    
    # Get total count
    cursor.execute('SELECT COUNT(*) FROM filings')
    total_filings = cursor.fetchone()[0]
    
    # Get recent filings (last 24 hours)
    yesterday = (datetime.now() - timedelta(days=1)).isoformat()
    cursor.execute('SELECT COUNT(*) FROM filings WHERE created_at > ?', (yesterday,))
    recent_filings = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        'service': 'Filing Service',
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'statistics': {
            'total_filings': total_filings,
            'recent_filings_24h': recent_filings,
            'status_breakdown': status_counts
        },
        'webhook_listener_url': WEBHOOK_LISTENER_URL,
        'airflow_webhook_url': AIRFLOW_WEBHOOK_URL
    })

@app.route('/health', methods=['GET'])
def health_check():
    '''Health check endpoint'''
    return jsonify({
        'status': 'healthy',
        'service': 'Filing Service',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Initialize database
    init_db()
    
    print("🚀 Filing Service starting...")
    print("📡 Available endpoints:")
    print("  POST /api/filing/submit - Submit new filing")
    print("  GET  /api/filing/status/<filing_id> - Get filing status")
    print("  GET  /api/filing/list - List all filings")
    print("  POST /api/filing/cancel/<filing_id> - Cancel filing")
    print("  POST /api/filing/retry/<filing_id> - Retry failed filing")
    print("  POST /api/filing/webhook/test/<filing_id> - Test webhook")
    print("  GET  /api/stats - Service statistics")
    print("  GET  /health - Health check")
    print(f"🔗 Webhook Listener URL: {WEBHOOK_LISTENER_URL}")
    print(f"🔗 Airflow Webhook URL: {AIRFLOW_WEBHOOK_URL}")
    
    app.run(host='0.0.0.0', port=5000, debug=True)