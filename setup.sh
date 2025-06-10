SETUP_SCRIPT = """#!/bin/bash

# Setup script for Filing Workflow POC
echo "🚀 Setting up Filing Workflow POC..."

# Create directory structure
mkdir -p filing_poc/{dags,plugins/{operators,hooks},mock_services,sql,logs}

# Set permissions
chmod -R 755 filing_poc/

# Copy configuration files
echo "📁 Creating configuration files..."

# Create docker-compose.yml
cat > filing_poc/docker-compose.yml << 'EOF'
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_db_volume:/var/lib/postgresql/data
      - ./sql/init_db.sql:/docker-entrypoint-initdb.d/init_db.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:latest
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  airflow-webserver:
    image: apache/airflow:2.7.0
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
      AIRFLOW__CORE__FERNET_KEY: 'YlCImzjge_TeZc7jPJ7Jz2pgOtKLk7TqbnBDfHaH0xI='
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: 'airflow'
      _AIRFLOW_WWW_USER_PASSWORD: 'airflow'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
    ports:
      - "8080:8080"
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5

  airflow-scheduler:
    image: apache/airflow:2.7.0
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
      AIRFLOW__CORE__FERNET_KEY: 'YlCImzjge_TeZc7jPJ7Jz2pgOtKLk7TqbnBDfHaH0xI='
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
    command: scheduler

  airflow-worker:
    image: apache/airflow:2.7.0
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
      AIRFLOW__CORE__FERNET_KEY: 'YlCImzjge_TeZc7jPJ7Jz2pgOtKLk7TqbnBDfHaH0xI='
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
    command: celery worker

  filing-service:
    build: ./mock_services
    ports:
      - "5000:5000"
    environment:
      - AIRFLOW_WEBHOOK_URL=http://airflow-webserver:8080/api/v1/dags/filing_callback_dag/dagRuns
    depends_on:
      - postgres

volumes:
  postgres_db_volume:
EOF

echo "✅ Docker Compose configuration created"

# Initialize database
echo "🗄️ Creating database initialization script..."
# (Database init SQL would be created here)

echo "📦 Setup completed!"
echo ""
echo "🚀 To start the demo:"
echo "1. cd filing_poc"
echo "2. docker-compose up -d"
echo "3. Wait for services to be healthy (check with: docker-compose ps)"
echo "4. Access Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "5. Access Filing Service: http://localhost:5001/health"
echo ""
echo "📋 To run a demo filing:"
echo "1. Go to Airflow UI -> DAGs"
echo "2. Find 'intelligent_filing_workflow'"
echo "3. Click 'Trigger DAG w/ Config'"
echo "4. Use this config:"
echo '{'
echo '  "filing_data": {'
echo '    "company_name": "Demo Corp",'
echo '    "filing_type": "standard",'
echo '    "documents": ["doc1.pdf", "doc2.pdf"]'
echo '  }'
echo '}'