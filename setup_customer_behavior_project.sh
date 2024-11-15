#!/bin/bash

# Project Name
PROJECT_NAME="customer_behaviour_pipeline"

# Create Project Root Directory
mkdir -p $PROJECT_NAME
cd $PROJECT_NAME || exit

# Create Project Structure
echo "Creating project structure..."

# Core Directories
mkdir -p dags data/bronze data/silver data/gold database logs notebooks scripts tests customer_behaviour/functions customer_behaviour/jobs

# Essential Python Module Structure
touch customer_behaviour/functions/{__init__.py,bronze_ingestion.py,silver_transformation.py,gold_aggregations.py,bigquery_loading.py,schema.py,session.py}
touch customer_behaviour/jobs/{__init__.py,ingest_bronze.py,transform_silver.py,aggregate_gold.py}

# Create Airflow DAGs
touch dags/{bronze_dag.py,silver_dag.py,gold_dag.py}

# Create Test Files
mkdir -p tests/integration
touch tests/{test_bronze_ingestion.py,test_silver_transformation.py,test_gold_aggregations.py,test_bigquery_loading.py,test_schema.py,test_session.py,test_config.py}
touch tests/integration/{test_bronze_silver_pipeline.py,test_silver_gold_pipeline.py}

# Initialize Poetry
echo "Initializing Poetry..."
poetry init --name "$PROJECT_NAME" --no-interaction

# Set Up direnv and Environment Files
echo "Setting up direnv and environment files..."
touch .env .envrc
echo "export $(cat .env | xargs)" > .envrc

# Create Makefile
echo "Creating Makefile..."

cat <<EOL > Makefile
# Makefile

.PHONY: setup lint test

setup:
\tpoetry install
\tdirenv allow

lint:
\tpoetry run flake8 customer_behaviour tests

test:
\tpoetry run pytest tests
EOL

# Dockerfile and Docker Compose
echo "Creating Docker and Docker Compose files..."

cat <<EOL > Dockerfile
# Dockerfile for ${PROJECT_NAME}

FROM python:3.10-slim

# Set up working directory
WORKDIR /app

# Install dependencies
COPY pyproject.toml poetry.lock /app/
RUN pip install poetry && poetry install --no-root

# Copy project files
COPY . /app

# Set entry point
CMD ["poetry", "run", "python"]
EOL

cat <<EOL > docker-compose.yml
# Docker Compose for ${PROJECT_NAME}

version: '3.8'

services:
  app:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - .:/app
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/keyfile.json
EOL

# Create README
echo "Creating README.md..."

cat <<EOL > README.md
# Customer Behaviour Data Pipeline

This project processes eCommerce behavior data for customer insights, following the Medallion Architecture (Bronze, Silver, Gold).

## Project Structure

### Directories
- **dags/**: Contains Airflow DAGs for orchestrating each pipeline stage.
- **data/**: Holds data in Bronze, Silver, and Gold layers for raw, processed, and aggregated data.
- **customer_behaviour/functions/**: Core modules for each ETL stage:
  - **bronze_ingestion.py**: Ingestion functions for Bronze layer.
  - **silver_transformation.py**: Transformation functions for Silver layer.
  - **gold_aggregations.py**: Aggregations for analytics-ready data at Gold layer.
  - **bigquery_loading.py**: Loading functions to push data to BigQuery.
  - **schema.py**: Schema validation functions.
  - **session.py**: Session management functions (e.g., Spark session setup).
- **customer_behaviour/jobs/**: Contains jobs for each ETL stage (ingest, transform, aggregate).
- **tests/**: Unit and integration tests for pipeline components.

### Getting Started

1. **Set Up Environment**:
   \`\`\`
   make setup
   \`\`\`
   This installs dependencies using Poetry and sets up \`direnv\`.

2. **Run Tests**:
   \`\`\`
   make test
   \`\`\`
   This runs unit and integration tests to ensure functionality.

3. **Lint Code**:
   \`\`\`
   make lint
   \`\`\`
   Lints the code to check for syntax and style issues.

### Workflow

The pipeline follows the Medallion Architecture:
1. **Bronze Layer**: Raw data ingestion via \`bronze_ingestion.py\`.
2. **Silver Layer**: Data cleaning and transformation via \`silver_transformation.py\`.
3. **Gold Layer**: Data aggregation for analytics via \`gold_aggregations.py\`.
4. **BigQuery Loading**: Final loading into BigQuery for BI tools.

## Docker Usage

Build and run the container with Docker Compose:
\`\`\`
docker-compose up --build
\`\`\`

EOL

# Create .gitignore
echo "Creating .gitignore..."

cat <<EOL > .gitignore
# Python
__pycache__/
*.py[cod]
.venv/
.env
.envrc

# Poetry
poetry.lock

# Docker
*.log
EOL

echo "Project structure created successfully!"
