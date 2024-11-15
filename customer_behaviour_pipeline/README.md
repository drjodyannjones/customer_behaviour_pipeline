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
   ```
   make setup
   ```
   This installs dependencies using Poetry and sets up `direnv`.

2. **Run Tests**:
   ```
   make test
   ```
   This runs unit and integration tests to ensure functionality.

3. **Lint Code**:
   ```
   make lint
   ```
   Lints the code to check for syntax and style issues.

### Workflow

The pipeline follows the Medallion Architecture:
1. **Bronze Layer**: Raw data ingestion via `bronze_ingestion.py`.
2. **Silver Layer**: Data cleaning and transformation via `silver_transformation.py`.
3. **Gold Layer**: Data aggregation for analytics via `gold_aggregations.py`.
4. **BigQuery Loading**: Final loading into BigQuery for BI tools.

## Docker Usage

Build and run the container with Docker Compose:
```
docker-compose up --build
```

