# Air Quality Data Pipeline for Sofia, Bulgaria

Automated pipeline for downloading and loading PM2.5 and PM10 air quality measurements from the European Air Quality Portal into PostgreSQL.

## Architecture

- **Apache Airflow 3.1.5** - Workflow orchestration
- **PostgreSQL 16** - Data warehouse (external container)
- **Docker Compose** - Container management
- **Data Source** - European Environment Agency (EEA) via airbase library

## Pipeline Flow

```
1. Download Data (daily) → 2. Load to PostgreSQL
```

1. **Download Task**: Fetches verified PM2.5 and PM10 data for Bulgaria as parquet files
2. **Load Task**: Reads parquet files and loads them into PostgreSQL

## Prerequisites

- Docker and Docker Compose
- PostgreSQL container running (e.g., `postgres-sql-workshop`)

## Setup

### 1. Configure PostgreSQL Connection

Edit `.env` file with your PostgreSQL credentials:

```bash
POSTGRES_HOST=host.docker.internal  # Use this for external containers on the same host
POSTGRES_PORT=5432
POSTGRES_DB=airquality              # Database name (will be created if doesn't exist)
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

**Note:** `host.docker.internal` allows Airflow containers to connect to containers outside the docker-compose network.

### 2. Start Airflow

```bash
# First time setup
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f
```

### 3. Access Airflow UI

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 4. Enable the DAG

1. In the Airflow UI, find `bulgaria_air_quality_pipeline`
2. Toggle it ON
3. Manually trigger a run or wait for the scheduled time (daily at midnight)

## Project Structure

```
.
├── dags/
│   ├── test_dag.py              # Main DAG definition
│   └── tasks/
│       ├── __init__.py
│       └── load_to_postgres.py  # PostgreSQL loading logic
├── data/                        # Downloaded parquet files (not in git)
├── logs/                        # Airflow logs (not in git)
├── config/                      # Airflow configuration
├── docker-compose.yaml          # Docker services definition
├── .env                         # Environment variables
└── script.py                    # Standalone download script

```

## Database Schema

The pipeline creates this table in PostgreSQL:

```sql
CREATE TABLE air_quality_measurements (
    station_code VARCHAR(50),
    pollutant VARCHAR(20),
    datetime TIMESTAMP,
    value FLOAT,
    unit VARCHAR(20),
    averaging_time VARCHAR(50),
    validity INTEGER,
    verification INTEGER,
    data_capture FLOAT,
    country_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (station_code, pollutant, datetime)
);
```

## Connecting to External PostgreSQL

Your Airflow containers can connect to the external `postgres-sql-workshop` container using:

**Option 1: host.docker.internal (Recommended)**
```bash
POSTGRES_HOST=host.docker.internal
```

**Option 2: Host network (if option 1 doesn't work)**
Add to docker-compose.yaml:
```yaml
network_mode: host
```

**Option 3: Shared Docker network**
Add both containers to the same network.

## Monitoring

- **Airflow UI**: Task status, logs, execution history
- **PostgreSQL**: Query the `air_quality_measurements` table

```sql
-- Check loaded data
SELECT
    COUNT(*) as total_records,
    MIN(datetime) as earliest_date,
    MAX(datetime) as latest_date,
    COUNT(DISTINCT station_code) as station_count
FROM air_quality_measurements;
```

## Troubleshooting

### Connection issues to PostgreSQL

1. Check if postgres-sql-workshop is running:
   ```bash
   docker ps | grep postgres
   ```

2. Test connection from Airflow container:
   ```bash
   docker exec -it air-sofia-airflow-airflow-worker-1 bash
   psql -h host.docker.internal -p 5432 -U postgres -d airquality
   ```

3. Check logs:
   ```bash
   docker-compose logs airflow-worker
   ```

### DAG not showing up

1. Check for import errors:
   ```bash
   docker-compose logs airflow-dag-processor
   ```

2. Restart containers:
   ```bash
   docker-compose restart
   ```

## Development

To modify the data loading logic, edit:
- `dags/tasks/load_to_postgres.py`

Changes will be automatically picked up when Airflow refreshes the DAG.

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v
```
