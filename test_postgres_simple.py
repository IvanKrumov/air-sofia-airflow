#!/usr/bin/env python3
"""
Simple PostgreSQL connection test script.
This version doesn't require the tasks module and can run locally.

Install dependencies first:
    pip install psycopg2-binary sqlalchemy
"""

import os
import sys

def test_connection():
    """Test PostgreSQL connection."""

    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("ERROR: SQLAlchemy not installed")
        print("Install with: pip install sqlalchemy psycopg2-binary")
        return False

    print("=" * 60)
    print("PostgreSQL Connection Test")
    print("=" * 60)

    # Get connection parameters from .env or use defaults
    # Note: Use localhost for local testing, even if .env has host.docker.internal
    host = os.environ.get('POSTGRES_HOST', 'localhost')
    if host == 'host.docker.internal':
        host = 'localhost'  # Override for local testing
    port = os.environ.get('POSTGRES_PORT', '5432')
    database = os.environ.get('POSTGRES_DB', 'airquality')
    user = os.environ.get('POSTGRES_USER', 'postgres')
    password = os.environ.get('POSTGRES_PASSWORD', 'postgres')

    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    print(f"\nConnection Details:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    print(f"  Password: {'*' * len(password)}")

    try:
        print("\n1. Creating connection...")
        engine = create_engine(conn_string)

        print("2. Testing connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"   ✓ Connected to PostgreSQL!")
            print(f"   Version: {version[:80]}...")

        print("\n3. Checking if database exists...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database()"))
            db_name = result.fetchone()[0]
            print(f"   ✓ Using database: {db_name}")

        print("\n4. Checking for air_quality_measurements table...")
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = 'air_quality_measurements'
            """))

            table = result.fetchone()
            if table:
                print(f"   ✓ Table 'air_quality_measurements' exists")

                # Check row count
                result = conn.execute(text("SELECT COUNT(*) FROM air_quality_measurements"))
                count = result.fetchone()[0]
                print(f"   ✓ Row count: {count:,}")
            else:
                print(f"   ℹ Table 'air_quality_measurements' does not exist yet")
                print(f"     (It will be created when the Airflow DAG runs)")

        print("\n" + "=" * 60)
        print("SUCCESS! PostgreSQL connection is working correctly.")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        print("\nTroubleshooting:")
        print("\n1. Check if postgres container is running:")
        print("   docker ps | grep postgres")

        print("\n2. Create the database if it doesn't exist:")
        print(f"   docker exec -it postgres-sql-workshop psql -U {user} -c 'CREATE DATABASE {database};'")

        print("\n3. Test connection directly:")
        print(f"   docker exec -it postgres-sql-workshop psql -U {user} -d {database} -c '\\dt'")

        print("\n4. If host.docker.internal doesn't work, try:")
        print("   - localhost (if running script locally)")
        print("   - 172.17.0.1 (Docker bridge IP)")
        print("   - Get container IP: docker inspect postgres-sql-workshop | grep IPAddress")

        return False

if __name__ == "__main__":
    # Load .env file if it exists
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        print(f"Loading environment from: {env_file}\n")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

    success = test_connection()
    sys.exit(0 if success else 1)
