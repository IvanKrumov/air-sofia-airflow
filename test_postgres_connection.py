#!/usr/bin/env python3
"""
Test script to verify PostgreSQL connection and manually create the table.
Run this to test if the connection works before running the DAG.
"""

import os
import sys

# Add dags to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))

from tasks.load_to_postgres import get_postgres_connection_string, create_table_if_not_exists
from sqlalchemy import create_engine, text

def test_connection():
    """Test PostgreSQL connection and create table."""

    print("=" * 60)
    print("PostgreSQL Connection Test")
    print("=" * 60)

    # Get connection string
    conn_string = get_postgres_connection_string()
    print(f"\n1. Connection String: {conn_string.replace('postgres:postgres@', 'postgres:***@')}")

    try:
        # Create engine
        print("\n2. Creating SQLAlchemy engine...")
        engine = create_engine(conn_string)

        # Test connection
        print("\n3. Testing connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"   ✓ Connected to PostgreSQL!")
            print(f"   Version: {version}")

        # Create table
        print("\n4. Creating table...")
        create_table_if_not_exists(engine)
        print("   ✓ Table created/verified!")

        # Verify table exists
        print("\n5. Verifying table...")
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'air_quality_measurements'
                ORDER BY ordinal_position
            """))

            columns = result.fetchall()
            if columns:
                print("   ✓ Table 'air_quality_measurements' exists with columns:")
                for col_name, col_type in columns:
                    print(f"      - {col_name}: {col_type}")
            else:
                print("   ✗ Table not found!")
                return False

        print("\n" + "=" * 60)
        print("SUCCESS! PostgreSQL connection is working correctly.")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Check if postgres-sql-workshop container is running:")
        print("   docker ps | grep postgres")
        print("\n2. Verify .env settings:")
        print("   POSTGRES_HOST=host.docker.internal (or 172.17.0.1)")
        print("   POSTGRES_PORT=5432")
        print("   POSTGRES_DB=airquality")
        print("   POSTGRES_USER=postgres")
        print("   POSTGRES_PASSWORD=<your-password>")
        print("\n3. Create database if it doesn't exist:")
        print("   docker exec -it postgres-sql-workshop psql -U postgres -c 'CREATE DATABASE airquality;'")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
