#!/usr/bin/env python3
"""
Test script to validate Snowflake migration and connection.
This script tests the database connection and basic functionality.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from src.utils.database import DatabaseManager, test_database_connection


def test_snowflake_connection():
    """Test basic Snowflake connection"""
    print("Testing Snowflake connection...")
    
    # Get credentials from environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER') 
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE', 'AUSTRALIAN_COMPANIES')
    
    if not all([account, user, password, warehouse]):
        print("❌ Missing required environment variables:")
        print("   - SNOWFLAKE_ACCOUNT")
        print("   - SNOWFLAKE_USER") 
        print("   - SNOWFLAKE_PASSWORD")
        print("   - SNOWFLAKE_WAREHOUSE")
        return False
    
    success = test_database_connection(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database
    )
    
    if success:
        print("✅ Snowflake connection successful!")
        return True
    else:
        print("❌ Snowflake connection failed!")
        return False


def test_database_operations():
    """Test basic database operations"""
    print("\nTesting database operations...")
    
    try:
        db = DatabaseManager(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'AUSTRALIAN_COMPANIES')
        )
        
        # Test basic query
        result = db.fetch_one("SELECT CURRENT_VERSION() as version")
        print(f"✅ Snowflake version: {result['VERSION'] if result else 'Unknown'}")
        
        # Test schema existence (will fail if schemas don't exist yet)
        try:
            schemas = db.fetch_all("SHOW SCHEMAS")
            print(f"✅ Found {len(schemas)} schemas")
        except Exception as e:
            print(f"ℹ️  Schema query failed (expected if database not set up): {e}")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Database operations test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("=== Snowflake Migration Test ===\n")
    
    # Test connection
    connection_success = test_snowflake_connection()
    
    if connection_success:
        # Test operations
        operations_success = test_database_operations()
        
        print("\n=== Test Summary ===")
        if connection_success and operations_success:
            print("✅ All tests passed! Snowflake migration appears successful.")
            print("\nNext steps:")
            print("1. Run DDL scripts to create schemas and tables")
            print("2. Test dbt with: dbt debug && dbt run")
            print("3. Run the full pipeline")
            return 0
        else:
            print("⚠️  Some tests failed. Check configuration and credentials.")
            return 1
    else:
        print("\n❌ Connection test failed. Cannot proceed with further tests.")
        return 1


if __name__ == "__main__":
    sys.exit(main())