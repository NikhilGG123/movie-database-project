"""
Quick test script to verify PostgreSQL connection and basic functionality
"""

import psycopg2
from psycopg2 import OperationalError


def test_connection(db_config):
    """Test database connection"""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        print("✓ Database connection successful!")
        
        # Test query
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"✓ PostgreSQL version: {version[0]}")
        
        # Check if tables exist
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cur.fetchall()
        if tables:
            print(f"\n✓ Found {len(tables)} tables:")
            for table in tables:
                print(f"  - {table[0]}")
        else:
            print("\n⚠ No tables found. Run setup_database.sql first.")
        
        cur.close()
        conn.close()
        
        return True
        
    except OperationalError as e:
        print(f"✗ Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check PostgreSQL is running")
        print("2. Verify database 'movie_db' exists")
        print("3. Verify user credentials are correct")
        print("4. Check PostgreSQL is listening on port 5432")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("PostgreSQL Connection Test")
    print("=" * 60 + "\n")
    
    # Update these with your actual credentials
    db_config = {
        'host': 'localhost',
        'database': 'movie_db',
        'user': 'movie_user',
        'password': 'movie_pass_123',  # UPDATE THIS
        'port': 5433  # Updated to 5433
    }
    
    print("Testing connection with:")
    print(f"  Host: {db_config['host']}")
    print(f"  Database: {db_config['database']}")
    print(f"  User: {db_config['user']}")
    print(f"  Port: {db_config['port']}\n")
    
    test_connection(db_config)
