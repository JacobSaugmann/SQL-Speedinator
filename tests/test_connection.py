"""
Test script for SQL Speedinator
Tests connection and basic functionality
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.core.config_manager import ConfigManager
from src.core.sql_connection import SQLServerConnection

def test_configuration():
    """Test configuration loading"""
    print("Testing configuration loading...")
    try:
        config = ConfigManager(".env")
        print(f"✓ Configuration loaded successfully")
        print(f"  - SQL Driver: {config.sql_driver}")
        print(f"  - Windows Authentication: {config.use_windows_auth}")
        print(f"  - Trusted Connection: {config.sql_trusted_connection}")
        print(f"  - Night Mode Delay: {config.night_mode_delay}")
        
        if not config.use_windows_auth:
            print(f"  - SQL Username: {config.sql_username or '(not set)'}")
            if not config.sql_username or not config.sql_password:
                print("  ⚠️  Warning: SQL Authentication enabled but credentials missing")
        
        return True
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_sql_connection(server_name):
    """Test SQL Server connection"""
    print(f"\nTesting SQL Server connection to {server_name}...")
    try:
        config = ConfigManager(".env")
        with SQLServerConnection(server_name, config) as conn:
            if conn.test_connection():
                print("✓ SQL Server connection successful")
                
                # Test server info query
                server_info = conn.get_server_info()
                if server_info and len(server_info) > 0:
                    info = server_info[0]
                    print(f"  - Server: {info.get('server_name', 'Unknown')}")
                    print(f"  - Version: {info.get('product_version', 'Unknown')}")
                    print(f"  - Edition: {info.get('edition', 'Unknown')}")
                return True
            else:
                print("✗ SQL Server connection test failed")
                return False
    except Exception as e:
        print(f"✗ SQL Server connection failed: {e}")
        return False

def test_basic_queries(server_name):
    """Test basic analysis queries"""
    print(f"\nTesting basic analysis queries...")
    try:
        config = ConfigManager(".env")
        with SQLServerConnection(server_name, config) as conn:
            # Test wait stats query
            wait_query = "SELECT TOP 5 wait_type, wait_time_ms FROM sys.dm_os_wait_stats WHERE wait_time_ms > 0 ORDER BY wait_time_ms DESC"
            wait_results = conn.execute_query(wait_query)
            
            if wait_results:
                print(f"✓ Wait stats query returned {len(wait_results)} results")
            else:
                print("✗ Wait stats query failed")
                return False
            
            # Test disk I/O query
            io_query = "SELECT TOP 5 database_id, file_id, num_of_reads, num_of_writes FROM sys.dm_io_virtual_file_stats(NULL, NULL) WHERE num_of_reads > 0 ORDER BY num_of_reads DESC"
            io_results = conn.execute_query(io_query)
            
            if io_results:
                print(f"✓ Disk I/O query returned {len(io_results)} results")
            else:
                print("✗ Disk I/O query failed")
                return False
            
            return True
            
    except Exception as e:
        print(f"✗ Basic query test failed: {e}")
        return False

def main():
    """Main test function"""
    print("SQL Speedinator - Test Suite")
    print("=" * 60)
    
    # Test configuration
    if not test_configuration():
        return 1
    
    # Get server name from command line or prompt
    if len(sys.argv) > 1:
        server_name = sys.argv[1]
    else:
        server_name = input("\nEnter SQL Server name to test (or press Enter to skip connection tests): ").strip()
    
    if server_name:
        # Test SQL connection
        if not test_sql_connection(server_name):
            return 1
        
        # Test basic queries
        if not test_basic_queries(server_name):
            return 1
        
        print("\n✓ All tests passed! The tool should work correctly.")
    else:
        print("\n⚠ Skipped connection tests. Configuration test passed.")
    
    print("\nNext steps:")
    print("1. Update the .env file with your SQL Server connection details")
    print("2. Run: python main.py -s YOUR_SERVER_NAME")
    print("3. Check the reports directory for the generated PDF")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())