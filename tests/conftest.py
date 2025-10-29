"""
Pytest configuration and shared fixtures for SQL Speedinator tests
"""

import os
import sys
import tempfile
import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path

# Add src to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Import modules for testing
try:
    from src.core.config_manager import ConfigManager
    from src.core.sql_connection import SQLServerConnection
except ImportError:
    # Fallback for development
    ConfigManager = None
    SQLServerConnection = None


@pytest.fixture(scope="session")
def project_root_path():
    """Provide the project root path"""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def temp_directory():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_config():
    """Mock configuration object with default test values"""
    if ConfigManager:
        config = Mock(spec=ConfigManager)
    else:
        config = Mock()
    config.server_name = "localhost"
    config.database_name = "master"
    config.use_windows_auth = True
    config.username = None
    config.password = None
    config.connection_timeout = 30
    config.query_timeout = 30
    config.max_retries = 3
    config.retry_delay = 1
    
    # Analysis config
    config.analysis = Mock()
    config.analysis.min_index_size_mb = 1
    config.analysis.max_fragmentation_threshold = 30
    config.analysis.enable_ai_analysis = False
    config.analysis.ai_model = "gpt-4"
    config.analysis.ai_temperature = 0.3
    
    # Perfmon config
    config.perfmon = Mock()
    config.perfmon.duration_minutes = 1
    config.perfmon.sample_interval = 1
    config.perfmon.counters = [
        "\\Processor(_Total)\\% Processor Time",
        "\\Memory\\Available MBytes"
    ]
    
    return config


@pytest.fixture
def mock_sql_connection():
    """Mock SQL Server connection"""
    if SQLServerConnection:
        connection = Mock(spec=SQLServerConnection)
    else:
        connection = Mock()
    
    connection.is_connected = True
    connection.server_name = "localhost"
    connection.database_name = "master"
    
    # Mock methods
    connection.connect.return_value = True
    connection.disconnect.return_value = None
    connection.execute_query.return_value = []
    connection.change_database.return_value = True
    connection.get_server_info.return_value = {
        'server_name': 'localhost',
        'version': '15.0.2000.5',
        'edition': 'Developer Edition',
        'product_level': 'RTM'
    }
    
    return connection


@pytest.fixture
def sample_wait_stats():
    """Sample wait statistics data for testing"""
    return [
        {
            'wait_type': 'CXPACKET',
            'wait_time_ms': 1500000,
            'wait_percentage': 45.2,
            'waiting_tasks_count': 1250,
            'signal_wait_time_ms': 150000
        },
        {
            'wait_type': 'PAGEIOLATCH_SH',
            'wait_time_ms': 800000,
            'wait_percentage': 24.1,
            'waiting_tasks_count': 2100,
            'signal_wait_time_ms': 80000
        },
        {
            'wait_type': 'LCK_M_S',
            'wait_time_ms': 450000,
            'wait_percentage': 13.6,
            'waiting_tasks_count': 890,
            'signal_wait_time_ms': 45000
        }
    ]


@pytest.fixture
def sample_index_data():
    """Sample index analysis data for testing"""
    return {
        'fragmented_indexes': [
            {
                'database_name': 'AdventureWorks2022',
                'schema_name': 'Production',
                'table_name': 'Product',
                'index_name': 'IX_Product_Name',
                'avg_fragmentation_in_percent': 85.2,
                'page_count': 1250,
                'size_mb': 9.76,
                'recommended_action': 'REBUILD'
            }
        ],
        'unused_indexes': [
            {
                'database_name': 'AdventureWorks2022',
                'schema_name': 'Sales',
                'table_name': 'SalesOrderDetail',
                'index_name': 'IX_Unused_Index',
                'user_seeks': 0,
                'user_scans': 0,
                'user_lookups': 0,
                'user_updates': 1250,
                'size_mb': 15.5
            }
        ],
        'duplicate_indexes': [
            {
                'database_name': 'AdventureWorks2022',
                'schema_name': 'Person',
                'table_name': 'Person',
                'index1_name': 'IX_Person_FirstName',
                'index1_key_columns': 'FirstName ASC',
                'index2_name': 'IX_Person_FirstName_LastName',
                'index2_key_columns': 'FirstName ASC, LastName ASC',
                'duplicate_type': 'OVERLAPPING'
            }
        ]
    }


@pytest.fixture
def sample_performance_data():
    """Sample performance counter data for testing"""
    return {
        'cpu_usage': [
            {'timestamp': '2025-10-29 10:00:00', 'value': 25.5},
            {'timestamp': '2025-10-29 10:01:00', 'value': 30.2},
            {'timestamp': '2025-10-29 10:02:00', 'value': 28.7}
        ],
        'memory_usage': [
            {'timestamp': '2025-10-29 10:00:00', 'value': 8192},
            {'timestamp': '2025-10-29 10:01:00', 'value': 7890},
            {'timestamp': '2025-10-29 10:02:00', 'value': 8050}
        ]
    }


@pytest.fixture
def sample_query_stats():
    """Sample query statistics for testing"""
    return [
        {
            'sql_handle': '0x0100123456789ABC',
            'execution_count': 1250,
            'total_worker_time': 15000000,
            'total_elapsed_time': 18000000,
            'total_logical_reads': 450000,
            'avg_cpu_time': 12000,
            'avg_elapsed_time': 14400,
            'query_text_sample': 'SELECT * FROM Production.Product WHERE Name LIKE'
        }
    ]


@pytest.fixture
def mock_ai_service():
    """Mock AI service for testing"""
    ai_service = Mock()
    ai_service.analyze_performance_data.return_value = {
        'bottlenecks': [
            {
                'type': 'CPU',
                'severity': 'HIGH',
                'description': 'CPU usage consistently above 80%',
                'recommendations': ['Add more CPU cores', 'Optimize queries']
            }
        ],
        'recommendations': [
            'Consider adding more memory',
            'Review index strategy',
            'Monitor wait statistics'
        ]
    }
    return ai_service


@pytest.fixture
def mock_pdf_generator():
    """Mock PDF generator for testing"""
    pdf_gen = Mock()
    pdf_gen.generate_report.return_value = "reports/test_report.pdf"
    pdf_gen.add_section.return_value = None
    pdf_gen.add_table.return_value = None
    return pdf_gen


@pytest.fixture
def sample_log_entries():
    """Sample log entries for testing"""
    return [
        {
            'log_date': '2025-10-29 10:00:00',
            'process_info': 'spid52',
            'text': 'Login succeeded for user [DOMAIN\\user]'
        },
        {
            'log_date': '2025-10-29 10:05:00',
            'process_info': 'spid15s',
            'text': 'Error: 18456, Severity: 14, State: 1. Login failed for user'
        },
        {
            'log_date': '2025-10-29 10:10:00',
            'process_info': 'Backup',
            'text': 'Database backup completed successfully'
        }
    ]


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, temp_directory):
    """Setup test environment variables and paths"""
    # Set test environment variables
    monkeypatch.setenv("SQL_SPEEDINATOR_TEST", "1")
    monkeypatch.setenv("SQL_SPEEDINATOR_CONFIG_PATH", str(temp_directory))
    
    # Create test directories
    (temp_directory / "reports").mkdir(exist_ok=True)
    (temp_directory / "logs").mkdir(exist_ok=True)
    (temp_directory / "temp").mkdir(exist_ok=True)


@pytest.fixture
def disable_real_connections(monkeypatch):
    """Disable real database connections in tests"""
    def mock_connect(*args, **kwargs):
        return False
    
    monkeypatch.setattr("pyodbc.connect", mock_connect)


# Performance test fixtures
@pytest.fixture
def benchmark_data():
    """Standard benchmark data for performance tests"""
    return {
        'small_dataset': list(range(100)),
        'medium_dataset': list(range(1000)),
        'large_dataset': list(range(10000))
    }


# Parametrized fixtures for different SQL Server versions
@pytest.fixture(params=[
    "Microsoft SQL Server 2019",
    "Microsoft SQL Server 2022", 
    "Microsoft SQL Server 2017"
])
def sql_server_version(request):
    """Parametrized SQL Server versions for compatibility testing"""
    return request.param


# Error simulation fixtures
@pytest.fixture
def connection_error():
    """Simulate connection errors"""
    from unittest.mock import Mock
    error = Mock()
    error.side_effect = Exception("Connection failed")
    return error


@pytest.fixture
def timeout_error():
    """Simulate timeout errors"""
    from unittest.mock import Mock
    error = Mock()
    error.side_effect = TimeoutError("Query timeout")
    return error