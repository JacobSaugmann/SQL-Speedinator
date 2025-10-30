"""
Simple unit tests for ServerDatabaseAnalyzer class
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import logging

from src.analyzers.server_database_analyzer import ServerDatabaseAnalyzer


class TestServerDatabaseAnalyzer:
    """Test class for ServerDatabaseAnalyzer functionality"""
    
    @pytest.fixture
    def mock_connection(self):
        """Mock SQL connection"""
        connection = Mock()
        connection.execute_query.return_value = [
            {
                'server_name': 'TestServer',
                'instance_name': 'MSSQLSERVER',
                'version': '15.0.2000.5',
                'edition': 'Developer Edition',
                'product_level': 'RTM'
            }
        ]
        return connection
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration manager"""
        config = Mock()
        config.timeout = 30
        return config
    
    def test_init_creates_instance_with_proper_attributes(self, mock_connection, mock_config):
        """Test that ServerDatabaseAnalyzer initializes correctly"""
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert isinstance(analyzer.logger, logging.Logger)
    
    def test_analyze_returns_structure_on_success(self, mock_connection, mock_config):
        """Test that analyze method returns expected structure"""
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        
        # Mock all the internal methods to return simple data
        analyzer._get_server_instance_info = Mock(return_value={'server_name': 'TestServer'})
        analyzer._get_server_configuration = Mock(return_value=[{'name': 'max_memory', 'value': 2048}])
        analyzer._get_memory_info = Mock(return_value={'total_memory': 8192})
        analyzer._get_cpu_info = Mock(return_value={'cpu_count': 4})
        analyzer._get_database_overview = Mock(return_value=[{'name': 'TestDB'}])
        analyzer._get_database_files_info = Mock(return_value=[{'file_name': 'test.mdf'}])
        analyzer._get_security_info = Mock(return_value={'authentication': 'Windows'})
        analyzer._get_backup_info = Mock(return_value=[{'database': 'TestDB', 'last_backup': '2024-01-01'}])
        
        result = analyzer.analyze()
        
        # Verify structure
        expected_keys = [
            'server_instance_info', 'server_configuration', 'memory_info', 'cpu_info',
            'database_overview', 'database_files', 'security_info', 'backup_info'
        ]
        
        for key in expected_keys:
            assert key in result
    
    def test_analyze_handles_exception(self, mock_connection, mock_config):
        """Test that analyze method handles exceptions gracefully"""
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        
        # Force an exception in one of the methods
        analyzer._get_server_instance_info = Mock(side_effect=Exception("Database error"))
        
        result = analyzer.analyze()
        
        assert 'error' in result
        assert 'Database error' in result['error']
    
    def test_get_server_instance_info_success(self, mock_connection, mock_config):
        """Test successful server instance info retrieval"""
        expected_data = [
            {
                'server_name': 'TestServer',
                'instance_name': 'MSSQLSERVER',
                'version': '15.0.2000.5',
                'edition': 'Developer Edition'
            }
        ]
        mock_connection.execute_query.return_value = expected_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_instance_info()
        
        assert result == expected_data[0]  # Should return first item
        mock_connection.execute_query.assert_called_once()
    
    def test_get_server_instance_info_empty_result(self, mock_connection, mock_config):
        """Test server instance info with empty result"""
        mock_connection.execute_query.return_value = []
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_instance_info()
        
        assert result == {}
    
    def test_get_server_instance_info_exception(self, mock_connection, mock_config):
        """Test server instance info with exception"""
        mock_connection.execute_query.side_effect = Exception("Query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_instance_info()
        
        assert result == {}
    
    def test_get_server_configuration_success(self, mock_connection, mock_config):
        """Test successful server configuration retrieval"""
        config_data = [
            {
                'name': 'max server memory (MB)',
                'value': 2048,
                'minimum': 0,
                'maximum': 2147483647,
                'is_dynamic': 1
            },
            {
                'name': 'max degree of parallelism',
                'value': 4,
                'minimum': 0,
                'maximum': 32767,
                'is_dynamic': 1
            }
        ]
        mock_connection.execute_query.return_value = config_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_configuration()
        
        assert result == config_data
        mock_connection.execute_query.assert_called_once()
    
    def test_get_server_configuration_exception(self, mock_connection, mock_config):
        """Test server configuration with exception"""
        mock_connection.execute_query.side_effect = Exception("Configuration error")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_configuration()
        
        assert result == []
    
    def test_get_memory_info_success(self, mock_connection, mock_config):
        """Test successful memory info retrieval"""
        memory_data = [
            {
                'counter_name': 'Total Server Memory (KB)',
                'cntr_value': 2097152,  # 2GB in KB
                'counter_type': 'Memory Usage'
            },
            {
                'counter_name': 'Target Server Memory (KB)',
                'cntr_value': 8388608,  # 8GB in KB
                'counter_type': 'Memory Usage'
            }
        ]
        mock_connection.execute_query.return_value = memory_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_memory_info()
        
        assert result == memory_data
    
    def test_get_memory_info_exception(self, mock_connection, mock_config):
        """Test memory info with exception"""
        mock_connection.execute_query.side_effect = Exception("Memory query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_memory_info()
        
        assert result == []
    
    def test_get_cpu_info_success(self, mock_connection, mock_config):
        """Test successful CPU info retrieval"""
        cpu_data = [
            {
                'cpu_count': 8,
                'hyperthread_ratio': 2,
                'physical_cpu_count': 4,
                'cpu_usage_percent': 25.5
            }
        ]
        mock_connection.execute_query.return_value = cpu_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_cpu_info()
        
        assert result == cpu_data[0]  # Should return first item
    
    def test_get_cpu_info_empty_result(self, mock_connection, mock_config):
        """Test CPU info with empty result"""
        mock_connection.execute_query.return_value = []
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_cpu_info()
        
        assert result == {}
    
    def test_get_cpu_info_exception(self, mock_connection, mock_config):
        """Test CPU info with exception"""
        mock_connection.execute_query.side_effect = Exception("CPU query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_cpu_info()
        
        assert result == {}
    
    def test_get_database_overview_success(self, mock_connection, mock_config):
        """Test successful database overview retrieval"""
        db_data = [
            {
                'name': 'TestDB1',
                'database_id': 5,
                'state_desc': 'ONLINE',
                'compatibility_level': 150,
                'collation_name': 'SQL_Latin1_General_CP1_CI_AS'
            },
            {
                'name': 'TestDB2',
                'database_id': 6,
                'state_desc': 'ONLINE',
                'compatibility_level': 150,
                'collation_name': 'SQL_Latin1_General_CP1_CI_AS'
            }
        ]
        mock_connection.execute_query.return_value = db_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_database_overview()
        
        assert result == db_data
        assert len(result) == 2
    
    def test_get_database_overview_exception(self, mock_connection, mock_config):
        """Test database overview with exception"""
        mock_connection.execute_query.side_effect = Exception("Database query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_database_overview()
        
        assert result == []
    
    def test_get_database_files_info_success(self, mock_connection, mock_config):
        """Test successful database files info retrieval"""
        files_data = [
            {
                'database_name': 'TestDB',
                'file_name': 'TestDB_Data',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'type_desc': 'ROWS',
                'size_mb': 1024,
                'growth': '10%'
            }
        ]
        mock_connection.execute_query.return_value = files_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_database_files_info()
        
        assert result == files_data
    
    def test_get_database_files_info_exception(self, mock_connection, mock_config):
        """Test database files info with exception"""
        mock_connection.execute_query.side_effect = Exception("Files query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_database_files_info()
        
        assert result == []
    
    def test_get_security_info_success(self, mock_connection, mock_config):
        """Test successful security info retrieval"""
        security_data = [
            {
                'authentication_mode': 'Windows Authentication',
                'is_sysadmin': 1,
                'login_time': '2024-01-01 10:00:00',
                'server_principal_id': 1
            }
        ]
        mock_connection.execute_query.return_value = security_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_security_info()
        
        assert result == security_data[0]  # Should return first item
    
    def test_get_security_info_empty_result(self, mock_connection, mock_config):
        """Test security info with empty result"""
        mock_connection.execute_query.return_value = []
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_security_info()
        
        assert result == {}
    
    def test_get_security_info_exception(self, mock_connection, mock_config):
        """Test security info with exception"""
        mock_connection.execute_query.side_effect = Exception("Security query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_security_info()
        
        assert result == {}
    
    def test_get_backup_info_success(self, mock_connection, mock_config):
        """Test successful backup info retrieval"""
        backup_data = [
            {
                'database_name': 'TestDB',
                'backup_type': 'FULL',
                'backup_start_date': '2024-01-01 02:00:00',
                'backup_finish_date': '2024-01-01 02:30:00',
                'backup_size_mb': 1024
            }
        ]
        mock_connection.execute_query.return_value = backup_data
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_backup_info()
        
        assert result == backup_data
    
    def test_get_backup_info_exception(self, mock_connection, mock_config):
        """Test backup info with exception"""
        mock_connection.execute_query.side_effect = Exception("Backup query failed")
        
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        result = analyzer._get_backup_info()
        
        assert result == []
    
    def test_multiple_method_calls_independence(self, mock_connection, mock_config):
        """Test that multiple method calls work independently"""
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        
        # Set up different return values for different calls
        mock_connection.execute_query.side_effect = [
            [{'server_name': 'Test'}],  # server info
            [{'name': 'config1'}],      # server config  
            [{'memory': 8192}],         # memory info
            [{'cpu_count': 4}]          # cpu info
        ]
        
        # Call methods in sequence
        server_info = analyzer._get_server_instance_info()
        config_info = analyzer._get_server_configuration()
        memory_info = analyzer._get_memory_info()
        cpu_info = analyzer._get_cpu_info()
        
        # Verify each method got correct data
        assert server_info == {'server_name': 'Test'}
        assert config_info == [{'name': 'config1'}]
        assert memory_info == [{'memory': 8192}]
        assert cpu_info == {'cpu_count': 4}
        
        # Verify execute_query was called 4 times
        assert mock_connection.execute_query.call_count == 4