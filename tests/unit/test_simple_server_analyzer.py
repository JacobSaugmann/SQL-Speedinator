"""
Unit tests for SimpleServerAnalyzer
Testing basic SQL Server analysis functionality with broad compatibility
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.analyzers.simple_server_analyzer import SimpleServerAnalyzer


class TestSimpleServerAnalyzer:
    """Test SimpleServerAnalyzer functionality"""
    
    @pytest.fixture
    def mock_connection(self):
        """Mock SQL connection"""
        connection = Mock()
        return connection
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration manager"""
        config = Mock()
        return config
    
    @pytest.fixture
    def analyzer(self, mock_connection, mock_config):
        """Create SimpleServerAnalyzer instance"""
        return SimpleServerAnalyzer(mock_connection, mock_config)
    
    @pytest.fixture
    def sample_server_info(self):
        """Sample server info data"""
        return [{
            'server_name': 'TestServer',
            'version_full': 'Microsoft SQL Server 2019 (RTM) - 15.0.2000.5 (X64)',
            'language_setting': 'us_english',
            'lock_timeout': -1,
            'max_connections': 32767,
            'current_spid': 55,
            'analysis_time': datetime.now()
        }]
    
    @pytest.fixture
    def sample_database_info(self):
        """Sample database info data"""
        return [
            {
                'database_name': 'TestDB1',
                'database_id': 5,
                'create_date': datetime.now(),
                'state': 'ONLINE',
                'user_access': 'MULTI_USER'
            },
            {
                'database_name': 'TestDB2',
                'database_id': 6,
                'create_date': datetime.now(),
                'state': 'ONLINE',
                'user_access': 'MULTI_USER'
            }
        ]
    
    @pytest.fixture
    def sample_memory_info(self):
        """Sample memory info data"""
        return [{
            'total_physical_memory_gb': 16.0,
            'available_memory_gb': 8.0,
            'sql_memory_gb': 12.0,
            'memory_usage_percentage': 75.0
        }]
    
    @pytest.fixture
    def sample_file_info(self):
        """Sample file info data"""
        return [
            {
                'database_name': 'TestDB',
                'logical_name': 'TestDB_Data',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'type_desc': 'ROWS',
                'size_mb': 1024.0,
                'growth_mb': 64.0
            },
            {
                'database_name': 'TestDB',
                'logical_name': 'TestDB_Log',
                'physical_name': 'C:\\Data\\TestDB.ldf',
                'type_desc': 'LOG',
                'size_mb': 256.0,
                'growth_mb': 32.0
            }
        ]
    
    def test_init_creates_instance_with_proper_attributes(self, analyzer):
        """Test that initialization creates proper instance"""
        assert analyzer.connection is not None
        assert analyzer.config is not None
        assert analyzer.logger is not None
        assert analyzer.logger.name == 'src.analyzers.simple_server_analyzer'
    
    def test_analyze_returns_structure_on_success(self, analyzer, sample_server_info, sample_database_info, sample_memory_info, sample_file_info):
        """Test that analyze returns proper structure"""
        # Mock all method calls to return sample data
        analyzer.connection.execute_query.side_effect = [
            sample_server_info,     # _get_basic_server_info
            sample_database_info,   # _get_basic_database_info  
            sample_memory_info,     # _get_basic_memory_info
            sample_file_info        # _get_basic_file_info
        ]
        
        result = analyzer.analyze()
        
        assert isinstance(result, dict)
        assert 'server_instance_info' in result
        assert 'database_overview' in result
        assert 'memory_info' in result
        assert 'database_files' in result
        assert 'server_configuration' in result
        assert 'cpu_info' in result
        assert 'security_info' in result
        assert 'backup_info' in result
    
    def test_analyze_handles_exception(self, analyzer):
        """Test that analyze handles exceptions gracefully"""
        analyzer.connection.execute_query.side_effect = Exception("Database error")
        
        result = analyzer.analyze()
        
        assert result == {}
    
    def test_get_basic_server_info_success(self, analyzer, sample_server_info):
        """Test successful basic server info retrieval"""
        analyzer.connection.execute_query.return_value = sample_server_info
        
        result = analyzer._get_basic_server_info()
        
        assert isinstance(result, dict)
        assert result['server_name'] == 'TestServer'
        assert 'product_version' in result
        assert result['product_version'] == '2019'
        assert result['edition'] == 'Unknown'
        assert result['instance_name'] == 'DEFAULT'
    
    def test_get_basic_server_info_version_parsing(self, analyzer):
        """Test version parsing from full version string"""
        version_data = [{
            'server_name': 'TestServer',
            'version_full': 'Microsoft SQL Server 2017 (RTM-CU31) - 14.0.3456.2 (X64)',
            'language_setting': 'us_english',
            'lock_timeout': -1,
            'max_connections': 32767,
            'current_spid': 55,
            'analysis_time': datetime.now()
        }]
        
        analyzer.connection.execute_query.return_value = version_data
        
        result = analyzer._get_basic_server_info()
        
        assert result['product_version'] == '2017'
    
    def test_get_basic_server_info_no_version_in_string(self, analyzer):
        """Test server info when version string doesn't contain recognizable version"""
        version_data = [{
            'server_name': 'TestServer',
            'version_full': 'Some other database system',
            'language_setting': 'us_english',
            'lock_timeout': -1,
            'max_connections': 32767,
            'current_spid': 55,
            'analysis_time': datetime.now()
        }]
        
        analyzer.connection.execute_query.return_value = version_data
        
        result = analyzer._get_basic_server_info()
        
        assert 'product_version' not in result or result.get('product_version') is None
    
    def test_get_basic_server_info_empty_result(self, analyzer):
        """Test server info with empty result"""
        analyzer.connection.execute_query.return_value = []
        
        result = analyzer._get_basic_server_info()
        
        assert result == {}
    
    def test_get_basic_server_info_exception(self, analyzer):
        """Test server info with exception"""
        analyzer.connection.execute_query.side_effect = Exception("Query failed")
        
        result = analyzer._get_basic_server_info()
        
        assert result == {}
    
    def test_get_basic_database_info_success(self, analyzer, sample_database_info):
        """Test successful database info retrieval"""
        analyzer.connection.execute_query.return_value = sample_database_info
        
        result = analyzer._get_basic_database_info()
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['database_name'] == 'TestDB1'
        assert result[1]['database_name'] == 'TestDB2'
    
    def test_get_basic_database_info_empty_result(self, analyzer):
        """Test database info with empty result"""
        analyzer.connection.execute_query.return_value = []
        
        result = analyzer._get_basic_database_info()
        
        assert result == []
    
    def test_get_basic_database_info_exception(self, analyzer):
        """Test database info with exception"""
        analyzer.connection.execute_query.side_effect = Exception("Query failed")
        
        result = analyzer._get_basic_database_info()
        
        assert result == []
    
    def test_get_basic_memory_info_success(self, analyzer, sample_memory_info):
        """Test successful memory info retrieval"""
        analyzer.connection.execute_query.return_value = sample_memory_info
        
        result = analyzer._get_basic_memory_info()
        
        assert isinstance(result, dict)
        assert result['total_physical_memory_gb'] == 16.0
        assert result['memory_usage_percentage'] == 75.0
    
    def test_get_basic_memory_info_empty_result(self, analyzer):
        """Test memory info with empty result"""
        analyzer.connection.execute_query.return_value = []
        
        result = analyzer._get_basic_memory_info()
        
        assert result == {}
    
    def test_get_basic_memory_info_exception(self, analyzer):
        """Test memory info with exception"""
        analyzer.connection.execute_query.side_effect = Exception("Query failed")
        
        result = analyzer._get_basic_memory_info()
        
        assert result == {}
    
    def test_get_basic_file_info_success(self, analyzer, sample_file_info):
        """Test successful file info retrieval"""
        analyzer.connection.execute_query.return_value = sample_file_info
        
        result = analyzer._get_basic_file_info()
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['type_desc'] == 'ROWS'
        assert result[1]['type_desc'] == 'LOG'
    
    def test_get_basic_file_info_empty_result(self, analyzer):
        """Test file info with empty result"""
        analyzer.connection.execute_query.return_value = []
        
        result = analyzer._get_basic_file_info()
        
        assert result == []
    
    def test_get_basic_file_info_exception(self, analyzer):
        """Test file info with exception"""
        analyzer.connection.execute_query.side_effect = Exception("Query failed")
        
        result = analyzer._get_basic_file_info()
        
        assert result == []
    
    def test_analyze_integration_with_partial_failures(self, analyzer, sample_server_info):
        """Test analyze method when some sub-methods fail"""
        # Mock some methods to succeed and others to fail
        analyzer.connection.execute_query.side_effect = [
            sample_server_info,                    # _get_basic_server_info - success
            Exception("Database query failed"),    # _get_basic_database_info - failure
            [],                                    # _get_basic_memory_info - empty
            Exception("File query failed")        # _get_basic_file_info - failure
        ]
        
        result = analyzer.analyze()
        
        # Should still return structure even with partial failures
        assert isinstance(result, dict)
        assert 'server_instance_info' in result
        assert result['server_instance_info']['server_name'] == 'TestServer'
        assert result['database_overview'] == []  # Failed, should return empty list
        assert result['memory_info'] == {}        # Empty result
        assert result['database_files'] == []     # Failed, should return empty list
    
    def test_multiple_sql_server_versions_parsing(self, analyzer):
        """Test parsing different SQL Server version strings"""
        test_cases = [
            ('Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (X64)', '2022'),
            ('Microsoft SQL Server 2019 (RTM-CU18) - 15.0.4261.1 (X64)', '2019'),
            ('Microsoft SQL Server 2017 (RTM-CU31) - 14.0.3456.2 (X64)', '2017'),
            ('Microsoft SQL Server 2016 (SP3) (KB5003279) - 13.0.6300.2 (X64)', '2016'),
            ('Microsoft SQL Server 2014 (SP3) (KB4022619) - 12.0.6108.1 (X64)', '2014'),
            ('No version info', None)
        ]
        
        for version_string, expected_version in test_cases:
            version_data = [{
                'server_name': 'TestServer',
                'version_full': version_string,
                'language_setting': 'us_english',
                'lock_timeout': -1,
                'max_connections': 32767,
                'current_spid': 55,
                'analysis_time': datetime.now()
            }]
            
            analyzer.connection.execute_query.return_value = version_data
            result = analyzer._get_basic_server_info()
            
            if expected_version:
                assert result.get('product_version') == expected_version
            else:
                assert 'product_version' not in result or result.get('product_version') is None
    
    def test_error_logging_on_exceptions(self, analyzer):
        """Test that errors are properly logged"""
        with patch.object(analyzer.logger, 'error') as mock_logger:
            analyzer.connection.execute_query.side_effect = Exception("Test error")
            
            # This should trigger error logging
            result = analyzer.analyze()
            
            # Verify error was logged
            mock_logger.assert_called()
            assert result == {}
    
    def test_success_logging_on_completion(self, analyzer, sample_server_info, sample_database_info, sample_memory_info, sample_file_info):
        """Test that success is properly logged"""
        with patch.object(analyzer.logger, 'info') as mock_logger:
            analyzer.connection.execute_query.side_effect = [
                sample_server_info,
                sample_database_info,
                sample_memory_info,
                sample_file_info
            ]
            
            result = analyzer.analyze()
            
            # Verify success was logged
            mock_logger.assert_called_with("Simple server analysis completed successfully")
            assert isinstance(result, dict)
    
    def test_all_result_keys_present_even_with_failures(self, analyzer):
        """Test that all expected keys are present in result even when methods fail"""
        # All methods fail
        analyzer.connection.execute_query.side_effect = Exception("All queries fail")
        
        result = analyzer.analyze()
        
        # Should still return empty dict due to exception in main analyze method
        assert result == {}
        
        # Test with partial success - mock the main analyze method to not fail completely
        with patch.object(analyzer, '_get_basic_server_info', return_value={}):
            with patch.object(analyzer, '_get_basic_database_info', return_value=[]):
                with patch.object(analyzer, '_get_basic_memory_info', return_value={}):
                    with patch.object(analyzer, '_get_basic_file_info', return_value=[]):
                        result = analyzer.analyze()
                        
                        expected_keys = [
                            'server_instance_info', 'database_overview', 'memory_info',
                            'database_files', 'server_configuration', 'cpu_info',
                            'security_info', 'backup_info'
                        ]
                        
                        for key in expected_keys:
                            assert key in result