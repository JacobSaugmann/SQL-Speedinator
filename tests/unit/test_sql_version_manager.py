"""
Unit tests for SQLVersionManager class.

This module tests SQL Server version detection, capabilities management,
and version-compatible query generation across different SQL Server versions.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.core.sql_version_manager import SQLVersionManager


class TestSQLVersionManager:
    """Test suite for SQLVersionManager class"""
    
    def test_init_creates_instance_with_proper_attributes(self, mock_sql_connection):
        """Test SQLVersionManager initialization"""
        manager = SQLVersionManager(mock_sql_connection)
        
        assert manager.connection == mock_sql_connection
        assert manager.logger is not None
        assert manager._version_info is None
        assert manager._capabilities is None
    
    def test_detect_version_returns_cached_info_when_available(self, mock_sql_connection):
        """Test detect_version returns cached version info when already detected"""
        manager = SQLVersionManager(mock_sql_connection)
        cached_info = {
            'version_string': 'Microsoft SQL Server 2019',
            'server_name': 'TestServer',
            'major_version': 15,
            'minor_version': 0,
            'build_number': 0,
            'engine_edition': 3
        }
        manager._version_info = cached_info
        
        result = manager.detect_version()
        
        assert result == cached_info
        # Verify connection was not called when using cache
        mock_sql_connection.execute_query.assert_not_called()
    
    def test_detect_version_successful_detection_sql_2019(self, mock_sql_connection):
        """Test successful version detection for SQL Server 2019"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock successful query result
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Microsoft SQL Server 2019 (RTM) - 15.0.2000.5',
            'server_name': 'TestServer2019',
            'major_version': 15,
            'minor_version': 0,
            'engine_edition': 3
        }]
        
        result = manager.detect_version()
        
        # Verify query was executed
        mock_sql_connection.execute_query.assert_called_once()
        query_arg = mock_sql_connection.execute_query.call_args[0][0]
        assert "@@VERSION" in query_arg
        assert "SERVERPROPERTY('ProductMajorVersion')" in query_arg
        
        # Verify result structure
        assert result['version_string'] == 'Microsoft SQL Server 2019 (RTM) - 15.0.2000.5'
        assert result['server_name'] == 'TestServer2019'
        assert result['major_version'] == 15
        assert result['minor_version'] == 0
        assert result['engine_edition'] == 3
        
        # Verify caching
        assert manager._version_info == result
        
        # Verify logging (just check that no exceptions occurred)
        # manager.logger.info was called but we can't easily assert on real logger
    
    def test_detect_version_successful_detection_sql_2016(self, mock_sql_connection):
        """Test successful version detection for SQL Server 2016"""
        manager = SQLVersionManager(mock_sql_connection)
        
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Microsoft SQL Server 2016 (SP2) - 13.0.5026.0',
            'server_name': 'TestServer2016',
            'major_version': 13,
            'minor_version': 0,
            'engine_edition': 1
        }]
        
        result = manager.detect_version()
        
        assert result['major_version'] == 13
        assert result['version_string'] == 'Microsoft SQL Server 2016 (SP2) - 13.0.5026.0'
        assert result['server_name'] == 'TestServer2016'
    
    def test_detect_version_fallback_string_parsing_sql_2012(self, mock_sql_connection):
        """Test version detection with string parsing fallback for SQL Server 2012"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock result without major_version (SERVERPROPERTY fails)
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Microsoft SQL Server 2012 (SP4) - 11.0.7001.0',
            'server_name': 'TestServer2012',
            'major_version': None,  # SERVERPROPERTY failed
            'minor_version': 0,
            'engine_edition': 1
        }]
        
        result = manager.detect_version()
        
        # Should detect version 11 from string parsing
        assert result['major_version'] == 11
        assert result['version_string'] == 'Microsoft SQL Server 2012 (SP4) - 11.0.7001.0'
    
    def test_detect_version_fallback_string_parsing_sql_2022(self, mock_sql_connection):
        """Test version detection with string parsing fallback for SQL Server 2022"""
        manager = SQLVersionManager(mock_sql_connection)
        
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Microsoft SQL Server 2022 (RTM) - 16.0.1000.6',
            'server_name': 'TestServer2022',
            'major_version': 0,  # SERVERPROPERTY returned 0
            'minor_version': 0,
            'engine_edition': 3
        }]
        
        result = manager.detect_version()
        
        # Should detect version 16 from string parsing
        assert result['major_version'] == 16
    
    def test_detect_version_fallback_string_parsing_version_number(self, mock_sql_connection):
        """Test version detection with version number fallback"""
        manager = SQLVersionManager(mock_sql_connection)
        
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Some version string with 14.0 number',
            'server_name': 'TestServer',
            'major_version': 0,
            'minor_version': 0,
            'engine_edition': 1
        }]
        
        result = manager.detect_version()
        
        # Should detect version 14 from '14.0' in string
        assert result['major_version'] == 14
    
    def test_detect_version_handles_none_values_gracefully(self, mock_sql_connection):
        """Test version detection handles None values gracefully"""
        manager = SQLVersionManager(mock_sql_connection)
        
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Test version',
            'server_name': 'TestServer',
            'major_version': None,
            'minor_version': None,
            'engine_edition': None
        }]
        
        result = manager.detect_version()
        
        # Should handle None values gracefully
        assert result['major_version'] == 0
        assert result['minor_version'] == 0
        assert result['engine_edition'] == 0
    
    def test_detect_version_empty_result_returns_none_bug(self, mock_sql_connection):
        """Test version detection returns None for empty results - This is a bug!
        
        This test documents current behavior where empty results return None
        instead of fallback default values. This should be fixed in the future.
        """
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock empty result - this exposes a bug in the implementation
        mock_sql_connection.execute_query.return_value = []
        
        # Current behavior: returns None (this is a bug!)
        result = manager.detect_version()
        assert result is None
        
        # TODO: Fix the implementation to handle empty results like exceptions
    
    def test_detect_version_exception_handling(self, mock_sql_connection):
        """Test version detection handles database exceptions"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock database exception
        mock_sql_connection.execute_query.side_effect = Exception("Database connection failed")
        
        result = manager.detect_version()
        
        # Should return default version info
        assert result['major_version'] == 11  # SQL Server 2012 default
        assert result['version_string'] == 'Unknown'
        assert result['server_name'] == 'Unknown'
        assert result['minor_version'] == 0
        assert result['build_number'] == 0
        assert result['engine_edition'] == 1
        
        # Verify warning was logged (just check no exceptions occurred)
        # We can't easily assert on real logger calls
    
    def test_get_capabilities_returns_cached_capabilities(self, mock_sql_connection):
        """Test get_capabilities returns cached capabilities when available"""
        manager = SQLVersionManager(mock_sql_connection)
        
        cached_capabilities = {
            'has_physical_cpu_count': True,
            'has_socket_count': True,
            'has_cores_per_socket': True
        }
        manager._capabilities = cached_capabilities
        
        result = manager.get_capabilities()
        
        assert result == cached_capabilities
        # Verify detect_version was not called when using cache
        mock_sql_connection.execute_query.assert_not_called()
    
    def test_get_capabilities_sql_2016_features(self, mock_sql_connection):
        """Test capabilities detection for SQL Server 2016"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock SQL Server 2016 version info
        manager._version_info = {
            'major_version': 13,
            'minor_version': 0
        }
        
        result = manager.get_capabilities()
        
        # SQL 2016+ features
        assert result['has_physical_cpu_count'] is True
        assert result['has_socket_count'] is True
        assert result['has_cores_per_socket'] is True
        assert result['has_advanced_analytics'] is True
        
        # SQL 2014+ features
        assert result['has_pages_in_use_kb'] is True
        
        # Pre-SQL 2025 features
        assert result['supports_nvarchar_cast'] is True
        assert result['has_performance_counter_name'] is True
        
        # SQL 2012+ features
        assert result['supports_query_plan_cross_apply'] is True
        assert result['supports_extended_events'] is True
        
        # Verify caching
        assert manager._capabilities == result
        
        # Verify logging (just check no exceptions occurred)
        # We can't easily assert on real logger calls
    
    def test_get_capabilities_sql_2012_features(self, mock_sql_connection):
        """Test capabilities detection for SQL Server 2012"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock SQL Server 2012 version info
        manager._version_info = {
            'major_version': 11,
            'minor_version': 0
        }
        
        result = manager.get_capabilities()
        
        # SQL 2016+ features (not available)
        assert result['has_physical_cpu_count'] is False
        assert result['has_socket_count'] is False
        assert result['has_cores_per_socket'] is False
        assert result['has_advanced_analytics'] is False
        
        # SQL 2014+ features (not available)
        assert result['has_pages_in_use_kb'] is False
        
        # Pre-SQL 2025 features (available)
        assert result['supports_nvarchar_cast'] is True
        assert result['has_performance_counter_name'] is True
        
        # SQL 2012+ features (available)
        assert result['supports_query_plan_cross_apply'] is True
        assert result['supports_extended_events'] is True
    
    def test_get_capabilities_sql_2025_features(self, mock_sql_connection):
        """Test capabilities detection for SQL Server 2025"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock SQL Server 2025 version info
        manager._version_info = {
            'major_version': 17,
            'minor_version': 0
        }
        
        result = manager.get_capabilities()
        
        # SQL 2016+ features (available)
        assert result['has_physical_cpu_count'] is True
        assert result['has_socket_count'] is True
        assert result['has_cores_per_socket'] is True
        assert result['has_advanced_analytics'] is True
        
        # SQL 2014+ features (available)
        assert result['has_pages_in_use_kb'] is True
        
        # Pre-SQL 2025 features (not available)
        assert result['supports_nvarchar_cast'] is False
        assert result['has_performance_counter_name'] is False
        
        # SQL 2012+ features (available)
        assert result['supports_query_plan_cross_apply'] is True
        assert result['supports_extended_events'] is True
    
    def test_get_capabilities_calls_detect_version_when_no_cached_info(self, mock_sql_connection):
        """Test get_capabilities calls detect_version when no cached version info"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock successful version detection
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Microsoft SQL Server 2019',
            'server_name': 'TestServer',
            'major_version': 15,
            'minor_version': 0,
            'engine_edition': 3
        }]
        
        result = manager.get_capabilities()
        
        # Verify detect_version was called
        mock_sql_connection.execute_query.assert_called_once()
        
        # Verify SQL 2019 capabilities
        assert result['has_physical_cpu_count'] is True
        assert result['has_advanced_analytics'] is True
    
    def test_get_compatible_server_info_query_with_nvarchar_cast_support(self, mock_sql_connection):
        """Test server info query generation with NVARCHAR CAST support"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities with NVARCHAR CAST support (pre-SQL 2025)
        manager._capabilities = {
            'supports_nvarchar_cast': True
        }
        
        result = manager.get_compatible_server_info_query()
        
        # Should use CAST syntax
        assert "@@SERVERNAME" in result
        assert "@@VERSION" in result
        assert "CAST(SERVERPROPERTY('ProductVersion')" in result
        assert "CAST(SERVERPROPERTY('Edition')" in result
        assert "CONVERT" not in result
    
    def test_get_compatible_server_info_query_without_nvarchar_cast_support(self, mock_sql_connection):
        """Test server info query generation without NVARCHAR CAST support"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities without NVARCHAR CAST support (SQL 2025+)
        manager._capabilities = {
            'supports_nvarchar_cast': False
        }
        
        result = manager.get_compatible_server_info_query()
        
        # Should use CONVERT syntax
        assert "@@SERVERNAME" in result
        assert "@@VERSION" in result
        assert "CONVERT(VARCHAR(50), SERVERPROPERTY('ProductVersion'))" in result
        assert "CONVERT(VARCHAR(200), SERVERPROPERTY('Edition'))" in result
        assert "CAST" not in result
    
    def test_get_compatible_cpu_query_with_advanced_cpu_features(self, mock_sql_connection):
        """Test CPU query generation with advanced CPU features (SQL 2016+)"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities with advanced CPU features
        manager._capabilities = {
            'has_physical_cpu_count': True,
            'has_socket_count': True,
            'has_cores_per_socket': True
        }
        
        result = manager.get_compatible_cpu_info_query()
        
        # Should include advanced CPU columns
        assert "sys.dm_os_sys_info" in result
        assert "socket_count" in result
        assert "cores_per_socket" in result
        assert "physical_cpu_count" in result
    
    def test_get_compatible_cpu_query_without_advanced_cpu_features(self, mock_sql_connection):
        """Test CPU query generation without advanced CPU features (pre-SQL 2016)"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities without advanced CPU features
        manager._capabilities = {
            'has_physical_cpu_count': False,
            'has_socket_count': False,
            'has_cores_per_socket': False
        }
        
        result = manager.get_compatible_cpu_info_query()
        
        # Should include basic CPU info
        assert "sys.dm_os_sys_info" in result
        assert "cpu_count" in result
        assert "hyperthread_ratio" in result
        
        # For older versions, socket_count and cores_per_socket are included as fallback values
        assert "1 as socket_count" in result
        assert "cpu_count as cores_per_socket" in result
        assert "actual_physical_cpu_count" in result
    
    def test_get_compatible_configuration_query_with_cast_support(self, mock_sql_connection):
        """Test configuration query generation with CAST support"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities with CAST support
        manager._capabilities = {
            'supports_nvarchar_cast': True
        }
        
        result = manager.get_compatible_configuration_query()
        
        # Should include configuration query elements
        assert "sys.configurations" in result
        assert "configuration_id" in result
        assert "name" in result
        assert "value" in result
        assert "is_dynamic" in result
        assert "is_advanced" in result
        assert "ORDER BY name" in result
    
    def test_get_compatible_configuration_query_without_cast_support(self, mock_sql_connection):
        """Test configuration query generation without CAST support"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities without CAST support
        manager._capabilities = {
            'supports_nvarchar_cast': False
        }
        
        result = manager.get_compatible_configuration_query()
        
        # Should still include configuration query elements
        assert "sys.configurations" in result
        assert "configuration_id" in result
        assert "CONVERT" in result
    
    def test_get_compatible_performance_counters_query_with_counter_name(self, mock_sql_connection):
        """Test performance counters query with counter name support"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities with performance counter name
        manager._capabilities = {
            'has_performance_counter_name': True
        }
        
        result = manager.get_compatible_performance_counters_query()
        
        # Should include performance counter elements
        assert "sys.dm_os_performance_counters" in result
        assert "instance_name as name" in result
        assert "counter_name" in result
        assert "cntr_value" in result
        assert "Plan Cache" in result
        assert "Cache Hit Ratio" in result
    
    def test_get_compatible_performance_counters_query_without_counter_name(self, mock_sql_connection):
        """Test performance counters query without counter name support"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock capabilities without performance counter name
        manager._capabilities = {
            'has_performance_counter_name': False
        }
        
        result = manager.get_compatible_performance_counters_query()
        
        # Should include performance counter elements but no alias
        assert "sys.dm_os_performance_counters" in result
        assert "instance_name," in result  # No alias
        assert "counter_name" in result
        assert "cntr_value" in result
        assert "Plan Cache" in result
    
    def test_get_compatible_query_stats_query(self, mock_sql_connection):
        """Test query stats query generation"""
        manager = SQLVersionManager(mock_sql_connection)
        
        result = manager.get_compatible_query_stats_query()
        
        # Should include query stats elements
        assert "sys.dm_exec_query_stats" in result
        assert "sys.dm_exec_sql_text" in result
        assert "execution_count" in result
        assert "total_worker_time" in result
        assert "total_elapsed_time" in result
        assert "total_logical_reads" in result
        assert "avg_cpu_time" in result
        assert "avg_elapsed_time" in result
        assert "query_text_sample" in result
        assert "CROSS APPLY" in result
        assert "ORDER BY" in result
        assert "TOP 20" in result
    
    def test_get_compatible_time_query(self, mock_sql_connection):
        """Test time query generation"""
        manager = SQLVersionManager(mock_sql_connection)
        
        result = manager.get_compatible_time_query()
        
        # Should be simple GETDATE query
        assert "GETDATE()" in result
        assert "query_datetime" in result
    
    def test_test_connection_compatibility_supported_version(self, mock_sql_connection):
        """Test connection compatibility for supported SQL Server version"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock SQL Server 2019 version info
        manager._version_info = {
            'version_string': 'Microsoft SQL Server 2019',
            'server_name': 'TestServer',
            'major_version': 15,
            'minor_version': 0,
            'engine_edition': 3
        }
        
        success, message = manager.test_connection_compatibility()
        
        assert success is True
        assert "SUPPORTED" in message
        assert "SQL Server version 15 is supported" in message
    
    def test_test_connection_compatibility_limited_version(self, mock_sql_connection):
        """Test connection compatibility for limited support SQL Server version"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock SQL Server 2008 version info (unsupported)
        manager._version_info = {
            'version_string': 'Microsoft SQL Server 2008',
            'server_name': 'TestServer',
            'major_version': 10,
            'minor_version': 0,
            'engine_edition': 1
        }
        
        success, message = manager.test_connection_compatibility()
        
        assert success is True
        assert "LIMITED" in message
        assert "SQL Server version 10 has limited support" in message
    
    def test_test_connection_compatibility_calls_detect_version(self, mock_sql_connection):
        """Test connection compatibility calls detect_version and get_capabilities"""
        manager = SQLVersionManager(mock_sql_connection)
        
        # Mock successful version detection
        mock_sql_connection.execute_query.return_value = [{
            'version_string': 'Microsoft SQL Server 2019',
            'server_name': 'TestServer',
            'major_version': 15,
            'minor_version': 0,
            'engine_edition': 3
        }]
        
        success, message = manager.test_connection_compatibility()
        
        # Verify detect_version was called (through execute_query)
        mock_sql_connection.execute_query.assert_called_once()
        assert success is True
        assert "SUPPORTED" in message


@pytest.mark.parametrize("major_version,expected_physical_cpu,expected_socket", [
    (11, False, False),  # SQL 2012
    (12, False, False),  # SQL 2014
    (13, True, True),    # SQL 2016
    (14, True, True),    # SQL 2017
    (15, True, True),    # SQL 2019
    (16, True, True),    # SQL 2022
    (17, True, True),    # SQL 2025
])
def test_get_capabilities_version_matrix(major_version, expected_physical_cpu, expected_socket, mock_sql_connection):
    """Parametrized test for capabilities across different SQL Server versions"""
    manager = SQLVersionManager(mock_sql_connection)
    
    # Mock version info
    manager._version_info = {
        'major_version': major_version,
        'minor_version': 0
    }
    
    result = manager.get_capabilities()
    
    assert result['has_physical_cpu_count'] == expected_physical_cpu
    assert result['has_socket_count'] == expected_socket


@pytest.mark.parametrize("version_string,expected_major", [
    ('Microsoft SQL Server 2012 (SP4) - 11.0.7001.0', 11),
    ('SQL Server 2014 (SP3) - 12.0.6024.0', 12),
    ('SQL Server 2016 (RTM) - 13.0.1601.5', 13),
    ('Some text with 14.0 version number', 14),
    ('SQL Server 2019 (RTM) - 15.0.2000.5', 15),
    ('SQL Server 2022 Preview - 16.0.1000.6', 16),
    ('SQL Server 2025 - 17.0.1000.1', 17),
    ('Unknown version string', 0)  # No match
])
def test_detect_version_string_parsing_matrix(version_string, expected_major, mock_sql_connection):
    """Parametrized test for version string parsing logic"""
    manager = SQLVersionManager(mock_sql_connection)
    
    mock_sql_connection.execute_query.return_value = [{
        'version_string': version_string,
        'server_name': 'TestServer',
        'major_version': 0,  # Force string parsing
        'minor_version': 0,
        'engine_edition': 1
    }]
    
    result = manager.detect_version()
    
    assert result['major_version'] == expected_major
