"""
Comprehensive unit tests for ServerConfigAnalyzer class
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import logging

from src.analyzers.server_config_analyzer import ServerConfigAnalyzer


class TestServerConfigAnalyzer:
    """Test class for ServerConfigAnalyzer functionality"""
    
    @pytest.fixture
    def mock_connection(self):
        """Mock SQL connection"""
        connection = Mock()
        connection.execute_query.return_value = [
            {
                'server_name': 'TestServer',
                'instance_name': 'MSSQLSERVER',
                'version': '15.0.2000.5',
                'edition': 'Developer Edition'
            }
        ]
        return connection
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration manager"""
        config = Mock()
        config.timeout = 30
        return config
    
    @pytest.fixture 
    def mock_version_manager(self):
        """Mock SQL version manager"""
        version_manager = Mock()
        version_manager.get_compatible_server_info_query.return_value = "SELECT @@SERVERNAME"
        version_manager.get_compatible_configuration_query.return_value = "SELECT * FROM sys.configurations"
        return version_manager
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_init_creates_instance_with_proper_attributes(self, mock_version_class, mock_connection, mock_config):
        """Test that ServerConfigAnalyzer initializes correctly"""
        mock_version_class.return_value = Mock()
        
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert isinstance(analyzer.logger, logging.Logger)
        assert analyzer.version_manager is not None
        mock_version_class.assert_called_once_with(mock_connection)
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_returns_structure_on_success(self, mock_version_class, mock_connection, mock_config):
        """Test that analyze method returns expected structure"""
        mock_version_class.return_value = Mock()
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        # Mock all the internal methods to return simple data
        analyzer._get_server_info = Mock(return_value=[{'server_name': 'TestServer'}])
        analyzer._get_configuration_settings = Mock(return_value=[{'name': 'max_memory', 'value': 2048}])
        analyzer._analyze_memory_configuration = Mock(return_value={'status': 'good'})
        analyzer._analyze_parallelism_settings = Mock(return_value={'maxdop': 4})
        analyzer._analyze_database_settings = Mock(return_value={'growth': 'auto'})
        analyzer._analyze_security_settings = Mock(return_value={'authentication': 'windows'})
        analyzer._identify_configuration_issues = Mock(return_value=[])
        analyzer._generate_config_recommendations = Mock(return_value=[])
        
        result = analyzer.analyze()
        
        # Verify structure
        expected_keys = [
            'server_info', 'configuration_settings', 'memory_configuration',
            'parallelism_settings', 'database_settings', 'security_settings',
            'issues', 'recommendations'
        ]
        
        for key in expected_keys:
            assert key in result
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_handles_exception(self, mock_version_class, mock_connection, mock_config):
        """Test that analyze method handles exceptions gracefully"""
        mock_version_class.return_value = Mock()
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        # Force an exception in one of the methods
        analyzer._get_server_info = Mock(side_effect=Exception("Database error"))
        
        result = analyzer.analyze()
        
        assert 'error' in result
        assert 'Database error' in result['error']
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_get_server_info_success(self, mock_version_class, mock_connection, mock_config):
        """Test successful server info retrieval"""
        mock_version_manager = Mock()
        mock_version_manager.get_compatible_server_info_query.return_value = "SELECT @@SERVERNAME"
        mock_version_class.return_value = mock_version_manager
        
        expected_data = [
            {
                'server_name': 'TestServer',
                'instance_name': 'MSSQLSERVER',
                'version': '15.0.2000.5'
            }
        ]
        mock_connection.execute_query.return_value = expected_data
        
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_info()
        
        assert result == expected_data
        mock_connection.execute_query.assert_called_once_with("SELECT @@SERVERNAME")
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_get_configuration_settings_success(self, mock_version_class, mock_connection, mock_config):
        """Test successful configuration settings retrieval"""
        mock_version_manager = Mock()
        mock_version_manager.get_compatible_configuration_query.return_value = "SELECT * FROM sys.configurations"
        mock_version_class.return_value = mock_version_manager
        
        config_data = [
            {
                'name': 'max server memory (MB)',
                'value': 2048,
                'value_in_use': 2048,
                'minimum': 0,
                'maximum': 2147483647
            }
        ]
        mock_connection.execute_query.return_value = config_data
        
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        result = analyzer._get_configuration_settings()
        
        assert result == config_data
        mock_connection.execute_query.assert_called_once_with("SELECT * FROM sys.configurations")
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_memory_configuration_with_good_settings(self, mock_version_class, mock_connection, mock_config):
        """Test memory configuration analysis with good settings"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version_class.return_value = mock_version

        # Mock configuration data with good memory settings
        good_config = [
            {
                'name': 'max server memory (MB)',
                'value_in_use': '6144',  # 6GB as string
                'minimum': '0',
                'maximum': '2147483647'
            },
            {
                'name': 'min server memory (MB)',
                'value_in_use': '0',
                'minimum': '0',
                'maximum': '2147483647'
            }
        ]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.return_value = good_config

        result = analyzer._analyze_memory_configuration()

        assert 'settings' in result
        assert 'issues' in result
        assert len(result['issues']) == 0  # Should have no issues
        assert 'min_server_memory' in result
        assert 'issues' in result
        assert result['max_server_memory'] == 6144
        assert result['min_server_memory'] == 0
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_memory_configuration_with_issues(self, mock_version_class, mock_connection, mock_config):
        """Test memory configuration analysis with issues"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version_class.return_value = mock_version

        # Mock configuration data with memory issues
        bad_config = [
            {
                'name': 'max server memory (MB)',
                'value_in_use': '2147483647',  # Default max - bad
                'minimum': '0',
                'maximum': '2147483647'
            }
        ]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.return_value = bad_config

        result = analyzer._analyze_memory_configuration()

        assert 'issues' in result
        assert len(result['issues']) > 0
        assert len(result['issues']) > 0
        assert any('max server memory' in issue['issue'] for issue in result['issues'])
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_parallelism_settings_with_good_maxdop(self, mock_version_class, mock_connection, mock_config):
        """Test parallelism settings analysis with good MAXDOP"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version.get_compatible_cpu_info_query.return_value = "SELECT 4 as cpu_count"
        mock_version_class.return_value = mock_version

        # Mock configuration with good MAXDOP setting
        good_config = [
            {
                'name': 'max degree of parallelism',
                'value_in_use': '4',  # Good value as string
                'minimum': '0',
                'maximum': '32767'
            },
            {
                'name': 'cost threshold for parallelism',
                'value_in_use': '50',  # Good value as string
                'minimum': '0',
                'maximum': '32767'
            }
        ]

        cpu_info = [{'cpu_count': 8}]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.side_effect = [good_config, cpu_info]

        result = analyzer._analyze_parallelism_settings()

        assert 'settings' in result
        assert 'cpu_info' in result
        assert 'issues' in result
        assert 'cost_threshold' in result
        assert 'issues' in result
        assert result['maxdop'] == 4
        assert result['cost_threshold'] == 50
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_parallelism_settings_with_maxdop_zero(self, mock_version_class, mock_connection, mock_config):
        """Test parallelism settings analysis with MAXDOP = 0 (bad)"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version.get_compatible_cpu_info_query.return_value = "SELECT 4 as cpu_count"
        mock_version_class.return_value = mock_version

        # Mock configuration with MAXDOP = 0 (problematic)
        bad_config = [
            {
                'name': 'max degree of parallelism',
                'value_in_use': '0',  # Bad value as string
                'minimum': '0',
                'maximum': '32767'
            }
        ]

        cpu_info = [{'cpu_count': 8}]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.side_effect = [bad_config, cpu_info]

        result = analyzer._analyze_parallelism_settings()

        assert 'issues' in result
        assert len(result['issues']) > 0
        assert any('MAXDOP' in issue['issue'] for issue in result['issues'])

    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_database_settings_success(self, mock_version_class, mock_connection, mock_config):
        """Test database settings analysis"""
        mock_version = Mock()
        mock_version_class.return_value = mock_version

        # Mock database settings
        db_settings = [
            {
                'name': 'TestDB',
                'auto_close': 0,  # Good
                'auto_shrink': 0,  # Good
                'page_verify_option_desc': 'CHECKSUM'  # Good
            }
        ]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.return_value = db_settings

        result = analyzer._analyze_database_settings()

        assert 'databases' in result
        assert 'issues' in result
        assert 'issues' in result
        assert result['databases_analyzed'] == 1
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_database_settings_with_issues(self, mock_version_class, mock_connection, mock_config):
        """Test database settings analysis with issues"""
        mock_version_class.return_value = Mock()
        
        # Mock database settings with issues
        bad_db_settings = [
            {
                'name': 'TestDB',
                'auto_close': 1,  # Bad
                'auto_shrink': 1,  # Bad
                'page_verify_option_desc': 'NONE'  # Bad
            }
        ]
        
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.return_value = bad_db_settings
        
        result = analyzer._analyze_database_settings()

        assert 'issues' in result
        assert len(result['issues']) >= 3  # Should have multiple issues
        issues_text = ' '.join([issue['issue'] for issue in result['issues']])
        assert 'Page verify set to NONE' in issues_text
        assert 'auto_shrink' in issues_text
        assert 'page verification' in issues_text
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_security_settings_with_safe_config(self, mock_version_class, mock_connection, mock_config):
        """Test security settings analysis with safe configuration"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version_class.return_value = mock_version

        # Mock safe security settings
        safe_config = [
            {
                'name': 'xp_cmdshell',
                'value_in_use': '0'  # Disabled - good
            },
            {
                'name': 'Ad Hoc Distributed Queries',
                'value_in_use': '0'  # Disabled - good
            }
        ]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.return_value = safe_config

        result = analyzer._analyze_security_settings()

        assert 'issues' in result
        assert 'settings' in result
        assert len(result['issues']) == 0  # No issues with safe config
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_analyze_security_settings_with_risky_config(self, mock_version_class, mock_connection, mock_config):
        """Test security settings analysis with risky configuration"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version_class.return_value = mock_version

        # Mock risky security settings
        risky_config = [
            {
                'name': 'xp_cmdshell',
                'value_in_use': '1'  # Enabled - risky
            },
            {
                'name': 'Ad Hoc Distributed Queries',
                'value_in_use': '1'  # Enabled - risky
            }
        ]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.return_value = risky_config

        result = analyzer._analyze_security_settings()

        assert 'issues' in result
        assert len(result['issues']) > 0
        assert len(result['issues']) >= 2  # Should have multiple security issues
        severities = [issue['severity'] for issue in result['issues']]
        assert 'HIGH' in severities  # xp_cmdshell should be HIGH severity
        assert 'MEDIUM' in severities  # Ad Hoc should be MEDIUM severity
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_identify_configuration_issues_aggregates_all_issues(self, mock_version_class, mock_connection, mock_config):
        """Test that identify_configuration_issues aggregates all issues"""
        mock_version_class.return_value = Mock()
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        # Mock different analyzers to return issues
        analyzer._analyze_memory_configuration = Mock(return_value={
            'issues': [{'issue': 'Memory issue', 'severity': 'HIGH'}]
        })
        analyzer._analyze_parallelism_settings = Mock(return_value={
            'issues': [{'issue': 'MAXDOP issue', 'severity': 'MEDIUM'}]
        })
        analyzer._analyze_database_settings = Mock(return_value={
            'issues': [{'issue': 'Database issue', 'severity': 'LOW'}]
        })
        analyzer._analyze_security_settings = Mock(return_value={
            'issues': [{'issue': 'Security issue', 'severity': 'HIGH'}]
        })
        
        result = analyzer._identify_configuration_issues()
        
        assert len(result) == 4  # Should aggregate all issues
        # Verify sorting by severity (HIGH first, then MEDIUM, then LOW)
        assert result[0]['severity'] in ['HIGH']
        assert result[-1]['severity'] == 'LOW'
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_generate_config_recommendations_with_issues(self, mock_version_class, mock_connection, mock_config):
        """Test config recommendations generation with issues"""
        mock_version_class.return_value = Mock()
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        # Mock issues to generate recommendations from
        mock_issues = [
            {
                'setting': 'max server memory (MB)',
                'issue': 'Memory not configured',
                'severity': 'HIGH',
                'recommendation': 'Set appropriate memory limit'
            },
            {
                'setting': 'max degree of parallelism',
                'issue': 'MAXDOP is 0',
                'severity': 'MEDIUM',
                'recommendation': 'Set MAXDOP to appropriate value'
            }
        ]
        analyzer._identify_configuration_issues = Mock(return_value=mock_issues)
        
        result = analyzer._generate_config_recommendations()
        
        assert len(result) >= 2  # Should have recommendations for the issues
        priorities = [rec['priority'] for rec in result]
        assert 'HIGH' in priorities
        assert 'MEDIUM' in priorities
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_generate_config_recommendations_no_issues(self, mock_version_class, mock_connection, mock_config):
        """Test config recommendations generation with no issues"""
        mock_version_class.return_value = Mock()
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        # Mock no issues
        analyzer._identify_configuration_issues = Mock(return_value=[])
        
        result = analyzer._generate_config_recommendations()
        
        # Should still have some general recommendations even with no issues
        assert isinstance(result, list)
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_exception_handling_in_individual_methods(self, mock_version_class, mock_connection, mock_config):
        """Test exception handling in individual analysis methods"""
        mock_version_class.return_value = Mock()
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        
        # Test memory configuration with exception
        analyzer._get_configuration_settings = Mock(side_effect=Exception("Config error"))
        
        result = analyzer._analyze_memory_configuration()
        assert 'error' in result
        
        # Test security settings with exception
        result = analyzer._analyze_security_settings()
        assert 'error' in result
    
    @patch('src.analyzers.server_config_analyzer.SQLVersionManager')
    def test_value_conversion_handles_invalid_data(self, mock_version_class, mock_connection, mock_config):
        """Test that value conversion handles invalid data gracefully"""
        mock_version = Mock()
        mock_version.get_capabilities.return_value = {'supports_nvarchar_cast': True}
        mock_version.get_compatible_cpu_info_query.return_value = "SELECT 4 as cpu_count"
        mock_version_class.return_value = mock_version

        # Mock configuration with invalid value types
        invalid_config = [
            {
                'name': 'max server memory (MB)',
                'value_in_use': 'invalid_number',  # Invalid
                'minimum': '0',
                'maximum': '2147483647'
            },
            {
                'name': 'max degree of parallelism',
                'value_in_use': None,  # None value
                'minimum': '0',
                'maximum': '32767'
            }
        ]

        cpu_info = [{'cpu_count': 8}]

        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        mock_connection.execute_query.side_effect = [invalid_config, cpu_info, invalid_config, cpu_info]

        # Should not raise exceptions despite invalid data
        memory_result = analyzer._analyze_memory_configuration()
        parallelism_result = analyzer._analyze_parallelism_settings()

        assert 'settings' in memory_result or 'error' in memory_result
        assert 'settings' in parallelism_result or 'error' in parallelism_result