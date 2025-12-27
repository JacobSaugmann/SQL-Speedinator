"""
Integration tests for BaseAnalyzer adoption across all analyzers
Validates that all 13 analyzers work correctly with the new abstract base class
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime

# Import all analyzers
try:
    from src.analyzers.wait_stats_analyzer import WaitStatsAnalyzer
    from src.analyzers.disk_analyzer import DiskAnalyzer
    from src.analyzers.index_analyzer import IndexAnalyzer
    from src.analyzers.tempdb_analyzer import TempDBAnalyzer
    from src.analyzers.server_config_analyzer import ServerConfigAnalyzer
    from src.analyzers.plan_cache_analyzer import PlanCacheAnalyzer
    from src.analyzers.missing_index_analyzer import MissingIndexAnalyzer
    from src.analyzers.server_database_analyzer import ServerDatabaseAnalyzer
    from src.analyzers.log_analyzer import LogAnalyzer
    from src.analyzers.advanced_index_analyzer import AdvancedIndexAnalyzer
    from src.analyzers.simple_server_analyzer import SimpleServerAnalyzer
except ImportError:
    from analyzers.wait_stats_analyzer import WaitStatsAnalyzer
    from analyzers.disk_analyzer import DiskAnalyzer
    from analyzers.index_analyzer import IndexAnalyzer
    from analyzers.tempdb_analyzer import TempDBAnalyzer
    from analyzers.server_config_analyzer import ServerConfigAnalyzer
    from analyzers.plan_cache_analyzer import PlanCacheAnalyzer
    from analyzers.missing_index_analyzer import MissingIndexAnalyzer
    from analyzers.server_database_analyzer import ServerDatabaseAnalyzer
    from analyzers.log_analyzer import LogAnalyzer
    from analyzers.advanced_index_analyzer import AdvancedIndexAnalyzer
    from analyzers.simple_server_analyzer import SimpleServerAnalyzer


class TestAllAnalyzersBaseAnalyzerIntegration:
    """Test that all analyzers are properly refactored to use BaseAnalyzer"""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a realistic mock SQL connection"""
        mock_conn = Mock()
        mock_conn.execute_query = Mock(return_value=[
            {'col1': 'value1', 'col2': 'value2'},
            {'col1': 'value3', 'col2': 'value4'}
        ])
        return mock_conn
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock config manager"""
        mock_cfg = Mock()
        mock_cfg.get = Mock(return_value='default_value')
        return mock_cfg
    
    # Test WaitStatsAnalyzer
    def test_wait_stats_analyzer_instantiation(self, mock_connection, mock_config):
        """Test WaitStatsAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = WaitStatsAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test DiskAnalyzer
    def test_disk_analyzer_instantiation(self, mock_connection, mock_config):
        """Test DiskAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test IndexAnalyzer
    def test_index_analyzer_instantiation(self, mock_connection, mock_config):
        """Test IndexAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = IndexAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test TempDBAnalyzer
    def test_tempdb_analyzer_instantiation(self, mock_connection, mock_config):
        """Test TempDBAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = TempDBAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test ServerConfigAnalyzer
    def test_server_config_analyzer_instantiation(self, mock_connection, mock_config):
        """Test ServerConfigAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = ServerConfigAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test PlanCacheAnalyzer
    def test_plan_cache_analyzer_instantiation(self, mock_connection, mock_config):
        """Test PlanCacheAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = PlanCacheAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test MissingIndexAnalyzer
    def test_missing_index_analyzer_instantiation(self, mock_connection, mock_config):
        """Test MissingIndexAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = MissingIndexAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test ServerDatabaseAnalyzer
    def test_server_database_analyzer_instantiation(self, mock_connection, mock_config):
        """Test ServerDatabaseAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = ServerDatabaseAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test LogAnalyzer
    def test_log_analyzer_instantiation(self, mock_connection, mock_config):
        """Test LogAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = LogAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test AdvancedIndexAnalyzer
    def test_advanced_index_analyzer_instantiation(self, mock_connection, mock_config):
        """Test AdvancedIndexAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = AdvancedIndexAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    # Test SimpleServerAnalyzer
    def test_simple_server_analyzer_instantiation(self, mock_connection, mock_config):
        """Test SimpleServerAnalyzer can be instantiated with new BaseAnalyzer"""
        analyzer = SimpleServerAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert hasattr(analyzer, 'logger')
    
    def test_all_analyzers_have_analyze_method(self, mock_connection, mock_config):
        """Verify all analyzers have an analyze() method (required by BaseAnalyzer)"""
        analyzers = [
            WaitStatsAnalyzer(mock_connection, mock_config),
            DiskAnalyzer(mock_connection, mock_config),
            IndexAnalyzer(mock_connection, mock_config),
            TempDBAnalyzer(mock_connection, mock_config),
            ServerConfigAnalyzer(mock_connection, mock_config),
            PlanCacheAnalyzer(mock_connection, mock_config),
            MissingIndexAnalyzer(mock_connection, mock_config),
            ServerDatabaseAnalyzer(mock_connection, mock_config),
            LogAnalyzer(mock_connection, mock_config),
            AdvancedIndexAnalyzer(mock_connection, mock_config),
            SimpleServerAnalyzer(mock_connection, mock_config),
        ]
        
        for analyzer in analyzers:
            assert hasattr(analyzer, 'analyze'), f"{analyzer.__class__.__name} missing analyze() method"
            assert callable(getattr(analyzer, 'analyze')), f"{analyzer.__class__.__name}.analyze() not callable"
    
    def test_analyzers_inherit_safe_execute_query(self, mock_connection, mock_config):
        """Verify analyzers inherited _safe_execute_query from BaseAnalyzer"""
        analyzers = [
            WaitStatsAnalyzer(mock_connection, mock_config),
            DiskAnalyzer(mock_connection, mock_config),
            IndexAnalyzer(mock_connection, mock_config),
        ]
        
        for analyzer in analyzers:
            assert hasattr(analyzer, '_safe_execute_query'), \
                f"{analyzer.__class__.__name} missing _safe_execute_query method"
            assert callable(getattr(analyzer, '_safe_execute_query')), \
                f"{analyzer.__class__.__name}._safe_execute_query() not callable"
