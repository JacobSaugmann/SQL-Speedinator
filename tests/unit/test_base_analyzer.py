"""
Unit tests for BaseAnalyzer abstract base class
Tests the core functionality of all analyzer subclasses
"""

import pytest
import logging
from unittest.mock import Mock, MagicMock
from abc import abstractmethod

# Import BaseAnalyzer - try both relative and absolute paths
try:
    from src.core.base_analyzer import BaseAnalyzer
except ImportError:
    from src.core.base_analyzer import BaseAnalyzer


class ConcreteAnalyzer(BaseAnalyzer):
    """Concrete implementation of BaseAnalyzer for testing"""
    
    def analyze(self):
        """Simple implementation for testing"""
        return {'test': 'data'}


class TestBaseAnalyzer:
    """Test suite for BaseAnalyzer abstract base class"""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL connection"""
        mock_conn = Mock()
        mock_conn.execute_query = Mock(return_value=[{'test': 'data'}])
        return mock_conn
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock config manager"""
        mock_cfg = Mock()
        mock_cfg.get = Mock(return_value='default')
        return mock_cfg
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that BaseAnalyzer cannot be instantiated directly"""
        mock_connection = Mock()
        mock_config = Mock()
        
        # BaseAnalyzer is abstract and should not be instantiable
        with pytest.raises(TypeError):
            BaseAnalyzer(mock_connection, mock_config)
    
    def test_concrete_subclass_instantiation(self, mock_connection, mock_config):
        """Test that concrete subclass can be instantiated"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        assert analyzer is not None
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
    
    def test_initialization_stores_dependencies(self, mock_connection, mock_config):
        """Test that __init__ properly stores connection and config"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        assert analyzer.connection is mock_connection
        assert analyzer.config is mock_config
    
    def test_initialization_creates_logger(self, mock_connection, mock_config):
        """Test that __init__ creates a logger instance"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        assert analyzer.logger is not None
        assert isinstance(analyzer.logger, logging.Logger)
    
    def test_rejects_none_connection(self, mock_config):
        """Test that __init__ rejects None connection"""
        with pytest.raises(TypeError):
            ConcreteAnalyzer(None, mock_config)
    
    def test_rejects_none_config(self, mock_connection):
        """Test that __init__ rejects None config"""
        with pytest.raises(TypeError):
            ConcreteAnalyzer(mock_connection, None)
    
    def test_abstract_method_must_be_implemented(self, mock_connection, mock_config):
        """Test that subclass must implement analyze() method"""
        
        class IncompleteAnalyzer(BaseAnalyzer):
            """Analyzer that doesn't implement analyze()"""
            pass
        
        # Should not be able to instantiate without implementing analyze()
        with pytest.raises(TypeError):
            IncompleteAnalyzer(mock_connection, mock_config)
    
    def test_analyze_method_is_called(self, mock_connection, mock_config):
        """Test that concrete implementation's analyze() can be called"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        result = analyzer.analyze()
        assert result == {'test': 'data'}
    
    def test_safe_execute_query_success(self, mock_connection, mock_config):
        """Test _safe_execute_query with successful query"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        query = "SELECT * FROM test"
        
        result = analyzer._safe_execute_query(query)
        
        mock_connection.execute_query.assert_called_once_with(query)
        assert result == [{'test': 'data'}]
    
    def test_safe_execute_query_with_timeout(self, mock_config, caplog):
        """Test _safe_execute_query with timeout error"""
        mock_connection = Mock()
        mock_connection.execute_query = Mock(side_effect=TimeoutError("Query timeout"))
        
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        
        # Should handle timeout gracefully and log it, returning None
        result = analyzer._safe_execute_query("SELECT * FROM test")
        
        # Check that error was logged
        assert "Query failed" in caplog.text or result is None
    
    def test_safe_execute_query_with_database_error(self, mock_config, caplog):
        """Test _safe_execute_query with database error"""
        mock_connection = Mock()
        mock_connection.execute_query = Mock(side_effect=Exception("Database error"))
        
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        
        # Should handle database error and log it, returning None
        result = analyzer._safe_execute_query("SELECT * FROM test")
        
        # Check that error was logged
        assert "Query failed" in caplog.text or result is None
    
    def test_logger_naming_convention(self, mock_connection, mock_config):
        """Test that logger name follows convention"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        # Logger should be named after the class
        assert 'ConcreteAnalyzer' in analyzer.logger.name or 'test_base_analyzer' in analyzer.logger.name
    
    def test_multiple_subclasses_independent_loggers(self, mock_connection, mock_config):
        """Test that each subclass gets its own logger"""
        analyzer1 = ConcreteAnalyzer(mock_connection, mock_config)
        
        class AnotherAnalyzer(BaseAnalyzer):
            def analyze(self):
                return {}
        
        analyzer2 = AnotherAnalyzer(mock_connection, mock_config)
        
        # Loggers should be different (different class names)
        assert analyzer1.logger.name != analyzer2.logger.name


class TestBaseAnalyzerIntegration:
    """Integration tests with real analyzer implementations"""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a realistic mock connection"""
        mock_conn = Mock()
        mock_conn.execute_query = Mock(return_value=[
            {'name': 'index1', 'fragmentation': 45.0},
            {'name': 'index2', 'fragmentation': 12.0}
        ])
        return mock_conn
    
    @pytest.fixture
    def mock_config(self):
        """Create a realistic mock config"""
        mock_cfg = Mock()
        mock_cfg.get = Mock(side_effect=lambda key, default=None: {
            'FRAG_THRESHOLD': 30.0,
            'TIMEOUT_MS': 30000
        }.get(key, default))
        return mock_cfg
    
    def test_analyzer_can_be_instantiated_and_used(self, mock_connection, mock_config):
        """Test complete workflow: instantiate, call analyze()"""
        analyzer = ConcreteAnalyzer(mock_connection, mock_config)
        result = analyzer.analyze()
        assert isinstance(result, dict)
        assert result == {'test': 'data'}
    
    def test_error_in_subclass_analyze_is_propagated(self, mock_connection, mock_config):
        """Test that errors in analyze() implementation are propagated"""
        
        class FailingAnalyzer(BaseAnalyzer):
            def analyze(self):
                raise RuntimeError("Something went wrong")
        
        analyzer = FailingAnalyzer(mock_connection, mock_config)
        with pytest.raises(RuntimeError):
            analyzer.analyze()
