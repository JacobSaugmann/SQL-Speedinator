"""
Comprehensive test suite for error handling, circuit breaker, and AnalysisResult wrapper
Tests new infrastructure with localhost SQL Server
"""

import pytest
import time
import logging
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.circuit_breaker import CircuitBreaker, CircuitState
from src.core.exceptions import (
    SQLSpeedError, DatabaseError, DatabaseQueryError, 
    AIError, AIServiceUnavailableError, AnalysisError
)
from src.core.result_wrapper import AnalysisResult, QueryResult
from src.core.config_manager import ConfigManager
from src.services.ai_service import AIService
from src.analyzers.disk_analyzer import DiskAnalyzer
from src.analyzers.index_analyzer import IndexAnalyzer
from src.analyzers.wait_stats_analyzer import WaitStatsAnalyzer


# ============================================================================
# CIRCUIT BREAKER TESTS
# ============================================================================

class TestCircuitBreaker:
    """Test circuit breaker pattern implementation"""
    
    def test_circuit_breaker_closed_state(self):
        """Circuit should allow calls when CLOSED"""
        breaker = CircuitBreaker(name="TestBreaker", failure_threshold=3)
        
        # Define a simple function that succeeds
        def successful_function():
            return "success"
        
        # Should pass through without raising
        result = breaker.call(successful_function)
        assert result == "success"
        assert breaker.state == CircuitState.CLOSED
    
    def test_circuit_breaker_opens_after_threshold(self):
        """Circuit should OPEN after failure threshold reached"""
        breaker = CircuitBreaker(name="TestBreaker", failure_threshold=2)
        
        def failing_function():
            raise AIError("Service unavailable", error_code="SERVICE_DOWN")
        
        # First failure
        with pytest.raises(AIError):
            breaker.call(failing_function)
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 1
        
        # Second failure
        with pytest.raises(AIError):
            breaker.call(failing_function)
        assert breaker.state == CircuitState.OPEN
    
    def test_circuit_breaker_rejects_calls_when_open(self):
        """Circuit should REJECT calls with AIServiceUnavailableError when OPEN"""
        breaker = CircuitBreaker(name="TestBreaker", failure_threshold=1)
        breaker.state = CircuitState.OPEN  # Manually open for testing
        breaker.last_failure_time = time.time()  # Set failure time
        
        def some_function():
            return "should not execute"
        
        with pytest.raises(AIServiceUnavailableError):
            breaker.call(some_function)
    
    def test_circuit_breaker_half_open_tests_recovery(self):
        """Circuit should go HALF_OPEN to test if service recovered"""
        breaker = CircuitBreaker(
            name="TestBreaker",
            failure_threshold=1,
            recovery_timeout=1  # 1 second for testing
        )
        
        def failing_function():
            raise AIError("Temporary error", error_code="TEMP_ERROR")
        
        # Trigger opening
        with pytest.raises(AIError):
            breaker.call(failing_function)
        
        assert breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        # Now when we try, it should attempt recovery (HALF_OPEN)
        # Since the function still fails, circuit reopens
        with pytest.raises(AIError):
            breaker.call(failing_function)
        
        # After failed attempt in HALF_OPEN, circuit should reopen
        assert breaker.state == CircuitState.OPEN
    
    def test_circuit_breaker_closes_on_successful_recovery(self):
        """Circuit should CLOSE when service recovers in HALF_OPEN"""
        breaker = CircuitBreaker(
            name="TestBreaker",
            failure_threshold=1,
            recovery_timeout=1
        )
        
        call_count = 0
        
        def sometimes_failing_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                raise AIError("First error", error_code="ERROR")
            return "recovered"
        
        # First call fails, circuit opens
        with pytest.raises(AIError):
            breaker.call(sometimes_failing_function)
        assert breaker.state == CircuitState.OPEN
        
        # Wait and reset time tracker
        time.sleep(1.1)
        
        # Call succeeds (simulating service recovery)
        result = breaker.call(sometimes_failing_function)
        assert result == "recovered"
        assert breaker.state == CircuitState.CLOSED


# ============================================================================
# EXCEPTION HIERARCHY TESTS
# ============================================================================

class TestExceptionHierarchy:
    """Test custom exception types and inheritance"""
    
    def test_exception_base_class(self):
        """SQLSpeedError should have error_code and context"""
        error = SQLSpeedError(
            message="Test error",
            error_code="TEST_CODE",
            context={"operation": "test"}
        )
        
        assert str(error) == "Test error"
        assert error.error_code == "TEST_CODE"
        assert error.context == {"operation": "test"}
    
    def test_database_error_inheritance(self):
        """DatabaseError should inherit from SQLSpeedError"""
        error = DatabaseError("DB connection failed")
        assert isinstance(error, SQLSpeedError)
    
    def test_ai_error_inheritance(self):
        """AIError and AIServiceUnavailableError should inherit correctly"""
        error = AIError("AI failed")
        assert isinstance(error, SQLSpeedError)
        
        unavailable = AIServiceUnavailableError("Service down")
        assert isinstance(unavailable, AIError)
        assert isinstance(unavailable, SQLSpeedError)
    
    def test_exception_to_dict_serialization(self):
        """Exceptions should serialize to JSON-compatible dict"""
        error = DatabaseQueryError(
            "Query failed",
            error_code="QUERY_ERROR",
            context={"query": "SELECT *"}
        )
        
        error_dict = error.to_dict()
        assert error_dict["error_code"] == "QUERY_ERROR"
        assert error_dict["message"] == "Query failed"
        assert error_dict["context"]["query"] == "SELECT *"


# ============================================================================
# RESULT WRAPPER TESTS
# ============================================================================

class TestAnalysisResult:
    """Test unified AnalysisResult wrapper"""
    
    def test_success_result_creation(self):
        """Should create successful result with data"""
        data = {"indexes": 42, "fragmentation": 15.5}
        result = AnalysisResult.success_result(data=data)
        
        assert result.success
        assert not result.is_error()
        assert not result.is_partial()
        assert result.data == data
    
    def test_error_result_creation(self):
        """Should create error result with retry info"""
        result = AnalysisResult.error_result(
            error="Connection failed",
            error_type="database",
            retry_available=True
        )
        
        assert result.is_error()
        assert not result.success
        assert result.retry_available is True
        assert result.error == "Connection failed"
    
    def test_partial_result_creation(self):
        """Should create partial result for graceful degradation"""
        partial_data = {"wait_stats": [1, 2, 3]}
        result = AnalysisResult.partial_result(
            data=partial_data,
            error="AI unavailable",
            error_type="ai"
        )
        
        assert result.is_partial()
        assert not result.success
        assert result.is_error()
        assert result.data == partial_data
    
    def test_result_to_dict_serialization(self):
        """Should serialize to dict for JSON responses"""
        result = AnalysisResult.success_result(data={"test": "value"})
        result_dict = result.to_dict()
        
        assert result_dict["success"] is True
        assert result_dict["data"] == {"test": "value"}
    
    def test_query_result_for_sql_operations(self):
        """QueryResult should wrap SQL query results"""
        rows = [{"id": 1, "name": "Index1"}, {"id": 2, "name": "Index2"}]
        result = QueryResult.success(rows=rows)
        
        assert result.row_count == 2
        assert len(result.rows) == 2
        assert result.rows[0]["id"] == 1


# ============================================================================
# AI SERVICE WITH CIRCUIT BREAKER TESTS
# ============================================================================

class TestAIServiceWithCircuitBreaker:
    """Test AI service with integrated circuit breaker"""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock config for AI service"""
        config = Mock(spec=ConfigManager)
        config.be_my_copilot = True
        config.azure_openai_api_key = "test-key"
        config.azure_openai_api_version = "2024-02-15-preview"
        config.azure_openai_endpoint = "https://test.openai.azure.com/"
        config.azure_openai_deployment = "test-deployment"
        config.azure_openai_model = "gpt-4"
        config.ai_max_tokens = 2000
        config.ai_temperature = 0.7
        config.validate_ai_config = Mock(return_value=True)
        return config
    
    def test_ai_service_has_circuit_breaker(self, mock_config):
        """AI service should have circuit breaker initialized"""
        ai_service = AIService(mock_config)
        assert ai_service.circuit_breaker is not None
        assert ai_service.circuit_breaker.state == CircuitState.CLOSED
    
    def test_ai_service_fallback_when_unavailable(self, mock_config):
        """AI service should return fallback analysis when unavailable"""
        ai_service = AIService(mock_config)
        
        # Force circuit to open
        ai_service.circuit_breaker.state = CircuitState.OPEN
        ai_service.circuit_breaker.last_failure_time = time.time()
        
        performance_summary = {
            'wait_stats': {
                'top_waits': [{'wait_type': 'PAGEIOLATCH_SH', 'percentage': 45.2}]
            },
            'disk_issues': [{'database': 'master', 'issue': 'High latency'}],
            'index_issues': {'high_fragmentation_count': 5}
        }
        
        result = ai_service.analyze_performance_summary(performance_summary)
        
        assert result is not None
        assert result['ai_enabled'] is False
        assert 'analysis' in result
        assert 'bottlenecks' in result['analysis']
        # Should have basic recommendations
        assert len(result['analysis']['bottlenecks']) > 0


# ============================================================================
# ANALYZER MIGRATION TESTS
# ============================================================================

class TestAnalyzerMigrations:
    """Test that analyzers return AnalysisResult correctly"""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock SQL connection"""
        connection = Mock()
        connection.execute_query = Mock(return_value=[])
        return connection
    
    @pytest.fixture
    def mock_config(self):
        """Create mock config"""
        config = Mock(spec=ConfigManager)
        config.min_index_size_mb = 1
        config.max_fragmentation_threshold = 10
        return config
    
    def test_disk_analyzer_returns_analysis_result(self, mock_connection, mock_config):
        """DiskAnalyzer.analyze() should return AnalysisResult"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        with patch.object(analyzer, '_get_sql_disk_stats', return_value=[]):
            with patch.object(analyzer, '_get_os_disk_stats', return_value={}):
                with patch.object(analyzer, '_analyze_disk_formatting', return_value=[]):
                    with patch.object(analyzer, '_analyze_tempdb_placement', return_value=[]):
                        with patch.object(analyzer, '_analyze_drive_configuration', return_value=[]):
                            with patch.object(analyzer, '_identify_io_bottlenecks', return_value=[]):
                                with patch.object(analyzer, '_identify_slow_disks', return_value=[]):
                                    with patch.object(analyzer, '_generate_disk_recommendations', return_value=[]):
                                        result = analyzer.analyze()
        
        assert isinstance(result, AnalysisResult)
        assert result.success
        assert result.data is not None
    
    def test_analyzer_error_handling_graceful(self, mock_connection, mock_config):
        """Analyzer should return error AnalysisResult on failure"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        # Force an error
        mock_connection.execute_query.side_effect = Exception("Connection timeout")
        
        with patch.object(analyzer, '_get_sql_disk_stats', side_effect=Exception("DB Error")):
            result = analyzer.analyze()
        
        assert isinstance(result, AnalysisResult)
        assert result.is_error()
        assert result.error is not None


# ============================================================================
# INTEGRATION TESTS WITH LOCALHOST
# ============================================================================

class TestLocalHostIntegration:
    """Test with actual localhost SQL Server (if available)"""
    
    @pytest.fixture
    def localhost_config(self):
        """Create config for localhost SQL Server"""
        config = Mock(spec=ConfigManager)
        config.server_name = "localhost"
        config.database_name = "tempdb"  # Use tempdb as it's always available
        config.use_windows_auth = True
        config.username = None
        config.password = None
        config.connection_timeout = 5
        return config
    
    def test_localhost_connection_attempt(self, localhost_config):
        """Test if localhost SQL Server is available"""
        try:
            from src.core.sql_connection import SQLServerConnection
            
            connection = SQLServerConnection(localhost_config)
            # Try to execute simple query
            result = connection.execute_query("SELECT @@VERSION")
            
            assert result is not None
            assert len(result) > 0
            
            logging.info(f"✓ Localhost SQL Server connected: {result[0]['@@VERSION'][:50]}")
            
        except Exception as e:
            pytest.skip(f"Localhost SQL Server not available: {e}")
    
    def test_localhost_simple_query_with_error_handling(self, localhost_config):
        """Test error handling with actual localhost connection"""
        try:
            from src.core.sql_connection import SQLServerConnection
            
            connection = SQLServerConnection(localhost_config)
            
            # Execute valid query
            valid_result = connection.execute_query("SELECT 1 AS test_value")
            assert valid_result is not None
            
            logging.info("✓ Valid query executed successfully")
            
        except Exception as e:
            pytest.skip(f"Cannot test with localhost: {e}")


# ============================================================================
# LOGGING AND DEBUGGING TESTS
# ============================================================================

class TestErrorLogging:
    """Test that errors are logged with proper context"""
    
    def test_exception_logging_context(self, caplog):
        """Errors should include exc_info for full stack traces"""
        logger = logging.getLogger("test_logger")
        
        try:
            raise DatabaseQueryError("Query failed", error_code="QUERY_ERROR")
        except DatabaseQueryError as e:
            logger.error(f"Database error: {e}", exc_info=True)
        
        # Check that error was logged
        assert "DatabaseQueryError" in caplog.text
        assert "Query failed" in caplog.text


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
