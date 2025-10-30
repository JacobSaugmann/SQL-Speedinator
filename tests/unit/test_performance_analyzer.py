"""
Unit tests for PerformanceAnalyzer class
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import time
import logging

from src.core.performance_analyzer import PerformanceAnalyzer
from src.analyzers.advanced_index_analyzer import IndexAnalysisSettings


class TestPerformanceAnalyzer:
    """Test class for PerformanceAnalyzer functionality"""
    
    @pytest.fixture
    def mock_connection(self):
        """Mock SQL connection"""
        connection = Mock()
        connection.get_server_info.return_value = [{
            'server_name': 'TestServer',
            'instance_name': 'MSSQLSERVER',
            'version': '15.0.2000.5',
            'edition': 'Developer Edition'
        }]
        return connection
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration manager"""
        config = Mock()
        config.night_mode_delay = 1
        config.be_my_copilot = False
        config.index_min_advantage = 10
        config.index_calculate_selectability = True
        config.index_only_analysis = False
        config.index_limit_to_table = None
        config.index_limit_to_index = None
        return config
    
    @patch('src.core.performance_analyzer.DiskAnalyzer')
    @patch('src.core.performance_analyzer.IndexAnalyzer')
    @patch('src.core.performance_analyzer.AdvancedIndexAnalyzer')
    @patch('src.core.performance_analyzer.ServerConfigAnalyzer')
    @patch('src.core.performance_analyzer.TempDBAnalyzer')
    @patch('src.core.performance_analyzer.PlanCacheAnalyzer')
    @patch('src.core.performance_analyzer.WaitStatsAnalyzer')
    @patch('src.core.performance_analyzer.MissingIndexAnalyzer')
    @patch('src.core.performance_analyzer.ServerDatabaseAnalyzer')
    @patch('src.core.performance_analyzer.LogAnalyzer')
    @patch('src.core.performance_analyzer.AIAnalyzer')
    @patch('src.core.performance_analyzer.IntelligentRecommendationsEngine')
    def test_init_creates_instance_with_proper_attributes(self, mock_intelligent_recs, mock_ai, mock_log, 
                                                          mock_server_db, mock_missing_idx, mock_wait_stats,
                                                          mock_plan_cache, mock_tempdb, mock_server_config,
                                                          mock_advanced_idx, mock_idx, mock_disk,
                                                          mock_connection, mock_config):
        """Test initialization creates analyzer with proper attributes"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config, night_mode=True)
        
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert analyzer.night_mode is True
        assert analyzer.analysis_results == {}
        assert isinstance(analyzer.logger, logging.Logger)
        
        # Verify all analyzer classes were instantiated
        mock_disk.assert_called_once_with(mock_connection, mock_config)
        mock_idx.assert_called_once_with(mock_connection, mock_config)
        mock_advanced_idx.assert_called_once_with(mock_connection)
        mock_server_config.assert_called_once_with(mock_connection, mock_config)
        mock_tempdb.assert_called_once_with(mock_connection, mock_config)
        mock_plan_cache.assert_called_once_with(mock_connection, mock_config)
        mock_wait_stats.assert_called_once_with(mock_connection, mock_config)
        mock_missing_idx.assert_called_once_with(mock_connection, mock_config)
        mock_server_db.assert_called_once_with(mock_connection, mock_config)
        mock_log.assert_called_once_with(mock_connection, mock_config)
        mock_ai.assert_called_once_with(mock_config)
        mock_intelligent_recs.assert_called_once_with(mock_config)
    
    def test_init_without_night_mode(self, mock_connection, mock_config):
        """Test initialization with default night_mode=False"""
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
            assert analyzer.night_mode is False
    
    def test_get_server_info_success(self, mock_connection, mock_config):
        """Test successful server info retrieval"""
        expected_info = {
            'server_name': 'TestServer',
            'instance_name': 'MSSQLSERVER',
            'version': '15.0.2000.5'
        }
        mock_connection.get_server_info.return_value = [expected_info]
        
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
            result = analyzer._get_server_info()
        
        assert result == expected_info
        mock_connection.get_server_info.assert_called_once()
    
    def test_get_server_info_empty_result(self, mock_connection, mock_config):
        """Test server info retrieval with empty result"""
        mock_connection.get_server_info.return_value = []
        
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
            result = analyzer._get_server_info()
        
        assert result == {'error': 'Could not retrieve server information'}
    
    def test_get_server_info_exception(self, mock_connection, mock_config):
        """Test server info retrieval with exception"""
        mock_connection.get_server_info.side_effect = Exception("Connection error")
        
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
            result = analyzer._get_server_info()
        
        assert result == {'error': 'Connection error'}
    
    def test_get_wait_recommendation(self, mock_connection, mock_config):
        """Test wait type recommendation mapping"""
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Test known wait types
        assert 'memory' in analyzer._get_wait_recommendation('PAGEIOLATCH_SH').lower()
        assert 'tempdb' in analyzer._get_wait_recommendation('PAGEIOLATCH_EX').lower()
        assert 'log' in analyzer._get_wait_recommendation('WRITELOG').lower()
        assert 'index' in analyzer._get_wait_recommendation('LCK_M_S').lower()
        assert 'blocking' in analyzer._get_wait_recommendation('LCK_M_X').lower()
        assert 'maxdop' in analyzer._get_wait_recommendation('CXPACKET').lower()
        assert 'cpu' in analyzer._get_wait_recommendation('SOS_SCHEDULER_YIELD').lower()
        assert 'connection' in analyzer._get_wait_recommendation('THREADPOOL').lower()
        
        # Test unknown wait type
        result = analyzer._get_wait_recommendation('UNKNOWN_WAIT')
        assert 'documentation' in result.lower()
    
    def test_analyze_advanced_indexes_success(self, mock_connection, mock_config):
        """Test successful advanced index analysis"""
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Mock IndexAnalysisResults
        mock_missing_index = Mock()
        mock_missing_index.table_name = 'Orders'
        mock_missing_index.equality_columns = 'CustomerID'
        mock_missing_index.inequality_columns = ''
        mock_missing_index.included_columns = 'OrderDate'
        mock_missing_index.create_index_advantage = 95.5
        mock_missing_index.avg_user_impact = 85.2
        mock_missing_index.avg_total_user_cost = 1250.75
        mock_missing_index.user_scans = 100
        mock_missing_index.user_seeks = 50
        mock_missing_index.create_index_statement = 'CREATE INDEX IX_Orders_CustomerID ON Orders (CustomerID) INCLUDE (OrderDate)'
        
        mock_results = Mock()
        mock_results.missing_indexes = [mock_missing_index]
        mock_results.existing_indexes = [Mock(), Mock()]
        mock_results.overlapping_indexes = []
        mock_results.unused_indexes = []
        mock_results.total_wasted_space_mb = 125.5
        mock_results.metadata_age_days = 7
        mock_results.warnings = ['Test warning']
        
        # Mock analyzer methods
        analyzer.advanced_index_analyzer.analyze_indexes.return_value = mock_results
        analyzer.advanced_index_analyzer.get_index_recommendations.return_value = ['Recommendation 1']
        analyzer.advanced_index_analyzer.get_maintenance_recommendations.return_value = ['Maintenance 1']
        
        result = analyzer._analyze_advanced_indexes()
        
        # Verify result structure
        assert 'missing_indexes_count' in result
        assert 'existing_indexes_count' in result
        assert 'overlapping_indexes_count' in result
        assert 'unused_indexes_count' in result
        assert 'total_wasted_space_mb' in result
        assert 'metadata_age_days' in result
        assert 'warnings' in result
        assert 'top_recommendations' in result
        assert 'maintenance_recommendations' in result
        assert 'missing_indexes' in result
        assert 'unused_indexes' in result
        assert 'overlapping_indexes' in result
        
        # Verify counts
        assert result['missing_indexes_count'] == 1
        assert result['existing_indexes_count'] == 2
        assert result['total_wasted_space_mb'] == 125.5
        assert result['metadata_age_days'] == 7
        
        # Verify missing index details
        missing_idx = result['missing_indexes'][0]
        assert missing_idx['table_name'] == 'Orders'
        assert missing_idx['equality_columns'] == 'CustomerID'
        assert missing_idx['advantage_score'] == 95.5
        assert 'CREATE INDEX' in missing_idx['create_statement']
    
    def test_analyze_advanced_indexes_exception(self, mock_connection, mock_config):
        """Test advanced index analysis with exception"""
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Mock analyzer to raise exception
        analyzer.advanced_index_analyzer.analyze_indexes.side_effect = Exception("Index analysis failed")
        
        result = analyzer._analyze_advanced_indexes()
        
        # Verify error handling
        assert 'error' in result
        assert result['error'] == 'Index analysis failed'
    
    def test_update_metadata_success(self, mock_connection, mock_config):
        """Test successful metadata update"""
        with patch.multiple('src.core.performance_analyzer',
                           DiskAnalyzer=Mock, IndexAnalyzer=Mock, AdvancedIndexAnalyzer=Mock,
                           ServerConfigAnalyzer=Mock, TempDBAnalyzer=Mock, PlanCacheAnalyzer=Mock,
                           WaitStatsAnalyzer=Mock, MissingIndexAnalyzer=Mock, ServerDatabaseAnalyzer=Mock,
                           LogAnalyzer=Mock, AIAnalyzer=Mock, IntelligentRecommendationsEngine=Mock):
            
            analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Set up analysis results
        analyzer.analysis_results = {
            'analysis_metadata': {},
            'server_database_info': {
                'data': {
                    'database_overview': [
                        {'name': 'Database1'},
                        {'name': 'Database2'},
                        {'name': 'Database3'}
                    ]
                }
            }
        }
        
        analyzer._update_metadata()
        
        # Verify metadata was updated
        assert 'databases_count' in analyzer.analysis_results['analysis_metadata']
        assert analyzer.analysis_results['analysis_metadata']['databases_count'] == 3