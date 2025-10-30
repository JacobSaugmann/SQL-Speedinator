"""
Simple unit tests for PerformanceAnalyzer class focusing on testable methods
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import logging

from src.core.performance_analyzer import PerformanceAnalyzer


class TestPerformanceAnalyzerSimple:
    """Simple test class for PerformanceAnalyzer functionality"""
    
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
    def test_init_with_night_mode_true(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                      mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                      mock_server_config, mock_advanced, mock_index, mock_disk,
                                      mock_connection, mock_config):
        """Test initialization with night_mode=True"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config, night_mode=True)
        
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert analyzer.night_mode is True
        assert analyzer.analysis_results == {}
        assert isinstance(analyzer.logger, logging.Logger)
    
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
    def test_init_with_night_mode_false(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                       mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                       mock_server_config, mock_advanced, mock_index, mock_disk,
                                       mock_connection, mock_config):
        """Test initialization with night_mode=False (default)"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        assert analyzer.night_mode is False
    
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
    def test_get_server_info_success(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                     mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                     mock_server_config, mock_advanced, mock_index, mock_disk,
                                     mock_connection, mock_config):
        """Test successful server info retrieval"""
        expected_info = {
            'server_name': 'TestServer',
            'instance_name': 'MSSQLSERVER',
            'version': '15.0.2000.5'
        }
        mock_connection.get_server_info.return_value = [expected_info]
        
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_info()
        
        assert result == expected_info
        mock_connection.get_server_info.assert_called_once()
    
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
    def test_get_server_info_empty_result(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                         mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                         mock_server_config, mock_advanced, mock_index, mock_disk,
                                         mock_connection, mock_config):
        """Test server info retrieval with empty result"""
        mock_connection.get_server_info.return_value = []
        
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_info()
        
        assert result == {'error': 'Could not retrieve server information'}
    
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
    def test_get_server_info_exception(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                      mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                      mock_server_config, mock_advanced, mock_index, mock_disk,
                                      mock_connection, mock_config):
        """Test server info retrieval with exception"""
        mock_connection.get_server_info.side_effect = Exception("Connection error")
        
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        result = analyzer._get_server_info()
        
        assert result == {'error': 'Connection error'}
    
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
    def test_get_wait_recommendation_known_types(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                                mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                                mock_server_config, mock_advanced, mock_index, mock_disk,
                                                mock_connection, mock_config):
        """Test wait type recommendation mapping for known types"""
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
    def test_get_wait_recommendation_unknown_type(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                                 mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                                 mock_server_config, mock_advanced, mock_index, mock_disk,
                                                 mock_connection, mock_config):
        """Test wait type recommendation for unknown wait type"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        result = analyzer._get_wait_recommendation('UNKNOWN_WAIT_TYPE')
        assert 'documentation' in result.lower()
    
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
    def test_update_metadata_success(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                    mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                    mock_server_config, mock_advanced, mock_index, mock_disk,
                                    mock_connection, mock_config):
        """Test successful metadata update"""
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
    def test_update_metadata_no_database_info(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                             mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                             mock_server_config, mock_advanced, mock_index, mock_disk,
                                             mock_connection, mock_config):
        """Test metadata update with no database info"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Set up analysis results without database info
        analyzer.analysis_results = {
            'analysis_metadata': {}
        }
        
        analyzer._update_metadata()
        
        # Verify metadata defaults
        assert analyzer.analysis_results['analysis_metadata']['databases_count'] == 0
    
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
    def test_generate_summary_healthy_system(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                            mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                            mock_server_config, mock_advanced, mock_index, mock_disk,
                                            mock_connection, mock_config):
        """Test summary generation for healthy system"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Set up analysis results with no issues
        analyzer.analysis_results = {
            'server_database_info': {
                'data': {
                    'database_overview': [{'name': 'db1'}],
                    'server_configuration': [
                        {
                            'name': 'max degree of parallelism',
                            'value': 4,
                            'best_practice_status': 'OK'
                        }
                    ]
                }
            },
            'wait_stats': {
                'data': {
                    'high_waits': []
                }
            },
            'disk_performance': {
                'data': {
                    'slow_disks': []
                }
            },
            'index_analysis': {
                'data': {
                    'fragmented_indexes': []
                }
            },
            'missing_indexes': {
                'data': {
                    'high_impact_indexes': []
                }
            }
        }
        
        summary = analyzer._generate_summary()
        
        # Verify healthy system summary
        assert len(summary['critical_issues']) == 0
        assert len(summary['warnings']) == 0
        assert summary['total_databases'] == 1
        assert summary['total_issues'] == 0
        assert summary['overall_health_score'] == 100
    
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
    def test_generate_summary_with_critical_issues(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                                  mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                                  mock_server_config, mock_advanced, mock_index, mock_disk,
                                                  mock_connection, mock_config):
        """Test summary generation with critical issues"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Set up analysis results with critical issues
        analyzer.analysis_results = {
            'server_database_info': {
                'data': {
                    'database_overview': [{'name': 'db1'}, {'name': 'db2'}],
                    'server_configuration': [
                        {
                            'name': 'max degree of parallelism',
                            'value': 0,
                            'best_practice_status': 'CRITICAL: MAXDOP should not be 0'
                        }
                    ]
                }
            },
            'wait_stats': {
                'data': {
                    'high_waits': [
                        {'wait_type': 'PAGEIOLATCH_SH', 'wait_percentage': 25.5},
                        {'wait_type': 'WRITELOG', 'wait_percentage': 15.2}
                    ]
                }
            },
            'disk_performance': {
                'data': {
                    'slow_disks': [
                        {'drive': 'C:', 'avg_latency': 50}
                    ]
                }
            },
            'index_analysis': {
                'data': {
                    'fragmented_indexes': [{'name': f'IX_{i}'} for i in range(25)]
                }
            },
            'missing_indexes': {
                'data': {
                    'high_impact_indexes': [{'impact': f'idx_{i}'} for i in range(8)]
                }
            }
        }
        
        summary = analyzer._generate_summary()
        
        # Verify summary structure
        assert 'critical_issues' in summary
        assert 'warnings' in summary
        assert 'total_databases' in summary
        assert 'total_issues' in summary
        assert 'overall_health_score' in summary
        
        # Verify issues were detected
        assert len(summary['critical_issues']) > 0
        assert len(summary['warnings']) > 0
        assert summary['total_databases'] == 2
        assert summary['overall_health_score'] < 100
        assert summary['total_issues'] > 0
    
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
    def test_generate_recommendations_with_wait_stats(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                                     mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                                     mock_server_config, mock_advanced, mock_index, mock_disk,
                                                     mock_connection, mock_config):
        """Test recommendation generation with wait stats"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Set up analysis results with wait stats
        analyzer.analysis_results = {
            'wait_stats': {
                'data': {
                    'high_waits': [
                        {'wait_type': 'PAGEIOLATCH_SH', 'wait_percentage': 25.5},
                        {'wait_type': 'LCK_M_X', 'wait_percentage': 5.2}  # Below 10% threshold
                    ]
                }
            },
            'missing_indexes': {
                'data': {
                    'high_impact_indexes': [
                        {
                            'table_name': 'Orders',
                            'column_names': 'CustomerID',
                            'equality_columns': 'CustomerID',
                            'inequality_columns': '',
                            'avg_user_impact': 85.5
                        }
                    ]
                }
            },
            'server_database_info': {
                'data': {
                    'server_configuration': [
                        {
                            'name': 'max degree of parallelism',
                            'value_in_use': 0,
                            'best_practice_status': 'CRITICAL: MAXDOP should not be 0'
                        }
                    ]
                }
            },
            'server_config': {
                'data': {
                    'issues': [
                        {
                            'description': 'TempDB file count issue',
                            'recommendation': 'Add more TempDB files',
                            'severity': 'MEDIUM',
                            'impact': 'Performance improvement'
                        }
                    ]
                }
            }
        }
        
        recommendations = analyzer._generate_recommendations()
        
        # Verify recommendations structure and content
        assert len(recommendations) >= 4
        
        # Check that recommendations are sorted by priority
        priorities = [rec['priority'] for rec in recommendations]
        priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        assert priorities == sorted(priorities, key=lambda x: priority_order.get(x, 3))
        
        # Verify specific recommendation types exist
        rec_categories = [rec['category'] for rec in recommendations]
        assert 'Wait Stats' in rec_categories
        assert 'Missing Indexes' in rec_categories
        assert 'Server Configuration' in rec_categories
    
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
    def test_generate_recommendations_with_exception(self, mock_intelligent, mock_ai, mock_log, mock_server_db,
                                                    mock_missing, mock_wait, mock_plan, mock_tempdb, 
                                                    mock_server_config, mock_advanced, mock_index, mock_disk,
                                                    mock_connection, mock_config):
        """Test recommendation generation with exception"""
        analyzer = PerformanceAnalyzer(mock_connection, mock_config)
        
        # Set up malformed analysis results
        analyzer.analysis_results = {
            'wait_stats': 'invalid_data'  # This will cause error
        }
        
        recommendations = analyzer._generate_recommendations()
        
        # Verify error handling
        assert len(recommendations) == 1
        assert recommendations[0]['priority'] == 'ERROR'
        assert 'Error generating recommendations' in recommendations[0]['issue']