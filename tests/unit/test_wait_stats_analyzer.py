"""
Unit tests for Wait Stats Analyzer
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.analyzers.wait_stats_analyzer import WaitStatsAnalyzer


class TestWaitStatsAnalyzer:
    """Test cases for WaitStatsAnalyzer class"""

    def test_init(self, mock_sql_connection, mock_config):
        """Test analyzer initialization"""
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        assert analyzer.connection == mock_sql_connection
        assert analyzer.config == mock_config.analysis
        assert analyzer.logger is not None

    def test_analyze_success(self, mock_sql_connection, mock_config, sample_wait_stats):
        """Test successful complete analysis"""
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        # Mock all the individual analysis methods
        with patch.object(analyzer, '_get_current_waits', return_value=sample_wait_stats):
            with patch.object(analyzer, '_get_wait_history', return_value=[]):
                with patch.object(analyzer, '_identify_problematic_waits', return_value=[]):
                    with patch.object(analyzer, '_analyze_wait_patterns', return_value={}):
                        with patch.object(analyzer, '_generate_wait_recommendations', return_value=[]):
                            
                            result = analyzer.analyze()
                            
                            assert 'current_waits' in result
                            assert 'wait_history' in result
                            assert 'high_waits' in result
                            assert 'wait_analysis' in result
                            assert 'recommendations' in result
                            assert result['current_waits'] == sample_wait_stats

    def test_analyze_failure(self, mock_sql_connection, mock_config):
        """Test analysis failure handling"""
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        # Mock method to raise exception
        with patch.object(analyzer, '_get_current_waits', side_effect=Exception("Test error")):
            result = analyzer.analyze()
            
            assert 'error' in result
            assert "Test error" in result['error']

    def test_get_current_waits_success(self, mock_sql_connection, mock_config):
        """Test successful current waits retrieval"""
        mock_data = [
            {
                'wait_type': 'CXPACKET',
                'wait_time_ms': 1500000,
                'wait_percentage': 45.2,
                'waiting_tasks_count': 50,
                'signal_wait_time_ms': 150000,
                'max_wait_time_ms': 120000,
                'severity': 'HIGH'
            },
            {
                'wait_type': 'PAGEIOLATCH_SH',
                'wait_time_ms': 800000,
                'wait_percentage': 24.1,
                'waiting_tasks_count': 25,
                'signal_wait_time_ms': 80000,
                'max_wait_time_ms': 85000,
                'severity': 'HIGH'
            }
        ]
        mock_sql_connection.execute_query.return_value = mock_data
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_current_waits()
        
        assert result == mock_data
        mock_sql_connection.execute_query.assert_called_once()

    def test_get_current_waits_empty(self, mock_sql_connection, mock_config):
        """Test current waits retrieval when none found"""
        mock_sql_connection.execute_query.return_value = []
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_current_waits()
        
        assert result == []

    def test_get_current_waits_failure(self, mock_sql_connection, mock_config):
        """Test current waits retrieval failure"""
        mock_sql_connection.execute_query.return_value = None
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_current_waits()
        
        assert result is None

    def test_get_wait_history_success(self, mock_sql_connection, mock_config):
        """Test successful wait history retrieval"""
        mock_data = [
            {
                'session_id': 52,
                'wait_type': 'LCK_M_S',
                'wait_time': 45000,
                'wait_resource': 'RID: 5:1:104:0',
                'blocking_session_id': 63
            }
        ]
        mock_sql_connection.execute_query.return_value = mock_data
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_wait_history()
        
        assert result == mock_data

    def test_identify_problematic_waits_high_io(self, mock_sql_connection, mock_config):
        """Test identification of problematic I/O waits"""
        mock_waits = [
            {
                'wait_type': 'PAGEIOLATCH_SH',
                'wait_time_ms': 1500000,
                'wait_percentage': 55.2,
                'waiting_tasks_count': 50
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._identify_problematic_waits()
            
            assert len(result) == 1
            assert result[0]['wait_type'] == 'PAGEIOLATCH_SH'
            assert result[0]['category'] == 'Disk I/O'
            assert 'memory' in result[0]['likely_cause'].lower()

    def test_identify_problematic_waits_locking(self, mock_sql_connection, mock_config):
        """Test identification of problematic locking waits"""
        mock_waits = [
            {
                'wait_type': 'LCK_M_X',
                'wait_time_ms': 800000,
                'wait_percentage': 35.0,
                'waiting_tasks_count': 25
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._identify_problematic_waits()
            
            assert len(result) == 1
            assert result[0]['wait_type'] == 'LCK_M_X'
            assert result[0]['category'] == 'Locking'

    def test_identify_problematic_waits_below_threshold(self, mock_sql_connection, mock_config):
        """Test that low percentage waits are not identified as problematic"""
        mock_waits = [
            {
                'wait_type': 'PAGEIOLATCH_SH',
                'wait_time_ms': 50000,
                'wait_percentage': 1.5,  # Below 2% threshold
                'waiting_tasks_count': 5
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._identify_problematic_waits()
            
            assert len(result) == 0

    def test_analyze_wait_patterns_io_bottleneck(self, mock_sql_connection, mock_config):
        """Test wait pattern analysis for I/O bottlenecks"""
        mock_waits = [
            {
                'wait_type': 'PAGEIOLATCH_SH',
                'wait_time_ms': 2000000,
                'wait_percentage': 60.0,
                'waiting_tasks_count': 100
            },
            {
                'wait_type': 'WRITELOG',
                'wait_time_ms': 500000,
                'wait_percentage': 15.0,
                'waiting_tasks_count': 25
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._analyze_wait_patterns()
            
            assert result['top_wait_category'] == 'I/O Bottleneck'
            assert result['io_waits_percentage'] > 40
            assert 'I/O waits' in result['patterns'][0]

    def test_analyze_wait_patterns_lock_contention(self, mock_sql_connection, mock_config):
        """Test wait pattern analysis for lock contention"""
        mock_waits = [
            {
                'wait_type': 'LCK_M_S',
                'wait_time_ms': 1200000,
                'wait_percentage': 40.0,
                'waiting_tasks_count': 75
            },
            {
                'wait_type': 'LCK_M_X',
                'wait_time_ms': 800000,
                'wait_percentage': 25.0,
                'waiting_tasks_count': 50
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._analyze_wait_patterns()
            
            assert result['top_wait_category'] == 'Locking/Blocking'
            assert result['lock_waits_percentage'] > 30
            assert 'lock waits' in result['patterns'][0]

    def test_analyze_wait_patterns_cpu_pressure(self, mock_sql_connection, mock_config):
        """Test wait pattern analysis for CPU pressure"""
        mock_waits = [
            {
                'wait_type': 'SOS_SCHEDULER_YIELD',
                'wait_time_ms': 1000000,
                'wait_percentage': 35.0,
                'waiting_tasks_count': 100
            },
            {
                'wait_type': 'CXPACKET',
                'wait_time_ms': 600000,
                'wait_percentage': 20.0,
                'waiting_tasks_count': 50
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._analyze_wait_patterns()
            
            assert result['top_wait_category'] == 'CPU Pressure'
            assert result['cpu_waits_percentage'] > 25
            assert 'CPU waits' in result['patterns'][0]

    def test_generate_wait_recommendations_pageiolatch(self, mock_sql_connection, mock_config):
        """Test recommendations for PAGEIOLATCH waits"""
        mock_problematic_waits = [
            {
                'wait_type': 'PAGEIOLATCH_SH',
                'wait_percentage': 15.0,
                'category': 'Disk I/O'
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_identify_problematic_waits', return_value=mock_problematic_waits):
            result = analyzer._generate_wait_recommendations()
            
            assert len(result) == 1
            assert result[0]['priority'] == 'HIGH'
            assert result[0]['wait_type'] == 'PAGEIOLATCH_SH'
            assert 'memory' in str(result[0]['recommendations']).lower()

    def test_generate_wait_recommendations_writelog(self, mock_sql_connection, mock_config):
        """Test recommendations for WRITELOG waits"""
        mock_problematic_waits = [
            {
                'wait_type': 'WRITELOG',
                'wait_percentage': 8.0,
                'category': 'Log I/O'
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_identify_problematic_waits', return_value=mock_problematic_waits):
            result = analyzer._generate_wait_recommendations()
            
            assert len(result) == 1
            assert result[0]['priority'] == 'HIGH'
            assert result[0]['wait_type'] == 'WRITELOG'
            assert 'log' in str(result[0]['recommendations']).lower()

    def test_generate_wait_recommendations_locking(self, mock_sql_connection, mock_config):
        """Test recommendations for locking waits"""
        mock_problematic_waits = [
            {
                'wait_type': 'LCK_M_X',
                'wait_percentage': 12.0,
                'category': 'Locking'
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_identify_problematic_waits', return_value=mock_problematic_waits):
            result = analyzer._generate_wait_recommendations()
            
            assert len(result) == 1
            assert result[0]['priority'] == 'MEDIUM'
            assert result[0]['wait_type'] == 'LCK_M_X'
            assert 'blocking' in str(result[0]['recommendations']).lower()

    def test_generate_wait_recommendations_cxpacket(self, mock_sql_connection, mock_config):
        """Test recommendations for CXPACKET waits"""
        mock_problematic_waits = [
            {
                'wait_type': 'CXPACKET',
                'wait_percentage': 15.0,
                'category': 'Parallelism'
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_identify_problematic_waits', return_value=mock_problematic_waits):
            result = analyzer._generate_wait_recommendations()
            
            assert len(result) == 1
            assert result[0]['priority'] == 'MEDIUM'
            assert result[0]['wait_type'] == 'CXPACKET'
            assert 'MAXDOP' in str(result[0]['recommendations'])

    def test_generate_wait_recommendations_no_issues(self, mock_sql_connection, mock_config):
        """Test recommendation generation with no significant issues"""
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_identify_problematic_waits', return_value=[]):
            result = analyzer._generate_wait_recommendations()
            
            assert len(result) == 0

    @pytest.mark.parametrize("wait_type,category", [
        ("PAGEIOLATCH_SH", "Disk I/O"),
        ("PAGEIOLATCH_EX", "Disk I/O"),
        ("WRITELOG", "Log I/O"),
        ("LCK_M_S", "Locking"),
        ("LCK_M_X", "Locking"),
        ("CXPACKET", "Parallelism"),
        ("SOS_SCHEDULER_YIELD", "CPU"),
        ("THREADPOOL", "Threading")
    ])
    def test_wait_type_categorization(self, mock_sql_connection, mock_config, wait_type, category):
        """Test proper categorization of different wait types"""
        mock_waits = [
            {
                'wait_type': wait_type,
                'wait_time_ms': 1000000,
                'wait_percentage': 10.0,
                'waiting_tasks_count': 50
            }
        ]
        
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=mock_waits):
            result = analyzer._identify_problematic_waits()
            
            if result:
                assert result[0]['category'] == category

    def test_error_handling_no_waits(self, mock_sql_connection, mock_config):
        """Test error handling when no waits are returned"""
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=None):
            problematic = analyzer._identify_problematic_waits()
            patterns = analyzer._analyze_wait_patterns()
            
            assert problematic == []
            assert patterns == {}

    def test_error_handling_empty_waits(self, mock_sql_connection, mock_config):
        """Test error handling when empty waits list is returned"""
        analyzer = WaitStatsAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_current_waits', return_value=[]):
            problematic = analyzer._identify_problematic_waits()
            patterns = analyzer._analyze_wait_patterns()
            
            assert problematic == []
            assert patterns == {}