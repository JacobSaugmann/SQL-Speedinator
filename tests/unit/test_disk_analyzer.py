"""
Simple unit tests for DiskAnalyzer class focusing on testable methods
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import logging

from src.analyzers.disk_analyzer import DiskAnalyzer


class TestDiskAnalyzer:
    """Test class for DiskAnalyzer functionality"""
    
    @pytest.fixture
    def mock_connection(self):
        """Mock SQL connection"""
        connection = Mock()
        connection.execute_query.return_value = [
            {
                'database_name': 'TestDB',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'file_type': 'ROWS',
                'avg_read_latency_ms': 5.2,
                'avg_write_latency_ms': 8.1,
                'drive_letter': 'C'
            }
        ]
        return connection
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration manager"""
        config = Mock()
        config.disk_latency_threshold = 20
        config.io_stall_threshold = 1000
        return config
    
    def test_init_creates_instance_with_proper_attributes(self, mock_connection, mock_config):
        """Test that DiskAnalyzer initializes correctly"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        assert analyzer.connection == mock_connection
        assert analyzer.config == mock_config
        assert isinstance(analyzer.logger, logging.Logger)
    
    def test_analyze_returns_structure_on_success(self, mock_connection, mock_config):
        """Test that analyze method returns expected structure"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        # Mock all the internal methods to return simple data
        analyzer._get_sql_disk_stats = Mock(return_value=[{'database_name': 'TestDB'}])
        analyzer._get_os_disk_stats = Mock(return_value={'C': {'free_space': 100}})
        analyzer._get_database_file_stats = Mock(return_value=[{'file_name': 'test.mdf'}])
        analyzer._analyze_disk_formatting = Mock(return_value={'C': {'block_size': 4096}})
        analyzer._analyze_tempdb_placement = Mock(return_value={'status': 'good'})
        analyzer._analyze_drive_configuration = Mock(return_value={'drives': 1})
        analyzer._identify_io_bottlenecks = Mock(return_value=[])
        analyzer._identify_slow_disks = Mock(return_value=[])
        analyzer._generate_disk_recommendations = Mock(return_value=[])
        
        result = analyzer.analyze()
        
        # Verify structure
        expected_keys = [
            'sql_disk_stats', 'os_disk_stats', 'database_files',
            'disk_formatting', 'tempdb_analysis', 'drive_configuration',
            'io_bottlenecks', 'slow_disks', 'recommendations'
        ]
        
        for key in expected_keys:
            assert key in result
    
    def test_analyze_handles_exception(self, mock_connection, mock_config):
        """Test that analyze method handles exceptions gracefully"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        # Force an exception in one of the methods
        analyzer._get_sql_disk_stats = Mock(side_effect=Exception("Database error"))
        
        result = analyzer.analyze()
        
        assert 'error' in result
        assert 'Database error' in result['error']
    
    def test_get_sql_disk_stats_success(self, mock_connection, mock_config):
        """Test successful SQL disk stats retrieval"""
        expected_data = [
            {
                'database_name': 'TestDB',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'file_type': 'ROWS',
                'avg_read_latency_ms': 5.2
            }
        ]
        mock_connection.execute_query.return_value = expected_data
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_sql_disk_stats()
        
        assert result == expected_data
        mock_connection.execute_query.assert_called_once()
    
    def test_get_sql_disk_stats_empty_result(self, mock_connection, mock_config):
        """Test SQL disk stats with empty result"""
        mock_connection.execute_query.return_value = []
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_sql_disk_stats()
        
        assert result == []
    
    def test_get_sql_disk_stats_exception(self, mock_connection, mock_config):
        """Test SQL disk stats with exception"""
        mock_connection.execute_query.side_effect = Exception("Query failed")
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_sql_disk_stats()
        
        assert result is None
    
    @patch('src.analyzers.disk_analyzer.psutil')
    def test_get_os_disk_stats_success(self, mock_psutil, mock_connection, mock_config):
        """Test successful OS disk stats retrieval"""
        # Mock psutil disk usage
        mock_usage = Mock()
        mock_usage.total = 1000000000000  # 1TB
        mock_usage.used = 500000000000   # 500GB
        mock_usage.free = 500000000000   # 500GB
        mock_usage.percent = 50.0
        
        mock_psutil.disk_usage.return_value = mock_usage
        mock_psutil.disk_partitions.return_value = [
            Mock(device='C:\\', mountpoint='C:\\', fstype='NTFS')
        ]
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_os_disk_stats()
        
        assert 'C' in result
        assert result['C']['total_gb'] == 931.32  # Roughly 1TB in GB
        assert result['C']['free_gb'] == 465.66   # Roughly 500GB in GB
        assert result['C']['used_percent'] == 50.0
        assert result['C']['filesystem'] == 'NTFS'
    
    @patch('src.analyzers.disk_analyzer.psutil')
    def test_get_os_disk_stats_exception(self, mock_psutil, mock_connection, mock_config):
        """Test OS disk stats with exception"""
        mock_psutil.disk_partitions.side_effect = Exception("OS Error")
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_os_disk_stats()
        
        assert result == {}
    
    def test_get_database_file_stats_success(self, mock_connection, mock_config):
        """Test successful database file stats retrieval"""
        expected_data = [
            {
                'database_name': 'TestDB',
                'file_name': 'TestDB_Data',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'size_mb': 1024
            }
        ]
        mock_connection.execute_query.return_value = expected_data
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_database_file_stats()
        
        assert result == expected_data
    
    def test_get_database_file_stats_exception(self, mock_connection, mock_config):
        """Test database file stats with exception"""
        mock_connection.execute_query.side_effect = Exception("Database error")
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._get_database_file_stats()
        
        assert result == []
    
    @patch('src.analyzers.disk_analyzer.subprocess.run')
    def test_analyze_disk_formatting_success(self, mock_subprocess, mock_connection, mock_config):
        """Test successful disk formatting analysis"""
        # Mock fsutil output
        mock_result = Mock()
        mock_result.stdout = "Bytes Per Sector  : 512\nSectors Per Allocation Unit  : 8\n"
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._analyze_disk_formatting()
        
        assert 'C' in result
        assert result['C']['block_size'] == 4096  # 512 * 8
        assert result['C']['sector_size'] == 512
    
    @patch('src.analyzers.disk_analyzer.subprocess.run')
    def test_analyze_disk_formatting_exception(self, mock_subprocess, mock_connection, mock_config):
        """Test disk formatting analysis with exception"""
        mock_subprocess.side_effect = Exception("Command failed")
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._analyze_disk_formatting()
        
        assert result == {}
    
    def test_analyze_tempdb_placement_success(self, mock_connection, mock_config):
        """Test successful tempdb placement analysis"""
        tempdb_data = [
            {
                'file_name': 'tempdev',
                'physical_name': 'C:\\TempDB\\tempdb.mdf',
                'drive_letter': 'C',
                'file_type': 'ROWS'
            },
            {
                'file_name': 'templog',
                'physical_name': 'C:\\TempDB\\templog.ldf',
                'drive_letter': 'C', 
                'file_type': 'LOG'
            }
        ]
        mock_connection.execute_query.return_value = tempdb_data
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._analyze_tempdb_placement()
        
        assert 'data_files' in result
        assert 'log_files' in result
        assert 'drive_separation' in result
        assert result['drive_separation'] is False  # Both on C drive
    
    def test_analyze_tempdb_placement_exception(self, mock_connection, mock_config):
        """Test tempdb placement analysis with exception"""
        mock_connection.execute_query.side_effect = Exception("Query failed")
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        result = analyzer._analyze_tempdb_placement()
        
        assert result == {}
    
    def test_identify_io_bottlenecks_with_slow_disk(self, mock_connection, mock_config):
        """Test IO bottleneck identification with slow disk"""
        slow_disk_data = [
            {
                'database_name': 'TestDB',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'avg_read_latency_ms': 25.0,  # Above threshold
                'avg_write_latency_ms': 30.0,  # Above threshold
                'drive_letter': 'C'
            }
        ]
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        analyzer._get_sql_disk_stats = Mock(return_value=slow_disk_data)
        
        result = analyzer._identify_io_bottlenecks()
        
        assert len(result) > 0
        assert result[0]['database_name'] == 'TestDB'
        assert result[0]['issue'] == 'High I/O latency detected'
    
    def test_identify_io_bottlenecks_no_issues(self, mock_connection, mock_config):
        """Test IO bottleneck identification with no issues"""
        good_disk_data = [
            {
                'database_name': 'TestDB',
                'physical_name': 'C:\\Data\\TestDB.mdf',
                'avg_read_latency_ms': 5.0,   # Below threshold
                'avg_write_latency_ms': 8.0,  # Below threshold
                'drive_letter': 'C'
            }
        ]
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        analyzer._get_sql_disk_stats = Mock(return_value=good_disk_data)
        
        result = analyzer._identify_io_bottlenecks()
        
        assert len(result) == 0
    
    def test_identify_slow_disks_with_latency_issues(self, mock_connection, mock_config):
        """Test slow disk identification with latency issues"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        analyzer._get_sql_disk_stats = Mock(return_value=[
            {
                'drive_letter': 'C',
                'avg_read_latency_ms': 25.0,
                'avg_write_latency_ms': 30.0,
                'database_name': 'TestDB'
            }
        ])
        
        result = analyzer._identify_slow_disks()
        
        assert len(result) > 0
        assert result[0]['drive'] == 'C'
        assert result[0]['avg_read_latency'] == 25.0
        assert result[0]['avg_write_latency'] == 30.0
    
    def test_identify_slow_disks_no_issues(self, mock_connection, mock_config):
        """Test slow disk identification with no issues"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        analyzer._get_sql_disk_stats = Mock(return_value=[
            {
                'drive_letter': 'D',
                'avg_read_latency_ms': 5.0,
                'avg_write_latency_ms': 8.0,
                'database_name': 'TestDB'
            }
        ])
        
        result = analyzer._identify_slow_disks()
        
        assert len(result) == 0
    
    def test_generate_disk_recommendations_with_issues(self, mock_connection, mock_config):
        """Test disk recommendations generation with issues"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        # Mock methods to return issues
        analyzer._identify_slow_disks = Mock(return_value=[
            {'drive': 'C', 'avg_read_latency': 25.0, 'avg_write_latency': 30.0}
        ])
        analyzer._analyze_tempdb_placement = Mock(return_value={
            'drive_separation': False
        })
        analyzer._analyze_disk_formatting = Mock(return_value={
            'C': {'block_size': 512}  # Small block size
        })
        
        result = analyzer._generate_disk_recommendations()
        
        assert len(result) > 0
        # Should have recommendations for slow disk, tempdb placement, and block size
        recommendation_types = [rec['category'] for rec in result]
        assert 'Disk Performance' in recommendation_types
    
    def test_generate_disk_recommendations_no_issues(self, mock_connection, mock_config):
        """Test disk recommendations generation with no issues"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        # Mock methods to return no issues
        analyzer._identify_slow_disks = Mock(return_value=[])
        analyzer._analyze_tempdb_placement = Mock(return_value={
            'drive_separation': True
        })
        analyzer._analyze_disk_formatting = Mock(return_value={
            'C': {'block_size': 65536}  # Good block size
        })
        
        result = analyzer._generate_disk_recommendations()
        
        # Should be empty or have very few recommendations
        assert len(result) <= 1  # Maybe one general recommendation
    
    def test_generate_disk_recommendations_exception(self, mock_connection, mock_config):
        """Test disk recommendations generation with exception"""
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        
        # Force exception in one of the dependency methods
        analyzer._identify_slow_disks = Mock(side_effect=Exception("Analysis failed"))
        
        result = analyzer._generate_disk_recommendations()
        
        # Should handle exception gracefully
        assert isinstance(result, list)
    
    def test_analyze_drive_configuration_with_multiple_drives(self, mock_connection, mock_config):
        """Test drive configuration analysis with multiple drives"""
        file_data = [
            {'physical_name': 'C:\\Data\\TestDB.mdf', 'file_type': 'ROWS'},
            {'physical_name': 'D:\\Logs\\TestDB.ldf', 'file_type': 'LOG'},
            {'physical_name': 'E:\\TempDB\\tempdb.mdf', 'file_type': 'ROWS'}
        ]
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        analyzer._get_database_file_stats = Mock(return_value=file_data)
        
        result = analyzer._analyze_drive_configuration()
        
        assert 'drives_used' in result
        assert result['drives_used'] == 3
        assert 'drive_distribution' in result
        assert 'C' in result['drive_distribution']
        assert 'D' in result['drive_distribution']
        assert 'E' in result['drive_distribution']
    
    def test_analyze_drive_configuration_single_drive(self, mock_connection, mock_config):
        """Test drive configuration analysis with single drive"""
        file_data = [
            {'physical_name': 'C:\\Data\\TestDB.mdf', 'file_type': 'ROWS'},
            {'physical_name': 'C:\\Data\\TestDB.ldf', 'file_type': 'LOG'}
        ]
        
        analyzer = DiskAnalyzer(mock_connection, mock_config)
        analyzer._get_database_file_stats = Mock(return_value=file_data)
        
        result = analyzer._analyze_drive_configuration()
        
        assert result['drives_used'] == 1
        assert len(result['drive_distribution']) == 1
        assert 'C' in result['drive_distribution']
        assert result['drive_distribution']['C'] == 2  # 2 files on C drive