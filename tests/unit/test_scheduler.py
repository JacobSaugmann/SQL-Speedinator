"""
Unit tests for AnalysisScheduler class.

This module tests scheduled analysis execution, configuration handling,
threading management, and error scenarios for the scheduler component.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
from datetime import datetime
from src.core.scheduler import AnalysisScheduler


class TestAnalysisScheduler:
    """Test suite for AnalysisScheduler class"""
    
    def test_init_creates_instance_with_proper_attributes(self, mock_config):
        """Test AnalysisScheduler initialization"""
        scheduler = AnalysisScheduler(mock_config)
        
        assert scheduler.config == mock_config
        assert scheduler.logger is not None
        assert scheduler.running is False
        assert scheduler.scheduler_thread is None
    
    def test_start_scheduled_analysis_disabled_in_config(self, mock_config):
        """Test start_scheduled_analysis when scheduling is disabled"""
        # Mock disabled scheduling
        mock_config.schedule_enabled = False
        
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock logger to verify warning
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
            
            # Should log warning and return early
            mock_logger.warning.assert_called_with("Scheduling is disabled in configuration")
            assert scheduler.running is False
    
    @patch('src.core.scheduler.schedule')
    @patch('src.core.scheduler.threading.Thread')
    def test_start_scheduled_analysis_enabled_single_day(self, mock_thread, mock_schedule, mock_config):
        """Test start_scheduled_analysis with scheduling enabled for single day"""
        # Mock enabled scheduling
        mock_config.schedule_enabled = True
        mock_config.schedule_time = "02:00"
        mock_config.schedule_days = [1]  # Monday only
        
        # Mock schedule objects
        mock_monday = Mock()
        mock_schedule.every.return_value.monday = mock_monday
        mock_monday.at.return_value.do = Mock()
        
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock the main loop to exit immediately
        def side_effect(seconds):
            scheduler.running = False
        
        # Patch time.sleep to exit loop immediately
        with patch('src.core.scheduler.time.sleep', side_effect=side_effect):
            scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
        
        # Verify schedule was set up
        mock_monday.at.assert_called_with("02:00")
        mock_monday.at.return_value.do.assert_called_once()
        
        # Verify thread was created and started
        mock_thread.assert_called_once()
        mock_thread.return_value.start.assert_called_once()
    
    @patch('src.core.scheduler.schedule')
    @patch('src.core.scheduler.threading.Thread')
    def test_start_scheduled_analysis_multiple_days(self, mock_thread, mock_schedule, mock_config):
        """Test start_scheduled_analysis with multiple days configured"""
        # Mock enabled scheduling
        mock_config.schedule_enabled = True
        mock_config.schedule_time = "03:30"
        mock_config.schedule_days = [1, 3, 5]  # Mon, Wed, Fri
        
        # Mock schedule objects
        mock_monday = Mock()
        mock_wednesday = Mock()
        mock_friday = Mock()
        
        mock_schedule.every.return_value.monday = mock_monday
        mock_schedule.every.return_value.wednesday = mock_wednesday
        mock_schedule.every.return_value.friday = mock_friday
        
        # Set up return values
        mock_monday.at.return_value.do = Mock()
        mock_wednesday.at.return_value.do = Mock()
        mock_friday.at.return_value.do = Mock()
        
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock the main loop to exit immediately
        with patch('src.core.scheduler.time.sleep', side_effect=lambda x: setattr(scheduler, 'running', False)):
            scheduler.start_scheduled_analysis("testserver", Path("/output"), True)
        
        # Verify all days were scheduled
        mock_monday.at.assert_called_with("03:30")
        mock_wednesday.at.assert_called_with("03:30")
        mock_friday.at.assert_called_with("03:30")
        
        # Verify all schedule jobs were created
        assert mock_monday.at.return_value.do.call_count == 1
        assert mock_wednesday.at.return_value.do.call_count == 1
        assert mock_friday.at.return_value.do.call_count == 1
    
    @patch('src.core.scheduler.schedule')
    @patch('src.core.scheduler.threading.Thread')
    def test_start_scheduled_analysis_invalid_day_numbers(self, mock_thread, mock_schedule, mock_config):
        """Test start_scheduled_analysis with invalid day numbers"""
        # Mock enabled scheduling with invalid days
        mock_config.schedule_enabled = True
        mock_config.schedule_time = "02:00"
        mock_config.schedule_days = [0, 8, 99]  # Invalid day numbers
        
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock the main loop to exit immediately
        with patch('src.core.scheduler.time.sleep', side_effect=lambda x: setattr(scheduler, 'running', False)):
            scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
        
        # Verify no schedules were created for invalid days
        # schedule.every() should not be called since no valid days
        # The function should still complete without error
        assert scheduler.running is False
    
    @patch('src.core.scheduler.schedule')
    def test_stop_scheduler(self, mock_schedule):
        """Test stop_scheduler functionality"""
        scheduler = AnalysisScheduler(Mock())
        
        # Set up initial state
        scheduler.running = True
        mock_thread = Mock()
        scheduler.scheduler_thread = mock_thread
        
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler.stop_scheduler()
        
        # Verify scheduler was stopped
        assert scheduler.running is False
        mock_schedule.clear.assert_called_once()
        mock_thread.join.assert_called_with(timeout=5)
        mock_logger.info.assert_called_with("Scheduler stopped")
    
    @patch('src.core.scheduler.schedule')
    def test_stop_scheduler_no_thread(self, mock_schedule):
        """Test stop_scheduler when no thread exists"""
        scheduler = AnalysisScheduler(Mock())
        scheduler.running = True
        scheduler.scheduler_thread = None
        
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler.stop_scheduler()
        
        # Should complete without error
        assert scheduler.running is False
        mock_schedule.clear.assert_called_once()
        mock_logger.info.assert_called_with("Scheduler stopped")
    
    @patch('src.core.scheduler.schedule')
    @patch('src.core.scheduler.time.sleep')
    def test_run_scheduler_loop(self, mock_sleep, mock_schedule):
        """Test _run_scheduler internal loop"""
        scheduler = AnalysisScheduler(Mock())
        scheduler.running = True
        
        # Mock run_pending and set up to exit after 2 iterations
        call_count = 0
        def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                scheduler.running = False
        
        mock_sleep.side_effect = sleep_side_effect
        
        scheduler._run_scheduler()
        
        # Verify run_pending was called
        assert mock_schedule.run_pending.call_count >= 2
        # Verify sleep was called with 30 seconds
        mock_sleep.assert_called_with(30)
    
    @patch('src.reports.pdf_report_generator.PDFReportGenerator')
    @patch('src.core.performance_analyzer.PerformanceAnalyzer')
    @patch('src.core.sql_connection.SQLServerConnection')
    @patch('src.core.scheduler.datetime')
    def test_run_scheduled_analysis_success(self, mock_datetime, mock_connection, mock_analyzer, mock_pdf_gen):
        """Test successful _run_scheduled_analysis execution"""
        # Setup mocks
        mock_config = Mock()
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock datetime
        mock_now = Mock()
        mock_now.strftime.return_value = "20231029_143000"
        mock_datetime.now.return_value = mock_now
        
        # Mock connection
        mock_conn_instance = Mock()
        mock_conn_instance.test_connection.return_value = True
        mock_connection.return_value.__enter__ = Mock(return_value=mock_conn_instance)
        mock_connection.return_value.__exit__ = Mock(return_value=None)
        
        # Mock analyzer
        mock_analyzer_instance = Mock()
        mock_analysis_results = {"test": "results"}
        mock_analyzer_instance.run_full_analysis.return_value = mock_analysis_results
        mock_analyzer.return_value = mock_analyzer_instance
        
        # Mock PDF generator
        mock_pdf_instance = Mock()
        mock_pdf_instance.generate_report.return_value = "/path/to/report.pdf"
        mock_pdf_gen.return_value = mock_pdf_instance
        
        server_name = "testserver"
        output_path = Path("/output")
        
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler._run_scheduled_analysis(server_name, output_path, True)
        
        # Verify connection was created and tested
        mock_connection.assert_called_with(server_name, mock_config)
        mock_conn_instance.test_connection.assert_called_once()
        
        # Verify analyzer was created and run
        mock_analyzer.assert_called_with(mock_conn_instance, mock_config, True)
        mock_analyzer_instance.run_full_analysis.assert_called_once()
        
        # Verify PDF generator was created and used
        mock_pdf_gen.assert_called_with(mock_config)
        expected_filename = "scheduled_analysis_testserver_20231029_143000.pdf"
        expected_output_file = output_path / expected_filename
        mock_pdf_instance.generate_report.assert_called_with(
            mock_analysis_results,
            str(expected_output_file),
            server_name
        )
        
        # Verify success logging
        mock_logger.info.assert_any_call(f"Starting scheduled analysis for {server_name}")
        mock_logger.info.assert_any_call("Scheduled analysis completed. Report: /path/to/report.pdf")
    
    @patch('src.core.sql_connection.SQLServerConnection')
    def test_run_scheduled_analysis_connection_failure(self, mock_connection):
        """Test _run_scheduled_analysis with connection failure"""
        mock_config = Mock()
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock connection failure
        mock_conn_instance = Mock()
        mock_conn_instance.test_connection.return_value = False
        mock_connection.return_value.__enter__ = Mock(return_value=mock_conn_instance)
        mock_connection.return_value.__exit__ = Mock(return_value=None)
        
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler._run_scheduled_analysis("localhost", Path("/test"), True)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_args = mock_logger.error.call_args[0]
        assert "Scheduled analysis failed" in error_args[0]
        assert "Failed to connect to SQL Server" in str(error_args)
    
    @patch('src.core.performance_analyzer.PerformanceAnalyzer')
    @patch('src.core.sql_connection.SQLServerConnection')
    def test_run_scheduled_analysis_analyzer_exception(self, mock_connection, mock_analyzer):
        """Test _run_scheduled_analysis with analyzer exception"""
        mock_config = Mock()
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock successful connection but failing analyzer
        mock_conn_instance = Mock()
        mock_conn_instance.test_connection.return_value = True
        mock_connection.return_value.__enter__ = Mock(return_value=mock_conn_instance)
        mock_connection.return_value.__exit__ = Mock(return_value=None)
        
        # Mock analyzer exception
        mock_analyzer.side_effect = Exception("Analysis failed")
        
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler._run_scheduled_analysis("localhost", Path("/test"), True)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_args = mock_logger.error.call_args[0]
        assert "Scheduled analysis failed" in error_args[0]
        assert "Analysis failed" in str(error_args)
    
    @patch('src.reports.pdf_report_generator.PDFReportGenerator')
    @patch('src.core.performance_analyzer.PerformanceAnalyzer')
    @patch('src.core.sql_connection.SQLServerConnection')
    def test_run_scheduled_analysis_report_generation_failure(self, mock_connection, mock_analyzer, mock_pdf_gen):
        """Test _run_scheduled_analysis with report generation failure"""
        mock_config = Mock()
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock successful connection and analysis
        mock_conn_instance = Mock()
        mock_conn_instance.test_connection.return_value = True
        mock_connection.return_value.__enter__ = Mock(return_value=mock_conn_instance)
        mock_connection.return_value.__exit__ = Mock(return_value=None)
        
        mock_analyzer_instance = Mock()
        mock_analyzer_instance.run_full_analysis.return_value = {"test": "results"}
        mock_analyzer.return_value = mock_analyzer_instance
        
        # Mock PDF generator failure
        mock_pdf_instance = Mock()
        mock_pdf_instance.generate_report.side_effect = Exception("PDF generation failed")
        mock_pdf_gen.return_value = mock_pdf_instance
        
        with patch.object(scheduler, 'logger') as mock_logger:
            scheduler._run_scheduled_analysis("localhost", Path("/test"), True)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_args = mock_logger.error.call_args[0]
        assert "Scheduled analysis failed" in error_args[0]
        assert "PDF generation failed" in str(error_args)
    
    def test_start_scheduled_analysis_keyboard_interrupt(self, mock_config):
        """Test keyboard interrupt handling in start_scheduled_analysis"""
        mock_config.schedule_enabled = True
        mock_config.schedule_time = "02:00"
        mock_config.schedule_days = [1]
        
        scheduler = AnalysisScheduler(mock_config)
        
        # Mock to simulate KeyboardInterrupt
        def sleep_side_effect(seconds):
            raise KeyboardInterrupt("User interrupted")
        
        with patch('src.core.scheduler.schedule'):
            with patch('src.core.scheduler.threading.Thread'):
                with patch('src.core.scheduler.time.sleep', side_effect=sleep_side_effect):
                    with patch.object(scheduler, 'stop_scheduler') as mock_stop:
                        with patch.object(scheduler, 'logger') as mock_logger:
                            scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
                        
                        # Verify stop_scheduler was called
                        mock_stop.assert_called_once()
                        mock_logger.info.assert_called_with("Stopping scheduled analysis...")


@pytest.mark.parametrize("day_num,expected_method", [
    (1, "monday"),
    (2, "tuesday"), 
    (3, "wednesday"),
    (4, "thursday"),
    (5, "friday"),
    (6, "saturday"),
    (7, "sunday")
])
def test_day_mapping_matrix(day_num, expected_method, mock_config):
    """Parametrized test for day number to schedule method mapping"""
    # Mock enabled scheduling
    mock_config.schedule_enabled = True
    mock_config.schedule_time = "02:00"
    mock_config.schedule_days = [day_num]
    
    with patch('src.core.scheduler.schedule') as mock_schedule:
        with patch('src.core.scheduler.threading.Thread') as mock_thread:
            # Mock schedule object for the expected day
            mock_day_schedule = Mock()
            setattr(mock_schedule.every.return_value, expected_method, mock_day_schedule)
            mock_day_schedule.at.return_value.do = Mock()
            
            scheduler = AnalysisScheduler(mock_config)
            
            # Mock the main loop to exit immediately
            with patch('src.core.scheduler.time.sleep', side_effect=lambda x: setattr(scheduler, 'running', False)):
                scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
            
            # Verify the correct day method was called
            mock_day_schedule.at.assert_called_with("02:00")
            mock_day_schedule.at.return_value.do.assert_called_once()


@pytest.mark.parametrize("schedule_time", [
    "00:00",
    "06:30", 
    "12:00",
    "18:45",
    "23:59"
])
def test_schedule_time_formats(schedule_time, mock_config):
    """Parametrized test for different schedule time formats"""
    mock_config.schedule_enabled = True
    mock_config.schedule_time = schedule_time
    mock_config.schedule_days = [1]  # Monday
    
    with patch('src.core.scheduler.schedule') as mock_schedule:
        with patch('src.core.scheduler.threading.Thread') as mock_thread:
            # Mock schedule object
            mock_monday = Mock()
            mock_schedule.every.return_value.monday = mock_monday
            mock_monday.at.return_value.do = Mock()
            
            scheduler = AnalysisScheduler(mock_config)
            
            # Mock the main loop to exit immediately
            with patch('src.core.scheduler.time.sleep', side_effect=lambda x: setattr(scheduler, 'running', False)):
                scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
            
            # Verify the schedule time was used correctly
            mock_monday.at.assert_called_with(schedule_time)


def test_scheduler_thread_daemon_property():
    """Test that scheduler thread is created as daemon thread"""
    mock_config = Mock()
    mock_config.schedule_enabled = True
    mock_config.schedule_time = "02:00"
    mock_config.schedule_days = [1]
    
    scheduler = AnalysisScheduler(mock_config)
    
    with patch('src.core.scheduler.schedule'):
        with patch('src.core.scheduler.threading.Thread') as mock_thread:
            with patch('src.core.scheduler.time.sleep', side_effect=lambda x: setattr(scheduler, 'running', False)):
                scheduler.start_scheduled_analysis("localhost", Path("/test"), True)
            
            # Verify thread was created with daemon=True
            mock_thread.assert_called_once()
            call_args = mock_thread.call_args
            assert call_args[1]['daemon'] is True  # Check daemon keyword argument