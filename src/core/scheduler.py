"""
Analysis Scheduler
Handles scheduled execution of SQL Server performance analysis
"""

import logging
import schedule
import time
import threading
from datetime import datetime, time as dt_time
from pathlib import Path

class AnalysisScheduler:
    """Manages scheduled analysis execution"""
    
    def __init__(self, config):
        """Initialize scheduler
        
        Args:
            config: ConfigManager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.scheduler_thread = None
    
    def start_scheduled_analysis(self, server_name, output_path, night_mode=True):
        """Start scheduled analysis
        
        Args:
            server_name (str): SQL Server instance name
            output_path (Path): Output directory for reports
            night_mode (bool): Whether to run in night mode
        """
        if not self.config.schedule_enabled:
            self.logger.warning("Scheduling is disabled in configuration")
            return
        
        self.logger.info("Starting scheduled analysis mode")
        
        # Set up the schedule
        schedule_time = self.config.schedule_time
        schedule_days = self.config.schedule_days
        
        # Convert day numbers to schedule day names
        day_mapping = {
            1: schedule.every().monday,
            2: schedule.every().tuesday,
            3: schedule.every().wednesday,
            4: schedule.every().thursday,
            5: schedule.every().friday,
            6: schedule.every().saturday,
            7: schedule.every().sunday
        }
        
        # Schedule for specified days
        for day_num in schedule_days:
            if day_num in day_mapping:
                day_mapping[day_num].at(schedule_time).do(
                    self._run_scheduled_analysis,
                    server_name,
                    output_path,
                    night_mode
                )
                self.logger.info(f"Scheduled analysis for day {day_num} at {schedule_time}")
        
        # Start the scheduler in a separate thread
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        # Keep the main thread alive
        try:
            while self.running:
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            self.logger.info("Stopping scheduled analysis...")
            self.stop_scheduler()
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.running = False
        schedule.clear()
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        self.logger.info("Scheduler stopped")
    
    def _run_scheduler(self):
        """Internal scheduler loop"""
        while self.running:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
    
    def _run_scheduled_analysis(self, server_name, output_path, night_mode):
        """Execute a scheduled analysis run"""
        from .sql_connection import SQLServerConnection
        from .performance_analyzer import PerformanceAnalyzer
        from ..reports.pdf_report_generator import PDFReportGenerator
        
        try:
            self.logger.info(f"Starting scheduled analysis for {server_name}")
            
            # Create connection and run analysis
            with SQLServerConnection(server_name, self.config) as conn:
                if not conn.test_connection():
                    raise Exception("Failed to connect to SQL Server")
                
                # Run analysis
                analyzer = PerformanceAnalyzer(conn, self.config, night_mode)
                analysis_results = analyzer.run_full_analysis()
                
                # Generate report
                report_generator = PDFReportGenerator(self.config)
                report_path = report_generator.generate_report(
                    analysis_results,
                    output_path,
                    server_name
                )
                
                self.logger.info(f"Scheduled analysis completed. Report: {report_path}")
                
        except Exception as e:
            self.logger.error(f"Scheduled analysis failed: {e}", exc_info=True)