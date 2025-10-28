#!/usr/bin/env python3
"""
SQL Speedinator
Main entry point for the application

Inspired by the great ones and SQL Server community best practices.

Author: JSA
Date: October 2025
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.core.sql_connection import SQLServerConnection
from src.core.performance_analyzer import PerformanceAnalyzer
from src.reports.pdf_report_generator import PDFReportGenerator
from src.core.config_manager import ConfigManager
from src.core.scheduler import AnalysisScheduler
from src.core.file_cleanup_manager import FileCleanupManager

# Global variable to track verbose mode
VERBOSE_MODE = False

def setup_logging(night_mode=False, verbose=False):
    """Setup logging configuration"""
    global VERBOSE_MODE
    VERBOSE_MODE = verbose
    
    if verbose:
        log_level = logging.DEBUG if not night_mode else logging.WARNING
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        console_handler = logging.StreamHandler(sys.stdout)
        # Set root logger to DEBUG to capture all debug messages
        root_log_level = logging.DEBUG
    else:
        # Simplified output for normal mode - only show ERROR and above
        log_level = logging.ERROR if not night_mode else logging.ERROR
        log_format = '%(message)s'  # Simple format without timestamps
        console_handler = logging.StreamHandler(sys.stdout)
        root_log_level = logging.INFO
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # File handler always gets detailed format
    file_handler = logging.FileHandler(logs_dir / f"sql_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Console handler respects verbose setting
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(root_log_level)
    root_logger.handlers.clear()  # Clear any existing handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="SQL Speedinator - Lightning Fast SQL Server Performance Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py -s SQLSERVER01
  python main.py -s SQLSERVER01 -n --output ./custom_reports --verbose
  python main.py -s SQLSERVER01 --schedule
  python main.py -s SQLSERVER01 --perfmon-file "C:\\PerfLogs\\sql_perf.blg"
  python main.py -s SQLSERVER01 --perfmon-duration 120 --ai-analysis --verbose
  python main.py -s SQLSERVER01 --cleanup-retention-days 7
  python main.py -s SQLSERVER01 --cleanup-preview
  python main.py -s SQLSERVER01 --disable-cleanup
        """
    )
    
    parser.add_argument(
        "-s", "--server",
        required=True,
        help="SQL Server instance name or IP address"
    )
    
    parser.add_argument(
        "-n", "--night-mode",
        action="store_true",
        help="Run in night mode with minimal performance impact"
    )
    
    parser.add_argument(
        "--output",
        default="./reports",
        help="Output directory for reports (default: ./reports)"
    )
    
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run in scheduled mode (continuous)"
    )
    
    parser.add_argument(
        "--config",
        default=".env",
        help="Configuration file path (default: .env)"
    )
    
    parser.add_argument(
        "--ai-analysis",
        action="store_true",
        help="Run enhanced AI-powered analysis with advanced recommendations"
    )
    
    parser.add_argument(
        "--perfmon-file",
        help="Path to Performance Monitor log file (.blg) for additional analysis"
    )
    
    parser.add_argument(
        "--perfmon-duration",
        type=float,
        default=0.5,  # 30 seconds (0.5 minutes)
        help="Duration in minutes for PerfMon data collection (default: 0.5 minutes = 30 seconds)"
    )
    
    parser.add_argument(
        "--cleanup-retention-days",
        type=int,
        default=5,
        help="Number of days to keep old files (default: 5 days)"
    )
    
    parser.add_argument(
        "--disable-cleanup",
        action="store_true",
        help="Disable automatic file cleanup"
    )
    
    parser.add_argument(
        "--cleanup-preview",
        action="store_true",
        help="Show what files would be cleaned up without actually deleting them"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output with detailed logging"
    )

    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.night_mode, args.verbose)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = ConfigManager(args.config)
        
        # Perform automatic file cleanup before starting analysis
        if not args.disable_cleanup:
            logger.info("Performing automatic file cleanup...")
            if not VERBOSE_MODE:
                print("🧹 Performing file cleanup...")
            cleanup_manager = FileCleanupManager(config)
            
            # Configure cleanup settings based on command line args
            cleanup_manager.configure_cleanup(
                retention_days=args.cleanup_retention_days,
                dry_run=args.cleanup_preview
            )
            
            if args.cleanup_preview:
                logger.info("Preview mode: showing what would be cleaned up...")
                cleanup_results = cleanup_manager.get_cleanup_preview()
            else:
                cleanup_results = cleanup_manager.cleanup_old_files()
        else:
            logger.info("🚫 File cleanup disabled by command line flag")
        
        # Create output directory
        output_path = Path(args.output)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if args.schedule:
            # Run in scheduled mode
            logger.info("Starting scheduled analysis mode")
            scheduler = AnalysisScheduler(config)
            scheduler.start_scheduled_analysis(args.server, output_path, args.night_mode)
        else:
            # Run single analysis
            logger.info(f"Starting SQL Server analysis for: {args.server}")
            if not VERBOSE_MODE:
                print(f"🚀 Starting SQL Server analysis for: {args.server}")
            run_analysis(args.server, output_path, config, args.night_mode, args.ai_analysis, args.perfmon_file, args.perfmon_duration)
            
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        return 1
    
    return 0

def run_analysis(server_name, output_path, config, night_mode=False, ai_analysis=False, perfmon_file=None, perfmon_duration=240):
    """Run a single analysis"""
    logger = logging.getLogger(__name__)
    
    def simple_print(message):
        """Print simple message only in non-verbose mode"""
        if not VERBOSE_MODE:
            print(message)
    
    # Enable AI analysis if requested via command line
    if ai_analysis:
        # Temporarily override the AI_ANALYSIS_ENABLED setting
        import os
        os.environ['AI_ANALYSIS_ENABLED'] = 'true'
        logger.info("AI analysis enabled via command line flag")
        simple_print("🧠 AI Analysis: Enabled")
    
    # Initialize PerfMon analysis if requested
    perfmon_results = None
    if perfmon_file:
        logger.info(f"Analyzing Performance Monitor data from: {perfmon_file}")
        simple_print(f"📊 Analyzing Performance Monitor data from: {perfmon_file}")
        from src.perfmon.performance_analyzer import PerformanceCounterAnalyzer
        perfmon_analyzer = PerformanceCounterAnalyzer(config)
        perfmon_results = perfmon_analyzer.analyze_performance_log(perfmon_file)
        
        if perfmon_results.get('error'):
            logger.warning(f"PerfMon analysis failed: {perfmon_results['error']}")
            perfmon_results = None
        else:
            logger.info("Performance Monitor analysis completed successfully")
            simple_print("✅ Performance Monitor analysis completed")
    elif perfmon_duration and perfmon_duration > 0:
        # Start automatic PerfMon data collection if no file provided but duration specified
        logger.info(f"Starting automatic Performance Monitor data collection for {perfmon_duration} minutes...")
        simple_print(f"📊 Starting Performance Monitor data collection ({perfmon_duration} minutes)...")
        perfmon_data_file = None
        collection_name = None
        
        try:
            from src.perfmon.template_manager import PerfMonTemplateManager
            from pathlib import Path
            import time
            
            template_manager = PerfMonTemplateManager(config)
            template_file = Path(__file__).parent / "perfmon" / "templates" / "sql_performance_template.xml"
            
            if template_file.exists():
                # Create unique collection name
                collection_name = f"SQLSpeedinator_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                template_info = template_manager.parse_template(template_file)
                
                if template_info:
                    # Check for existing compatible collection first
                    matching_collection = template_manager.find_matching_collection(template_info['counters'])
                    
                    if matching_collection and isinstance(matching_collection, dict):
                        logger.info(f"Found compatible existing collection: {matching_collection.get('name', 'Unknown')}")
                        collection_name = matching_collection.get('name', collection_name)
                        xml_path = matching_collection.get('xml_path', '')
                        dcs_file = Path(xml_path) if xml_path else None
                    else:
                        # Create new data collector set
                        dcs_file_path = template_manager.create_data_collector_set(template_info, collection_name)
                        dcs_file = Path(dcs_file_path) if dcs_file_path else None
                        
                        if not dcs_file_path:
                            logger.warning("Failed to create data collector set - no file path returned")
                        elif not dcs_file or not dcs_file.exists():
                            logger.warning(f"Data collector set file not found: {dcs_file}")
                        else:
                            logger.info(f"Data collector set created: {dcs_file}")
                    
                    if dcs_file and dcs_file.exists():
                        # Start data collection
                        duration_hours = perfmon_duration / 60  # Convert minutes to hours
                        collection_result = template_manager.start_data_collection(str(dcs_file), duration_hours=duration_hours)
                        collection_started = collection_result.get('success', False)
                        
                        if collection_started:
                            logger.info(f"Performance Monitor collection '{collection_name}' started successfully")
                            logger.info(f"Waiting {perfmon_duration} minutes for data collection...")
                            
                            # **WAIT FOR DATA COLLECTION TO COMPLETE**
                            wait_seconds = perfmon_duration * 60
                            start_time = time.time()
                            
                            # Show progress bar while waiting
                            import sys
                            print(f"\n🔄 Collecting Performance Monitor data for {perfmon_duration} minutes...")
                            
                            while time.time() - start_time < wait_seconds:
                                elapsed = int(time.time() - start_time)
                                remaining = int(wait_seconds - elapsed)
                                progress_percent = (elapsed / wait_seconds) * 100
                                
                                # Create progress bar
                                bar_width = 50
                                filled_length = int(bar_width * elapsed // wait_seconds)
                                bar = '█' * filled_length + '░' * (bar_width - filled_length)
                                
                                # Format time remaining
                                minutes_remaining = remaining // 60
                                seconds_remaining = remaining % 60
                                
                                # Clear line and show progress
                                sys.stdout.write(f'\r[{bar}] {progress_percent:5.1f}% - {minutes_remaining:02d}m {seconds_remaining:02d}s remaining')
                                sys.stdout.flush()
                                
                                time.sleep(1)  # Update every second for smooth progress
                            
                            print(f"\n✅ Data collection completed!")
                            logger.info("Stopping data collection...")
                            
                            # Stop collection - pass the full collection result
                            stop_success = template_manager.stop_data_collection(collection_result)
                            if stop_success:
                                logger.info("Data collection stopped successfully")
                                
                                # Get the data file from collection result
                                perfmon_data_file = collection_result.get('output_file')
                                
                                # If not in collection result, search for it
                                if not perfmon_data_file or not Path(perfmon_data_file).exists():
                                    potential_paths = [
                                        Path(f"C:/PerfLogs/{collection_name}.blg"),
                                        Path(f"C:/PerfLogs/Admin/{collection_name}.blg"),
                                        Path(__file__).parent / "perfmon" / "data" / f"{collection_name}.blg"
                                    ]
                                    
                                    for path in potential_paths:
                                        if path.exists():
                                            perfmon_data_file = str(path)
                                            logger.info(f"Found performance data: {perfmon_data_file}")
                                            break
                                
                                if perfmon_data_file and Path(perfmon_data_file).exists():
                                    # Analyze the collected data
                                    from src.perfmon.performance_analyzer import PerformanceCounterAnalyzer
                                    perfmon_analyzer = PerformanceCounterAnalyzer(config)
                                    perfmon_results = perfmon_analyzer.analyze_performance_log(perfmon_data_file)
                                    
                                    if perfmon_results and not perfmon_results.get('error'):
                                        logger.info("Performance Monitor analysis completed successfully")
                                    else:
                                        logger.warning(f"PerfMon analysis failed: {perfmon_results.get('error', 'Unknown error')}")
                                        perfmon_results = None
                                else:
                                    logger.warning("Could not find generated performance data file")
                            else:
                                logger.warning("Failed to stop data collection cleanly")
                        else:
                            logger.warning("Failed to start automatic Performance Monitor collection")
                    else:
                        logger.warning("Failed to create data collector set")
                else:
                    logger.warning("Failed to parse PerfMon template")
            else:
                logger.warning(f"PerfMon template not found: {template_file}")
                
        except Exception as e:
            logger.error(f"PerfMon collection failed: {e}")
            # Clean up if collection was started
            if collection_name:
                try:
                    template_manager.stop_data_collection(collection_name)
                except:
                    pass
    
    # Create connection
    logger.info("Establishing SQL Server connection...")
    simple_print(f"🔗 Connecting to SQL Server: {server_name}")
    with SQLServerConnection(server_name, config) as conn:
        if not conn.test_connection():
            raise Exception("Failed to connect to SQL Server")
        
        logger.info("Connection established successfully")
        simple_print("✅ Connection established")
        
        # Initialize analyzer
        analyzer = PerformanceAnalyzer(conn, config, night_mode)
        
        # Run analysis
        logger.info("Starting performance analysis...")
        simple_print("🔍 Running SQL Server performance analysis...")
        analysis_results = analyzer.run_full_analysis()
        simple_print("✅ Performance analysis completed")
        
        # Add PerfMon results to analysis if available
        if perfmon_results:
            analysis_results['perfmon_analysis'] = perfmon_results
            
            # Run AI analysis on PerfMon data if AI is enabled
            if ai_analysis:
                logger.info("Running AI analysis on Performance Monitor data...")
                simple_print("🧠 Running AI analysis on Performance Monitor data...")
                from src.services.ai_service import AIService
                ai_service = AIService(config)
                perfmon_ai_analysis = ai_service.analyze_perfmon_bottlenecks(perfmon_results)
                
                if perfmon_ai_analysis:
                    analysis_results['perfmon_analysis']['ai_analysis'] = perfmon_ai_analysis
                    logger.info("AI Performance Monitor analysis completed")
                    simple_print("✅ AI Performance Monitor analysis completed")
                else:
                    logger.info("AI Performance Monitor analysis skipped or failed")
        
        # Run AI analysis on log data if AI is enabled and log analysis was performed
        if ai_analysis and 'log_analysis' in analysis_results:
            logger.info("Running AI analysis on log data...")
            from src.services.ai_service import AIService
            ai_service = AIService(config)
            log_ai_analysis = ai_service.analyze_log_entries(analysis_results['log_analysis'])
            
            if log_ai_analysis:
                analysis_results['log_analysis']['ai_analysis'] = log_ai_analysis
                logger.info("AI log analysis completed")
            else:
                logger.info("AI log analysis skipped or failed")
        
        # Generate report
        logger.info("Generating PDF report...")
        simple_print("📄 Generating PDF report...")
        report_generator = PDFReportGenerator(config)
        
        # Create output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"sql_analysis_{server_name}_{timestamp}.pdf"
        output_file = output_path / filename
        
        report_path = report_generator.generate_report(
            analysis_results, 
            str(output_file), 
            server_name
        )
        
        logger.info(f"Analysis completed successfully!")
        logger.info(f"Report saved to: {report_path}")
        simple_print(f"✅ Analysis completed!")
        simple_print(f"📋 Report saved to: {report_path}")

if __name__ == "__main__":
    sys.exit(main())