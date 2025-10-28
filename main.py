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

def setup_logging(night_mode=False):
    """Setup logging configuration"""
    log_level = logging.INFO if not night_mode else logging.WARNING
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(logs_dir / f"sql_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="⚡ SQL Speedinator - Lightning Fast SQL Server Performance Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py -s SQLSERVER01
  python main.py -s SQLSERVER01 -n --output ./custom_reports
  python main.py -s SQLSERVER01 --schedule
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

    args = parser.parse_args()    # Setup logging
    setup_logging(args.night_mode)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = ConfigManager(args.config)
        
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
            run_analysis(args.server, output_path, config, args.night_mode, args.ai_analysis)
            
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        return 1
    
    return 0

def run_analysis(server_name, output_path, config, night_mode=False, ai_analysis=False):
    """Run a single analysis"""
    logger = logging.getLogger(__name__)
    
    # Enable AI analysis if requested via command line
    if ai_analysis:
        # Temporarily override the AI_ANALYSIS_ENABLED setting
        import os
        os.environ['AI_ANALYSIS_ENABLED'] = 'true'
        logger.info("AI analysis enabled via command line flag")
    
    # Create connection
    logger.info("Establishing SQL Server connection...")
    with SQLServerConnection(server_name, config) as conn:
        if not conn.test_connection():
            raise Exception("Failed to connect to SQL Server")
        
        logger.info("Connection established successfully")
        
        # Initialize analyzer
        analyzer = PerformanceAnalyzer(conn, config, night_mode)
        
        # Run analysis
        logger.info("Starting performance analysis...")
        analysis_results = analyzer.run_full_analysis()
        
        # Generate report
        logger.info("Generating PDF report...")
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

if __name__ == "__main__":
    sys.exit(main())