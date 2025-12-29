"""
PDF Report Generator for SQL Speedinator
Orchestrates report generation using PDFStyleManager and SectionBuilder
"""

from pathlib import Path
from typing import Dict, Any
import logging

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, PageBreak

from .pdf_style_manager import PDFStyleManager
from .section_builder import SectionBuilder


class PDFReportGenerator:
    """
    Generate SQL Server analysis PDF reports.
    
    Orchestrates report generation using helper classes for clean architecture.
    
    Responsibilities:
    - Manage PDF document creation and settings
    - Coordinate section generation through SectionBuilder
    - Apply styling through PDFStyleManager
    - Handle file I/O and directory creation
    """
    
    def __init__(self, config=None):
        """Initialize report generator.
        
        Args:
            config: Optional ConfigManager instance for PDF settings
        """
        self.config = config
        self.style_manager = PDFStyleManager()
        self.section_builder = SectionBuilder(self.style_manager)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generate_report(self, analysis_results: Dict[str, Any], 
                       output_file: str, server_name: str) -> str:
        """Generate complete PDF report.
        
        Args:
            analysis_results: Dictionary containing all analysis results
            output_file: Path where PDF should be saved
            server_name: SQL Server name for report header
            
        Returns:
            Path to generated PDF file
            
        Raises:
            Exception: If PDF generation fails
        """
        try:
            # Setup file path
            filepath = Path(output_file)
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Create PDF document with compact margins
            doc = SimpleDocTemplate(
                str(filepath),
                pagesize=letter,
                rightMargin=0.5 * inch,
                leftMargin=0.5 * inch,
                topMargin=0.4 * inch,
                bottomMargin=0.4 * inch
            )
            
            # Build report sections
            story = self._build_report_sections(analysis_results, server_name)
            
            # Generate PDF
            doc.build(story)
            self.logger.info(f"PDF report generated: {filepath}")
            
            return str(filepath)
            
        except Exception as e:
            self.logger.error(f"Error generating PDF report: {str(e)}", exc_info=True)
            raise
    
    def _build_report_sections(self, analysis_results: Dict[str, Any], 
                               server_name: str) -> list:
        """Build all report sections.
        
        Args:
            analysis_results: Analysis results from all analyzers
            server_name: Server name for report header
            
        Returns:
            List of Platypus elements for PDF
        """
        story = []
        
        # Title page
        story.extend(self.section_builder.create_title_page(
            analysis_results, server_name
        ))
        story.append(PageBreak())
        
        # Executive summary
        story.extend(self.section_builder.create_executive_summary(
            analysis_results
        ))
        
        # Server information
        if 'server_database_info' in analysis_results:
            server_db_data = analysis_results['server_database_info'].get('data', {})
            story.extend(self.section_builder.create_comprehensive_server_info_section(
                server_db_data
            ))
        else:
            story.extend(self.section_builder.create_server_info_section(
                analysis_results
            ))
        
        # Wait statistics
        if 'wait_stats' in analysis_results:
            wait_data = analysis_results['wait_stats'].get('data', {})
            story.extend(self.section_builder.create_wait_stats_section(wait_data))
        
        # Disk performance - consistent data access
        if 'disk_performance' in analysis_results:
            disk_data = analysis_results['disk_performance'].get('data', {})
            story.extend(self.section_builder.create_disk_analysis_section(disk_data))
        
        # Index analysis
        if 'index_analysis' in analysis_results:
            index_data = analysis_results['index_analysis'].get('data', {})
            story.extend(self.section_builder.create_index_analysis_section(index_data))
        
        # Missing indexes - consistent data access
        if 'missing_indexes' in analysis_results:
            missing_data = analysis_results['missing_indexes'].get('data', {})
            all_missing = []
            for category in ['high_impact_indexes', 'medium_impact_indexes', 'low_impact_indexes']:
                if category in missing_data:
                    all_missing.extend(missing_data[category])
            story.extend(self.section_builder.create_missing_index_section(all_missing))
        
        # Server configuration
        if 'server_database_info' in analysis_results:
            server_db_data = analysis_results['server_database_info'].get('data', {})
            if server_db_data and 'server_configuration' in server_db_data:
                config_formatted = {
                    'configuration_settings': server_db_data['server_configuration']
                }
                story.extend(self.section_builder.create_config_analysis_section(
                    config_formatted
                ))
        elif 'server_config' in analysis_results:
            story.extend(self.section_builder.create_config_analysis_section(
                analysis_results['server_config']
            ))
        
        # AI analysis
        if 'ai_analysis' in analysis_results:
            story.extend(self.section_builder.create_ai_analysis_section(
                analysis_results['ai_analysis']
            ))
        
        # Performance Monitor
        if 'perfmon_analysis' in analysis_results:
            story.extend(self.section_builder.create_perfmon_analysis_section(
                analysis_results['perfmon_analysis']
            ))
        
        # Log analysis - consistent data access
        if 'log_analysis' in analysis_results:
            log_data = analysis_results['log_analysis'].get('data', {})
            story.extend(self.section_builder.create_log_analysis_section(log_data))
        
        return story
