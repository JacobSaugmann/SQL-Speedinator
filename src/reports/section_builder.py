"""
PDF Report Section Builder

Consolidates all PDF report section generation logic into a single class.
Each section builder method creates a specific part of the PDF report.

Extracted from PDFReportGenerator to improve:
- Code organization (each section is a self-contained method)
- Reusability (sections can be used independently)
- Testability (each section can be tested separately)
- Maintainability (changes to one section don't affect others)
"""

import logging
from typing import Dict, List, Any, Optional
from reportlab.platypus import Table, Spacer, Paragraph
from reportlab.lib.units import inch

try:
    from .pdf_style_manager import PDFStyleManager
except ImportError:
    from pdf_style_manager import PDFStyleManager


class SectionBuilder:
    """
    Builds individual PDF report sections.
    
    Consolidates all section creation logic that was previously in PDFReportGenerator.
    Each method creates one section of the PDF report and returns a list of Platypus elements.
    
    Usage:
        builder = SectionBuilder(style_manager)
        wait_stats_section = builder.create_wait_stats_section(wait_stats_data)
        disk_section = builder.create_disk_analysis_section(disk_data)
    """
    
    def __init__(self, style_manager: PDFStyleManager):
        """
        Initialize section builder with style manager.
        
        Args:
            style_manager: PDFStyleManager instance for styling
        """
        self.style_manager = style_manager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("SectionBuilder initialized")
    
    # =======================
    # Table Helper Methods
    # =======================
    
    def _get_modern_table_style(self) -> Any:
        """Get modern table style from style manager"""
        # Access through style_manager to get the TableStyle
        return self.style_manager._get_modern_table_style()
    
    def _wrap_text_for_table(self, text: str, max_length: int = 30) -> str:
        """Wrap text for table display"""
        return self.style_manager.wrap_text(text, max_length)
    
    def _create_table_paragraph(self, text: str, font_size: int = 8, max_width: int = 150) -> Paragraph:
        """Create paragraph for table cell"""
        return self.style_manager.create_table_paragraph(text, font_size, max_width)
    
    def _create_table_header(self, headers: List[str], font_size: int = 9) -> List[Paragraph]:
        """Create table header row"""
        return self.style_manager.create_table_header(headers, font_size)
    
    # =======================
    # Section Creation Methods
    # =======================
    
    def create_title_page(self, analysis_results: Dict[str, Any], 
                         server_name: str) -> List:
        """Create title page for PDF report.
        
        Args:
            analysis_results: Analysis results containing metadata
            server_name: SQL Server name to display
            
        Returns:
            List of Platypus elements for title page
        """
        from reportlab.platypus import Spacer, Paragraph
        from datetime import datetime
        
        story = []
        
        # Minimal spacing
        story.append(Spacer(1, 0.1 * inch))
        
        # Title
        story.append(Paragraph(
            "SQL Speedinator Report",
            self.style_manager.get_style('CustomTitle')
        ))
        story.append(Spacer(1, 0.1 * inch))
        
        # Server info
        story.append(Paragraph(
            f"Server: {server_name}",
            self.style_manager.get_style('SubHeader')
        ))
        story.append(Spacer(1, 0.05 * inch))
        
        # Metadata
        metadata = analysis_results.get('analysis_metadata', {})
        if metadata:
            start_time = metadata.get('start_time', datetime.now())
            duration = metadata.get('duration_seconds', 0)
            
            info_text = f"""
            Report Generated: {start_time.strftime("%Y-%m-%d %H:%M:%S")}<br/>
            Analysis Duration: {duration:.1f} seconds<br/>
            Databases Analyzed: {metadata.get('databases_count', 'N/A')}
            """
            story.append(Paragraph(info_text, self.style_manager.get_style('ExecutiveSummary')))
        
        return story
    
    def create_executive_summary(self, analysis_results: Dict[str, Any]) -> List:
        """Create executive summary section.
        
        Args:
            analysis_results: Analysis results from all analyzers
            
        Returns:
            List of Platypus elements for summary section
        """
        from reportlab.platypus import Spacer, Paragraph
        
        story = []
        
        story.append(Paragraph("Executive Summary", 
                              self.style_manager.get_style('SectionHeader')))
        story.append(Spacer(1, 0.03 * inch))
        
        # Summary text
        summary = analysis_results.get('summary', {})
        if summary:
            total_databases = summary.get('total_databases', 'N/A')
            total_issues = summary.get('total_issues', 0)
            critical_count = len(summary.get('critical_issues', []))
            
            text = f"""
            Analysis examined {total_databases} databases and identified {total_issues} performance issues.
            {critical_count} critical issues requiring immediate attention.
            """
            story.append(Paragraph(text, self.style_manager.get_style('ExecutiveSummary')))
            story.append(Spacer(1, 0.03 * inch))
            
            # Top issues
            critical_issues = summary.get('critical_issues', [])
            if critical_issues:
                story.append(Paragraph("Critical Issues:", 
                                      self.style_manager.get_style('SubHeader')))
                for issue in critical_issues[:3]:
                    story.append(Paragraph(f"• {issue}", 
                                          self.style_manager.get_style('HighPriority')))
                story.append(Spacer(1, 0.03 * inch))
        
        return story
    
    def create_server_info_section(self, analysis_results: Dict[str, Any]) -> List:
        """
        Create compact server information section.
        
        Args:
            analysis_results: Analysis results containing server_info
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("Server Information", self.style_manager.get_style('KeepTogetherSection')))
        
        server_info = analysis_results.get('server_info', {})
        if server_info:
            data = []
            for key, value in server_info.items():
                if value and str(value) != 'None':
                    data.append([key.replace('_', ' ').title(), str(value)])
            
            if data:
                table = Table(data, colWidths=self.style_manager.get_responsive_column_widths(2))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
        
        return story
    
    def create_wait_stats_section(self, wait_stats: Dict[str, Any]) -> List:
        """
        Create wait statistics analysis section.
        
        Args:
            wait_stats: Wait statistics data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("Wait Statistics Analysis", self.style_manager.get_style('KeepTogetherSection')))
        
        current_waits = wait_stats.get('current_waits', [])
        high_waits = wait_stats.get('high_waits', [])
        
        if current_waits:
            total_waits = len(current_waits)
            high_count = len(high_waits) if high_waits else 0
            summary_text = f"Analyzed {total_waits} wait types. "
            
            if high_count > 0:
                summary_text += f"Found {high_count} high-impact wait types requiring attention."
                story.append(Paragraph("Issues Found:", self.style_manager.get_style('SubHeader')))
            else:
                summary_text += "No critical wait types detected."
                story.append(Paragraph("Status: Good", self.style_manager.get_style('SubHeader')))
                
            story.append(Paragraph(summary_text, self.style_manager.get_style('ExecutiveSummary')))
        else:
            story.append(Paragraph("No wait statistics data available.", self.style_manager.get_style('BodyText')))
            story.append(Spacer(1, 0.02*inch))
            return story
        
        story.append(Spacer(1, 0.02*inch))
        
        if current_waits:
            story.append(Paragraph("Top Wait Types", self.style_manager.get_style('KeepTogetherSub')))
            
            table_data = [['Wait Type', 'Wait Time (ms)', '%', 'Wait Count', 'Avg Wait (ms)']]
            
            for wait in current_waits[:10]:
                wait_type = wait.get('wait_type', 'Unknown')
                wait_time = wait.get('wait_time_ms', 0)
                wait_count = wait.get('waiting_tasks_count', 0)
                wait_percentage = wait.get('wait_percentage', 0)
                avg_wait = (wait_time / wait_count) if wait_count > 0 else 0
                
                table_data.append([
                    self._create_table_paragraph(self._wrap_text_for_table(wait_type, 16)),
                    self._create_table_paragraph(f"{wait_time:,.0f}"),
                    self._create_table_paragraph(f"{wait_percentage:.1f}%"),
                    self._create_table_paragraph(f"{wait_count:,}"),
                    self._create_table_paragraph(f"{avg_wait:.1f}")
                ])
            
            wait_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(5))
            wait_table.setStyle(self._get_modern_table_style())
            story.append(wait_table)
            story.append(Spacer(1, 0.02*inch))
        
        return story
    
    def create_disk_analysis_section(self, disk_performance: Dict[str, Any]) -> List:
        """
        Create disk analysis section.
        
        Args:
            disk_performance: Disk performance data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("Disk Performance Analysis", self.style_manager.get_style('KeepTogetherSection')))
        
        disk_performance_list = disk_performance.get('disk_performance', [])
        issues_found = disk_performance.get('issues_found', False)
        
        if disk_performance_list:
            if issues_found:
                story.append(Paragraph("Issues Found:", self.style_manager.get_style('SubHeader')))
            else:
                story.append(Paragraph("Status: Good", self.style_manager.get_style('SubHeader')))
            
            summary_text = f"Analyzed {len(disk_performance_list)} disk(s). "
            if issues_found:
                summary_text += "Some disks show latency issues that may impact performance."
            else:
                summary_text += "All disks performing within acceptable parameters."
            
            story.append(Paragraph(summary_text, self.style_manager.get_style('ExecutiveSummary')))
            story.append(Spacer(1, 0.02*inch))
            
            table_data = [['Drive', 'Avg Latency (ms)', 'Read %', 'Write %']]
            for disk in disk_performance_list[:8]:
                table_data.append([
                    self._create_table_paragraph(disk.get('disk', 'Unknown')),
                    self._create_table_paragraph(f"{disk.get('avg_latency_ms', 0):.2f}"),
                    self._create_table_paragraph(f"{disk.get('read_percentage', 0):.1f}%"),
                    self._create_table_paragraph(f"{disk.get('write_percentage', 0):.1f}%")
                ])
            
            disk_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(4))
            disk_table.setStyle(self._get_modern_table_style())
            story.append(disk_table)
        else:
            story.append(Paragraph("No disk performance data available.", self.style_manager.get_style('BodyText')))
        
        return story
    
    def create_index_analysis_section(self, index_analysis: Dict[str, Any]) -> List:
        """
        Create index analysis section.
        
        Args:
            index_analysis: Index analysis data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("Index Analysis", self.style_manager.get_style('KeepTogetherSection')))
        
        fragmented_indexes = index_analysis.get('fragmented_indexes', [])
        unused_indexes = index_analysis.get('unused_indexes', [])
        duplicate_indexes = index_analysis.get('duplicate_indexes', [])
        rebuild_indexes = index_analysis.get('rebuild_indexes', [])
        reorg_indexes = index_analysis.get('reorg_indexes', [])
        
        total_issues = len(fragmented_indexes) + len(unused_indexes) + len(duplicate_indexes)
        
        if total_issues > 0:
            story.append(Paragraph("Issues Found:", self.style_manager.get_style('SubHeader')))
            frag_summary = f"""
            Index maintenance analysis found {len(rebuild_indexes)} indexes requiring rebuild, 
            {len(reorg_indexes)} needing reorganization, {len(unused_indexes)} unused indexes,
            and {len(duplicate_indexes)} overlapping/duplicate indexes that could be optimized.
            """
            story.append(Paragraph(frag_summary, self.style_manager.get_style('ExecutiveSummary')))
        else:
            story.append(Paragraph("Status: Good", self.style_manager.get_style('SubHeader')))
            summary_text = f"""
            Index analysis completed on all user databases. No critical fragmentation issues detected. 
            Current indexing strategy appears well-maintained.
            """
            story.append(Paragraph(summary_text, self.style_manager.get_style('ExecutiveSummary')))
        
        story.append(Spacer(1, 0.02*inch))
        
        if fragmented_indexes:
            story.append(Paragraph("Top Fragmented Indexes", self.style_manager.get_style('KeepTogetherSub')))
            
            table_data = [['Index', 'Table', 'Fragmentation %', 'Pages']]
            for idx in fragmented_indexes[:5]:
                table_data.append([
                    self._create_table_paragraph(self._wrap_text_for_table(idx.get('index_name', ''), 15)),
                    self._create_table_paragraph(self._wrap_text_for_table(idx.get('table_name', ''), 15)),
                    self._create_table_paragraph(f"{idx.get('fragmentation_percentage', 0):.1f}%"),
                    self._create_table_paragraph(f"{idx.get('pages', 0):,}")
                ])
            
            frag_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(4))
            frag_table.setStyle(self._get_modern_table_style())
            story.append(frag_table)
        
        return story
    
    def create_missing_index_section(self, missing_indexes: List[Dict]) -> List:
        """
        Create missing indexes section.
        
        Args:
            missing_indexes: List of missing index data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        if not missing_indexes:
            return story
        
        story.append(Paragraph("Missing Indexes", self.style_manager.get_style('KeepTogetherSection')))
        
        story.append(Paragraph(f"Found {len(missing_indexes)} potentially beneficial missing indexes", 
                             self.style_manager.get_style('SubHeader')))
        story.append(Spacer(1, 0.02*inch))
        
        table_data = [['Table', 'Column(s)', 'Impact %']]
        for idx in missing_indexes[:5]:
            table_data.append([
                self._create_table_paragraph(self._wrap_text_for_table(idx.get('table', ''), 15)),
                self._create_table_paragraph(self._wrap_text_for_table(idx.get('column', ''), 15)),
                self._create_table_paragraph(f"{idx.get('impact', 0):.1f}%")
            ])
        
        missing_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(3))
        missing_table.setStyle(self._get_modern_table_style())
        story.append(missing_table)
        
        return story
    
    def create_config_analysis_section(self, server_config: Dict[str, Any]) -> List:
        """
        Create server configuration analysis section.
        
        Args:
            server_config: Server configuration data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("Configuration Analysis", self.style_manager.get_style('KeepTogetherSection')))
        
        config_settings = server_config.get('configuration_settings', [])
        
        if config_settings:
            story.append(Paragraph(f"Analyzed {len(config_settings)} server settings", 
                                 self.style_manager.get_style('SubHeader')))
            story.append(Spacer(1, 0.02*inch))
            
            table_data = [['Setting', 'Value', 'Status']]
            for setting in config_settings[:8]:
                name = setting.get('name', 'Unknown')
                value = setting.get('value', 'N/A')
                status = setting.get('best_practice_status', 'UNKNOWN')
                
                table_data.append([
                    self._create_table_paragraph(self._wrap_text_for_table(name, 15)),
                    self._create_table_paragraph(str(value)),
                    self._create_table_paragraph(status)
                ])
            
            config_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(3))
            config_table.setStyle(self._get_modern_table_style())
            story.append(config_table)
        else:
            story.append(Paragraph("No configuration data available.", 
                                 self.style_manager.get_style('BodyText')))
        
        return story
    
    def create_ai_analysis_section(self, ai_analysis: Dict[str, Any]) -> List:
        """
        Create AI analysis section.
        
        Args:
            ai_analysis: AI analysis data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("AI Analysis & Recommendations", self.style_manager.get_style('KeepTogetherSection')))
        
        if not ai_analysis or isinstance(ai_analysis, str):
            story.append(Paragraph("No AI analysis available.", self.style_manager.get_style('BodyText')))
            return story
        
        # Extract data from AnalysisResult if needed
        ai_data = ai_analysis.get('data', {}) if hasattr(ai_analysis, 'get') else ai_analysis.data if hasattr(ai_analysis, 'data') else {}
        
        summary_text = ai_data.get('summary', 'AI analysis completed successfully.')
        story.append(Paragraph(summary_text, self.style_manager.get_style('ExecutiveSummary')))
        
        recommendations = ai_data.get('recommendations', [])
        if recommendations:
            story.append(Spacer(1, 0.02*inch))
            story.append(Paragraph("Recommendations", self.style_manager.get_style('KeepTogetherSub')))
            for i, rec in enumerate(recommendations[:5], 1):
                story.append(Paragraph(f"{i}. {rec}", self.style_manager.get_style('NormalText')))
        
        return story
    
    def create_comprehensive_server_info_section(self, server_db_info: Dict[str, Any]) -> List:
        """
        Create comprehensive server and database information section.
        
        Args:
            server_db_info: Server and database info
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        story.append(Paragraph("Server & Database Information", self.style_manager.get_style('KeepTogetherSection')))
        
        databases = server_db_info.get('databases', [])
        server_info = server_db_info.get('server_info', {})
        
        if server_info:
            info_data = []
            for key, value in list(server_info.items())[:5]:
                if value and str(value) != 'None':
                    info_data.append([key.replace('_', ' ').title(), str(value)])
            
            if info_data:
                table = Table(info_data, colWidths=self.style_manager.get_responsive_column_widths(2))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
                story.append(Spacer(1, 0.02*inch))
        
        if databases:
            story.append(Paragraph(f"Databases: {len(databases)}", self.style_manager.get_style('SubHeader')))
            
            table_data = [['Database', 'Size', 'Status']]
            for db in databases[:8]:
                table_data.append([
                    self._create_table_paragraph(db.get('name', 'Unknown')),
                    self._create_table_paragraph(str(db.get('size', 'N/A'))),
                    self._create_table_paragraph(db.get('status', 'UNKNOWN'))
                ])
            
            db_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(3))
            db_table.setStyle(self._get_modern_table_style())
            story.append(db_table)
        
        return story
    
    def create_perfmon_analysis_section(self, perfmon_data: Dict[str, Any]) -> List:
        """
        Create Performance Monitor analysis section.
        
        Args:
            perfmon_data: Performance Monitor data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        if not perfmon_data:
            return story
        
        story.append(Paragraph("Performance Monitor Analysis", self.style_manager.get_style('KeepTogetherSection')))
        
        counters = perfmon_data.get('counters', [])
        if counters:
            story.append(Paragraph(f"Analyzed {len(counters)} performance counters", 
                                 self.style_manager.get_style('SubHeader')))
            story.append(Spacer(1, 0.02*inch))
            
            table_data = [['Counter', 'Avg', 'Min', 'Max']]
            for counter in counters[:8]:
                table_data.append([
                    self._create_table_paragraph(self._wrap_text_for_table(counter.get('name', ''), 15)),
                    self._create_table_paragraph(f"{counter.get('average', 0):.2f}"),
                    self._create_table_paragraph(f"{counter.get('minimum', 0):.2f}"),
                    self._create_table_paragraph(f"{counter.get('maximum', 0):.2f}")
                ])
            
            perf_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(4))
            perf_table.setStyle(self._get_modern_table_style())
            story.append(perf_table)
        else:
            story.append(Paragraph("No Performance Monitor data available.", 
                                 self.style_manager.get_style('BodyText')))
        
        return story
    
    def create_log_analysis_section(self, log_data: Dict[str, Any]) -> List:
        """
        Create log analysis section.
        
        Args:
            log_data: Log analysis data
            
        Returns:
            List of Platypus elements for this section
        """
        story = []
        
        if not log_data:
            return story
        
        story.append(Paragraph("Log Analysis", self.style_manager.get_style('KeepTogetherSection')))
        
        error_count = log_data.get('error_count', 0)
        warning_count = log_data.get('warning_count', 0)
        
        summary = f"Error log analysis: {error_count} errors, {warning_count} warnings"
        story.append(Paragraph(summary, self.style_manager.get_style('ExecutiveSummary')))
        
        errors = log_data.get('top_errors', [])
        if errors:
            story.append(Spacer(1, 0.02*inch))
            story.append(Paragraph("Top Errors", self.style_manager.get_style('KeepTogetherSub')))
            
            table_data = [['Error', 'Count', 'Last Occurrence']]
            for error in errors[:5]:
                table_data.append([
                    self._create_table_paragraph(self._wrap_text_for_table(error.get('message', ''), 20)),
                    self._create_table_paragraph(str(error.get('count', 0))),
                    self._create_table_paragraph(str(error.get('last_occurrence', 'N/A')))
                ])
            
            error_table = Table(table_data, colWidths=self.style_manager.get_responsive_column_widths(3))
            error_table.setStyle(self._get_modern_table_style())
            story.append(error_table)
        
        return story
