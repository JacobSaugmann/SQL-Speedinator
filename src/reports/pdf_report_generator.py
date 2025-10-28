"""
PDF Report Generator for SQL Speedinator with Compact Modern Schultz Design
Generates comprehensive SQL Server performance analysis reports with minimal spacing
"""

from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import os

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

class PDFReportGenerator:
    """Generate compact PDF reports with modern Schultz design"""
    
    def __init__(self, config=None):
        self.config = config
        self.styles = getSampleStyleSheet()
        self._setup_compact_styles()
    
    def _setup_compact_styles(self):
        """Setup compact Schultz color palette and styles"""
        
        # Schultz Corporate Color Palette
        self.schultz_colors = {
            'primary': colors.HexColor('#f1ebe4'),        # Light beige/cream
            'purple': colors.HexColor('#953a8c'),         # Corporate purple
            'pink': colors.HexColor('#e52a74'),           # Bright pink
            'cyan': colors.HexColor('#00b8f2'),           # Bright cyan/blue
            'dark_blue': colors.HexColor('#32327b'),      # Dark navy blue
            'light_blue': colors.HexColor('#e6f3ff'),     # Very light blue
            'light_purple': colors.HexColor('#f0e6f7'),   # Very light purple
            'dark_gray': colors.HexColor('#333333'),      # Dark gray text
            'medium_gray': colors.HexColor('#666666'),    # Medium gray
            'light_gray': colors.HexColor('#f5f5f5'),     # Light gray background
            'green': colors.HexColor('#22c55e'),          # Success green
            'red': colors.HexColor('#ef4444'),            # Error red
            'orange': colors.HexColor('#f97316'),         # Warning orange
            'dark_purple': colors.Color(84/255, 62/255, 119/255)     # #543e77 - Dark purple
        }
        
        # Compact Title Style with Schultz branding
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=26,
            spaceAfter=8,   # Minimal spacing
            spaceBefore=5,
            alignment=TA_CENTER,
            textColor=self.schultz_colors['dark_blue'],
            fontName='Helvetica-Bold',
            underline=1,
            underlineColor=self.schultz_colors['cyan'],
            underlineWidth=3
        ))
        
        # Compact Section Headers with Schultz colors
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=16,
            spaceAfter=4,   # Minimal space after
            spaceBefore=6,  # Minimal space before
            textColor=self.schultz_colors['dark_blue'],
            fontName='Helvetica-Bold',
            underline=1,
            underlineColor=self.schultz_colors['cyan'],
            underlineWidth=2
        ))
        
        # Compact SubHeaders with Schultz styling
        self.styles.add(ParagraphStyle(
            name='SubHeader',
            parent=self.styles['Heading3'],
            fontSize=12,
            spaceAfter=2,   # Minimal space
            spaceBefore=4,  # Minimal space
            textColor=self.schultz_colors['purple'],
            fontName='Helvetica-Bold',
            underline=1,
            underlineColor=self.schultz_colors['pink'],
            underlineWidth=1
        ))
        
        # Compact Priority Styling
        self.styles.add(ParagraphStyle(
            name='HighPriority',
            parent=self.styles['Normal'],
            textColor=self.schultz_colors['pink'],
            fontSize=9,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='MediumPriority',
            parent=self.styles['Normal'],
            textColor=self.schultz_colors['purple'],
            fontSize=9
        ))
        
        self.styles.add(ParagraphStyle(
            name='LowPriority',
            parent=self.styles['Normal'],
            textColor=self.schultz_colors['cyan'],
            fontSize=9
        ))
        
        # Ultra Compact Executive Summary Style
        self.styles.add(ParagraphStyle(
            name='ExecutiveSummary',
            parent=self.styles['Normal'],
            fontSize=9,
            spaceAfter=2,   # Minimal space
            spaceBefore=1,  # Minimal space
            leftIndent=8,   # Reduced indent
            rightIndent=8,  # Reduced indent
            leading=11,     # Tight leading
            textColor=self.schultz_colors['dark_gray'],
            fontName='Helvetica'
        ))
        
        # Compact Footer Style
        self.styles.add(ParagraphStyle(
            name='Footer',
            parent=self.styles['Normal'],
            fontSize=7,
            textColor=self.schultz_colors['dark_gray'],
            alignment=TA_CENTER
        ))
        
        # Ultra Compact Normal Text Style
        self.styles.add(ParagraphStyle(
            name='NormalText',
            parent=self.styles['Normal'],
            fontSize=9,
            spaceAfter=2,   # Minimal space
            spaceBefore=1,  # Minimal space
            leading=11,     # Tight line spacing
            textColor=self.schultz_colors['dark_gray'],
            fontName='Helvetica'
        ))
        
        # Ultra compact keep together style for headers
        self.styles.add(ParagraphStyle(
            name='KeepTogetherSection',
            parent=self.styles['SectionHeader'],
            keepWithNext=True,
            spaceAfter=2,   # Very minimal space
            spaceBefore=4   # Very minimal space
        ))
        
        # Ultra compact keep together style for sub headers
        self.styles.add(ParagraphStyle(
            name='KeepTogetherSub',
            parent=self.styles['SubHeader'],
            keepWithNext=True,
            spaceAfter=1,   # Very minimal space
            spaceBefore=3   # Very minimal space
        ))

    def _get_responsive_column_widths(self, num_columns: int, page_width: float = 6.5) -> list:
        """Calculate responsive column widths that fit exactly within page margins"""
        if num_columns <= 0:
            return []
        
        # Total available width (6.5 inches to use full page width)
        total_width = page_width
        
        # Calculate equal column widths that sum to exact page width
        col_width = total_width / num_columns
        
        return [col_width * inch] * num_columns

    def _get_optimized_column_widths(self, num_columns: int, priority_weights = None) -> list:
        """Get optimized column widths for specific table types with exact page fit"""
        total_width = 6.5  # inches - use full page width within margins
        
        if priority_weights and len(priority_weights) == num_columns:
            # Custom weights for different columns
            total_weight = sum(priority_weights)
            return [(weight/total_weight * total_width)*inch for weight in priority_weights]
        
        # Default: equal width distribution
        col_width = total_width / num_columns
        return [col_width*inch] * num_columns

    def _create_table_paragraph(self, text: str, font_size: int = 8, max_width: int = 150) -> Paragraph:
        """Create a paragraph object for table cells with proper text wrapping"""
        if text is None:
            text = ""
        
        # Create paragraph style for table cells
        cell_style = ParagraphStyle(
            name='TableCell',
            fontName='Helvetica',
            fontSize=font_size,
            leading=font_size + 2,  # Line height
            leftIndent=0,
            rightIndent=0,
            spaceAfter=0,
            spaceBefore=0,
            alignment=0,  # Left alignment
            wordWrap='CJK'  # Enable word wrapping
        )
        
        # Convert text to string and handle long text
        text_str = str(text)
        if max_width and len(text_str) > max_width:
            text_str = self._wrap_text(text_str, max_width)
        
        return Paragraph(text_str, cell_style)

    def _create_table_header(self, headers: List[str], font_size: int = 9) -> List[Paragraph]:
        """Create table header row with Paragraph objects for proper formatting"""
        return [self._create_table_header_cell(header, font_size) for header in headers]
    
    def _create_table_header_cell(self, text: str, font_size: int = 9) -> Paragraph:
        """Create a paragraph object for table headers with proper formatting"""
        header_style = ParagraphStyle(
            name='TableHeader',
            fontName='Helvetica-Bold',
            fontSize=font_size,
            leading=font_size + 2,
            leftIndent=0,
            alignment=0,  # Left alignment
            textColor=colors.white
        )
        
        return Paragraph(str(text), header_style)

    def _wrap_text(self, text: str, max_length: int) -> str:
        """Enhanced text wrapping for better table readability with improved line breaks"""
        if len(text) <= max_length:
            return text
        
        # Try to break at word boundaries
        words = text.split()
        lines = []
        current_line = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) + 1 <= max_length:
                current_line.append(word)
                current_length += len(word) + 1
            else:
                if current_line:
                    lines.append(' '.join(current_line))
                current_line = [word]
                current_length = len(word)
        
        if current_line:
            lines.append(' '.join(current_line))
        
        # Allow up to 3 lines for better readability
        if len(lines) > 3:
            lines = lines[:3]
            lines[-1] = lines[-1][:max_length-3] + '...'
        
        return '\n'.join(lines)

    def _create_wrapped_paragraph(self, text: str, style, max_width_chars: int = 30) -> Paragraph:
        """Create a paragraph with automatic text wrapping for table cells"""
        if len(text) <= max_width_chars:
            return Paragraph(text, style)
        
        # Smart wrap text
        wrapped_text = self._wrap_text(text, max_width_chars)
        return Paragraph(wrapped_text, style)

    def _add_compact_spacer(self, story: List, height: float = 0.02) -> None:
        """Add ultra minimal spacing between sections"""
        story.append(Spacer(1, height*inch))

    def _get_modern_table_style(self, header_color=None, data_color=None, grid_color=None):
        """Get compact Schultz-styled table formatting with enhanced text wrapping"""
        if header_color is None:
            header_color = self.schultz_colors['purple']
        if data_color is None:
            data_color = self.schultz_colors['light_blue']
        if grid_color is None:
            grid_color = self.schultz_colors['light_purple']
        
        return TableStyle([
            # Header styling with modern rounded corners
            ('BACKGROUND', (0, 0), (-1, 0), header_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),  # More padding for headers
            ('TOPPADDING', (0, 0), (-1, 0), 6),     # More padding for headers
            ('LEFTPADDING', (0, 0), (-1, -1), 6),   # Consistent padding
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),  # Consistent padding
            
            # Data rows styling with enhanced multi-line text support
            ('BACKGROUND', (0, 1), (-1, -1), data_color),
            ('TEXTCOLOR', (0, 1), (-1, -1), self.schultz_colors['dark_gray']),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 12), # More space for multi-line text
            ('TOPPADDING', (0, 1), (-1, -1), 8),     # More space for multi-line text
            ('LEFTPADDING', (0, 1), (-1, -1), 6),    
            ('RIGHTPADDING', (0, 1), (-1, -1), 6),   
            
            # Enhanced cell formatting for text wrapping
            ('LEADING', (0, 1), (-1, -1), 12),       # Better line spacing for readability
            ('WORDWRAP', (0, 0), (-1, -1), 'CJK'),   # Enable word wrapping for all cells
            
            # Soft modern borders instead of hard grid
            ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#e8e8e8')),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#f0f0f0')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),     # Align content to top of cells
            
            # Modern card-like appearance
            ('BACKGROUND', (0, 0), (-1, 0), header_color),
            
            # Alternating row colors for better readability
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafafa')]),
            
            # Auto row height to accommodate content
            ('MINHEIGHT', (0, 0), (-1, -1), 24),     # Minimum row height
        ])

    def generate_report(self, analysis_results: Dict[str, Any], 
                       output_file: str, server_name: str) -> str:
        """Generate ultra compact PDF report"""
        
        # Use provided output file path
        filepath = output_file
        
        # Create output directory if it doesn't exist
        output_dir = Path(filepath).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create PDF document with compact margins
        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=letter,
            rightMargin=0.5*inch,  # Smaller margins
            leftMargin=0.5*inch,
            topMargin=0.4*inch,
            bottomMargin=0.4*inch
        )
        
        # Build story
        story = []
        
        try:
            # Title page
            story.extend(self._create_compact_title_page(analysis_results, server_name))
            story.append(PageBreak())
            
            # Executive Summary
            story.extend(self._create_executive_summary(analysis_results))
            
            # Server Information
            if 'server_database_info' in analysis_results:
                server_db_data = analysis_results['server_database_info'].get('data', {})
                story.extend(self._create_comprehensive_server_info_section(server_db_data))
            else:
                story.extend(self._create_server_info_section(analysis_results))
            
            # Wait Statistics Analysis
            if 'wait_stats' in analysis_results:
                story.extend(self._create_wait_stats_section(analysis_results['wait_stats']))
            
            # Disk Performance Analysis
            if 'disk_performance' in analysis_results:
                story.extend(self._create_disk_analysis_section(analysis_results['disk_performance']))
            
            # Index Analysis
            if 'index_analysis' in analysis_results:
                story.extend(self._create_index_analysis_section(analysis_results['index_analysis']))
            
            # Missing Index Analysis
            if 'missing_indexes' in analysis_results:
                missing_data = analysis_results['missing_indexes']
                # Handle both old and new data structures
                if isinstance(missing_data, dict) and 'data' in missing_data:
                    all_missing = []
                    data = missing_data['data']
                    for category in ['high_impact_indexes', 'medium_impact_indexes', 'low_impact_indexes']:
                        if category in data:
                            all_missing.extend(data[category])
                    story.extend(self._create_missing_index_section(all_missing))
                else:
                    story.extend(self._create_missing_index_section(missing_data))
            
            # Server Configuration
            if 'server_database_info' in analysis_results and 'data' in analysis_results['server_database_info']:
                server_db_data = analysis_results['server_database_info']['data']
                if server_db_data and 'server_configuration' in server_db_data:
                    # Create a formatted config section from server_database_info data
                    server_config_formatted = {
                        'configuration_settings': server_db_data['server_configuration']
                    }
                    story.extend(self._create_config_analysis_section(server_config_formatted))
            elif 'server_config' in analysis_results:
                story.extend(self._create_config_analysis_section(analysis_results['server_config']))
            
            # AI Analysis Section
            if 'ai_analysis' in analysis_results:
                story.extend(self._create_ai_analysis_section(analysis_results['ai_analysis']))
            
            # Performance Monitor Analysis Section
            if 'perfmon_analysis' in analysis_results:
                story.extend(self._create_perfmon_analysis_section(analysis_results['perfmon_analysis']))
            
            # Build PDF
            doc.build(story)
            
            return str(filepath)
            
        except Exception as e:
            print(f"Error generating PDF report: {str(e)}")
            raise

    def _create_compact_title_page(self, analysis_results: Dict[str, Any], server_name: str) -> List:
        """Create ultra compact title page with Schultz branding"""
        story = []
        
        # Minimal top spacing
        story.append(Spacer(1, 0.05*inch))
        
        # Compact separator
        separator_style = ParagraphStyle(
            name='Separator',
            fontSize=8,
            textColor=self.schultz_colors['cyan'],
            alignment=TA_CENTER
        )
        story.append(Paragraph("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", separator_style))
        story.append(Spacer(1, 0.02*inch))
        
        # Main title with compact styling
        title = Paragraph(
            f"⚡ SQL Speedinator Report",
            self.styles['CustomTitle']
        )
        story.append(title)
        story.append(Spacer(1, 0.05*inch))
        
        # Server info
        server_style = ParagraphStyle(
            name='ServerInfo',
            fontSize=12,
            textColor=self.schultz_colors['dark_blue'],
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        server_card = Paragraph(
            f"🖥️ Target Server: {server_name}",
            server_style
        )
        story.append(server_card)
        story.append(Spacer(1, 0.02*inch))
        
        # Analysis metadata in compact grid layout
        metadata = analysis_results.get('analysis_metadata', {})
        start_time = metadata.get('start_time', datetime.now())
        
        metadata_data = [
            ['Report Generated', start_time.strftime("%Y-%m-%d %H:%M:%S")],
            ['Analysis Duration', f"{metadata.get('duration_seconds', 0):.1f} seconds"],
            ['Databases Analyzed', str(metadata.get('databases_count', 'N/A'))],  
            ['Report Version', '2.0 - AI Enhanced']
        ]
        
        metadata_table = Table(metadata_data, colWidths=self._get_responsive_column_widths(2))
        metadata_table.setStyle(self._get_modern_table_style())
        story.append(metadata_table)
        
        story.append(Spacer(1, 0.02*inch))
        
        # Footer
        footer_style = ParagraphStyle(
            name='TitleFooter',
            fontSize=8,
            textColor=self.schultz_colors['medium_gray'],
            alignment=TA_CENTER
        )
        story.append(Paragraph("Performance Analysis Report - Powered by AI", footer_style))
        
        return story

    def _create_executive_summary(self, analysis_results: Dict[str, Any]) -> List:
        """Create compact executive summary"""
        story = []
        
        story.append(Paragraph("📊 Executive Summary", self.styles['KeepTogetherSection']))
        
        # Performance overview
        summary = analysis_results.get('summary', {})
        if summary:
            story.append(Paragraph("Analysis Overview:", self.styles['SubHeader']))
            
            overview_text = f"""
            This comprehensive analysis examined {summary.get('total_databases', 'N/A')} databases 
            and identified {summary.get('total_issues', 0)} performance issues requiring attention.
            """
            story.append(Paragraph(overview_text, self.styles['ExecutiveSummary']))
            
            # Critical issues
            critical_issues = summary.get('critical_issues', [])
            if critical_issues:
                story.append(Paragraph("Critical Issues Found:", self.styles['SubHeader']))
                for issue in critical_issues[:3]:  # Top 3
                    story.append(Paragraph(f"• {issue}", self.styles['HighPriority']))
                story.append(Spacer(1, 0.02*inch))
            
            # Warnings
            warnings = summary.get('warnings', [])
            if warnings:
                story.append(Paragraph("Warnings:", self.styles['SubHeader']))
                for warning in warnings[:3]:  # Top 3
                    story.append(Paragraph(f"• {warning}", self.styles['MediumPriority']))
                story.append(Spacer(1, 0.02*inch))
            
            # Top recommendations
            recommendations = summary.get('top_recommendations', [])
            if recommendations:
                story.append(Paragraph("Top Priority Recommendations:", self.styles['SubHeader']))
                for rec in recommendations[:3]:  # Top 3
                    story.append(Paragraph(f"• {rec}", self.styles['ExecutiveSummary']))
                story.append(Spacer(1, 0.02*inch))
        
        return story

    def _create_server_info_section(self, analysis_results: Dict[str, Any]) -> List:
        """Create compact server information section"""
        story = []
        
        story.append(Paragraph("🖥️ Server Information", self.styles['KeepTogetherSection']))
        
        server_info = analysis_results.get('server_info', {})
        if server_info:
            data = []
            for key, value in server_info.items():
                if value and str(value) != 'None':
                    data.append([key.replace('_', ' ').title(), str(value)])
            
            if data:
                table = Table(data, colWidths=self._get_responsive_column_widths(2))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
        
        return story

    def _create_wait_stats_section(self, wait_stats: Dict[str, Any]) -> List:
        """Create compact wait statistics section"""
        story = []
        
        story.append(Paragraph("⏱️ Wait Statistics Analysis", self.styles['KeepTogetherSection']))
        
        wait_types = wait_stats.get('wait_types', [])
        if wait_types:
            high_waits = [w for w in wait_types if w.get('wait_time_ms', 0) > 10000]
            if high_waits:
                story.append(Paragraph("Issues Found:", self.styles['SubHeader']))
                summary_text = f"Analyzed {len(wait_types)} wait types. Found {len(high_waits)} high-impact wait types requiring attention."
                story.append(Paragraph(summary_text, self.styles['ExecutiveSummary']))
        
        story.append(Spacer(1, 0.02*inch))
        
        # Wait Types Analysis
        if wait_types:
            story.append(Paragraph("📊 Wait Types Analysis", self.styles['KeepTogetherSub']))
            
            # Create compact table for wait types
            table_data = [['Wait Type', 'Wait Time (ms)', 'Tasks', 'Signal Time', '%']]
            
            # Calculate total wait time for percentage
            total_wait_time = sum(w.get('wait_time_ms', 0) for w in wait_types)
            
            for wait in wait_types[:10]:  # Top 10
                wait_time = wait.get('wait_time_ms', 0)
                percentage = (wait_time / total_wait_time * 100) if total_wait_time > 0 else 0
                
                table_data.append([
                    wait.get('wait_type', 'Unknown')[:20],  # Truncate long names
                    f"{wait_time:,.0f}",
                    str(wait.get('waiting_tasks_count', 0)),
                    f"{wait.get('signal_wait_time_ms', 0):,.0f}",
                    f"{percentage:.1f}%"
                ])
            
            # Use responsive column widths for better fit
            wait_table = Table(table_data, colWidths=self._get_responsive_column_widths(5))
            wait_table.setStyle(self._get_modern_table_style())
            story.append(wait_table)
            story.append(Spacer(1, 0.02*inch))
        
        return story

    def _create_disk_analysis_section(self, disk_performance: Dict[str, Any]) -> List:
        """Create compact disk analysis section"""
        story = []
        
        story.append(Paragraph("💾 Disk Performance Analysis", self.styles['KeepTogetherSection']))
        
        io_stats = disk_performance.get('io_statistics', [])
        if io_stats:
            avg_read_latency = sum(stat.get('avg_read_latency_ms', 0) for stat in io_stats) / len(io_stats)
            avg_write_latency = sum(stat.get('avg_write_latency_ms', 0) for stat in io_stats) / len(io_stats)
            
            # Only show if there are performance issues
            if avg_read_latency >= 20 or avg_write_latency >= 20:
                story.append(Paragraph("Issues Found:", self.styles['SubHeader']))
                summary_text = f"""
                Analyzed {len(io_stats)} database files. Average read latency: {avg_read_latency:.1f}ms, 
                write latency: {avg_write_latency:.1f}ms. High latency detected - optimization recommended.
                """
                story.append(Paragraph(summary_text, self.styles['ExecutiveSummary']))
        
        story.append(Spacer(1, 0.02*inch))
        
        # I/O Statistics compact table
        if io_stats:
            story.append(Paragraph("📊 Database File I/O Statistics", self.styles['KeepTogetherSub']))
            
            table_data = [['Database', 'File Type', 'Reads', 'Writes', 'Read Latency', 'Write Latency']]
            
            for stat in io_stats[:10]:  # Top 10
                table_data.append([
                    stat.get('database_name', 'Unknown')[:15],
                    stat.get('file_type', 'Unknown')[:8],
                    f"{stat.get('num_of_reads', 0):,.0f}",
                    f"{stat.get('num_of_writes', 0):,.0f}",
                    f"{stat.get('avg_read_latency_ms', 0):.1f}ms",
                    f"{stat.get('avg_write_latency_ms', 0):.1f}ms"
                ])
            
            io_table = Table(table_data, colWidths=self._get_responsive_column_widths(6))
            io_table.setStyle(self._get_modern_table_style())
            story.append(io_table)
            story.append(Spacer(1, 0.02*inch))
        
        return story

    def _create_index_analysis_section(self, index_analysis: Dict[str, Any]) -> List:
        """Create compact index analysis section"""
        story = []
        
        story.append(Paragraph("📑 Index Analysis", self.styles['KeepTogetherSection']))
        
        # Summary of index issues
        rebuild_indexes = index_analysis.get('rebuild_recommendations', [])
        reorg_indexes = index_analysis.get('reorganize_recommendations', [])
        unused_indexes = index_analysis.get('unused_indexes', [])
        fragmentation_usage = index_analysis.get('fragmentation_usage_analysis', [])
        
        # Only show summary if there are issues
        if rebuild_indexes or reorg_indexes or unused_indexes or fragmentation_usage:
            story.append(Paragraph("Issues Found:", self.styles['SubHeader']))
            
            # Enhanced summary with fragmentation usage analysis
            frag_summary = ""
            if fragmentation_usage:
                rebuild_count = len([idx for idx in fragmentation_usage if idx.get('ActionRecommendation') == 'REBUILD'])
                reorg_count = len([idx for idx in fragmentation_usage if idx.get('ActionRecommendation') == 'REORGANIZE'])
                high_usage_issues = len([idx for idx in fragmentation_usage if idx.get('UsageCategory') in ['VERY_HIGH_USAGE', 'HIGH_USAGE'] and idx.get('ActionRecommendation') != 'IGNORE'])
                
                frag_summary = f"""
                Smart fragmentation analysis found {rebuild_count} high-priority indexes needing rebuild, 
                {reorg_count} requiring reorganization, and {high_usage_issues} heavily-used indexes with performance issues. 
                """
            else:
                frag_summary = f"""
                Index maintenance analysis found {len(rebuild_indexes)} indexes requiring rebuild, 
                {len(reorg_indexes)} needing reorganization, and {len(unused_indexes)} unused indexes 
                that could be considered for removal.
                """
            
            story.append(Paragraph(frag_summary, self.styles['ExecutiveSummary']))
        
        story.append(Spacer(1, 0.02*inch))
        
        # Maintenance recommendations
        if rebuild_indexes or reorg_indexes:
            story.append(Paragraph("🔧 Index Maintenance Recommendations", self.styles['KeepTogetherSub']))
            
            if rebuild_indexes:
                table_data = [['Schema.Table', 'Index', 'Frag %', 'Pages', 'Action']]
                
                for idx in rebuild_indexes[:8]:  # Top 8
                    table_data.append([
                        f"{idx.get('schema_name', '')}.{idx.get('table_name', '')}",
                        idx.get('index_name', 'Unknown')[:25],
                        f"{idx.get('fragmentation_percent', 0):.1f}%",
                        str(idx.get('page_count', 0)),
                        'REBUILD'
                    ])
                
                rebuild_table = Table(table_data, colWidths=self._get_responsive_column_widths(5))
                rebuild_table.setStyle(self._get_modern_table_style(
                    header_color=self.schultz_colors['red']
                ))
                story.append(rebuild_table)
                story.append(Spacer(1, 0.02*inch))
        
        # Smart Fragmentation & Usage Analysis
        if fragmentation_usage:
            story.append(Paragraph("🧠 Smart Fragmentation & Usage Analysis", self.styles['KeepTogetherSub']))
            
            # Filter for high-priority items (not IGNORE)
            priority_items = [idx for idx in fragmentation_usage if idx.get('ActionRecommendation') != 'IGNORE']
            
            if priority_items:
                table_data = [self._create_table_header(['Database.Schema.Table', 'Index', 'Frag %', 'Size MB', 'Usage', 'Action'])]
                
                for idx in priority_items[:10]:  # Top 10 priority items
                    db_name = idx.get('database_name', '')
                    schema_name = idx.get('SchemaName', '')
                    table_name = idx.get('TableName', '')
                    index_name = idx.get('IndexName', 'Unknown')
                    fragmentation = idx.get('FragmentationPct', 0)
                    size_mb = idx.get('size_mb', 0)
                    usage_category = idx.get('UsageCategory', 'UNKNOWN')
                    action = idx.get('ActionRecommendation', 'IGNORE')
                    
                    # Create readable identifiers
                    full_name = f"{db_name}.{schema_name}.{table_name}" if db_name else f"{schema_name}.{table_name}"
                    
                    # Color code usage categories
                    usage_color = 'red' if usage_category in ['VERY_HIGH_USAGE', 'HIGH_USAGE'] else 'orange' if usage_category == 'MODERATE_USAGE' else 'gray'
                    action_color = 'red' if action == 'REBUILD' else 'orange' if action == 'REORGANIZE' else 'green'
                    
                    table_data.append([
                        self._create_table_paragraph(full_name[:30] + '...' if len(full_name) > 30 else full_name),
                        self._create_table_paragraph(index_name[:20] + '...' if len(index_name) > 20 else index_name),
                        self._create_table_paragraph(f"{fragmentation:.1f}%"),
                        self._create_table_paragraph(f"{size_mb:.1f}"),
                        self._create_table_paragraph(f'<font color="{usage_color}">{usage_category.replace("_", " ")}</font>'),
                        self._create_table_paragraph(f'<font color="{action_color}">{action}</font>')
                    ])
                
                frag_usage_table = Table(table_data, colWidths=self._get_responsive_column_widths(6))
                frag_usage_table.setStyle(self._get_modern_table_style(
                    header_color=self.schultz_colors['purple']
                ))
                story.append(frag_usage_table)
                story.append(Spacer(1, 0.02*inch))
        
        return story

    def _create_missing_index_section(self, missing_indexes) -> List:
        """Create compact missing index section"""
        story = []
        
        story.append(Paragraph("🔍 Missing Index Analysis", self.styles['KeepTogetherSection']))
        
        # Ensure missing_indexes is a list
        if not isinstance(missing_indexes, list):
            missing_indexes = []
            
        if missing_indexes:
            story.append(Paragraph("Executive Summary", self.styles['SubHeader']))
            
            # Handle different ways impact might be stored
            high_impact = []
            for idx in missing_indexes:
                if isinstance(idx, dict):
                    impact = idx.get('improvement_measure', idx.get('avg_user_impact', 0))
                    if impact > 50:  # Use 50 as threshold instead of 50000
                        high_impact.append(idx)
            summary_text = f"""
            Identified {len(missing_indexes)} missing index opportunities. 
            {len(high_impact)} have high performance impact and should be prioritized.
            """
            story.append(Paragraph(summary_text, self.styles['ExecutiveSummary']))
            
            story.append(Spacer(1, 0.02*inch))
            
            # Missing indexes table
            table_data = [['Database.Table', 'Equality Columns', 'Impact', 'Seeks', 'Priority']]
            
            for idx in missing_indexes[:8]:  # Top 8
                if isinstance(idx, dict):
                    db_table = f"{idx.get('database_name', '')}.{idx.get('table_name', '')}"
                    equality_cols = idx.get('equality_columns', 'N/A')[:25]
                    impact = idx.get('improvement_measure', idx.get('avg_user_impact', 0))
                    seeks = idx.get('user_seeks', 0)
                    priority = idx.get('priority', 'UNKNOWN')
                    
                    table_data.append([
                        db_table,
                        equality_cols,
                        f"{impact:.1f}" if isinstance(impact, (int, float)) else str(impact),
                        str(seeks),
                        priority
                    ])
            
            missing_table = Table(table_data, colWidths=self._get_responsive_column_widths(5))
            missing_table.setStyle(self._get_modern_table_style(
                header_color=self.schultz_colors['orange']
            ))
            story.append(missing_table)
            story.append(Spacer(1, 0.02*inch))
        
        return story

    def _create_config_analysis_section(self, server_config: Dict[str, Any]) -> List:
        """Create compact configuration analysis section"""
        story = []
        
        story.append(Paragraph("⚙️ Server Configuration Analysis", self.styles['KeepTogetherSection']))
        
        config_settings = server_config.get('configuration_settings', [])
        if config_settings:
            # Only show configurations with issues (not OK)
            problem_configs = []
            for cfg in config_settings:
                status = cfg.get('best_practice_status', 'OK')
                if not status.startswith('OK') and status != 'OK':
                    problem_configs.append(cfg)
            
            if problem_configs:
                story.append(Paragraph("Issues Found:", self.styles['SubHeader']))
                table_data = [self._create_table_header(['Setting', 'Current Value', 'Status'])]
                
                for cfg in problem_configs:
                    name = cfg.get('name', 'Unknown')
                    current_val = str(cfg.get('value_in_use', cfg.get('value', 'N/A')))
                    status = cfg.get('best_practice_status', 'OK')
                    
                    # Color code the status
                    if status.startswith('WARNING'):
                        status_color = 'orange'
                    elif status.startswith('CRITICAL'):
                        status_color = 'red'
                    else:
                        status_color = 'gray'
                    
                    table_data.append([
                        self._create_table_paragraph(name[:30]),  # Truncate long names
                        self._create_table_paragraph(current_val),
                        self._create_table_paragraph(f'<font color="{status_color}">{status}</font>')
                    ])
                
                config_table = Table(table_data, colWidths=self._get_responsive_column_widths(3))
                config_table.setStyle(self._get_modern_table_style(
                    header_color=self.schultz_colors['cyan']
                ))
                story.append(config_table)
                story.append(Spacer(1, 0.02*inch))
        # If no issues, don't show anything - section will be empty
        
        return story

    def _create_ai_analysis_section(self, ai_analysis: Dict[str, Any]) -> List:
        """Create compact AI analysis section with enhanced styling"""
        story = []
        
        # AI Analysis Header with modern styling
        ai_header_style = ParagraphStyle(
            name='AIHeader',
            parent=self.styles['SectionHeader'],
            fontSize=16,
            spaceAfter=4,
            spaceBefore=6,
            textColor=self.schultz_colors['purple'],
            fontName='Helvetica-Bold',
            underline=1,
            underlineColor=self.schultz_colors['cyan'],
            underlineWidth=2
        )
        
        story.append(Paragraph("🤖 AI-Powered Performance Analysis", ai_header_style))
        
        # AI Analysis Summary
        analysis_data = ai_analysis.get('analysis', {})
        if analysis_data:
            story.append(Paragraph("Executive Summary", self.styles['SubHeader']))
            
            summary_text = analysis_data.get('summary', 'AI analysis completed successfully.')
            story.append(Paragraph(summary_text, self.styles['ExecutiveSummary']))
            
            story.append(Spacer(1, 0.02*inch))
        
        # Display AI bottleneck analysis
        bottlenecks = analysis_data.get('bottlenecks', [])
        if bottlenecks:
            story.append(Paragraph("🎯 AI-Identified Performance Bottlenecks", self.styles['SubHeader']))
            
            # Create bottlenecks table using string data with enhanced text wrapping
            # Create header row
            table_data = [self._create_table_header(['Priority', 'Bottleneck Issue', 'Impact', 'AI Recommendation'])]
            
            for i, bottleneck in enumerate(bottlenecks[:5], 1):  # Top 5 bottlenecks
                issue = bottleneck.get('issue', 'Unknown issue')
                impact = bottleneck.get('impact', 'UNKNOWN')
                recommendation = bottleneck.get('recommendation', 'No recommendation provided')
                
                # Color code by impact
                if impact == 'HIGH':
                    priority_text = f"#{i} (CRITICAL)"
                elif impact == 'MEDIUM':
                    priority_text = f"#{i} (MODERATE)"
                else:
                    priority_text = f"#{i} (LOW)"
                
                # Use Paragraph objects for proper text wrapping
                table_data.append([
                    self._create_table_paragraph(priority_text),
                    self._create_table_paragraph(str(issue)),
                    self._create_table_paragraph(impact),
                    self._create_table_paragraph(str(recommendation))
                ])
            
            # Table with responsive column widths for full page coverage
            bottleneck_table = Table(table_data, colWidths=self._get_responsive_column_widths(4))
            bottleneck_table.setStyle(self._get_modern_table_style(
                header_color=self.schultz_colors['purple']
            ))
            story.append(bottleneck_table)
            story.append(Spacer(1, 0.02*inch))
        
        # AI Recommendations section
        recommendations = analysis_data.get('recommendations', [])
        if recommendations:
            story.append(Paragraph("💡 AI Strategic Recommendations", self.styles['SubHeader']))
            
            for i, rec in enumerate(recommendations[:5], 1):  # Top 5 recommendations
                summary_style = ParagraphStyle(
                    name='AIRecommendation',
                    parent=self.styles['ExecutiveSummary'],
                    leftIndent=15,
                    bulletIndent=10,
                    spaceAfter=3
                )
                
                rec_text = f"{i}. {rec.get('recommendation', 'No recommendation')}"
                impact = rec.get('impact', 'Unknown')
                if impact:
                    rec_text += f" (Impact: {impact})"
                
                story.append(Paragraph(rec_text, summary_style))
            
            story.append(Spacer(1, 0.02*inch))
        
        # AI Disclaimer
        disclaimer_style = ParagraphStyle(
            name='AIDisclaimer',
            fontSize=7,
            textColor=self.schultz_colors['medium_gray'],
            alignment=TA_CENTER,
            spaceAfter=3
        )
        
        story.append(Paragraph(
            "AI Analysis powered by Azure OpenAI • Results should be validated by database professionals", 
            disclaimer_style
        ))
        
        return story
    
    def _create_comprehensive_server_info_section(self, server_db_info: Dict[str, Any]) -> List:
        """Create comprehensive server and database information section"""
        story = []
        
        # Server Instance Information
        story.append(Paragraph("🖥️ SQL Server Instance Information", self.styles['KeepTogetherSection']))
        
        instance_info = server_db_info.get('server_instance_info', {})
        if instance_info:
            # Basic server info
            basic_data = []
            for key, display_name in [
                ('server_name', 'Server Name'),
                ('edition', 'Edition'),
                ('product_version', 'Version'),
                ('product_level', 'Service Pack'),
                ('machine_name', 'Machine'),
                ('instance_name', 'Instance'),
                ('collation', 'Collation'),
                ('is_clustered', 'Clustered'),
                ('is_hadr_enabled', 'Always On Enabled')
            ]:
                value = instance_info.get(key)
                if value is not None and str(value) != 'None':
                    basic_data.append([display_name, str(value)])
            
            if basic_data:
                table = Table(basic_data, colWidths=self._get_responsive_column_widths(2))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
            else:
                story.append(Paragraph("No server instance information available", self.styles['Normal']))
        else:
            story.append(Paragraph("No server instance information available", self.styles['Normal']))
        
        story.append(Spacer(1, 0.1*inch))
        
        # Memory Information
        story.append(Paragraph("💾 Memory Information", self.styles['KeepTogetherSub']))
        
        memory_info = server_db_info.get('memory_info', {})
        if memory_info:
            memory_data = []
            memory_data.append(['Total Physical Memory (GB)', f"{memory_info.get('total_physical_memory_gb', 'N/A')}"])
            memory_data.append(['Committed Memory (GB)', f"{memory_info.get('committed_memory_gb', 'N/A')}"])
            memory_data.append(['Memory Usage %', f"{memory_info.get('memory_usage_percentage', 'N/A')}%"])
            memory_data.append(['Memory Pressure', memory_info.get('memory_pressure', 'N/A')])
            memory_data.append(['Max Workers', memory_info.get('max_workers_count', 'N/A')])
            
            table = Table(memory_data, colWidths=self._get_responsive_column_widths(2))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
        
        story.append(Spacer(1, 0.1*inch))
        
        # CPU Information
        story.append(Paragraph("⚡ CPU Information", self.styles['KeepTogetherSub']))
        
        cpu_info = server_db_info.get('cpu_info', {})
        if cpu_info:
            cpu_data = []
            cpu_data.append(['Logical CPUs', cpu_info.get('cpu_count', 'N/A')])
            cpu_data.append(['Physical CPUs', cpu_info.get('physical_cpu_count', 'N/A')])
            cpu_data.append(['Hyperthread Ratio', cpu_info.get('hyperthread_ratio', 'N/A')])
            cpu_data.append(['Schedulers', cpu_info.get('scheduler_count', 'N/A')])
            
            table = Table(cpu_data, colWidths=self._get_responsive_column_widths(2))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
        
        story.append(Spacer(1, 0.1*inch))
        
        # Server Configuration with Best Practices
        story.append(Paragraph("⚙️ Server Configuration & Best Practices", self.styles['KeepTogetherSub']))
        
        config_data = server_db_info.get('server_configuration', [])
        if config_data:
            config_table_data = [self._create_table_header(['Setting', 'Current Value', 'Status'])]
            
            for config in config_data:
                name = config.get('name', '')
                value = str(config.get('value_in_use', config.get('value', '')))
                status = config.get('best_practice_status', 'OK')
                
                # Color code the status
                if status.startswith('WARNING'):
                    status_color = 'orange'
                elif status.startswith('CRITICAL'):
                    status_color = 'red'
                else:
                    status_color = 'green'
                
                config_table_data.append([
                    self._create_table_paragraph(name),
                    self._create_table_paragraph(value),
                    self._create_table_paragraph(f'<font color="{status_color}">{status}</font>')
                ])
            
            table = Table(config_table_data, colWidths=self._get_responsive_column_widths(3))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
        
        story.append(PageBreak())
        
        # Database Overview
        story.append(Paragraph("🗄️ Database Overview", self.styles['KeepTogetherSection']))
        
        databases = server_db_info.get('database_overview', [])
        if databases:
            db_table_data = [self._create_table_header(['Database', 'Recovery Model', 'Compatibility', 'State', 'Issues'])]
            
            for db in databases:
                name = db.get('database_name', '')
                recovery = db.get('recovery_model', '')
                compat = str(db.get('compatibility_level', ''))
                state = db.get('state', '')
                issues = db.get('configuration_issues', 'OK')
                
                # Color code issues
                if issues.startswith('WARNING'):
                    issues_color = 'orange'
                elif issues.startswith('CRITICAL'):
                    issues_color = 'red'
                else:
                    issues_color = 'green'
                
                db_table_data.append([
                    self._create_table_paragraph(name),
                    self._create_table_paragraph(recovery),
                    self._create_table_paragraph(compat),
                    self._create_table_paragraph(state),
                    self._create_table_paragraph(f'<font color="{issues_color}">{issues}</font>')
                ])
            
            table = Table(db_table_data, colWidths=self._get_responsive_column_widths(5))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
        
        story.append(Spacer(1, 0.1*inch))
        
        # Database Files Information
        story.append(Paragraph("📁 Database Files", self.styles['KeepTogetherSub']))
        
        db_files = server_db_info.get('database_files', [])
        if db_files:
            files_table_data = [self._create_table_header(['Database', 'File Type', 'Size (MB)', 'Growth', 'Issues'])]
            
            for file_info in db_files:
                db_name = file_info.get('database_name', '')
                file_type = file_info.get('file_type', '')
                size_mb = f"{file_info.get('size_mb', 0):.1f}"
                growth = file_info.get('growth_desc', '')
                issues = file_info.get('growth_issues', 'OK')
                
                # Color code issues
                if issues.startswith('WARNING'):
                    issues_color = 'orange'
                else:
                    issues_color = 'green'
                
                files_table_data.append([
                    self._create_table_paragraph(db_name),
                    self._create_table_paragraph(file_type),
                    self._create_table_paragraph(size_mb),
                    self._create_table_paragraph(growth),
                    self._create_table_paragraph(f'<font color="{issues_color}">{issues}</font>')
                ])
            
            table = Table(files_table_data, colWidths=self._get_responsive_column_widths(5))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
        
        story.append(Spacer(1, 0.1*inch))
        
        # Backup Information
        story.append(Paragraph("💾 Backup Status", self.styles['KeepTogetherSub']))
        
        backup_info = server_db_info.get('backup_info', [])
        if backup_info:
            backup_table_data = [self._create_table_header(['Database', 'Last Full Backup', 'Last Log Backup', 'Status'])]
            
            for backup in backup_info:
                db_name = backup.get('database_name', '')
                last_full = backup.get('last_full_backup', 'Never')
                last_log = backup.get('last_log_backup', 'N/A')
                status = backup.get('backup_status', 'Unknown')
                
                # Format dates
                if last_full and last_full != 'Never':
                    try:
                        last_full = str(last_full)[:19]  # Remove microseconds
                    except:
                        pass
                
                if last_log and last_log != 'N/A':
                    try:
                        last_log = str(last_log)[:19]  # Remove microseconds
                    except:
                        pass
                
                # Color code status
                if status.startswith('CRITICAL'):
                    status_color = 'red'
                elif status.startswith('WARNING'):
                    status_color = 'orange'
                else:
                    status_color = 'green'
                
                backup_table_data.append([
                    self._create_table_paragraph(db_name),
                    self._create_table_paragraph(str(last_full)),
                    self._create_table_paragraph(str(last_log)),
                    self._create_table_paragraph(f'<font color="{status_color}">{status}</font>')
                ])
            
            table = Table(backup_table_data, colWidths=self._get_responsive_column_widths(4))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
        
        return story
    
    def _create_perfmon_analysis_section(self, perfmon_data: Dict[str, Any]) -> List:
        """Create Performance Monitor analysis section"""
        story = []
        
        # Section Header
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph("📊 Performance Monitor Analysis", self.styles['CustomSectionHeader']))
        story.append(Spacer(1, 0.05*inch))
        
        if 'error' in perfmon_data:
            story.append(Paragraph(f"❌ Error analyzing Performance Monitor data: {perfmon_data['error']}", 
                                 self.styles['CustomBodyRed']))
            return story
        
        # Summary Information
        if 'summary' in perfmon_data:
            summary = perfmon_data['summary']
            story.append(Paragraph("📈 Collection Summary", self.styles['CustomSubheader']))
            
            summary_data = [
                ['Collection Period', f"{summary.get('duration_minutes', 0):.1f} minutes"],
                ['Total Counters', str(summary.get('total_counters', 0))],
                ['Total Samples', f"{summary.get('total_samples', 0):,}"]
            ]
            
            table = Table(summary_data, colWidths=self._get_responsive_column_widths(2))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
            story.append(Spacer(1, 0.05*inch))
        
        # Bottlenecks Analysis
        if 'bottlenecks' in perfmon_data and perfmon_data['bottlenecks']:
            story.append(Paragraph("🚨 Performance Bottlenecks", self.styles['CustomSubheader']))
            
            bottleneck_data = [['Category', 'Severity', 'Description']]
            
            for bottleneck in perfmon_data['bottlenecks']:
                severity = bottleneck.get('severity', 'UNKNOWN')
                if severity == 'CRITICAL':
                    severity_text = '<font color="red">🔴 CRITICAL</font>'
                elif severity == 'WARNING':
                    severity_text = '<font color="orange">🟠 WARNING</font>'
                else:
                    severity_text = f'<font color="green">🟢 {severity}</font>'
                
                bottleneck_data.append([
                    bottleneck.get('category', 'Unknown'),
                    severity_text,
                    bottleneck.get('description', 'No description')
                ])
            
            table = Table(bottleneck_data, colWidths=self._get_responsive_column_widths(3))
            table.setStyle(self._get_modern_table_style())
            story.append(table)
            story.append(Spacer(1, 0.05*inch))
        
        # CPU Analysis
        if 'cpu_analysis' in perfmon_data:
            cpu = perfmon_data['cpu_analysis']
            story.append(Paragraph("🖥️ CPU Performance", self.styles['CustomSubheader']))
            
            cpu_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in cpu:
                metrics = cpu['metrics']
                status = cpu.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_processor_time' in metrics:
                    cpu_data.append([
                        'Average CPU Usage',
                        f"{metrics['avg_processor_time']}%",
                        f'<font color="{status_color}">{status}</font>'
                    ])
                
                if 'max_processor_time' in metrics:
                    cpu_data.append([
                        'Peak CPU Usage',
                        f"{metrics['max_processor_time']}%",
                        ''
                    ])
                
                if 'avg_processor_queue' in metrics:
                    cpu_data.append([
                        'Average Queue Length',
                        str(metrics['avg_processor_queue']),
                        ''
                    ])
            
            if len(cpu_data) > 1:
                table = Table(cpu_data, colWidths=self._get_responsive_column_widths(3))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
                story.append(Spacer(1, 0.05*inch))
        
        # Memory Analysis
        if 'memory_analysis' in perfmon_data:
            memory = perfmon_data['memory_analysis']
            story.append(Paragraph("🧠 Memory Performance", self.styles['CustomSubheader']))
            
            memory_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in memory:
                metrics = memory['metrics']
                status = memory.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_available_mb' in metrics:
                    memory_data.append([
                        'Average Available Memory',
                        f"{metrics['avg_available_mb']:,.0f} MB",
                        f'<font color="{status_color}">{status}</font>'
                    ])
                
                if 'min_available_mb' in metrics:
                    memory_data.append([
                        'Minimum Available Memory',
                        f"{metrics['min_available_mb']:,.0f} MB",
                        ''
                    ])
                
                if 'avg_page_life_expectancy' in metrics:
                    memory_data.append([
                        'Average Page Life Expectancy',
                        f"{metrics['avg_page_life_expectancy']:,.0f} seconds",
                        ''
                    ])
            
            if len(memory_data) > 1:
                table = Table(memory_data, colWidths=self._get_responsive_column_widths(3))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
                story.append(Spacer(1, 0.05*inch))
        
        # Disk Analysis
        if 'disk_analysis' in perfmon_data:
            disk = perfmon_data['disk_analysis']
            story.append(Paragraph("💾 Disk Performance", self.styles['CustomSubheader']))
            
            disk_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in disk:
                metrics = disk['metrics']
                status = disk.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_disk_queue_length' in metrics:
                    disk_data.append([
                        'Average Disk Queue Length',
                        str(metrics['avg_disk_queue_length']),
                        f'<font color="{status_color}">{status}</font>'
                    ])
                
                if 'avg_disk_read_ms' in metrics:
                    disk_data.append([
                        'Average Read Latency',
                        f"{metrics['avg_disk_read_ms']} ms",
                        ''
                    ])
                
                if 'max_disk_queue_length' in metrics:
                    disk_data.append([
                        'Peak Queue Length',
                        str(metrics['max_disk_queue_length']),
                        ''
                    ])
            
            if len(disk_data) > 1:
                table = Table(disk_data, colWidths=self._get_responsive_column_widths(3))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
                story.append(Spacer(1, 0.05*inch))
        
        # SQL Server Analysis
        if 'sql_server_analysis' in perfmon_data:
            sql = perfmon_data['sql_server_analysis']
            story.append(Paragraph("🗃️ SQL Server Performance", self.styles['CustomSubheader']))
            
            sql_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in sql:
                metrics = sql['metrics']
                status = sql.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_batch_requests_per_sec' in metrics:
                    sql_data.append([
                        'Average Batch Requests/sec',
                        str(metrics['avg_batch_requests_per_sec']),
                        f'<font color="{status_color}">{status}</font>'
                    ])
                
                if 'avg_compilations_per_sec' in metrics:
                    sql_data.append([
                        'Average Compilations/sec',
                        str(metrics['avg_compilations_per_sec']),
                        ''
                    ])
                
                if 'avg_lock_waits_per_sec' in metrics:
                    sql_data.append([
                        'Average Lock Waits/sec',
                        str(metrics['avg_lock_waits_per_sec']),
                        ''
                    ])
            
            if len(sql_data) > 1:
                table = Table(sql_data, colWidths=self._get_responsive_column_widths(3))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
                story.append(Spacer(1, 0.05*inch))
        
        # Recommendations
        if 'recommendations' in perfmon_data and perfmon_data['recommendations']:
            story.append(Paragraph("💡 Performance Recommendations", self.styles['CustomSubheader']))
            
            for i, recommendation in enumerate(perfmon_data['recommendations'][:10], 1):  # Limit to 10
                story.append(Paragraph(f"{i}. {recommendation}", self.styles['CustomBody']))
            
            story.append(Spacer(1, 0.05*inch))
        
        # AI Analysis Section for PerfMon
        if 'ai_analysis' in perfmon_data:
            story.append(Paragraph("🤖 AI Performance Analysis", self.styles['CustomSubheader']))
            ai_data = perfmon_data['ai_analysis']
            
            if 'summary' in ai_data:
                story.append(Paragraph(f"📋 {ai_data['summary']}", self.styles['CustomBody']))
                story.append(Spacer(1, 0.03*inch))
            
            # AI Bottlenecks
            if 'bottlenecks' in ai_data and ai_data['bottlenecks']:
                story.append(Paragraph("🎯 AI-Identified Bottlenecks", self.styles['CustomBodySmall']))
                
                ai_bottleneck_data = [['Component', 'Severity', 'Root Cause', 'Recommendation']]
                
                for bottleneck in ai_data['bottlenecks'][:5]:  # Limit to top 5
                    component = bottleneck.get('component', 'Unknown')
                    severity = bottleneck.get('severity', 'UNKNOWN')
                    root_cause = bottleneck.get('root_cause', 'Not specified')
                    recommendation = bottleneck.get('recommendation', 'No recommendation')
                    
                    # Truncate long text for table
                    if len(root_cause) > 100:
                        root_cause = root_cause[:97] + "..."
                    if len(recommendation) > 100:
                        recommendation = recommendation[:97] + "..."
                    
                    # Color code severity
                    if severity == 'CRITICAL':
                        severity_text = '<font color="red">🔴 CRITICAL</font>'
                    elif severity == 'WARNING':
                        severity_text = '<font color="orange">🟠 WARNING</font>'
                    else:
                        severity_text = f'<font color="green">🟢 {severity}</font>'
                    
                    ai_bottleneck_data.append([
                        component,
                        severity_text,
                        root_cause,
                        recommendation
                    ])
                
                table = Table(ai_bottleneck_data, colWidths=self._get_responsive_column_widths(4))
                table.setStyle(self._get_modern_table_style())
                story.append(table)
                story.append(Spacer(1, 0.05*inch))
            
            # Correlation Analysis
            if 'correlation_analysis' in ai_data:
                story.append(Paragraph("🔗 Cross-Component Analysis", self.styles['CustomBodySmall']))
                story.append(Paragraph(ai_data['correlation_analysis'], self.styles['CustomBody']))
                story.append(Spacer(1, 0.05*inch))
        
        return story
