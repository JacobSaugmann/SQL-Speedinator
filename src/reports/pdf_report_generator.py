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

    def _get_responsive_column_widths(self, num_columns: int, page_width: float = 6.0) -> list:
        """Calculate responsive column widths that fit exactly within page margins"""
        if num_columns <= 0:
            return []
        
        # Total available width (6 inches to ensure it fits)
        total_width = page_width
        
        # Calculate equal column widths that sum to exact page width
        col_width = total_width / num_columns
        
        return [col_width * inch] * num_columns

    def _get_optimized_column_widths(self, num_columns: int, priority_weights = None) -> list:
        """Get optimized column widths for specific table types with exact page fit"""
        total_width = 6.0  # inches - fits within margins
        
        if priority_weights and len(priority_weights) == num_columns:
            # Custom weights for different columns
            total_weight = sum(priority_weights)
            return [(weight/total_weight * total_width)*inch for weight in priority_weights]
        
        # Default: equal width distribution
        col_width = total_width / num_columns
        return [col_width*inch] * num_columns

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
        """Get compact Schultz-styled table formatting"""
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
            ('BOTTOMPADDING', (0, 0), (-1, 0), 4),  # Minimal padding
            ('TOPPADDING', (0, 0), (-1, 0), 3),     # Minimal padding
            ('LEFTPADDING', (0, 0), (-1, -1), 4),   # Minimal padding
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),  # Minimal padding
            
            # Data rows styling with enhanced multi-line text support
            ('BACKGROUND', (0, 1), (-1, -1), data_color),
            ('TEXTCOLOR', (0, 1), (-1, -1), self.schultz_colors['dark_gray']),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),  # More space for multi-line
            ('TOPPADDING', (0, 1), (-1, -1), 8),     # More space for multi-line
            ('LEFTPADDING', (0, 1), (-1, -1), 6),    
            ('RIGHTPADDING', (0, 1), (-1, -1), 6),   
            
            # Enhanced cell formatting for text wrapping
            ('LEADING', (0, 1), (-1, -1), 10),       # Line height for multi-line text
            ('WORDWRAP', (0, 1), (-1, -1), 'CJK'),   # Enable automatic word wrapping
            
            # Soft modern borders instead of hard grid
            ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#e8e8e8')),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#f0f0f0')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            
            # Modern card-like appearance
            ('BACKGROUND', (0, 0), (-1, 0), header_color),
            
            # Alternating row colors for better readability
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafafa')])
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
            if 'server_config' in analysis_results:
                story.extend(self._create_config_analysis_section(analysis_results['server_config']))
            
            # AI Analysis Section
            if 'ai_analysis' in analysis_results:
                story.extend(self._create_ai_analysis_section(analysis_results['ai_analysis']))
            
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
        
        story.append(Paragraph("Executive Summary", self.styles['SubHeader']))
        
        wait_types = wait_stats.get('wait_types', [])
        if wait_types:
            summary_text = f"Analyzed {len(wait_types)} wait types. "
            high_waits = [w for w in wait_types if w.get('wait_time_ms', 0) > 10000]
            if high_waits:
                summary_text += f"Found {len(high_waits)} high-impact wait types requiring attention."
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
        
        story.append(Paragraph("Executive Summary", self.styles['SubHeader']))
        
        io_stats = disk_performance.get('io_statistics', [])
        if io_stats:
            avg_read_latency = sum(stat.get('avg_read_latency_ms', 0) for stat in io_stats) / len(io_stats)
            avg_write_latency = sum(stat.get('avg_write_latency_ms', 0) for stat in io_stats) / len(io_stats)
            
            summary_text = f"""
            Analyzed {len(io_stats)} database files. Average read latency: {avg_read_latency:.1f}ms, 
            write latency: {avg_write_latency:.1f}ms. 
            {'Performance is within acceptable ranges.' if avg_read_latency < 20 and avg_write_latency < 20 
             else 'High latency detected - optimization recommended.'}
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
        
        story.append(Paragraph("Executive Summary", self.styles['SubHeader']))
        
        # Summary of index issues
        rebuild_indexes = index_analysis.get('rebuild_recommendations', [])
        reorg_indexes = index_analysis.get('reorganize_recommendations', [])
        unused_indexes = index_analysis.get('unused_indexes', [])
        
        summary_text = f"""
        Index maintenance analysis complete. Found {len(rebuild_indexes)} indexes requiring rebuild, 
        {len(reorg_indexes)} needing reorganization, and {len(unused_indexes)} unused indexes 
        that could be considered for removal.
        """
        story.append(Paragraph(summary_text, self.styles['ExecutiveSummary']))
        
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
            # Only show non-default or important settings
            important_configs = [
                cfg for cfg in config_settings 
                if cfg.get('value') != cfg.get('default_value') or 
                cfg.get('name') in ['max server memory (MB)', 'min server memory (MB)', 'max degree of parallelism']
            ]
            
            if important_configs:
                table_data = [['Setting', 'Current', 'Default', 'Status']]
                
                for cfg in important_configs[:10]:  # Top 10
                    current_val = cfg.get('value', 'N/A')
                    default_val = cfg.get('default_value', 'N/A')
                    status = 'Custom' if current_val != default_val else 'Default'
                    
                    table_data.append([
                        cfg.get('name', 'Unknown')[:25],
                        str(current_val),
                        str(default_val),
                        status
                    ])
                
                config_table = Table(table_data, colWidths=self._get_responsive_column_widths(4))
                config_table.setStyle(self._get_modern_table_style())
                story.append(config_table)
                story.append(Spacer(1, 0.02*inch))
        
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
            table_data = [['Priority', 'Bottleneck Issue', 'Impact', 'AI Recommendation']]
            
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
                
                # Use string data - ReportLab will handle wrapping with WORDWRAP style
                table_data.append([
                    priority_text,
                    str(issue),
                    impact,
                    str(recommendation)
                ])
            
            # Table with optimized column weights for proper text display
            bottleneck_table = Table(table_data, colWidths=self._get_optimized_column_widths(4, [0.8, 2.5, 0.6, 2.1]))
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
