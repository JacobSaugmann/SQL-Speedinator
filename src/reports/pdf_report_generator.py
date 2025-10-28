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
        
        # Create paragraph style for table cells with enhanced wrapping
        cell_style = ParagraphStyle(
            name='TableCell',
            fontName='Helvetica',
            fontSize=font_size,
            leading=font_size + 2,  # Line height
            leftIndent=2,
            rightIndent=2,
            spaceAfter=2,
            spaceBefore=2,
            alignment=TA_LEFT,
            wordWrap='CJK',  # Enable word wrapping
            allowWidows=1,   # Allow single lines at bottom of column
            allowOrphans=1   # Allow single lines at top of column
        )
        
        # Convert text to string and clean up extensively
        text_str = str(text).strip()
        
        # Comprehensive HTML cleaning with regex
        import re
        
        # Remove all HTML/XML tags completely - this is the safest approach
        text_str = re.sub(r'<[^>]*>', '', text_str)
        
        # Remove any remaining XML/HTML artifacts
        text_str = re.sub(r'&[a-zA-Z0-9#]+;', '', text_str)  # Remove HTML entities
        text_str = re.sub(r'</?\w+[^>]*>', '', text_str)     # Remove any missed tags
        
        # Clean up whitespace and special characters
        text_str = re.sub(r'\s+', ' ', text_str)  # Replace multiple spaces with single space
        text_str = text_str.strip()
        
        # No text truncation - allow full text to wrap across multiple lines
        # The table will automatically adjust height to accommodate the content
        
        # Final cleanup - ensure no problematic characters remain
        text_str = text_str.replace('<', '').replace('>', '').replace('&', 'and')
        
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

    def _get_ai_table_style(self):
        """Get specialized table style for AI analysis tables with enhanced text wrapping"""
        return TableStyle([
            # Header styling with AI-specific colors
            ('BACKGROUND', (0, 0), (-1, 0), self.schultz_colors['purple']),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            
            # Data rows with enhanced spacing for long text
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('TEXTCOLOR', (0, 1), (-1, -1), self.schultz_colors['dark_gray']),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 20), # Much more space for AI content
            ('TOPPADDING', (0, 1), (-1, -1), 15),    # Much more space for AI content
            ('LEFTPADDING', (0, 1), (-1, -1), 8),    
            ('RIGHTPADDING', (0, 1), (-1, -1), 8),   
            
            # Optimized for multi-line AI text
            ('LEADING', (0, 1), (-1, -1), 16),       # Better line spacing for AI content
            ('WORDWRAP', (0, 0), (-1, -1), 'CJK'),   # Enable word wrapping
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),     # Align content to top
            
            # Clean modern borders
            ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#e8e8e8')),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#f0f0f0')),
            
            # Dynamic row height for AI content - much larger minimum
            ('MINHEIGHT', (0, 0), (-1, -1), 48),     # Much larger minimum for AI tables
            
            # Alternating background for readability
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
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
            if 'wait_stats' in analysis_results and 'data' in analysis_results['wait_stats']:
                story.extend(self._create_wait_stats_section(analysis_results['wait_stats']['data']))
            
            # Disk Performance Analysis
            if 'disk_performance' in analysis_results:
                story.extend(self._create_disk_analysis_section(analysis_results['disk_performance']))
            
            # Index Analysis
            if 'index_analysis' in analysis_results and 'data' in analysis_results['index_analysis']:
                story.extend(self._create_index_analysis_section(analysis_results['index_analysis']['data']))
            
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
            
            # Log Analysis Section
            if 'log_analysis' in analysis_results:
                story.extend(self._create_log_analysis_section(analysis_results['log_analysis']))
            
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
        
        # Get current waits - this is the actual data structure from the analyzer
        current_waits = wait_stats.get('current_waits', [])
        high_waits = wait_stats.get('high_waits', [])
        wait_analysis = wait_stats.get('wait_analysis', {})
        
        if current_waits:
            # Summary paragraph
            total_waits = len(current_waits)
            high_count = len(high_waits) if high_waits else 0
            summary_text = f"Analyzed {total_waits} wait types. "
            
            if high_count > 0:
                summary_text += f"Found {high_count} high-impact wait types requiring attention."
                story.append(Paragraph("Issues Found:", self.styles['SubHeader']))
            else:
                summary_text += "No critical wait types detected."
                story.append(Paragraph("Status: Good", self.styles['SubHeader']))
                
            story.append(Paragraph(summary_text, self.styles['ExecutiveSummary']))
        else:
            story.append(Paragraph("No wait statistics data available.", self.styles['BodyText']))
            story.append(Spacer(1, 0.02*inch))
            return story
        
        story.append(Spacer(1, 0.02*inch))
        
        # Wait Types Analysis
        if current_waits:
            story.append(Paragraph("📊 Top Wait Types", self.styles['KeepTogetherSub']))
            
            # Create compact table for wait types
            table_data = [['Wait Type', 'Wait Time (ms)', 'Wait Count', 'Avg Wait (ms)']]
            
            for wait in current_waits[:10]:  # Top 10
                wait_type = wait.get('wait_type', 'Unknown')
                wait_time = wait.get('wait_time_ms', 0)
                wait_count = wait.get('waiting_tasks_count', 0)
                avg_wait = (wait_time / wait_count) if wait_count > 0 else 0
                
                table_data.append([
                    wait_type,
                    f"{wait_time:,.0f}",
                    f"{wait_count:,}",
                    f"{avg_wait:.1f}"
                ])
            
            # Use responsive column widths for better fit
            wait_table = Table(table_data, colWidths=self._get_responsive_column_widths(4))
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
        else:
            story.append(Paragraph("Status: Good", self.styles['SubHeader']))
            summary_text = f"""
            Index analysis completed on all user databases. No critical fragmentation issues detected. 
            Current indexing strategy appears well-maintained.
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
                    
                    # Get emoji for usage and action
                    usage_emoji = '🔴' if usage_color == 'red' else ('🟠' if usage_color == 'orange' else ('🟡' if usage_color == 'yellow' else '🟢'))
                    action_emoji = '🔴' if action_color == 'red' else ('🟠' if action_color == 'orange' else ('🟡' if action_color == 'yellow' else '🔵'))
                    
                    table_data.append([
                        self._create_table_paragraph(full_name[:30] + '...' if len(full_name) > 30 else full_name),
                        self._create_table_paragraph(index_name[:20] + '...' if len(index_name) > 20 else index_name),
                        self._create_table_paragraph(f"{fragmentation:.1f}%"),
                        self._create_table_paragraph(f"{size_mb:.1f}"),
                        self._create_table_paragraph(f'{usage_emoji} {usage_category.replace("_", " ")}'),
                        self._create_table_paragraph(f'{action_emoji} {action}')
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
            story.append(Paragraph("Issues Found:", self.styles['SubHeader']))
            
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
        else:
            story.append(Paragraph("Status: Good", self.styles['SubHeader']))
            story.append(Paragraph("No missing indexes detected. Current indexing strategy appears adequate.", self.styles['ExecutiveSummary']))
            
        story.append(Spacer(1, 0.02*inch))
        
        # Missing indexes table (only if there are any)
        if missing_indexes:
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
                        self._create_table_paragraph(f'{"🔴" if status_color == "red" else ("🟠" if status_color == "orange" else ("🟡" if status_color == "yellow" else "🟢"))} {status}')
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
                
                # Use Paragraph objects with full text (no max_width limits for full content)
                table_data.append([
                    self._create_table_paragraph(priority_text, font_size=8),
                    self._create_table_paragraph(str(issue), font_size=8),      
                    self._create_table_paragraph(impact, font_size=8),
                    self._create_table_paragraph(str(recommendation), font_size=8)  
                ])
            
            # Table with optimized column widths for AI content
            # Priority: 0.8", Issue: 2.2", Impact: 0.7", Recommendation: 2.8"
            ai_column_widths = [0.8*inch, 2.2*inch, 0.7*inch, 2.8*inch]
            bottleneck_table = Table(table_data, colWidths=ai_column_widths)
            bottleneck_table.setStyle(self._get_ai_table_style())
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
                    self._create_table_paragraph(f'{"🔴" if status_color == "red" else ("🟠" if status_color == "orange" else "🟢")} {status}')
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
                    self._create_table_paragraph(f'{"🔴" if issues_color == "red" else ("🟠" if issues_color == "orange" else "🟢")} {issues}')
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
                    self._create_table_paragraph(f'{"🔴" if issues_color == "red" else ("🟠" if issues_color == "orange" else "🟢")} {issues}')
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
                    self._create_table_paragraph(f'{"🔴" if status_color == "red" else ("🟠" if status_color == "orange" else "🟢")} {status}')
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
        story.append(Paragraph("📊 Performance Monitor Analysis", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.05*inch))
        
        if 'error' in perfmon_data:
            story.append(Paragraph(f"❌ Error analyzing Performance Monitor data: {perfmon_data['error']}", 
                                 self.styles['CustomBodyRed']))
            return story
        
        # Summary Information
        if 'summary' in perfmon_data:
            summary = perfmon_data['summary']
            story.append(Paragraph("📈 Collection Summary", self.styles['SubHeader']))
            
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
            story.append(Paragraph("🚨 Performance Bottlenecks", self.styles['SubHeader']))
            
            bottleneck_data = [['Category', 'Severity', 'Description']]
            
            for bottleneck in perfmon_data['bottlenecks']:
                severity = bottleneck.get('severity', 'UNKNOWN')
                if severity == 'CRITICAL':
                    severity_text = '🔴 CRITICAL'
                elif severity == 'WARNING':
                    severity_text = '🟠 WARNING'
                else:
                    severity_text = f'🟢 {severity}'
                
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
            story.append(Paragraph("🖥️ CPU Performance", self.styles['SubHeader']))
            
            cpu_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in cpu:
                metrics = cpu['metrics']
                status = cpu.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_processor_time' in metrics:
                    status_emoji = '🔴' if status == 'CRITICAL' else ('🟠' if status == 'WARNING' else '🟢')
                    cpu_data.append([
                        'Average CPU Usage',
                        f"{metrics['avg_processor_time']}%",
                        f'{status_emoji} {status}'
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
            story.append(Paragraph("🧠 Memory Performance", self.styles['SubHeader']))
            
            memory_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in memory:
                metrics = memory['metrics']
                status = memory.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_available_mb' in metrics:
                    status_emoji = '🔴' if status == 'CRITICAL' else ('🟠' if status == 'WARNING' else '🟢')
                    memory_data.append([
                        'Average Available Memory',
                        f"{metrics['avg_available_mb']:,.0f} MB",
                        f'{status_emoji} {status}'
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
            story.append(Paragraph("💾 Disk Performance", self.styles['SubHeader']))
            
            disk_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in disk:
                metrics = disk['metrics']
                status = disk.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_disk_queue_length' in metrics:
                    status_emoji = '🔴' if status == 'CRITICAL' else ('🟠' if status == 'WARNING' else '🟢')
                    disk_data.append([
                        'Average Disk Queue Length',
                        str(metrics['avg_disk_queue_length']),
                        f'{status_emoji} {status}'
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
            story.append(Paragraph("🗃️ SQL Server Performance", self.styles['SubHeader']))
            
            sql_data = [['Metric', 'Value', 'Status']]
            
            if 'metrics' in sql:
                metrics = sql['metrics']
                status = sql.get('status', 'OK')
                status_color = 'red' if status == 'CRITICAL' else ('orange' if status == 'WARNING' else 'green')
                
                if 'avg_batch_requests_per_sec' in metrics:
                    status_emoji = '🔴' if status == 'CRITICAL' else ('🟠' if status == 'WARNING' else '🟢')
                    sql_data.append([
                        'Average Batch Requests/sec',
                        str(metrics['avg_batch_requests_per_sec']),
                        f'{status_emoji} {status}'
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
            story.append(Paragraph("💡 Performance Recommendations", self.styles['SubHeader']))
            
            for i, recommendation in enumerate(perfmon_data['recommendations'][:10], 1):  # Limit to 10
                story.append(Paragraph(f"{i}. {recommendation}", self.styles['NormalText']))
            
            story.append(Spacer(1, 0.05*inch))
        
        # AI Analysis Section for PerfMon
        if 'ai_analysis' in perfmon_data:
            story.append(Paragraph("🤖 AI Performance Analysis", self.styles['SubHeader']))
            ai_data = perfmon_data['ai_analysis']
            
            if 'summary' in ai_data:
                story.append(Paragraph(f"📋 {ai_data['summary']}", self.styles['NormalText']))
                story.append(Spacer(1, 0.03*inch))
            
            # AI Bottlenecks
            if 'bottlenecks' in ai_data and ai_data['bottlenecks']:
                story.append(Paragraph("🎯 AI-Identified Bottlenecks", self.styles['NormalText']))
                
                ai_bottleneck_data = [['Component', 'Severity', 'Root Cause', 'Recommendation']]
                
                for bottleneck in ai_data['bottlenecks'][:5]:  # Limit to top 5
                    component = bottleneck.get('component', 'Unknown')
                    severity = bottleneck.get('severity', 'UNKNOWN')
                    root_cause = bottleneck.get('root_cause', 'Not specified')
                    recommendation = bottleneck.get('recommendation', 'No recommendation')
                    
                    # Color code severity with plain text (no HTML tags)
                    if severity == 'CRITICAL':
                        severity_text = '🔴 CRITICAL'
                    elif severity == 'WARNING':
                        severity_text = '🟠 WARNING'
                    else:
                        severity_text = f'🟢 {severity}'
                    
                    # Use enhanced table paragraphs with full text (no truncation)
                    ai_bottleneck_data.append([
                        self._create_table_paragraph(component, font_size=8),
                        self._create_table_paragraph(severity_text, font_size=8),
                        self._create_table_paragraph(root_cause, font_size=8),
                        self._create_table_paragraph(recommendation, font_size=8)
                    ])
                
                # Use optimized column widths for PerfMon AI table
                perfmon_ai_widths = [1.0*inch, 1.0*inch, 2.0*inch, 2.5*inch]
                table = Table(ai_bottleneck_data, colWidths=perfmon_ai_widths)
                table.setStyle(self._get_ai_table_style())
                story.append(table)
                story.append(Spacer(1, 0.05*inch))
            
            # Correlation Analysis
            if 'correlation_analysis' in ai_data:
                story.append(Paragraph("🔗 Cross-Component Analysis", self.styles['NormalText']))
                story.append(Paragraph(ai_data['correlation_analysis'], self.styles['NormalText']))
                story.append(Spacer(1, 0.05*inch))
        
        return story

    def _create_log_analysis_section(self, log_data: Dict[str, Any]) -> List:
        """Create log analysis section for PDF report
        
        Args:
            log_data: Log analysis results
            
        Returns:
            List of reportlab elements
        """
        story = []
        
        # Section header
        story.append(Paragraph("📋 Log Analysis (Last 7 Days)", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.05*inch))
        
        if log_data.get('error'):
            story.append(Paragraph(f"⚠️ Error: {log_data['error']}", self.styles['NormalText']))
            story.append(Spacer(1, 0.05*inch))
            return story
        
        # Summary section
        if 'summary' in log_data:
            summary = log_data['summary']
            story.append(Paragraph("📊 Summary", self.styles['SubHeader']))
            
            summary_text = f"""
            • SQL Server Entries: {summary.get('total_sql_entries', 0):,}
            • Critical Errors (Severity ≥ 16): {summary.get('critical_sql_errors', 0):,}
            • Performance Issue Categories: {summary.get('performance_issue_categories', 0)}
            • Windows Events: {summary.get('total_windows_events', 0):,}
            • Event Categories: {summary.get('windows_categories', 0)}
            """
            story.append(Paragraph(summary_text, self.styles['NormalText']))
            story.append(Spacer(1, 0.05*inch))
        
        # SQL Server Error Log Analysis
        if 'sql_server_errors' in log_data:
            sql_errors = log_data['sql_server_errors']
            story.append(Paragraph("🔴 SQL Server Error Log Analysis", self.styles['SubHeader']))
            
            # Critical errors table
            critical_errors = sql_errors.get('critical_errors', [])
            if critical_errors:
                story.append(Paragraph(f"Critical Errors ({len(critical_errors)} found):", self.styles['NormalText']))
                
                # Create table for critical errors (show first 10)
                critical_data = [['Date/Time', 'Severity', 'Error #', 'Description']]
                for error in critical_errors[:10]:  # Limit to first 10
                    date_str = error.get('log_date', '').strftime('%m/%d %H:%M') if error.get('log_date') else 'Unknown'
                    severity = error.get('severity', 0)
                    error_num = error.get('error_number', 0)
                    description = error.get('text', '')[:80] + '...' if len(error.get('text', '')) > 80 else error.get('text', '')
                    
                    critical_data.append([
                        self._create_table_paragraph(date_str),
                        self._create_table_paragraph(f"🔴 {severity}"),
                        self._create_table_paragraph(str(error_num) if error_num else 'N/A'),
                        self._create_table_paragraph(description)
                    ])
                
                critical_table = Table(critical_data, colWidths=self._get_responsive_column_widths(4))
                critical_table.setStyle(self._get_modern_table_style())
                story.append(critical_table)
                story.append(Spacer(1, 0.05*inch))
            
            # Performance issues breakdown
            performance_issues = sql_errors.get('performance_issues', {})
            if performance_issues:
                story.append(Paragraph("⚡ Performance Issues Detected:", self.styles['NormalText']))
                
                perf_data = [['Issue Type', 'Count', 'Latest Occurrence', 'Impact']]
                
                impact_map = {
                    'deadlocks': ('🔴', 'HIGH - Query blocking'),
                    'io_errors': ('🔴', 'HIGH - Disk subsystem'),
                    'autogrow': ('🟠', 'MEDIUM - Performance spikes'),
                    'memory': ('🟠', 'MEDIUM - Resource pressure'),
                    'connectivity': ('🟡', 'LOW - User experience'),
                    'corruption': ('🔴', 'CRITICAL - Data integrity')
                }
                
                for issue_type, issues in performance_issues.items():
                    if issues:
                        emoji, impact = impact_map.get(issue_type, ('🔵', 'INFO'))
                        latest = max(issues, key=lambda x: x.get('log_date', datetime.min))
                        latest_date = latest.get('log_date', '').strftime('%m/%d %H:%M') if latest.get('log_date') else 'Unknown'
                        
                        perf_data.append([
                            self._create_table_paragraph(issue_type.replace('_', ' ').title()),
                            self._create_table_paragraph(f"{emoji} {len(issues)}"),
                            self._create_table_paragraph(latest_date),
                            self._create_table_paragraph(impact)
                        ])
                
                if len(perf_data) > 1:  # More than just header
                    perf_table = Table(perf_data, colWidths=self._get_responsive_column_widths(4))
                    perf_table.setStyle(self._get_modern_table_style())
                    story.append(perf_table)
                    story.append(Spacer(1, 0.05*inch))
        
        # Windows Event Log Analysis
        if 'windows_events' in log_data:
            windows_events = log_data['windows_events']
            categorized = windows_events.get('categorized_events', {})
            
            if categorized:
                story.append(Paragraph("🪟 Windows Event Log Analysis", self.styles['SubHeader']))
                
                event_data = [['Category', 'Count', 'Severity', 'Description']]
                
                category_descriptions = {
                    'disk_errors': ('🔴', 'Hardware/storage failures detected'),
                    'storage_warnings': ('🟠', 'Storage performance degradation'),
                    'sql_service_issues': ('🟠', 'SQL Server service problems'),
                    'performance_warnings': ('🟡', 'System performance concerns'),
                    'other': ('🔵', 'General system events')
                }
                
                for category, events in categorized.items():
                    if events:
                        emoji, description = category_descriptions.get(category, ('🔵', 'System events'))
                        
                        event_data.append([
                            self._create_table_paragraph(category.replace('_', ' ').title()),
                            self._create_table_paragraph(f"{len(events)}"),
                            self._create_table_paragraph(emoji),
                            self._create_table_paragraph(description)
                        ])
                
                if len(event_data) > 1:  # More than just header
                    event_table = Table(event_data, colWidths=self._get_responsive_column_widths(4))
                    event_table.setStyle(self._get_modern_table_style())
                    story.append(event_table)
                    story.append(Spacer(1, 0.05*inch))
        
        # Recommendations
        if 'recommendations' in log_data:
            recommendations = log_data['recommendations']
            if recommendations:
                story.append(Paragraph("💡 Log Analysis Recommendations", self.styles['SubHeader']))
                
                for i, recommendation in enumerate(recommendations[:8], 1):  # Limit to 8 recommendations
                    story.append(Paragraph(f"{i}. {recommendation}", self.styles['NormalText']))
                
                story.append(Spacer(1, 0.05*inch))
        
        return story
