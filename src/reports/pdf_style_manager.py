"""
PDF Style Manager
Centralized management of all PDF styling, colors, and formatting for SQL Speedinator reports

Extracted from PDFReportGenerator to improve maintainability and testability.
Responsibility: ALL styling, color management, and paragraph styles
Text formatting delegated to TextFormatter for centralized text operations
"""

import logging
from typing import Dict, List, Optional, Any
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph
from reportlab.lib.enums import TA_CENTER, TA_LEFT

try:
    from .text_formatter import TextFormatter
except ImportError:
    from text_formatter import TextFormatter


class PDFStyleManager:
    """
    Manages all PDF styling, colors, text formatting, and paragraph styles.
    
    This class encapsulates all styling logic that was previously embedded in
    PDFReportGenerator, making styles reusable, testable, and maintainable.
    
    Usage:
        style_manager = PDFStyleManager()
        title_style = style_manager.get_style('CustomTitle')
        purple_color = style_manager.get_color('purple')
    """
    
    def __init__(self):
        """Initialize PDF style manager with all Schultz branding colors and styles"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.text_formatter = TextFormatter()
        self.styles = getSampleStyleSheet()
        self.schultz_colors = self._setup_schultz_colors()
        self._setup_compact_styles()
        self.logger.info("PDF style manager initialized with Schultz branding and TextFormatter")
    
    def get_style(self, name: str) -> ParagraphStyle:
        """
        Get a paragraph style by name.
        
        Args:
            name: Style name (e.g., 'CustomTitle', 'SectionHeader', 'NormalText')
            
        Returns:
            ParagraphStyle object
            
        Raises:
            KeyError: If style name doesn't exist
        """
        if name not in self.styles:
            self.logger.warning(f"Style '{name}' not found, returning Normal style")
            return self.styles['Normal']
        return self.styles[name]
    
    def get_color(self, name: str) -> colors.HexColor:
        """
        Get a color by name from Schultz color palette.
        
        Args:
            name: Color name (e.g., 'purple', 'cyan', 'dark_blue')
            
        Returns:
            HexColor object
            
        Raises:
            KeyError: If color name doesn't exist
        """
        if name not in self.schultz_colors:
            self.logger.warning(f"Color '{name}' not found, returning dark gray")
            return self.schultz_colors['dark_gray']
        return self.schultz_colors[name]
    
    def get_all_colors(self) -> Dict[str, colors.HexColor]:
        """Get all available colors in the Schultz palette"""
        return self.schultz_colors.copy()
    
    def get_all_styles(self) -> Dict[str, ParagraphStyle]:
        """Get all available paragraph styles"""
        # Return only our custom styles that are actually defined
        return {
            'CustomTitle': self.styles['CustomTitle'],
            'SectionHeader': self.styles['SectionHeader'],
            'SubHeader': self.styles['SubHeader'],
            'HighPriority': self.styles['HighPriority'],
            'MediumPriority': self.styles['MediumPriority'],
            'LowPriority': self.styles['LowPriority'],
            'ExecutiveSummary': self.styles['ExecutiveSummary'],
            'Footer': self.styles['Footer'],
            'NormalText': self.styles['NormalText'],
            'KeepTogetherSection': self.styles['KeepTogetherSection'],
            'KeepTogetherSub': self.styles['KeepTogetherSub'],
            'Normal': self.styles['Normal'],  # Fallback style
        }
    
    # ========== Color Management ==========
    
    def _setup_schultz_colors(self) -> Dict[str, colors.HexColor]:
        """
        Setup complete Schultz corporate color palette.
        
        Returns:
            Dictionary of color name -> HexColor mappings
        """
        return {
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
    
    # ========== Paragraph Style Setup ==========
    
    def _setup_compact_styles(self):
        """Setup all compact paragraph styles with Schultz branding"""
        
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
    
    # ========== Table Utilities ==========
    
    def get_responsive_column_widths(self, num_columns: int, page_width: float = 6.5) -> List[float]:
        """
        Calculate responsive column widths that fit exactly within page margins.
        
        Args:
            num_columns: Number of columns
            page_width: Page width in inches (default 6.5 for standard margins)
            
        Returns:
            List of column widths in points
        """
        if num_columns <= 0:
            return []
        
        col_width = page_width / num_columns
        return [col_width * inch] * num_columns
    
    def get_optimized_column_widths(self, num_columns: int, priority_weights: Optional[List[float]] = None) -> List[float]:
        """
        Get optimized column widths for specific table types with exact page fit.
        
        Args:
            num_columns: Number of columns
            priority_weights: Optional list of weights for custom column sizing
            
        Returns:
            List of column widths in points
        """
        total_width = 6.5  # inches - use full page width within margins
        
        if priority_weights and len(priority_weights) == num_columns:
            total_weight = sum(priority_weights)
            return [(weight/total_weight * total_width)*inch for weight in priority_weights]
        
        col_width = total_width / num_columns
        return [col_width*inch] * num_columns
    
    # ========== Text Formatting ==========
    
    def create_table_paragraph(self, text: str, font_size: int = 8, max_width: int = 150) -> Paragraph:
        """
        Create a paragraph object for table cells with proper text wrapping.
        
        Args:
            text: Cell text content
            font_size: Font size for the text
            max_width: Maximum width in points (deprecated, kept for compatibility)
            
        Returns:
            Paragraph object suitable for table cells
        """
        if text is None:
            text = ""
        
        cell_style = ParagraphStyle(
            name='TableCell',
            fontName='Helvetica',
            fontSize=font_size,
            leading=font_size + 2,
            leftIndent=2,
            rightIndent=2,
            spaceAfter=2,
            spaceBefore=2,
            alignment=TA_LEFT,
            wordWrap='CJK',
            allowWidows=1,
            allowOrphans=1
        )
        
        text_str = self._clean_text(str(text))
        return Paragraph(text_str, cell_style)
    
    def create_table_header(self, headers: List[str], font_size: int = 9) -> List[Paragraph]:
        """
        Create table header row with Paragraph objects for proper formatting.
        
        Args:
            headers: List of header text
            font_size: Font size for headers
            
        Returns:
            List of Paragraph objects
        """
        return [self._create_table_header_cell(header, font_size) for header in headers]
    
    def _create_table_header_cell(self, text: str, font_size: int = 9) -> Paragraph:
        """Create a single table header cell"""
        header_style = ParagraphStyle(
            name='TableHeader',
            fontName='Helvetica-Bold',
            fontSize=font_size,
            leading=font_size + 2,
            leftIndent=0,
            alignment=TA_LEFT,
            textColor=colors.white
        )
        
        text_str = self._clean_text(str(text))
        return Paragraph(text_str, header_style)
    
    def wrap_text(self, text: str, max_length: int) -> str:
        """
        Wrap text to specified maximum length.
        
        Delegates to TextFormatter for centralized text handling.
        
        Args:
            text: Text to wrap
            max_length: Maximum characters per line
            
        Returns:
            Wrapped text with newlines
        """
        return self.text_formatter.wrap_text(text, max_length)
    
    def wrap_text_for_table(self, text: str, max_length: int = 30) -> str:
        """
        Wrap text for table cells with reasonable defaults.
        
        Args:
            text: Text to wrap
            max_length: Maximum characters per line (default 30 for tables)
            
        Returns:
            Wrapped text
        """
        return self.wrap_text(text, max_length)
    
    def _clean_text(self, text: str) -> str:
        """
        Clean text by removing HTML tags and special characters.
        
        Delegates to TextFormatter for centralized text cleaning.
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text safe for PDF
        """
        return self.text_formatter.clean_text(text)
    
    def clean_text(self, text: str) -> str:
        """
        Clean text by removing HTML tags and special characters.
        
        Public method that delegates to TextFormatter.
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text safe for PDF
        """
        return self.text_formatter.clean_text(text)
    
    def format_number(self, value: Any, format_type: str = 'default') -> str:
        """
        Format numbers with appropriate styling.
        
        Delegates to TextFormatter for centralized number formatting.
        
        Args:
            value: Numeric value to format
            format_type: 'percentage', 'bytes', 'default'
            
        Returns:
            Formatted string
        """
        return self.text_formatter.format_number(value, format_type)
