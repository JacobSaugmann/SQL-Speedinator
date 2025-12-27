"""
Text formatting and cleaning utilities for PDF reports and data processing.

This module centralizes all text manipulation operations including:
- Text wrapping for tables and display
- HTML tag removal and cleaning
- Number formatting
- Dictionary/list text cleaning

All text processing should go through TextFormatter for consistent handling
and single point of modification.
"""

import re
import logging
from typing import Any, Dict, List, Union


class TextFormatter:
    """
    Centralized text formatting and cleaning operations.
    
    Handles all text manipulation tasks including wrapping, cleaning HTML tags,
    and formatting numbers. This class ensures consistent text handling across
    the application and serves as a single source of truth for text operations.
    
    Benefits:
    - Single responsibility: All text operations in one place
    - DRY principle: Eliminates duplicate text processing code
    - Easy to maintain: Changes to text handling affect all code
    - Testable: All text operations can be tested independently
    - Reusable: Both PDF reports and AI services use same logic
    """
    
    def __init__(self):
        """Initialize TextFormatter with logging."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TextFormatter initialized")
    
    # =======================
    # Text Wrapping Methods
    # =======================
    
    def wrap_text(self, text: str, max_length: int = 30) -> str:
        """
        Wrap text to specified maximum length with line breaks.
        
        Useful for displaying long text in table cells or constrained spaces.
        Preserves word boundaries by breaking at spaces rather than mid-word.
        
        Args:
            text: Text to wrap
            max_length: Maximum characters per line (default 30)
            
        Returns:
            Text with line breaks inserted, or original if shorter than max_length
            
        Example:
            >>> formatter = TextFormatter()
            >>> long_text = "This is a very long text that needs wrapping"
            >>> wrapped = formatter.wrap_text(long_text, 20)
            >>> print(wrapped)
            This is a very long
            text that needs
            wrapping
        """
        if not text or len(text) <= max_length:
            return text
        
        words = str(text).split()
        lines = []
        current_line = []
        current_length = 0
        
        for word in words:
            word_len = len(word)
            if current_length + word_len + 1 > max_length and current_line:
                lines.append(' '.join(current_line))
                current_line = [word]
                current_length = word_len
            else:
                current_line.append(word)
                current_length += word_len + 1
        
        if current_line:
            lines.append(' '.join(current_line))
        
        return '\n'.join(lines)
    
    # =======================
    # Text Cleaning Methods
    # =======================
    
    def clean_text(self, text: str) -> str:
        """
        Remove HTML tags, XML artifacts, and special characters from text.
        
        Performs comprehensive cleaning of text that may contain malformed HTML,
        XML entities, and other artifacts. Used primarily for processing
        database output and AI responses.
        
        Removes:
        - HTML/XML tags (e.g., <tag>)
        - HTML entities (e.g., &nbsp;, &#65;)
        - XML artifacts
        - Excess whitespace
        - Special characters like < > &
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text safe for display in PDF
            
        Example:
            >>> formatter = TextFormatter()
            >>> dirty = "Text with <b>HTML</b> and &nbsp; entities"
            >>> clean = formatter.clean_text(dirty)
            >>> print(clean)
            Text with HTML and entities
        """
        text_str = str(text).strip()
        
        # Remove all HTML/XML tags
        text_str = re.sub(r'<[^>]*>', '', text_str)
        
        # Remove HTML entities
        text_str = re.sub(r'&[a-zA-Z0-9#]+;', '', text_str)
        
        # Remove any remaining XML artifacts
        text_str = re.sub(r'</?\w+[^>]*>', '', text_str)
        
        # Clean up whitespace
        text_str = re.sub(r'\s+', ' ', text_str)
        text_str = text_str.strip()
        
        # Final cleanup - remove stray brackets and ampersands
        text_str = text_str.replace('<', '').replace('>', '').replace('&', 'and')
        
        return text_str
    
    def clean_html_tags(self, text: str) -> str:
        """
        Remove malformed HTML tags from AI responses.
        
        Specifically designed to handle malformed tags from AI model outputs
        like '>green>', '>red>', '>orange>' which should be converted to
        meaningful indicators.
        
        Conversion map:
        - >green> → 🟢 OK
        - >red> → 🔴 CRITICAL
        - >orange> → 🟠 WARNING
        - >yellow> → 🟡 CAUTION
        
        Args:
            text: Text potentially containing malformed HTML tags
            
        Returns:
            Text with malformed tags cleaned
            
        Example:
            >>> formatter = TextFormatter()
            >>> text = "Status: >green> All systems operational"
            >>> clean = formatter.clean_html_tags(text)
            >>> print(clean)
            Status: 🟢 OK All systems operational
        """
        if not isinstance(text, str):
            return text
        
        # Fix malformed tags like '>green>', '>red>', '>orange>'
        text = re.sub(r'>green>', '🟢 OK', text)
        text = re.sub(r'>red>', '🔴 CRITICAL', text)
        text = re.sub(r'>orange>', '🟠 WARNING', text)
        text = re.sub(r'>yellow>', '🟡 CAUTION', text)
        
        # Remove any other malformed > tags (keep content)
        text = re.sub(r'>(\w+)>', r'\1', text)
        
        return text
    
    def clean_dict_recursively(self, obj: Any) -> Any:
        """
        Recursively clean HTML tags in dictionary/list structures.
        
        Traverses nested dictionaries and lists, cleaning HTML tags from
        all string values. Useful for processing AI responses that may
        contain HTML artifacts in nested data structures.
        
        Args:
            obj: Dictionary, list, string, or other value to clean
            
        Returns:
            Same structure with all string values cleaned
            
        Example:
            >>> formatter = TextFormatter()
            >>> data = {
            ...     'status': '>green>',
            ...     'items': ['>red>', '>orange>'],
            ...     'nested': {'text': '>yellow>'}
            ... }
            >>> clean = formatter.clean_dict_recursively(data)
            >>> print(clean['status'])
            🟢 OK
        """
        if isinstance(obj, dict):
            return {k: self.clean_dict_recursively(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_dict_recursively(item) for item in obj]
        elif isinstance(obj, str):
            return self.clean_html_tags(obj)
        else:
            return obj
    
    # =======================
    # Number Formatting Methods
    # =======================
    
    def format_number(self, value: Any, format_type: str = 'default') -> str:
        """
        Format numbers with appropriate styling and units.
        
        Converts numeric values to human-readable format with appropriate
        units. Supports percentage, byte sizes, and generic numeric formatting.
        
        Format types:
        - 'percentage': Decimal with 1 decimal place and % symbol
        - 'bytes': Size in B/KB/MB/GB/TB with auto-scaling
        - 'default': Generic number with thousand separators
        
        Args:
            value: Numeric value to format
            format_type: Type of formatting ('percentage', 'bytes', 'default')
            
        Returns:
            Formatted string representation
            
        Example:
            >>> formatter = TextFormatter()
            >>> formatter.format_number(45.678, 'percentage')
            '45.7%'
            >>> formatter.format_number(1048576, 'bytes')
            '1.0 MB'
            >>> formatter.format_number(1234567.89, 'default')
            '1,234,567.89'
        """
        try:
            if format_type == 'percentage':
                return f"{float(value):.1f}%"
            elif format_type == 'bytes':
                num = float(value)
                for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                    if num < 1024:
                        return f"{num:.1f} {unit}"
                    num /= 1024
                return f"{num:.1f} PB"
            else:  # default
                return f"{float(value):,.2f}"
        except (ValueError, TypeError):
            self.logger.debug(f"Could not format value {value} as {format_type}, returning string")
            return str(value)
    
    # =======================
    # Utility Methods
    # =======================
    
    def truncate_text(self, text: str, max_length: int = 50, suffix: str = "...") -> str:
        """
        Truncate text to specified length with optional suffix.
        
        Useful for displaying long strings in limited space (e.g., table cells).
        
        Args:
            text: Text to truncate
            max_length: Maximum characters (not counting suffix)
            suffix: Suffix to add if truncated (default "...")
            
        Returns:
            Truncated text with suffix if longer than max_length
            
        Example:
            >>> formatter = TextFormatter()
            >>> long_text = "This is a very long text that will be truncated"
            >>> truncated = formatter.truncate_text(long_text, 20)
            >>> print(truncated)
            This is a very long ...
        """
        if len(text) <= max_length:
            return text
        return text[:max_length] + suffix
    
    def sanitize_for_sql(self, text: str) -> str:
        """
        Sanitize text for safe use in SQL queries (basic escaping).
        
        Escapes single quotes which are critical in SQL. For full SQL injection
        protection, always use parameterized queries instead.
        
        Args:
            text: Text to sanitize
            
        Returns:
            Text with single quotes escaped
            
        Note:
            This is a basic helper. Always use parameterized queries for
            real SQL protection against injection attacks.
        """
        return str(text).replace("'", "''")
