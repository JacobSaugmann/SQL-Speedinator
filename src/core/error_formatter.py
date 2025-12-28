"""
User-friendly error message formatting

Provides clean, actionable error messages for end users while logging detailed
technical information for debugging.
"""

import logging
from typing import Optional, Tuple
from pathlib import Path


class UserFriendlyError:
    """Formats errors for user display while preserving technical details in logs"""
    
    # Error type patterns with user-friendly messages
    ERROR_PATTERNS = {
        'sql_connection': {
            'patterns': ['SQL', 'ODBC', 'Connection', 'driver', 'IM002', 'authenticate'],
            'user_message': "Unable to connect to SQL Server '{server}'.\n\nPlease verify:\n"
                          "  • Server name is correct (e.g., 'SERVERNAME' or 'SERVERNAME\\INSTANCENAME')\n"
                          "  • SQL Server service is running\n"
                          "  • Network connectivity to the server\n"
                          "  • Your login credentials and permissions",
            'help_url': 'https://docs.microsoft.com/en-us/sql/tools/configuration-manager/'
        },
        'authentication': {
            'patterns': ['Login failed', 'Authentication', 'credentials', 'permissions', 'access denied'],
            'user_message': "Authentication failed for SQL Server '{server}'.\n\n"
                          "Please verify:\n"
                          "  • Your Windows login has database access\n"
                          "  • You have permission to read system views\n"
                          "  • SQL authentication username and password are correct",
            'help_url': 'https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/authentication-mode'
        },
        'timeout': {
            'patterns': ['timeout', 'Timeout', 'TIMEOUT', 'did not respond', 'deadlock'],
            'user_message': "SQL Server connection timed out for '{server}'.\n\n"
                          "Possible causes:\n"
                          "  • Server is overloaded or unresponsive\n"
                          "  • Network connectivity issue\n"
                          "  • Query taking too long\n"
                          "  • Firewall blocking connection",
            'help_url': None
        },
        'database': {
            'patterns': ['database', 'table', 'column', 'schema', 'not found', 'does not exist'],
            'user_message': "Database access error for '{server}'.\n\n"
                          "Please verify:\n"
                          "  • Required system databases exist (master, msdb)\n"
                          "  • No conflicting locks or blocking queries\n"
                          "  • Database integrity is intact",
            'help_url': None
        },
        'permission': {
            'patterns': ['permission', 'denied', 'privilege', 'unauthorized', 'insufficient'],
            'user_message': "Insufficient permissions to analyze '{server}'.\n\n"
                          "Required:\n"
                          "  • View Server State permission\n"
                          "  • Read access to DMVs (Dynamic Management Views)\n"
                          "  • At least db_reader or equivalent role",
            'help_url': None
        },
        'perfmon': {
            'patterns': ['PerfMon', 'Performance Monitor', 'logman', 'counter', 'blg'],
            'user_message': "Performance Monitor data collection failed.\n\n"
                          "Please verify:\n"
                          "  • Windows Performance Monitor is installed\n"
                          "  • You have administrator privileges\n"
                          "  • Performance counter service is running",
            'help_url': None
        },
        'config': {
            'patterns': ['.env', 'configuration', 'config', 'invalid', 'missing'],
            'user_message': "Configuration error.\n\n"
                          "Please verify:\n"
                          "  • .env file exists with required settings\n"
                          "  • All configuration values are valid\n"
                          "  • File permissions allow reading",
            'help_url': None
        }
    }
    
    @staticmethod
    def categorize_error(error_message: str) -> Tuple[str, Optional[str]]:
        """Categorize error and return pattern key and user message
        
        Args:
            error_message: The error message to categorize
            
        Returns:
            Tuple of (category_key, user_message) or (None, None) if unknown
        """
        error_lower = error_message.lower()
        
        for category, config in UserFriendlyError.ERROR_PATTERNS.items():
            for pattern in config['patterns']:
                if pattern.lower() in error_lower:
                    return category, config['user_message']
        
        return None, None
    
    @staticmethod
    def format_for_user(error_message: str, server: Optional[str] = None) -> str:
        """Format error message for user display
        
        Args:
            error_message: The technical error message
            server: Optional server name to include in message
            
        Returns:
            User-friendly error message
        """
        category, template = UserFriendlyError.categorize_error(error_message)
        
        if template:
            if server:
                return template.format(server=server)
            return template
        
        # Fallback for unknown errors
        return f"An error occurred: {error_message[:100]}...\n\n" \
               f"Please check the log file for detailed information."
    
    @staticmethod
    def log_and_display(logger: logging.Logger, error: Exception, 
                       server: Optional[str] = None, 
                       context: Optional[str] = None) -> str:
        """Log technical error and return user-friendly message
        
        Args:
            logger: Logger instance
            error: Exception object
            server: Optional server name
            context: Optional context description
            
        Returns:
            User-friendly error message to display
        """
        error_msg = str(error)
        
        # Log full technical details with stack trace
        if context:
            logger.error(f"{context}: {error_msg}", exc_info=True)
        else:
            logger.error(f"Error: {error_msg}", exc_info=True)
        
        # Return user-friendly message
        return UserFriendlyError.format_for_user(error_msg, server)


class ErrorContext:
    """Context manager for error handling with friendly messages"""
    
    def __init__(self, logger: logging.Logger, operation: str, server: Optional[str] = None):
        """Initialize error context
        
        Args:
            logger: Logger instance
            operation: Description of operation being attempted
            server: Optional server name involved
        """
        self.logger = logger
        self.operation = operation
        self.server = server
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            user_message = UserFriendlyError.log_and_display(
                self.logger,
                exc_val,
                self.server,
                self.operation
            )
            # Return False to propagate exception but log it nicely
            return False
        return False


def create_error_report(error: Exception, server: Optional[str] = None) -> dict:
    """Create structured error report for logging
    
    Args:
        error: Exception object
        server: Optional server name
        
    Returns:
        Dictionary with error details
    """
    return {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'server': server,
        'category': UserFriendlyError.categorize_error(str(error))[0]
    }
