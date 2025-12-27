"""
Custom exception types for SQL Speedinator
Provides structured error handling with specific exception categories
"""

from typing import Optional, Dict, Any


class SQLSpeedError(Exception):
    """Base exception for all SQL Speedinator errors"""
    
    def __init__(self, message: str, error_code: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON serialization"""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'error_code': self.error_code,
            'context': self.context
        }


class DatabaseError(SQLSpeedError):
    """Database connection or query execution error"""
    pass


class DatabaseConnectionError(DatabaseError):
    """Specific error when connecting to database"""
    pass


class DatabaseQueryError(DatabaseError):
    """Error executing SQL query"""
    pass


class AnalysisError(SQLSpeedError):
    """Analysis execution error"""
    pass


class AIError(SQLSpeedError):
    """AI service error"""
    pass


class AIServiceUnavailableError(AIError):
    """AI service is temporarily unavailable"""
    pass


class AIProcessingError(AIError):
    """Error processing data with AI"""
    pass


class ConfigError(SQLSpeedError):
    """Configuration error"""
    pass


class ConfigValidationError(ConfigError):
    """Configuration validation error"""
    pass


class PerfmonError(SQLSpeedError):
    """Performance Monitor error"""
    pass


class PerfmonTemplateError(PerfmonError):
    """Performance Monitor template error"""
    pass


class PerfmonCollectionError(PerfmonError):
    """Performance Monitor data collection error"""
    pass


class ReportGenerationError(SQLSpeedError):
    """Report generation error"""
    pass


class PDFGenerationError(ReportGenerationError):
    """PDF report generation error"""
    pass
