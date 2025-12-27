"""
Unified result wrapper for analysis operations
Provides consistent error/success handling across all analyzers
"""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, Generic, TypeVar

T = TypeVar('T')


@dataclass
class AnalysisResult(Generic[T]):
    """Unified result object for analysis operations
    
    Ensures consistent handling of success/error cases across all analyzers.
    Supports graceful fallback when optional features (like AI) are unavailable.
    """
    
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    retry_available: bool = False
    partial: bool = False  # True if data is incomplete but valid
    
    def is_error(self) -> bool:
        """Check if result represents an error"""
        return not self.success
    
    def is_partial(self) -> bool:
        """Check if result is partial (some data available despite errors)"""
        return self.partial
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for JSON serialization"""
        result_dict = asdict(self)
        return {k: v for k, v in result_dict.items() if v is not None}
    
    @classmethod
    def success_result(cls, data: T, partial: bool = False) -> 'AnalysisResult[T]':
        """Create successful result"""
        return cls(success=True, data=data, partial=partial)
    
    @classmethod
    def error_result(cls, error: str, error_type: str = 'unknown',
                    retry_available: bool = False) -> 'AnalysisResult[T]':
        """Create error result"""
        return cls(success=False, error=error, error_type=error_type,
                  retry_available=retry_available)
    
    @classmethod
    def partial_result(cls, data: T, error: str, error_type: str = 'unknown'
                      ) -> 'AnalysisResult[T]':
        """Create partial result (data available despite errors)"""
        return cls(success=False, data=data, partial=True, error=error,
                  error_type=error_type)


@dataclass
class QueryResult:
    """Result wrapper for SQL queries
    
    Similar to AnalysisResult but optimized for query operations
    """
    
    success: bool
    rows: Optional[list] = None
    row_count: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None
    retry_available: bool = False
    
    def is_error(self) -> bool:
        return not self.success
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'row_count': self.row_count,
            'error': self.error,
            'error_type': self.error_type,
            'retry_available': self.retry_available
        }
    
    @classmethod
    def success(cls, rows: list) -> 'QueryResult':
        return cls(success=True, rows=rows, row_count=len(rows))
    
    @classmethod
    def error(cls, error: str, error_type: str = 'unknown',
             retry_available: bool = False) -> 'QueryResult':
        return cls(success=False, error=error, error_type=error_type,
                  retry_available=retry_available)
