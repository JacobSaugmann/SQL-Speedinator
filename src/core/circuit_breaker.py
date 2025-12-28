"""
Circuit Breaker pattern implementation for AI service
Prevents cascading failures from unreliable external services
"""

import logging
import time
from enum import Enum
from typing import Callable, Optional, Any, TypeVar
from src.core.exceptions import AIServiceUnavailableError, AIError

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"        # Normal operation
    OPEN = "open"            # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker pattern for AI service
    
    Prevents cascading failures by:
    1. Stopping requests when service repeatedly fails
    2. Periodically testing if service recovered
    3. Failing fast with clear error message
    
    States:
    - CLOSED: Normal operation
    - OPEN: Service is failing, reject new requests
    - HALF_OPEN: Testing if service recovered
    """
    
    def __init__(self, name: str = "CircuitBreaker", 
                 failure_threshold: int = 3,
                 recovery_timeout: int = 60,
                 expected_exception: type = AIError):
        """Initialize circuit breaker
        
        Args:
            name: Circuit breaker name for logging
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds before attempting recovery
            expected_exception: Exception type that triggers circuit opening
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        
        self.logger = logging.getLogger(__name__)
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection
        
        Args:
            func: Function to call
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of function call
            
        Raises:
            AIServiceUnavailableError: If circuit is open
            Original exception: If function fails
        """
        if self.state == CircuitState.OPEN:
            # Check if timeout has passed to allow recovery attempt
            if self._should_attempt_recovery():
                self.state = CircuitState.HALF_OPEN
                self.logger.info(f"{self.name}: Entering HALF_OPEN state, testing recovery")
            else:
                # Circuit is still open, fail fast
                if self.last_failure_time:
                    remaining = self.recovery_timeout - \
                               (time.time() - self.last_failure_time)
                    raise AIServiceUnavailableError(
                        f"Service unavailable. Retry in {int(max(0, remaining))}s",
                        error_code="CIRCUIT_OPEN"
                    )
                else:
                    raise AIServiceUnavailableError(
                        "Circuit breaker is open",
                        error_code="CIRCUIT_OPEN"
                    )
        
        try:
            # Call the function
            result = func(*args, **kwargs)
            
            # On success, update state
            self._on_success()
            return result
            
        except self.expected_exception as e:
            # On expected exception, track failure
            self._on_failure()
            raise
        except Exception as e:
            # Unexpected exception, also track as failure
            self._on_failure()
            raise
    
    def _should_attempt_recovery(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if self.last_failure_time is None:
            return False
        
        time_since_failure = time.time() - self.last_failure_time
        return time_since_failure >= self.recovery_timeout
    
    def _on_success(self) -> None:
        """Handle successful call"""
        self.success_count += 1
        
        if self.state == CircuitState.HALF_OPEN:
            # Recovery successful, close circuit
            self.logger.info(f"{self.name}: Circuit CLOSED (service recovered)")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
    
    def _on_failure(self) -> None:
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            # Recovery failed, reopen circuit
            self.logger.warning(f"{self.name}: Circuit OPEN (recovery failed)")
            self.state = CircuitState.OPEN
            self.failure_count = 0
        
        elif self.failure_count >= self.failure_threshold:
            # Threshold reached, open circuit
            self.logger.warning(
                f"{self.name}: Circuit OPEN after {self.failure_count} failures"
            )
            self.state = CircuitState.OPEN
    
    @property
    def current_state(self) -> str:
        """Get current circuit state"""
        return self.state.value
    
    def reset(self) -> None:
        """Manually reset circuit breaker"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.logger.info(f"{self.name}: Manually reset")
