"""
Server Performance Protection System
Monitors server load during analysis and protects against performance impact
"""

import logging
import time
import threading
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field

@dataclass
class PerformanceThresholds:
    """Performance thresholds for monitoring"""
    max_cpu_percent: float = 80.0
    max_wait_time_ms: float = 1000.0
    max_blocking_sessions: int = 5
    max_analysis_duration_minutes: int = 30
    check_interval_seconds: int = 10
    violation_count_threshold: int = 3

@dataclass 
class PerformanceMetrics:
    """Current performance metrics"""
    timestamp: datetime
    cpu_percent: float
    avg_wait_time_ms: float
    blocking_sessions: int
    active_requests: int
    analysis_duration_minutes: float

@dataclass
class ProtectionStatus:
    """Protection system status"""
    is_monitoring: bool = False
    is_analysis_safe: bool = True
    violation_count: int = 0
    last_violation: Optional[datetime] = None
    metrics_history: list = field(default_factory=list)
    abort_reason: Optional[str] = None

class ServerPerformanceProtector:
    """Monitors server performance and protects against analysis impact"""
    
    def __init__(self, sql_connection, config):
        """Initialize performance protector
        
        Args:
            sql_connection: SQL Server connection
            config: Configuration manager
        """
        self.connection = sql_connection
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Load thresholds from config
        self.thresholds = PerformanceThresholds(
            max_cpu_percent=getattr(config, 'protection_max_cpu_percent', 80.0),
            max_wait_time_ms=getattr(config, 'protection_max_wait_time_ms', 1000.0),
            max_blocking_sessions=getattr(config, 'protection_max_blocking_sessions', 5),
            max_analysis_duration_minutes=getattr(config, 'protection_max_analysis_duration_minutes', 30),
            check_interval_seconds=getattr(config, 'protection_check_interval_seconds', 10),
            violation_count_threshold=getattr(config, 'protection_violation_count_threshold', 3)
        )
        
        self.status = ProtectionStatus()
        self.monitoring_thread = None
        # Threading for background monitoring
        self._stop_monitoring = threading.Event()
        self.analysis_start_time = None
        
        # Callbacks for different events
        self.on_violation_detected: Optional[Callable] = None
        self.on_analysis_aborted: Optional[Callable] = None
    
    def start_monitoring(self) -> bool:
        """Start performance monitoring
        
        Returns:
            True if monitoring started successfully
        """
        if self.status.is_monitoring:
            self.logger.warning("Performance monitoring already active")
            return True
        
        try:
            self.analysis_start_time = datetime.now()
            self.status = ProtectionStatus(is_monitoring=True, is_analysis_safe=True)
            self._stop_monitoring.clear()
            
            # Start monitoring thread
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_loop,
                name="ServerPerformanceMonitor",
                daemon=True
            )
            self.monitoring_thread.start()
            
            self.logger.info(f"🛡️  Server performance protection started (CPU: {self.thresholds.max_cpu_percent}%, Waits: {self.thresholds.max_wait_time_ms}ms)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start performance monitoring: {e}")
            return False
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop performance monitoring and return summary
        
        Returns:
            Dictionary with monitoring summary
        """
        if not self.status.is_monitoring:
            return {'status': 'not_monitoring'}
        
        self._stop_monitoring.set()
        
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)
        
        analysis_duration = (datetime.now() - self.analysis_start_time).total_seconds() / 60 if self.analysis_start_time else 0
        
        summary = {
            'status': 'completed',
            'analysis_duration_minutes': analysis_duration,
            'total_violations': self.status.violation_count,
            'was_analysis_safe': self.status.is_analysis_safe,
            'abort_reason': self.status.abort_reason,
            'metrics_collected': len(self.status.metrics_history),
            'avg_cpu_percent': self._calculate_average_metric('cpu_percent'),
            'avg_wait_time_ms': self._calculate_average_metric('avg_wait_time_ms'),
            'max_blocking_sessions': self._calculate_max_metric('blocking_sessions')
        }
        
        self.status.is_monitoring = False
        self.logger.info(f"🛡️  Performance monitoring stopped. Duration: {analysis_duration:.1f}min, Violations: {self.status.violation_count}")
        
        return summary
    
    def is_analysis_safe(self) -> bool:
        """Check if analysis is still safe to continue
        
        Returns:
            True if analysis can continue safely
        """
        return self.status.is_analysis_safe
    
    def get_current_metrics(self) -> Optional[PerformanceMetrics]:
        """Get current performance metrics
        
        Returns:
            Current metrics or None if monitoring not active
        """
        if not self.status.is_monitoring:
            return None
        
        try:
            return self._collect_metrics()
        except Exception as e:
            self.logger.error(f"Error collecting current metrics: {e}")
            return None
    
    def _monitoring_loop(self):
        """Main monitoring loop (runs in separate thread)"""
        self.logger.info("🔄 Performance monitoring loop started")
        
        while not self._stop_monitoring.is_set():
            try:
                # Collect current metrics
                metrics = self._collect_metrics()
                
                if metrics:
                    # Check for violations
                    violations = self._check_violations(metrics)
                    
                    # Store metrics
                    self.status.metrics_history.append(metrics)
                    
                    # Keep only recent metrics (last hour)
                    cutoff_time = datetime.now() - timedelta(hours=1)
                    self.status.metrics_history = [
                        m for m in self.status.metrics_history 
                        if m.timestamp > cutoff_time
                    ]
                    
                    # Handle violations
                    if violations:
                        self._handle_violations(violations, metrics)
                    
                    # Check analysis duration
                    if self.analysis_start_time:
                        duration_minutes = (datetime.now() - self.analysis_start_time).total_seconds() / 60
                        if duration_minutes > self.thresholds.max_analysis_duration_minutes:
                            self._abort_analysis(f"Maximum analysis duration exceeded ({duration_minutes:.1f} minutes)")
                            break
                
                # Wait for next check
                self._stop_monitoring.wait(self.thresholds.check_interval_seconds)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)  # Short delay on error
        
        self.logger.info("🛑 Performance monitoring loop stopped")
    
    def _collect_metrics(self) -> Optional[PerformanceMetrics]:
        """Collect current server performance metrics"""
        try:
            # CPU usage query
            cpu_query = """
            SELECT TOP 1 
                AVG(CAST(cntr_value AS FLOAT)) as cpu_percent
            FROM sys.dm_os_performance_counters 
            WHERE counter_name LIKE '%Processor Time%' 
            AND instance_name = '_Total'
            """
            
            # Wait statistics query
            wait_query = """
            SELECT 
                AVG(wait_time_ms) as avg_wait_time_ms,
                COUNT(*) as active_requests
            FROM sys.dm_exec_requests 
            WHERE session_id > 50 
            AND wait_time > 0
            """
            
            # Blocking sessions query
            blocking_query = """
            SELECT COUNT(*) as blocking_sessions
            FROM sys.dm_exec_requests r1
            WHERE r1.blocking_session_id > 0
            """
            
            cpu_result = self.connection.execute_query(cpu_query)
            wait_result = self.connection.execute_query(wait_query)
            blocking_result = self.connection.execute_query(blocking_query)
            
            cpu_percent = cpu_result[0]['cpu_percent'] if cpu_result else 0.0
            avg_wait_time = wait_result[0]['avg_wait_time_ms'] if wait_result and wait_result[0]['avg_wait_time_ms'] else 0.0
            active_requests = wait_result[0]['active_requests'] if wait_result else 0
            blocking_sessions = blocking_result[0]['blocking_sessions'] if blocking_result else 0
            
            analysis_duration = 0.0
            if self.analysis_start_time:
                analysis_duration = (datetime.now() - self.analysis_start_time).total_seconds() / 60
            
            return PerformanceMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                avg_wait_time_ms=avg_wait_time,
                blocking_sessions=blocking_sessions,
                active_requests=active_requests,
                analysis_duration_minutes=analysis_duration
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting performance metrics: {e}")
            return None
    
    def _check_violations(self, metrics: PerformanceMetrics) -> List[str]:
        """Check for performance threshold violations
        
        Args:
            metrics: Current performance metrics
            
        Returns:
            List of violation descriptions
        """
        violations = []
        
        if metrics.cpu_percent > self.thresholds.max_cpu_percent:
            violations.append(f"High CPU usage: {metrics.cpu_percent:.1f}% (threshold: {self.thresholds.max_cpu_percent}%)")
        
        if metrics.avg_wait_time_ms > self.thresholds.max_wait_time_ms:
            violations.append(f"High wait times: {metrics.avg_wait_time_ms:.1f}ms (threshold: {self.thresholds.max_wait_time_ms}ms)")
        
        if metrics.blocking_sessions > self.thresholds.max_blocking_sessions:
            violations.append(f"Blocking sessions: {metrics.blocking_sessions} (threshold: {self.thresholds.max_blocking_sessions})")
        
        return violations
    
    def _handle_violations(self, violations: List[str], metrics: PerformanceMetrics):
        """Handle performance violations"""
        self.status.violation_count += 1
        self.status.last_violation = datetime.now()
        
        # Log violations
        for violation in violations:
            self.logger.warning(f"⚠️  Performance violation: {violation}")
        
        # Call violation callback if set
        if self.on_violation_detected:
            try:
                self.on_violation_detected(violations, metrics)
            except Exception as e:
                self.logger.error(f"Error in violation callback: {e}")
        
        # Check if we should abort
        if self.status.violation_count >= self.thresholds.violation_count_threshold:
            violation_summary = "; ".join(violations)
            self._abort_analysis(f"Multiple performance violations ({self.status.violation_count}): {violation_summary}")
    
    def _abort_analysis(self, reason: str):
        """Abort analysis due to performance issues"""
        self.status.is_analysis_safe = False
        self.status.abort_reason = reason
        
        self.logger.error(f"🚨 ABORTING ANALYSIS: {reason}")
        
        # Call abort callback if set
        if self.on_analysis_aborted:
            try:
                self.on_analysis_aborted(reason)
            except Exception as e:
                self.logger.error(f"Error in abort callback: {e}")
    
    def _calculate_average_metric(self, metric_name: str) -> float:
        """Calculate average value for a metric"""
        if not self.status.metrics_history:
            return 0.0
        
        values = [getattr(m, metric_name, 0) for m in self.status.metrics_history]
        return sum(values) / len(values) if values else 0.0
    
    def _calculate_max_metric(self, metric_name: str) -> float:
        """Calculate maximum value for a metric"""
        if not self.status.metrics_history:
            return 0.0
        
        values = [getattr(m, metric_name, 0) for m in self.status.metrics_history]
        return max(values) if values else 0.0
    
    def get_protection_summary(self) -> Dict[str, Any]:
        """Get comprehensive protection summary"""
        analysis_duration = 0.0
        if self.analysis_start_time:
            analysis_duration = (datetime.now() - self.analysis_start_time).total_seconds() / 60
        
        recent_metrics = self.status.metrics_history[-10:] if self.status.metrics_history else []
        
        return {
            'protection_status': {
                'is_monitoring': self.status.is_monitoring,
                'is_analysis_safe': self.status.is_analysis_safe,
                'violation_count': self.status.violation_count,
                'abort_reason': self.status.abort_reason
            },
            'thresholds': {
                'max_cpu_percent': self.thresholds.max_cpu_percent,
                'max_wait_time_ms': self.thresholds.max_wait_time_ms,
                'max_blocking_sessions': self.thresholds.max_blocking_sessions,
                'max_analysis_duration_minutes': self.thresholds.max_analysis_duration_minutes
            },
            'current_performance': {
                'analysis_duration_minutes': analysis_duration,
                'avg_cpu_percent': self._calculate_average_metric('cpu_percent'),
                'avg_wait_time_ms': self._calculate_average_metric('avg_wait_time_ms'),
                'max_blocking_sessions': self._calculate_max_metric('blocking_sessions'),
                'metrics_count': len(self.status.metrics_history)
            },
            'recent_metrics': [
                {
                    'timestamp': m.timestamp.isoformat(),
                    'cpu_percent': m.cpu_percent,
                    'avg_wait_time_ms': m.avg_wait_time_ms,
                    'blocking_sessions': m.blocking_sessions,
                    'active_requests': m.active_requests
                }
                for m in recent_metrics
            ]
        }