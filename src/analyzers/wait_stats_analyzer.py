"""
Wait Statistics Analyzer
Analyzes SQL Server wait statistics to identify performance bottlenecks
Inspired by the great ones and SQL Server community methodologies
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

class WaitStatsAnalyzer:
    """Analyzes SQL Server wait statistics for performance bottlenecks"""
    
    def __init__(self, connection, config):
        """Initialize wait stats analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete wait statistics analysis
        
        Returns:
            Dictionary containing wait statistics analysis results
        """
        try:
            results = {
                'current_waits': self._get_current_waits(),
                'wait_history': self._get_wait_history(),
                'high_waits': self._identify_problematic_waits(),
                'wait_analysis': self._analyze_wait_patterns(),
                'recommendations': self._generate_wait_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in wait stats analysis: {e}")
            return {'error': str(e)}
    
    def _get_current_waits(self) -> Optional[List[Dict[str, Any]]]:
        """Get current wait statistics snapshot"""
        query = """
        WITH Waits AS (
            SELECT 
                wait_type,
                wait_time_ms,
                waiting_tasks_count,
                signal_wait_time_ms,
                max_wait_time_ms,
                CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS wait_percentage
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN (
                'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
                'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
                'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
                'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT',
                'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
                'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP'
            )
            AND wait_time_ms > 0
        )
        SELECT TOP 20
            wait_type,
            wait_time_ms,
            waiting_tasks_count,
            signal_wait_time_ms,
            max_wait_time_ms,
            wait_percentage,
            CASE 
                WHEN wait_percentage > 10 THEN 'HIGH'
                WHEN wait_percentage > 5 THEN 'MEDIUM'
                ELSE 'LOW'
            END as severity
        FROM Waits
        WHERE wait_percentage > 1
        ORDER BY wait_time_ms DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_wait_history(self) -> Optional[List[Dict[str, Any]]]:
        """Get historical wait information if available"""
        # This would typically use Query Store or custom logging
        # For now, we'll get session-level waits
        query = """
        SELECT 
            session_id,
            wait_type,
            wait_duration_ms,
            wait_resource,
            blocking_session_id
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_sessions s
        WHERE r.session_id = s.session_id
        AND r.wait_type IS NOT NULL
        AND r.session_id > 50
        ORDER BY wait_duration_ms DESC
        """
        
        return self.connection.execute_query(query)
    
    def _identify_problematic_waits(self) -> List[Dict[str, Any]]:
        """Identify waits that indicate specific problems"""
        current_waits = self._get_current_waits()
        if not current_waits:
            return []
        
        problematic_waits = []
        
        # Define problematic wait types and their implications
        problem_waits = {
            'PAGEIOLATCH_SH': {
                'category': 'Disk I/O',
                'description': 'Data page reads from disk',
                'likely_cause': 'Insufficient memory or slow storage'
            },
            'PAGEIOLATCH_EX': {
                'category': 'Disk I/O', 
                'description': 'Data page writes to disk',
                'likely_cause': 'TempDB contention or slow storage'
            },
            'WRITELOG': {
                'category': 'Log I/O',
                'description': 'Transaction log writes',
                'likely_cause': 'Slow log disk or large transactions'
            },
            'LCK_M_S': {
                'category': 'Locking',
                'description': 'Shared lock waits',
                'likely_cause': 'Blocking or missing indexes'
            },
            'LCK_M_X': {
                'category': 'Locking',
                'description': 'Exclusive lock waits', 
                'likely_cause': 'Blocking transactions'
            },
            'CXPACKET': {
                'category': 'Parallelism',
                'description': 'Parallel query coordination',
                'likely_cause': 'Suboptimal parallelism settings'
            },
            'SOS_SCHEDULER_YIELD': {
                'category': 'CPU',
                'description': 'CPU scheduling delays',
                'likely_cause': 'CPU pressure or inefficient queries'
            },
            'THREADPOOL': {
                'category': 'Threading',
                'description': 'Worker thread shortage',
                'likely_cause': 'Too many concurrent requests'
            }
        }
        
        for wait in current_waits:
            wait_type = wait.get('wait_type')
            if wait_type in problem_waits and wait.get('wait_percentage', 0) > 2:
                problem_info = problem_waits[wait_type].copy()
                problem_info.update(wait)
                problematic_waits.append(problem_info)
        
        return problematic_waits
    
    def _analyze_wait_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in wait statistics"""
        current_waits = self._get_current_waits()
        if not current_waits:
            return {}
        
        analysis = {
            'total_waits': len(current_waits),
            'top_wait_category': None,
            'io_waits_percentage': 0,
            'lock_waits_percentage': 0,
            'cpu_waits_percentage': 0,
            'patterns': []
        }
        
        # Categorize waits
        io_waits = ['PAGEIOLATCH_SH', 'PAGEIOLATCH_EX', 'WRITELOG', 'LOGMGR']
        lock_waits = ['LCK_M_S', 'LCK_M_X', 'LCK_M_IX', 'LCK_M_IS']
        cpu_waits = ['SOS_SCHEDULER_YIELD', 'CXPACKET', 'THREADPOOL']
        
        total_wait_time = sum(wait.get('wait_time_ms', 0) for wait in current_waits)
        
        if total_wait_time > 0:
            io_time = sum(wait.get('wait_time_ms', 0) for wait in current_waits 
                         if any(io_wait in wait.get('wait_type', '') for io_wait in io_waits))
            lock_time = sum(wait.get('wait_time_ms', 0) for wait in current_waits 
                           if any(lock_wait in wait.get('wait_type', '') for lock_wait in lock_waits))
            cpu_time = sum(wait.get('wait_time_ms', 0) for wait in current_waits 
                          if any(cpu_wait in wait.get('wait_type', '') for cpu_wait in cpu_waits))
            
            analysis['io_waits_percentage'] = round((io_time / total_wait_time) * 100, 2)
            analysis['lock_waits_percentage'] = round((lock_time / total_wait_time) * 100, 2)
            analysis['cpu_waits_percentage'] = round((cpu_time / total_wait_time) * 100, 2)
            
            # Determine primary bottleneck
            if analysis['io_waits_percentage'] > 40:
                analysis['top_wait_category'] = 'I/O Bottleneck'
                analysis['patterns'].append('High I/O waits indicate storage performance issues')
            elif analysis['lock_waits_percentage'] > 30:
                analysis['top_wait_category'] = 'Locking/Blocking'
                analysis['patterns'].append('High lock waits indicate blocking or contention')
            elif analysis['cpu_waits_percentage'] > 25:
                analysis['top_wait_category'] = 'CPU Pressure'
                analysis['patterns'].append('High CPU waits indicate processing bottlenecks')
        
        return analysis
    
    def _generate_wait_recommendations(self) -> List[Dict[str, Any]]:
        """Generate specific recommendations based on wait analysis"""
        recommendations = []
        problematic_waits = self._identify_problematic_waits()
        
        for wait in problematic_waits:
            wait_type = wait.get('wait_type')
            percentage = wait.get('wait_percentage', 0)
            
            if wait_type in ['PAGEIOLATCH_SH', 'PAGEIOLATCH_EX'] and percentage > 10:
                recommendations.append({
                    'priority': 'HIGH',
                    'wait_type': wait_type,
                    'issue': f'High page I/O latches ({percentage}% of total waits)',
                    'recommendations': [
                        'Add more memory to reduce physical I/O',
                        'Check for index fragmentation',
                        'Consider faster storage subsystem',
                        'Review query plans for full table scans'
                    ]
                })
            
            elif wait_type == 'WRITELOG' and percentage > 5:
                recommendations.append({
                    'priority': 'HIGH',
                    'wait_type': wait_type,
                    'issue': f'High log write waits ({percentage}% of total waits)',
                    'recommendations': [
                        'Move transaction log to faster storage',
                        'Check transaction log file growth settings',
                        'Review large transaction patterns',
                        'Consider log file pre-sizing'
                    ]
                })
            
            elif wait_type in ['LCK_M_S', 'LCK_M_X'] and percentage > 8:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'wait_type': wait_type, 
                    'issue': f'High locking waits ({percentage}% of total waits)',
                    'recommendations': [
                        'Identify and resolve blocking queries',
                        'Consider READ_COMMITTED_SNAPSHOT isolation',
                        'Review missing indexes',
                        'Optimize long-running transactions'
                    ]
                })
            
            elif wait_type == 'CXPACKET' and percentage > 10:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'wait_type': wait_type,
                    'issue': f'High parallelism waits ({percentage}% of total waits)',
                    'recommendations': [
                        'Review MAXDOP setting',
                        'Adjust cost threshold for parallelism',
                        'Consider query plan optimization',
                        'Check for skewed parallel execution'
                    ]
                })
        
        return recommendations