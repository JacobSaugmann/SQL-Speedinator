"""
Intelligent Recommendations Engine
Correlates multiple performance metrics to provide smart, actionable recommendations
"""

import logging
from typing import Dict, List, Any, Optional

class IntelligentRecommendationsEngine:
    """Correlates performance metrics and generates intelligent recommendations"""
    
    def __init__(self, config):
        """Initialize intelligent recommendations engine
        
        Args:
            config: ConfigManager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze_correlations(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze correlations between different performance metrics
        
        Args:
            analysis_results: Complete analysis results from all analyzers
            
        Returns:
            Dictionary containing correlated analysis and smart recommendations
        """
        try:
            correlations = {
                'memory_disk_correlation': self._analyze_memory_disk_correlation(analysis_results),
                'cpu_io_correlation': self._analyze_cpu_io_correlation(analysis_results),
                'tempdb_correlation': self._analyze_tempdb_correlation(analysis_results),
                'index_performance_correlation': self._analyze_index_performance_correlation(analysis_results),
                'transaction_log_correlation': self._analyze_transaction_log_correlation(analysis_results),
                'smart_recommendations': self._generate_smart_recommendations(analysis_results)
            }
            
            return correlations
            
        except Exception as e:
            self.logger.error(f"Error in correlation analysis: {e}")
            return {'error': str(e)}
    
    def _analyze_memory_disk_correlation(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze correlation between memory pressure and disk I/O"""
        memory_pressure_indicators = []
        disk_io_issues = []
        correlation_analysis = {}
        
        # Check for memory pressure indicators
        disk_analysis = results.get('disk_analysis', {})
        server_config = results.get('server_configuration', {})
        
        # Look for Page Life Expectancy issues in disk analysis bottlenecks
        io_bottlenecks = disk_analysis.get('io_bottlenecks', [])
        for bottleneck in io_bottlenecks:
            for issue in bottleneck.get('issues', []):
                if issue.get('type') == 'READ_LATENCY' and issue.get('value', 0) > 20:
                    disk_io_issues.append({
                        'type': 'high_read_latency',
                        'value': issue.get('value'),
                        'file': bottleneck.get('physical_name'),
                        'database': bottleneck.get('database_name')
                    })
        
        # Check memory configuration
        if server_config.get('memory_settings'):
            max_memory_mb = server_config.get('memory_settings', {}).get('max_server_memory_mb', 0)
            if max_memory_mb and max_memory_mb < 4096:  # Less than 4GB
                memory_pressure_indicators.append({
                    'type': 'low_max_memory',
                    'value': max_memory_mb,
                    'description': f'Max server memory set to only {max_memory_mb}MB'
                })
        
        # Correlate memory pressure with disk I/O
        if memory_pressure_indicators and disk_io_issues:
            correlation_analysis = {
                'correlation_detected': True,
                'correlation_type': 'MEMORY_PRESSURE_CAUSING_DISK_IO',
                'severity': 'HIGH',
                'description': 'Memory pressure is likely causing excessive disk I/O',
                'memory_indicators': memory_pressure_indicators,
                'disk_indicators': disk_io_issues,
                'recommendations': [
                    'Increase max server memory if sufficient RAM is available',
                    'Add more physical memory to the server',
                    'Check for memory leaks in applications',
                    'Consider using buffer pool extension on fast storage',
                    'Review query plans for memory-intensive operations'
                ]
            }
        
        return correlation_analysis
    
    def _analyze_cpu_io_correlation(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze correlation between CPU and I/O bottlenecks"""
        cpu_issues = []
        io_issues = []
        correlation_analysis = {}
        
        # Check wait statistics for CPU vs I/O waits
        wait_stats = results.get('wait_statistics', {})
        top_waits = wait_stats.get('top_waits', [])
        
        for wait in top_waits:
            wait_type = wait.get('wait_type', '')
            if wait_type in ['PAGEIOLATCH_SH', 'PAGEIOLATCH_EX', 'WRITELOG', 'ASYNC_IO_COMPLETION']:
                io_issues.append({
                    'wait_type': wait_type,
                    'wait_time_ms': wait.get('wait_time_ms', 0),
                    'percentage': wait.get('percentage_of_total', 0)
                })
            elif wait_type in ['SOS_SCHEDULER_YIELD', 'CXPACKET', 'THREADPOOL']:
                cpu_issues.append({
                    'wait_type': wait_type,
                    'wait_time_ms': wait.get('wait_time_ms', 0),
                    'percentage': wait.get('percentage_of_total', 0)
                })
        
        # Analyze the correlation
        total_io_percentage = sum(issue.get('percentage', 0) for issue in io_issues)
        total_cpu_percentage = sum(issue.get('percentage', 0) for issue in cpu_issues)
        
        if total_io_percentage > 60:
            correlation_analysis = {
                'correlation_detected': True,
                'correlation_type': 'IO_BOUND_WORKLOAD',
                'severity': 'HIGH',
                'description': f'Workload is I/O bound ({total_io_percentage:.1f}% I/O waits)',
                'primary_bottleneck': 'DISK_IO',
                'recommendations': [
                    'Focus on disk subsystem optimization',
                    'Consider faster storage (SSD/NVMe)',
                    'Optimize indexes to reduce I/O',
                    'Check for missing indexes',
                    'Consider index maintenance and fragmentation'
                ]
            }
        elif total_cpu_percentage > 60:
            correlation_analysis = {
                'correlation_detected': True,
                'correlation_type': 'CPU_BOUND_WORKLOAD',
                'severity': 'HIGH',
                'description': f'Workload is CPU bound ({total_cpu_percentage:.1f}% CPU waits)',
                'primary_bottleneck': 'CPU',
                'recommendations': [
                    'Optimize queries and reduce CPU overhead',
                    'Check for missing indexes causing table scans',
                    'Review plan cache for expensive queries',
                    'Consider adding more CPU cores',
                    'Look for excessive compilations and recompilations'
                ]
            }
        
        return correlation_analysis
    
    def _analyze_tempdb_correlation(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze tempdb-related performance issues"""
        tempdb_issues = []
        correlation_analysis = {}
        
        # Get tempdb analysis
        disk_analysis = results.get('disk_analysis', {})
        tempdb_analysis = disk_analysis.get('tempdb_analysis', {})
        
        if isinstance(tempdb_analysis, dict) and 'issues' in tempdb_analysis:
            tempdb_issues = tempdb_analysis.get('issues', [])
        
        # Check wait statistics for tempdb-related waits
        wait_stats = results.get('wait_statistics', {})
        top_waits = wait_stats.get('top_waits', [])
        
        tempdb_waits = []
        for wait in top_waits:
            wait_type = wait.get('wait_type', '')
            if any(keyword in wait_type for keyword in ['PAGELATCH', 'PAGEIOLATCH', 'LATCH']):
                tempdb_waits.append({
                    'wait_type': wait_type,
                    'wait_time_ms': wait.get('wait_time_ms', 0),
                    'percentage': wait.get('percentage_of_total', 0)
                })
        
        # Correlate tempdb configuration issues with wait stats
        if tempdb_issues and tempdb_waits:
            total_latch_waits = sum(wait.get('percentage', 0) for wait in tempdb_waits)
            
            if total_latch_waits > 10:  # Significant latch waits
                correlation_analysis = {
                    'correlation_detected': True,
                    'correlation_type': 'TEMPDB_CONTENTION',
                    'severity': 'HIGH',
                    'description': f'Tempdb contention detected ({total_latch_waits:.1f}% latch waits)',
                    'tempdb_issues': tempdb_issues,
                    'wait_indicators': tempdb_waits,
                    'recommendations': [
                        'Add more tempdb data files (1 per CPU core, max 8)',
                        'Ensure all tempdb data files are the same size',
                        'Move tempdb to faster storage',
                        'Use trace flag 1118 to avoid mixed extent allocations',
                        'Consider separating tempdb from other databases'
                    ]
                }
        
        return correlation_analysis
    
    def _analyze_index_performance_correlation(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze correlation between index issues and performance"""
        index_issues = []
        performance_indicators = []
        correlation_analysis = {}
        
        # Get index analysis results
        index_analysis = results.get('index_analysis', {})
        missing_indexes = results.get('missing_indexes', {})
        
        # Check for fragmented indexes
        if index_analysis and 'fragmented_indexes' in index_analysis:
            fragmented = index_analysis.get('fragmented_indexes', [])
            high_fragmentation = [idx for idx in fragmented if idx.get('fragmentation_percentage', 0) > 30]
            if high_fragmentation:
                index_issues.append({
                    'type': 'HIGH_FRAGMENTATION',
                    'count': len(high_fragmentation),
                    'severity': 'MEDIUM'
                })
        
        # Check for missing indexes
        if missing_indexes and 'missing_indexes' in missing_indexes:
            missing = missing_indexes.get('missing_indexes', [])
            high_impact_missing = [idx for idx in missing if idx.get('avg_total_user_cost', 0) > 100]
            if high_impact_missing:
                index_issues.append({
                    'type': 'HIGH_IMPACT_MISSING_INDEXES',
                    'count': len(high_impact_missing),
                    'severity': 'HIGH'
                })
        
        # Check for performance indicators
        disk_analysis = results.get('disk_analysis', {})
        io_bottlenecks = disk_analysis.get('io_bottlenecks', [])
        
        high_read_latency_files = []
        for bottleneck in io_bottlenecks:
            for issue in bottleneck.get('issues', []):
                if issue.get('type') == 'READ_LATENCY' and issue.get('value', 0) > 15:
                    high_read_latency_files.append(bottleneck.get('database_name'))
        
        if high_read_latency_files:
            performance_indicators.append({
                'type': 'HIGH_READ_LATENCY',
                'affected_databases': list(set(high_read_latency_files))
            })
        
        # Correlate index issues with performance
        if index_issues and performance_indicators:
            correlation_analysis = {
                'correlation_detected': True,
                'correlation_type': 'INDEX_PERFORMANCE_CORRELATION',
                'severity': 'HIGH',
                'description': 'Index issues are likely contributing to performance problems',
                'index_issues': index_issues,
                'performance_indicators': performance_indicators,
                'recommendations': [
                    'Implement missing indexes with high impact scores',
                    'Rebuild or reorganize highly fragmented indexes',
                    'Update index statistics',
                    'Consider index usage patterns and remove unused indexes',
                    'Schedule regular index maintenance'
                ]
            }
        
        return correlation_analysis
    
    def _analyze_transaction_log_correlation(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze transaction log related performance issues"""
        log_issues = []
        correlation_analysis = {}
        
        # Check for log file I/O issues
        disk_analysis = results.get('disk_analysis', {})
        io_bottlenecks = disk_analysis.get('io_bottlenecks', [])
        
        for bottleneck in io_bottlenecks:
            if bottleneck.get('file_type') == 'LOG':
                for issue in bottleneck.get('issues', []):
                    if issue.get('type') == 'LOG_WRITE_LATENCY':
                        log_issues.append({
                            'database': bottleneck.get('database_name'),
                            'latency_ms': issue.get('value'),
                            'file_path': bottleneck.get('physical_name')
                        })
        
        # Check wait statistics for log-related waits
        wait_stats = results.get('wait_statistics', {})
        top_waits = wait_stats.get('top_waits', [])
        
        log_waits = []
        for wait in top_waits:
            wait_type = wait.get('wait_type', '')
            if wait_type in ['WRITELOG', 'LOGMGR', 'LOGBUFFER', 'LOGMGR_FLUSH']:
                log_waits.append({
                    'wait_type': wait_type,
                    'wait_time_ms': wait.get('wait_time_ms', 0),
                    'percentage': wait.get('percentage_of_total', 0)
                })
        
        # Correlate log issues
        if log_issues or log_waits:
            total_log_wait_percentage = sum(wait.get('percentage', 0) for wait in log_waits)
            
            correlation_analysis = {
                'correlation_detected': True,
                'correlation_type': 'TRANSACTION_LOG_BOTTLENECK',
                'severity': 'HIGH' if total_log_wait_percentage > 20 else 'MEDIUM',
                'description': f'Transaction log bottleneck detected ({total_log_wait_percentage:.1f}% log waits)',
                'log_issues': log_issues,
                'log_waits': log_waits,
                'recommendations': [
                    'Move transaction log files to fastest available storage',
                    'Use dedicated drives for transaction logs',
                    'Pre-size log files to avoid auto-growth',
                    'Consider changing recovery model if appropriate',
                    'Optimize transaction log backup frequency',
                    'Check for long-running transactions'
                ]
            }
        
        return correlation_analysis
    
    def _generate_smart_recommendations(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate prioritized smart recommendations based on correlation analysis"""
        recommendations = []
        
        # Priority 1: Critical issues that affect multiple systems
        disk_analysis = results.get('disk_analysis', {})
        tempdb_analysis = disk_analysis.get('tempdb_analysis', {})
        
        if isinstance(tempdb_analysis, dict) and tempdb_analysis.get('issues'):
            critical_tempdb_issues = [i for i in tempdb_analysis['issues'] if i.get('severity') == 'HIGH']
            if critical_tempdb_issues:
                recommendations.append({
                    'priority': 1,
                    'category': 'CRITICAL_SYSTEM_ISSUE',
                    'title': 'Critical TempDB Configuration Issues',
                    'impact': 'Affects entire SQL Server instance performance',
                    'actions': [
                        'Fix tempdb data file count and sizing immediately',
                        'Move tempdb to dedicated fast storage',
                        'Restart SQL Server to apply tempdb changes'
                    ],
                    'estimated_effort': 'Medium',
                    'estimated_impact': 'High'
                })
        
        # Priority 2: Memory pressure causing disk I/O
        io_bottlenecks = disk_analysis.get('io_bottlenecks', [])
        high_latency_count = sum(1 for b in io_bottlenecks for i in b.get('issues', []) 
                                if i.get('type') == 'READ_LATENCY' and i.get('value', 0) > 20)
        
        if high_latency_count > 3:  # Multiple files with high read latency
            recommendations.append({
                'priority': 2,
                'category': 'MEMORY_DISK_OPTIMIZATION',
                'title': 'Memory Pressure Causing Excessive Disk I/O',
                'impact': 'Significant performance degradation across multiple databases',
                'actions': [
                    'Increase max server memory setting',
                    'Add more physical RAM',
                    'Optimize queries to reduce memory usage',
                    'Consider buffer pool extension'
                ],
                'estimated_effort': 'Medium to High',
                'estimated_impact': 'High'
            })
        
        # Priority 3: Index optimization opportunities
        missing_indexes = results.get('missing_indexes', {})
        if missing_indexes and missing_indexes.get('missing_indexes'):
            high_impact_missing = [idx for idx in missing_indexes['missing_indexes'] 
                                 if idx.get('avg_total_user_cost', 0) > 100]
            if high_impact_missing:
                recommendations.append({
                    'priority': 3,
                    'category': 'INDEX_OPTIMIZATION',
                    'title': f'{len(high_impact_missing)} High-Impact Missing Indexes',
                    'impact': 'Could significantly improve query performance',
                    'actions': [
                        'Implement high-impact missing indexes',
                        'Monitor index usage after implementation',
                        'Schedule regular index maintenance'
                    ],
                    'estimated_effort': 'Low to Medium',
                    'estimated_impact': 'Medium to High'
                })
        
        # Priority 4: Storage optimization
        slow_disks = disk_analysis.get('slow_disks', [])
        if slow_disks:
            critical_space_issues = [d for d in slow_disks for i in d.get('issues', []) 
                                   if i.get('type') == 'DISK_SPACE' and i.get('severity') == 'HIGH']
            if critical_space_issues:
                recommendations.append({
                    'priority': 4,
                    'category': 'STORAGE_OPTIMIZATION',
                    'title': 'Critical Disk Space Issues',
                    'impact': 'Risk of database unavailability',
                    'actions': [
                        'Free up disk space immediately',
                        'Archive old data and backups',
                        'Add more storage capacity',
                        'Implement monitoring for disk space'
                    ],
                    'estimated_effort': 'Low to Medium',
                    'estimated_impact': 'Critical'
                })
        
        return recommendations