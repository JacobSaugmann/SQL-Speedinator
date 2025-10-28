"""
Disk Performance Analyzer
Analyzes disk I/O performance from both SQL Server and OS perspective
Inspired by the great ones and SQL Server community methodologies
"""

import logging
import psutil
import os
from typing import Dict, Any, List, Optional

class DiskAnalyzer:
    """Analyzes disk performance for SQL Server"""
    
    def __init__(self, connection, config):
        """Initialize disk analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete disk performance analysis
        
        Returns:
            Dictionary containing disk analysis results
        """
        try:
            results = {
                'sql_disk_stats': self._get_sql_disk_stats(),
                'os_disk_stats': self._get_os_disk_stats(),
                'database_files': self._get_database_file_stats(),
                'io_bottlenecks': self._identify_io_bottlenecks(),
                'slow_disks': self._identify_slow_disks(),
                'recommendations': self._generate_disk_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in disk analysis: {e}")
            return {'error': str(e)}
    
    def _get_sql_disk_stats(self) -> Optional[List[Dict[str, Any]]]:
        """Get disk I/O statistics from SQL Server DMVs"""
        query = """
        SELECT 
            DB_NAME(vfs.database_id) AS database_name,
            mf.physical_name,
            mf.type_desc as file_type,
            vfs.sample_ms,
            vfs.num_of_reads,
            vfs.num_of_bytes_read,
            vfs.io_stall_read_ms,
            vfs.num_of_writes, 
            vfs.num_of_bytes_written,
            vfs.io_stall_write_ms,
            vfs.io_stall,
            vfs.size_on_disk_bytes,
            CASE 
                WHEN vfs.num_of_reads = 0 THEN 0 
                ELSE CAST(vfs.io_stall_read_ms AS FLOAT) / vfs.num_of_reads 
            END AS avg_read_latency_ms,
            CASE 
                WHEN vfs.num_of_writes = 0 THEN 0 
                ELSE CAST(vfs.io_stall_write_ms AS FLOAT) / vfs.num_of_writes 
            END AS avg_write_latency_ms,
            CASE 
                WHEN (vfs.num_of_reads + vfs.num_of_writes) = 0 THEN 0 
                ELSE CAST(vfs.io_stall AS FLOAT) / (vfs.num_of_reads + vfs.num_of_writes) 
            END AS avg_io_latency_ms,
            LEFT(mf.physical_name, 1) AS drive_letter
        FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
        INNER JOIN sys.master_files mf ON vfs.database_id = mf.database_id 
            AND vfs.file_id = mf.file_id
        WHERE vfs.num_of_reads > 0 OR vfs.num_of_writes > 0
        ORDER BY vfs.io_stall DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_os_disk_stats(self) -> Dict[str, Any]:
        """Get OS-level disk statistics using psutil"""
        try:
            disk_info = {}
            
            # Get disk usage for all drives
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    io_stats = psutil.disk_io_counters(perdisk=False)
                    
                    disk_info[partition.device] = {
                        'mountpoint': partition.mountpoint,
                        'fstype': partition.fstype,
                        'total_gb': round(usage.total / (1024**3), 2),
                        'used_gb': round(usage.used / (1024**3), 2),
                        'free_gb': round(usage.free / (1024**3), 2),
                        'used_percentage': round((usage.used / usage.total) * 100, 2),
                        'io_stats': {
                            'read_count': io_stats.read_count if io_stats else 0,
                            'write_count': io_stats.write_count if io_stats else 0,
                            'read_bytes': io_stats.read_bytes if io_stats else 0,
                            'write_bytes': io_stats.write_bytes if io_stats else 0,
                            'read_time': io_stats.read_time if io_stats else 0,
                            'write_time': io_stats.write_time if io_stats else 0
                        }
                    }
                except (PermissionError, OSError) as e:
                    self.logger.warning(f"Could not get stats for {partition.device}: {e}")
            
            return disk_info
            
        except Exception as e:
            self.logger.error(f"Error getting OS disk stats: {e}")
            return {}
    
    def _get_database_file_stats(self) -> Optional[List[Dict[str, Any]]]:
        """Get database file statistics and configuration"""
        query = """
        SELECT 
            DB_NAME(database_id) AS database_name,
            name AS file_name,
            physical_name,
            type_desc,
            state_desc,
            CAST(size AS BIGINT) * 8 / 1024 AS size_mb,
            max_size,
            growth,
            is_percent_growth,
            CASE 
                WHEN max_size = -1 THEN 'UNLIMITED'
                WHEN max_size = 0 THEN 'NO GROWTH'
                ELSE CAST(CAST(max_size AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
            END AS max_size_desc,
            CASE
                WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR) + '%'
                ELSE CAST(CAST(growth AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
            END AS growth_desc
        FROM sys.master_files
        WHERE database_id > 4  -- Exclude system databases
        ORDER BY database_id, type_desc, name
        """
        
        return self.connection.execute_query(query)
    
    def _identify_io_bottlenecks(self) -> List[Dict[str, Any]]:
        """Identify I/O performance bottlenecks"""
        bottlenecks = []
        
        sql_stats = self._get_sql_disk_stats()
        if not sql_stats:
            return bottlenecks
        
        # Thresholds for problematic latencies (in milliseconds)
        read_latency_threshold = 20  # > 20ms is concerning for reads
        write_latency_threshold = 10  # > 10ms is concerning for writes
        
        for file_stat in sql_stats:
            issues = []
            
            avg_read_latency = file_stat.get('avg_read_latency_ms', 0)
            avg_write_latency = file_stat.get('avg_write_latency_ms', 0)
            file_type = file_stat.get('file_type', '')
            
            # Check read latency
            if avg_read_latency > read_latency_threshold:
                severity = 'HIGH' if avg_read_latency > 50 else 'MEDIUM'
                issues.append({
                    'type': 'READ_LATENCY',
                    'severity': severity,
                    'value': avg_read_latency,
                    'threshold': read_latency_threshold,
                    'description': f'High read latency: {avg_read_latency:.2f}ms'
                })
            
            # Check write latency  
            if avg_write_latency > write_latency_threshold:
                severity = 'HIGH' if avg_write_latency > 25 else 'MEDIUM'
                issues.append({
                    'type': 'WRITE_LATENCY',
                    'severity': severity,
                    'value': avg_write_latency,
                    'threshold': write_latency_threshold,
                    'description': f'High write latency: {avg_write_latency:.2f}ms'
                })
            
            # Special attention to log files
            if file_type == 'LOG' and avg_write_latency > 5:
                issues.append({
                    'type': 'LOG_WRITE_LATENCY',
                    'severity': 'HIGH',
                    'value': avg_write_latency,
                    'threshold': 5,
                    'description': f'High log write latency: {avg_write_latency:.2f}ms (should be < 5ms)'
                })
            
            if issues:
                bottlenecks.append({
                    'database_name': file_stat.get('database_name'),
                    'physical_name': file_stat.get('physical_name'),
                    'file_type': file_type,
                    'drive_letter': file_stat.get('drive_letter'),
                    'issues': issues
                })
        
        return bottlenecks
    
    def _identify_slow_disks(self) -> List[Dict[str, Any]]:
        """Identify slow performing disks"""
        slow_disks = []
        
        # Get OS disk statistics
        os_stats = self._get_os_disk_stats()
        
        for drive, stats in os_stats.items():
            issues = []
            
            # Check disk space
            used_percentage = stats.get('used_percentage', 0)
            if used_percentage > 90:
                issues.append({
                    'type': 'DISK_SPACE',
                    'severity': 'HIGH',
                    'description': f'Disk space critical: {used_percentage:.1f}% used'
                })
            elif used_percentage > 80:
                issues.append({
                    'type': 'DISK_SPACE',
                    'severity': 'MEDIUM', 
                    'description': f'Disk space warning: {used_percentage:.1f}% used'
                })
            
            # Calculate average I/O time if available
            io_stats = stats.get('io_stats', {})
            if io_stats:
                read_count = io_stats.get('read_count', 0)
                write_count = io_stats.get('write_count', 0)
                read_time = io_stats.get('read_time', 0)
                write_time = io_stats.get('write_time', 0)
                
                if read_count > 0:
                    avg_read_time = read_time / read_count
                    if avg_read_time > 20:  # > 20ms average
                        issues.append({
                            'type': 'OS_READ_LATENCY',
                            'severity': 'MEDIUM',
                            'description': f'High OS read latency: {avg_read_time:.2f}ms'
                        })
                
                if write_count > 0:
                    avg_write_time = write_time / write_count  
                    if avg_write_time > 10:  # > 10ms average
                        issues.append({
                            'type': 'OS_WRITE_LATENCY', 
                            'severity': 'MEDIUM',
                            'description': f'High OS write latency: {avg_write_time:.2f}ms'
                        })
            
            if issues:
                slow_disks.append({
                    'drive': drive,
                    'mountpoint': stats.get('mountpoint'),
                    'issues': issues,
                    'total_gb': stats.get('total_gb'),
                    'free_gb': stats.get('free_gb'),
                    'used_percentage': used_percentage
                })
        
        return slow_disks
    
    def _generate_disk_recommendations(self) -> List[Dict[str, Any]]:
        """Generate disk performance recommendations"""
        recommendations = []
        
        # Analyze bottlenecks and slow disks
        bottlenecks = self._identify_io_bottlenecks()
        slow_disks = self._identify_slow_disks()
        
        # Recommendations for I/O bottlenecks
        for bottleneck in bottlenecks:
            file_type = bottleneck.get('file_type')
            drive = bottleneck.get('drive_letter')
            
            for issue in bottleneck.get('issues', []):
                if issue['type'] == 'READ_LATENCY':
                    recommendations.append({
                        'priority': issue['severity'],
                        'category': 'Disk Performance',
                        'issue': f"High read latency on {file_type} file",
                        'file': bottleneck.get('physical_name'),
                        'recommendations': [
                            'Consider faster storage (SSD)',
                            'Add more memory to reduce I/O',
                            'Check for index fragmentation',
                            'Move file to dedicated drive'
                        ]
                    })
                
                elif issue['type'] == 'WRITE_LATENCY':
                    recommendations.append({
                        'priority': issue['severity'],
                        'category': 'Disk Performance',
                        'issue': f"High write latency on {file_type} file",
                        'file': bottleneck.get('physical_name'),
                        'recommendations': [
                            'Use faster storage for writes',
                            'Separate data and log files',
                            'Consider write-optimized storage',
                            'Check disk queue length'
                        ]
                    })
                
                elif issue['type'] == 'LOG_WRITE_LATENCY':
                    recommendations.append({
                        'priority': 'HIGH',
                        'category': 'Transaction Log',
                        'issue': "Critical log write latency",
                        'file': bottleneck.get('physical_name'),
                        'recommendations': [
                            'Move log file to fastest available storage',
                            'Use dedicated drive for transaction log',
                            'Consider log file pre-sizing',
                            'Check for disk contention'
                        ]
                    })
        
        # Recommendations for disk space issues
        for disk in slow_disks:
            for issue in disk.get('issues', []):
                if issue['type'] == 'DISK_SPACE':
                    recommendations.append({
                        'priority': issue['severity'],
                        'category': 'Disk Space',
                        'issue': f"Low disk space on {disk['drive']}",
                        'recommendations': [
                            'Clean up unnecessary files',
                            'Archive old backup files',
                            'Consider database compression',
                            'Add more storage capacity',
                            'Monitor database growth'
                        ]
                    })
        
        # General disk optimization recommendations
        db_files = self._get_database_file_stats()
        if db_files:
            # Check for files with unlimited growth
            unlimited_growth_files = [f for f in db_files if f.get('max_size') == -1]
            if unlimited_growth_files:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'File Management',
                    'issue': f"{len(unlimited_growth_files)} files with unlimited growth",
                    'recommendations': [
                        'Set appropriate max size limits',
                        'Monitor file growth patterns',
                        'Pre-size files to avoid auto-growth',
                        'Use fixed MB growth instead of percentage'
                    ]
                })
            
            # Check for percentage growth
            percent_growth_files = [f for f in db_files if f.get('is_percent_growth')]
            if percent_growth_files:
                recommendations.append({
                    'priority': 'LOW',
                    'category': 'File Management', 
                    'issue': f"{len(percent_growth_files)} files using percentage growth",
                    'recommendations': [
                        'Change to fixed MB growth increments',
                        'Percentage growth can cause large auto-growth events',
                        'Pre-size files appropriately'
                    ]
                })
        
        return recommendations