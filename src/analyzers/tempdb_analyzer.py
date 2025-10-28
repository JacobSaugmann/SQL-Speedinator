"""
TempDB Analyzer
Analyzes TempDB configuration and performance issues
Based on best practices from Microsoft and SQL Server experts
"""

import logging
from typing import Dict, Any, List, Optional

class TempDBAnalyzer:
    """Analyzes TempDB configuration and performance"""
    
    def __init__(self, connection, config):
        """Initialize TempDB analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete TempDB analysis
        
        Returns:
            Dictionary containing TempDB analysis results
        """
        try:
            results = {
                'tempdb_files': self._get_tempdb_files(),
                'tempdb_usage': self._get_tempdb_usage(),
                'tempdb_contention': self._analyze_tempdb_contention(),
                'tempdb_io_stats': self._get_tempdb_io_stats(),
                'space_usage': self._analyze_space_usage(),
                'configuration_issues': self._identify_configuration_issues(),
                'recommendations': self._generate_tempdb_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in TempDB analysis: {e}")
            return {'error': str(e)}
    
    def _get_tempdb_files(self) -> Optional[List[Dict[str, Any]]]:
        """Get TempDB file configuration"""
        query = """
        SELECT 
            f.name AS file_name,
            f.file_id,
            f.type_desc,
            f.physical_name,
            f.size * 8 / 1024 AS size_mb,
            f.max_size,
            f.growth,
            f.is_percent_growth,
            CASE 
                WHEN f.max_size = -1 THEN 'UNLIMITED'
                WHEN f.max_size = 0 THEN 'NO GROWTH'
                ELSE CAST(f.max_size * 8 / 1024 AS VARCHAR) + ' MB'
            END AS max_size_desc,
            CASE
                WHEN f.is_percent_growth = 1 THEN CAST(f.growth AS VARCHAR) + '%'
                ELSE CAST(f.growth * 8 / 1024 AS VARCHAR) + ' MB'
            END AS growth_desc,
            vs.size_on_disk_bytes / 1024 / 1024 AS actual_size_mb
        FROM sys.master_files f
        LEFT JOIN sys.dm_io_virtual_file_stats(2, NULL) vs ON f.file_id = vs.file_id
        WHERE f.database_id = 2  -- TempDB
        ORDER BY f.type_desc, f.file_id
        """
        
        return self.connection.execute_query(query)
    
    def _get_tempdb_usage(self) -> Optional[List[Dict[str, Any]]]:
        """Get current TempDB space usage"""
        query = """
        SELECT 
            SUM(unallocated_extent_page_count) AS unallocated_pages,
            SUM(version_store_reserved_page_count) AS version_store_pages,
            SUM(user_object_reserved_page_count) AS user_object_pages,
            SUM(internal_object_reserved_page_count) AS internal_object_pages,
            SUM(mixed_extent_page_count) AS mixed_extent_pages,
            (SUM(unallocated_extent_page_count) * 8 / 1024) AS unallocated_mb,
            (SUM(version_store_reserved_page_count) * 8 / 1024) AS version_store_mb,
            (SUM(user_object_reserved_page_count) * 8 / 1024) AS user_object_mb,
            (SUM(internal_object_reserved_page_count) * 8 / 1024) AS internal_object_mb,
            (SUM(mixed_extent_page_count) * 8 / 1024) AS mixed_extent_mb
        FROM sys.dm_db_file_space_usage
        WHERE database_id = 2
        """
        
        return self.connection.execute_query(query)
    
    def _analyze_tempdb_contention(self) -> Dict[str, Any]:
        """Analyze TempDB allocation contention"""
        try:
            # Check for allocation contention
            contention_query = """
            SELECT 
                wait_type,
                waiting_tasks_count,
                wait_time_ms,
                max_wait_time_ms,
                signal_wait_time_ms,
                CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS wait_percentage
            FROM sys.dm_os_wait_stats
            WHERE wait_type IN ('PAGELATCH_UP', 'PAGELATCH_EX', 'PAGELATCH_SH')
            AND wait_time_ms > 0
            ORDER BY wait_time_ms DESC
            """
            
            contention_waits = self.connection.execute_query(contention_query)
            
            # Check for sessions waiting on TempDB latches
            session_waits_query = """
            SELECT 
                s.session_id,
                r.wait_type,
                r.wait_time,
                r.wait_resource,
                r.blocking_session_id,
                s.login_name,
                s.program_name,
                t.text AS current_sql
            FROM sys.dm_exec_requests r
            INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.wait_type LIKE 'PAGELATCH%'
            AND r.wait_resource LIKE '2:%'  -- TempDB database_id = 2
            """
            
            session_waits = self.connection.execute_query(session_waits_query)
            
            # Analyze contention severity
            contention_level = 'LOW'
            if contention_waits:
                total_pagelatch_time = sum(w.get('wait_time_ms', 0) for w in contention_waits)
                max_wait_percentage = max(w.get('wait_percentage', 0) for w in contention_waits)
                
                if max_wait_percentage > 5 or total_pagelatch_time > 100000:
                    contention_level = 'HIGH'
                elif max_wait_percentage > 2 or total_pagelatch_time > 50000:
                    contention_level = 'MEDIUM'
            
            return {
                'contention_waits': contention_waits,
                'session_waits': session_waits,
                'contention_level': contention_level,
                'analysis_timestamp': self.connection.execute_query("SELECT GETDATE() AS current_time")
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing TempDB contention: {e}")
            return {'error': str(e)}
    
    def _get_tempdb_io_stats(self) -> Optional[List[Dict[str, Any]]]:
        """Get TempDB I/O statistics"""
        query = """
        SELECT 
            f.name AS file_name,
            f.type_desc,
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
            LEFT(f.physical_name, 1) AS drive_letter
        FROM sys.dm_io_virtual_file_stats(2, NULL) vfs
        INNER JOIN sys.master_files f ON vfs.database_id = f.database_id AND vfs.file_id = f.file_id
        WHERE f.database_id = 2  -- TempDB
        ORDER BY f.type_desc, f.file_id
        """
        
        return self.connection.execute_query(query)
    
    def _analyze_space_usage(self) -> Dict[str, Any]:
        """Analyze TempDB space usage patterns"""
        try:
            # Current space usage
            current_usage = self._get_tempdb_usage()
            
            # Get file sizes
            files = self._get_tempdb_files()
            
            analysis = {
                'current_usage': current_usage,
                'file_summary': {},
                'issues': []
            }
            
            if files:
                total_size_mb = sum(f.get('size_mb', 0) for f in files if f.get('type_desc') == 'ROWS')
                data_files_count = len([f for f in files if f.get('type_desc') == 'ROWS'])
                
                analysis['file_summary'] = {
                    'data_files_count': data_files_count,
                    'total_size_mb': total_size_mb,
                    'avg_file_size_mb': total_size_mb / data_files_count if data_files_count > 0 else 0
                }
                
                # Check for uneven file sizes
                if data_files_count > 1:
                    data_files = [f for f in files if f.get('type_desc') == 'ROWS']
                    file_sizes = [f.get('size_mb', 0) for f in data_files]
                    
                    if file_sizes:
                        min_size = min(file_sizes)
                        max_size = max(file_sizes)
                        
                        if max_size > min_size * 1.1:  # More than 10% difference
                            analysis['issues'].append({
                                'type': 'UNEVEN_FILE_SIZES',
                                'severity': 'MEDIUM',
                                'description': f'TempDB files have uneven sizes (min: {min_size}MB, max: {max_size}MB)',
                                'recommendation': 'Make all TempDB data files the same size'
                            })
            
            if current_usage and len(current_usage) > 0:
                usage = current_usage[0]
                version_store_mb = usage.get('version_store_mb', 0)
                total_used_mb = (usage.get('user_object_mb', 0) + 
                               usage.get('internal_object_mb', 0) + 
                               version_store_mb)
                
                if version_store_mb > 1000:  # More than 1GB in version store
                    analysis['issues'].append({
                        'type': 'HIGH_VERSION_STORE_USAGE',
                        'severity': 'MEDIUM',
                        'description': f'High version store usage: {version_store_mb}MB',
                        'recommendation': 'Check for long-running transactions or snapshot isolation usage'
                    })
                
                # Check space utilization
                if files:
                    total_allocated_mb = sum(f.get('size_mb', 0) for f in files if f.get('type_desc') == 'ROWS')
                    if total_allocated_mb > 0:
                        utilization_pct = (total_used_mb / total_allocated_mb) * 100
                        
                        if utilization_pct > 80:
                            analysis['issues'].append({
                                'type': 'HIGH_SPACE_UTILIZATION',
                                'severity': 'HIGH',
                                'description': f'High TempDB space utilization: {utilization_pct:.1f}%',
                                'recommendation': 'Monitor TempDB space usage and consider increasing size'
                            })
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing TempDB space usage: {e}")
            return {'error': str(e)}
    
    def _identify_configuration_issues(self) -> List[Dict[str, Any]]:
        """Identify TempDB configuration issues"""
        issues = []
        
        try:
            files = self._get_tempdb_files()
            if not files:
                return issues
            
            data_files = [f for f in files if f.get('type_desc') == 'ROWS']
            log_files = [f for f in files if f.get('type_desc') == 'LOG']
            
            # Check number of data files
            cpu_query = "SELECT cpu_count FROM sys.dm_os_sys_info"
            cpu_info = self.connection.execute_query(cpu_query)
            
            if cpu_info:
                cpu_count = cpu_info[0].get('cpu_count', 1)
                optimal_files = min(8, cpu_count)  # Microsoft recommendation
                
                if len(data_files) < optimal_files:
                    issues.append({
                        'type': 'INSUFFICIENT_DATA_FILES',
                        'severity': 'MEDIUM',
                        'description': f'TempDB has {len(data_files)} data files, recommended: {optimal_files}',
                        'recommendation': f'Add data files to reach {optimal_files} files for optimal performance'
                    })
                elif len(data_files) > optimal_files * 2:
                    issues.append({
                        'type': 'TOO_MANY_DATA_FILES',
                        'severity': 'LOW',
                        'description': f'TempDB has {len(data_files)} data files, may be excessive',
                        'recommendation': 'Consider if all data files are necessary'
                    })
            
            # Check for percentage growth
            for file in files:
                if file.get('is_percent_growth'):
                    issues.append({
                        'type': 'PERCENTAGE_GROWTH',
                        'severity': 'MEDIUM',
                        'description': f'File {file.get("file_name")} uses percentage growth',
                        'recommendation': 'Change to fixed MB growth to prevent large auto-growth events'
                    })
            
            # Check file locations
            drive_letters = set()
            for file in data_files:
                physical_name = file.get('physical_name', '')
                if physical_name:
                    drive_letters.add(physical_name[0].upper())
            
            if len(drive_letters) == 1 and len(data_files) > 1:
                issues.append({
                    'type': 'SAME_DRIVE_FILES',
                    'severity': 'LOW',
                    'description': 'All TempDB data files on same drive',
                    'recommendation': 'Consider spreading files across multiple drives for better I/O performance'
                })
            
            # Check if data and log files are on same drive
            if data_files and log_files:
                data_drive = data_files[0].get('physical_name', '')[0].upper()
                log_drive = log_files[0].get('physical_name', '')[0].upper()
                
                if data_drive == log_drive:
                    issues.append({
                        'type': 'DATA_LOG_SAME_DRIVE',
                        'severity': 'MEDIUM',
                        'description': 'TempDB data and log files on same drive',
                        'recommendation': 'Separate data and log files to different drives'
                    })
            
            return issues
            
        except Exception as e:
            self.logger.error(f"Error identifying TempDB configuration issues: {e}")
            return [{'type': 'ANALYSIS_ERROR', 'severity': 'ERROR', 'description': str(e)}]
    
    def _generate_tempdb_recommendations(self) -> List[Dict[str, Any]]:
        """Generate TempDB-specific recommendations"""
        recommendations = []
        
        # Configuration issues
        config_issues = self._identify_configuration_issues()
        high_issues = [i for i in config_issues if i.get('severity') == 'HIGH']
        medium_issues = [i for i in config_issues if i.get('severity') == 'MEDIUM']
        
        if high_issues:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'TempDB Configuration',
                'issue': f'{len(high_issues)} critical TempDB configuration issues',
                'recommendations': [issue.get('recommendation') for issue in high_issues]
            })
        
        if medium_issues:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'TempDB Optimization',
                'issue': f'{len(medium_issues)} TempDB optimization opportunities',
                'recommendations': [issue.get('recommendation') for issue in medium_issues]
            })
        
        # Contention analysis
        contention = self._analyze_tempdb_contention()
        if contention.get('contention_level') == 'HIGH':
            recommendations.append({
                'priority': 'HIGH',
                'category': 'TempDB Contention',
                'issue': 'High TempDB allocation contention detected',
                'recommendations': [
                    'Add more TempDB data files',
                    'Ensure all data files are same size',
                    'Move TempDB to faster storage',
                    'Review queries causing high TempDB usage'
                ]
            })
        
        # Space usage
        space_analysis = self._analyze_space_usage()
        space_issues = space_analysis.get('issues', [])
        critical_space_issues = [i for i in space_issues if i.get('severity') == 'HIGH']
        
        if critical_space_issues:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'TempDB Space',
                'issue': 'Critical TempDB space issues detected',
                'recommendations': [issue.get('recommendation') for issue in critical_space_issues]
            })
        
        # General best practices
        recommendations.append({
            'priority': 'LOW',
            'category': 'TempDB Best Practices',
            'issue': 'General TempDB best practices',
            'recommendations': [
                'Pre-size TempDB files to avoid auto-growth',
                'Use same initial size for all data files',
                'Monitor TempDB usage regularly',
                'Place TempDB on fast storage (SSD)',
                'Keep TempDB separate from user databases'
            ]
        })
        
        return recommendations