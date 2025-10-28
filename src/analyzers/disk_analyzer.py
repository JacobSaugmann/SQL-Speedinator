"""
Enhanced Disk Performance Analyzer
Analyzes disk I/O performance, formatting, block size, and tempdb placement
Provides comprehensive disk analysis with best practice recommendations
"""

import logging
import psutil
import os
import subprocess
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
            Dictionary containing comprehensive disk analysis results
        """
        try:
            results = {
                'sql_disk_stats': self._get_sql_disk_stats(),
                'os_disk_stats': self._get_os_disk_stats(),
                'database_files': self._get_database_file_stats(),
                'disk_formatting': self._analyze_disk_formatting(),
                'tempdb_analysis': self._analyze_tempdb_placement(),
                'drive_configuration': self._analyze_drive_configuration(),
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
    
    def _analyze_disk_formatting(self) -> Dict[str, Any]:
        """Analyze disk formatting and block size configuration"""
        try:
            formatting_info = {}
            
            # Get Windows disk information using PowerShell
            powershell_cmd = """
            Get-WmiObject -Class Win32_Volume | Where-Object {$_.DriveLetter -ne $null} | 
            Select-Object DriveLetter, FileSystem, BlockSize, 
            @{Name='AllocationUnitSize';Expression={$_.BlockSize}},
            @{Name='Capacity_GB';Expression={[math]::Round($_.Capacity/1GB,2)}},
            @{Name='FreeSpace_GB';Expression={[math]::Round($_.FreeSpace/1GB,2)}} |
            ConvertTo-Json
            """
            
            try:
                result = subprocess.run(
                    ['powershell', '-Command', powershell_cmd],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0 and result.stdout.strip():
                    import json
                    disk_data = json.loads(result.stdout.strip())
                    
                    # Handle single disk vs multiple disks
                    if isinstance(disk_data, dict):
                        disk_data = [disk_data]
                    
                    for disk in disk_data:
                        drive_letter = disk.get('DriveLetter', '')
                        if drive_letter:
                            allocation_unit = disk.get('AllocationUnitSize', 0)
                            
                            # Analyze allocation unit size
                            allocation_analysis = self._analyze_allocation_unit_size(allocation_unit)
                            
                            formatting_info[drive_letter] = {
                                'file_system': disk.get('FileSystem', 'Unknown'),
                                'allocation_unit_size': allocation_unit,
                                'allocation_unit_kb': allocation_unit // 1024 if allocation_unit else 0,
                                'capacity_gb': disk.get('Capacity_GB', 0),
                                'free_space_gb': disk.get('FreeSpace_GB', 0),
                                'analysis': allocation_analysis,
                                'recommendations': self._get_formatting_recommendations(
                                    disk.get('FileSystem', ''), allocation_unit
                                )
                            }
                
            except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
                self.logger.warning(f"Could not get disk formatting info via PowerShell: {e}")
                
                # Fallback to psutil for basic info
                for partition in psutil.disk_partitions():
                    drive_letter = partition.device
                    formatting_info[drive_letter] = {
                        'file_system': partition.fstype,
                        'allocation_unit_size': 'Unknown',
                        'allocation_unit_kb': 'Unknown',
                        'analysis': {'status': 'Unknown', 'message': 'Could not determine allocation unit size'},
                        'recommendations': ['Verify disk formatting manually', 'Ensure 64KB allocation unit for SQL Server data files']
                    }
            
            return formatting_info
            
        except Exception as e:
            self.logger.error(f"Error analyzing disk formatting: {e}")
            return {}
    
    def _analyze_allocation_unit_size(self, allocation_unit: int) -> Dict[str, str]:
        """Analyze allocation unit size for SQL Server best practices"""
        if not allocation_unit:
            return {'status': 'Unknown', 'message': 'Could not determine allocation unit size'}
        
        allocation_kb = allocation_unit // 1024
        
        if allocation_kb == 64:
            return {'status': 'Optimal', 'message': '64KB allocation unit - optimal for SQL Server'}
        elif allocation_kb >= 32:
            return {'status': 'Good', 'message': f'{allocation_kb}KB allocation unit - acceptable for SQL Server'}
        elif allocation_kb == 8:
            return {'status': 'Default', 'message': '8KB allocation unit - Windows default, not optimal for SQL Server'}
        elif allocation_kb == 4:
            return {'status': 'Poor', 'message': '4KB allocation unit - too small for SQL Server, consider reformatting'}
        else:
            return {'status': 'Suboptimal', 'message': f'{allocation_kb}KB allocation unit - consider 64KB for SQL Server'}
    
    def _get_formatting_recommendations(self, file_system: str, allocation_unit: int) -> List[str]:
        """Get formatting recommendations based on configuration"""
        recommendations = []
        
        if file_system.upper() != 'NTFS':
            recommendations.append(f'Consider using NTFS instead of {file_system} for SQL Server')
        
        allocation_kb = allocation_unit // 1024 if allocation_unit else 0
        
        if allocation_kb < 64:
            recommendations.append('Consider reformatting with 64KB allocation unit size for optimal SQL Server performance')
            recommendations.append('64KB allocation unit reduces fragmentation and improves I/O efficiency')
        
        if allocation_kb == 4:
            recommendations.append('CRITICAL: 4KB allocation unit is significantly suboptimal for SQL Server')
            recommendations.append('Reformatting with 64KB allocation unit is strongly recommended')
        
        return recommendations
    
    def _analyze_tempdb_placement(self) -> Dict[str, Any]:
        """Analyze tempdb placement and configuration"""
        try:
            # Get tempdb file information
            tempdb_query = """
            SELECT 
                name AS file_name,
                physical_name,
                type_desc,
                CAST(size AS BIGINT) * 8 / 1024 AS size_mb,
                max_size,
                growth,
                is_percent_growth,
                LEFT(physical_name, 1) AS drive_letter,
                SUBSTRING(physical_name, 1, CHARINDEX('\', physical_name, 4)) AS base_path
            FROM sys.master_files 
            WHERE database_id = DB_ID('tempdb')
            ORDER BY type_desc, file_id
            """
            
            tempdb_files = self.connection.execute_query(tempdb_query)
            if not tempdb_files:
                return {'error': 'Could not retrieve tempdb file information'}
            
            analysis = {
                'files': tempdb_files,
                'data_files_count': len([f for f in tempdb_files if f['type_desc'] == 'ROWS']),
                'log_files_count': len([f for f in tempdb_files if f['type_desc'] == 'LOG']),
                'drives_used': list(set(f['drive_letter'] for f in tempdb_files)),
                'issues': [],
                'recommendations': []
            }
            
            # Analyze tempdb configuration
            data_files = [f for f in tempdb_files if f['type_desc'] == 'ROWS']
            log_files = [f for f in tempdb_files if f['type_desc'] == 'LOG']
            
            # Check number of data files vs CPU cores
            cpu_count = psutil.cpu_count(logical=True) or 4  # Default to 4 if unknown
            optimal_data_files = min(cpu_count, 8)  # Max 8 data files recommended
            
            if len(data_files) < optimal_data_files:
                analysis['issues'].append({
                    'type': 'INSUFFICIENT_DATA_FILES',
                    'severity': 'MEDIUM',
                    'description': f'Only {len(data_files)} tempdb data files, consider {optimal_data_files} for {cpu_count} CPU cores'
                })
                analysis['recommendations'].append(f'Add {optimal_data_files - len(data_files)} more tempdb data files to match CPU cores')
            
            # Check if all data files are same size
            if data_files:
                sizes = [f['size_mb'] for f in data_files]
                if len(set(sizes)) > 1:
                    analysis['issues'].append({
                        'type': 'UNEQUAL_FILE_SIZES',
                        'severity': 'HIGH',
                        'description': 'Tempdb data files have different sizes, causing uneven allocation'
                    })
                    analysis['recommendations'].append('Make all tempdb data files the same size to ensure even allocation')
            
            # Check if tempdb is on same drive as other databases
            other_db_query = """
            SELECT DISTINCT LEFT(physical_name, 1) AS drive_letter
            FROM sys.master_files 
            WHERE database_id != DB_ID('tempdb') AND database_id > 4
            """
            
            other_drives = self.connection.execute_query(other_db_query)
            if other_drives:
                other_drive_letters = [d['drive_letter'] for d in other_drives]
                tempdb_drives = analysis['drives_used']
                
                shared_drives = set(tempdb_drives) & set(other_drive_letters)
                if shared_drives:
                    analysis['issues'].append({
                        'type': 'SHARED_DRIVE',
                        'severity': 'MEDIUM',
                        'description': f'Tempdb shares drive(s) {", ".join(shared_drives)} with other databases'
                    })
                    analysis['recommendations'].append('Consider moving tempdb to dedicated drive(s) to reduce I/O contention')
            
            # Check for percentage growth
            percent_growth_files = [f for f in tempdb_files if f['is_percent_growth']]
            if percent_growth_files:
                analysis['issues'].append({
                    'type': 'PERCENTAGE_GROWTH',
                    'severity': 'LOW',
                    'description': f'{len(percent_growth_files)} tempdb files using percentage growth'
                })
                analysis['recommendations'].append('Change tempdb files to use fixed MB growth instead of percentage')
            
            # Get tempdb usage statistics
            usage_query = """
            SELECT 
                SUM(total_page_count) * 8 / 1024 AS total_space_mb,
                SUM(allocated_extent_page_count) * 8 / 1024 AS allocated_space_mb,
                SUM(unallocated_extent_page_count) * 8 / 1024 AS unallocated_space_mb
            FROM sys.dm_db_file_space_usage
            WHERE database_id = DB_ID('tempdb')
            """
            
            usage_stats = self.connection.execute_query(usage_query)
            if usage_stats:
                analysis['usage'] = usage_stats[0]
                
                total_space = usage_stats[0].get('total_space_mb', 0) or 0
                allocated_space = usage_stats[0].get('allocated_space_mb', 0) or 0
                
                if total_space > 0:
                    usage_percentage = (allocated_space / total_space) * 100
                    analysis['usage_percentage'] = round(usage_percentage, 2)
                    
                    if usage_percentage > 80:
                        analysis['issues'].append({
                            'type': 'HIGH_USAGE',
                            'severity': 'HIGH',
                            'description': f'Tempdb usage at {usage_percentage:.1f}% - may need more space'
                        })
                        analysis['recommendations'].append('Consider increasing tempdb file sizes')
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing tempdb placement: {e}")
            return {'error': str(e)}
    
    def _analyze_drive_configuration(self) -> Dict[str, Any]:
        """Analyze overall drive configuration and detect shared usage"""
        try:
            configuration = {}
            
            # Get all database files and their drives
            all_files_query = """
            SELECT 
                DB_NAME(database_id) AS database_name,
                name AS file_name,
                physical_name,
                type_desc,
                LEFT(physical_name, 1) AS drive_letter,
                CAST(size AS BIGINT) * 8 / 1024 AS size_mb
            FROM sys.master_files
            WHERE database_id > 0
            ORDER BY drive_letter, database_id, type_desc
            """
            
            all_files = self.connection.execute_query(all_files_query)
            if not all_files:
                return {'error': 'Could not retrieve database file information'}
            
            # Group files by drive
            drives = {}
            for file_info in all_files:
                drive = file_info['drive_letter']
                if drive not in drives:
                    drives[drive] = {
                        'databases': set(),
                        'file_types': set(),
                        'total_files': 0,
                        'total_size_mb': 0,
                        'data_files': 0,
                        'log_files': 0,
                        'files': []
                    }
                
                drives[drive]['databases'].add(file_info['database_name'])
                drives[drive]['file_types'].add(file_info['type_desc'])
                drives[drive]['total_files'] += 1
                drives[drive]['total_size_mb'] += file_info['size_mb']
                drives[drive]['files'].append(file_info)
                
                if file_info['type_desc'] == 'ROWS':
                    drives[drive]['data_files'] += 1
                elif file_info['type_desc'] == 'LOG':
                    drives[drive]['log_files'] += 1
            
            # Convert sets to lists for JSON serialization
            for drive_info in drives.values():
                drive_info['databases'] = list(drive_info['databases'])
                drive_info['file_types'] = list(drive_info['file_types'])
            
            # Analyze drive usage patterns
            analysis = {
                'drives': drives,
                'issues': [],
                'recommendations': []
            }
            
            for drive, info in drives.items():
                # Check for mixed data and log files on same drive
                if 'ROWS' in info['file_types'] and 'LOG' in info['file_types']:
                    analysis['issues'].append({
                        'type': 'MIXED_FILE_TYPES',
                        'drive': drive,
                        'severity': 'MEDIUM',
                        'description': f'Drive {drive}: contains both data and log files'
                    })
                    analysis['recommendations'].append(f'Consider separating data and log files from drive {drive}:')
                
                # Check for multiple databases on same drive
                if len(info['databases']) > 3:
                    analysis['issues'].append({
                        'type': 'MULTIPLE_DATABASES',
                        'drive': drive,
                        'severity': 'LOW',
                        'description': f'Drive {drive}: contains {len(info["databases"])} databases'
                    })
                
                # Check for system databases with user databases
                system_dbs = {'master', 'model', 'msdb', 'tempdb'}
                user_dbs = set(info['databases']) - system_dbs
                if system_dbs & set(info['databases']) and user_dbs:
                    analysis['issues'].append({
                        'type': 'MIXED_SYSTEM_USER',
                        'drive': drive,
                        'severity': 'LOW',
                        'description': f'Drive {drive}: contains both system and user databases'
                    })
            
            configuration['analysis'] = analysis
            return configuration
            
        except Exception as e:
            self.logger.error(f"Error analyzing drive configuration: {e}")
            return {'error': str(e)}
    
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