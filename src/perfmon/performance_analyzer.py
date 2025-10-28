"""
Performance Counter Analyzer
Analyzes Windows Performance Monitor data for SQL Server bottlenecks
"""

import logging
import struct
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import subprocess
import json

class PerformanceCounterAnalyzer:
    """Analyzes Performance Monitor data and identifies bottlenecks"""
    
    def __init__(self, config):
        """Initialize performance counter analyzer
        
        Args:
            config: ConfigManager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Performance thresholds for analysis
        self.thresholds = {
            # CPU thresholds
            'cpu_high': 80.0,  # % Processor Time
            'cpu_critical': 95.0,
            'processor_queue_length_high': 2.0,
            
            # Memory thresholds
            'memory_available_low': 200,  # MB
            'memory_available_critical': 100,
            'page_life_expectancy_low': 300,  # seconds - CRITICAL THRESHOLD
            'page_life_expectancy_critical': 60,
            'pages_per_sec_high': 20,  # Memory pressure indicator
            
            # Disk I/O and Latency thresholds (as per requirements)
            'disk_queue_length_high': 2.0,  # Per disk - CRITICAL THRESHOLD
            'disk_queue_length_critical': 10.0,
            'disk_sec_per_read_slow': 0.010,  # 10ms - GOOD threshold
            'disk_sec_per_read_critical': 0.020,  # 20ms - CRITICAL threshold
            'disk_sec_per_write_slow': 0.010,  # 10ms - GOOD threshold  
            'disk_sec_per_write_critical': 0.020,  # 20ms - CRITICAL threshold
            'disk_time_high': 80.0,  # % Disk Time > 80% = bottleneck
            'disk_time_critical': 95.0,
            
            # SQL Server-specific thresholds
            'buffer_cache_hit_ratio_low': 95.0,  # %
            'buffer_cache_hit_ratio_critical': 90.0,
            'page_life_expectancy_memory_pressure': 300,  # PLE < 300 = memory pressure
            'lazy_writes_per_sec_high': 20,  # Memory pressure indicator
            'checkpoint_pages_per_sec_high': 100,  # Frequent flushes
            'page_splits_per_sec_high': 20,  # Fragmentation/poor indexing
            'log_flushes_per_sec_high': 20,  # Heavy transaction log activity
            'batch_requests_per_sec_high': 1000,
            'compilations_per_sec_high': 100,
            'recompilations_per_sec_high': 10,
            'lock_waits_per_sec_high': 1.0,
            'lock_wait_time_ms_high': 100,
            'deadlocks_per_sec_high': 0.1,
            'memory_grants_pending_high': 1,
            'processes_blocked_high': 1,
            
            # Disk Throughput thresholds
            'disk_bytes_per_sec_high': 100000000,  # 100MB/s baseline
            
            # Network thresholds
            'network_utilization_high': 70.0,  # % of bandwidth
            'network_utilization_critical': 90.0,
        }
    
    def analyze_performance_log(self, log_file: str) -> Dict[str, Any]:
        """Analyze performance counter log file (.blg or .json)
        
        Args:
            log_file: Path to .blg or .json performance log file
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Check file extension to determine processing method
            log_path = Path(log_file)
            
            if log_path.suffix.lower() == '.json':
                # Parse JSON data directly
                performance_data = self._parse_json_data(log_file)
            elif log_path.suffix.lower() == '.blg':
                # First extract data from .blg file to CSV for easier processing
                csv_file = self._extract_log_to_csv(log_file)
                
                if not csv_file:
                    return {'error': 'Failed to extract performance data'}
                
                # Parse CSV data
                performance_data = self._parse_csv_data(csv_file)
                
                # Cleanup temporary CSV file
                Path(csv_file).unlink(missing_ok=True)
            else:
                return {'error': f'Unsupported file format: {log_path.suffix}'}
            
            if not performance_data:
                return {'error': 'Failed to parse performance data'}
            
            # Analyze the data for bottlenecks
            analysis_results = {
                'summary': self._generate_summary(performance_data),
                'cpu_analysis': self._analyze_cpu_performance(performance_data),
                'memory_analysis': self._analyze_memory_performance(performance_data),
                'disk_analysis': self._analyze_disk_performance(performance_data),
                'sql_server_analysis': self._analyze_sql_server_performance(performance_data),
                'network_analysis': self._analyze_network_performance(performance_data),
                'bottlenecks': self._identify_bottlenecks(performance_data),
                'recommendations': self._generate_recommendations(performance_data),
                'raw_data_samples': self._get_data_samples(performance_data, 10)  # Last 10 samples
            }
            
            return analysis_results
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance log {log_file}: {e}")
            return {'error': str(e)}
    
    def take_realtime_snapshot(self, counters: List[str], duration_seconds: int = 30) -> Dict[str, Any]:
        """Take a real-time performance counter snapshot
        
        Args:
            counters: List of performance counter paths
            duration_seconds: How long to collect data (default 30 seconds)
            
        Returns:
            Dictionary containing snapshot analysis
        """
        try:
            # Create temporary data collector
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp_name = f"SQLSpeedinator_Snapshot_{timestamp}"
            
            # Create counter list for logman
            counter_list = ';'.join([f'"{counter}"' for counter in counters])
            
            # Start data collection
            start_cmd = f'logman create counter "{temp_name}" -c {counter_list} -si 1 -f bin -o "{temp_name}.blg"'
            subprocess.run(start_cmd, shell=True, check=True, capture_output=True)
            
            start_collection_cmd = f'logman start "{temp_name}"'
            subprocess.run(start_collection_cmd, shell=True, check=True, capture_output=True)
            
            # Wait for collection
            self.logger.info(f"Taking {duration_seconds}s performance snapshot...")
            time.sleep(duration_seconds)
            
            # Stop collection
            stop_cmd = f'logman stop "{temp_name}"'
            subprocess.run(stop_cmd, shell=True, capture_output=True)
            
            # Analyze the collected data
            log_file = f"{temp_name}.blg"
            analysis = self.analyze_performance_log(log_file)
            
            # Cleanup
            delete_cmd = f'logman delete "{temp_name}"'
            subprocess.run(delete_cmd, shell=True, capture_output=True)
            Path(log_file).unlink(missing_ok=True)
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error taking realtime snapshot: {e}")
            return {'error': str(e)}
    
    def _extract_log_to_csv(self, log_file: str) -> Optional[str]:
        """Extract .blg file to CSV using Windows relog command"""
        try:
            csv_file = log_file.replace('.blg', '.csv')
            cmd = f'relog "{log_file}" -f csv -o "{csv_file}"'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0 and Path(csv_file).exists():
                return csv_file
            else:
                self.logger.error(f"Failed to extract log to CSV: {result.stderr}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting log to CSV: {e}")
            return None
    
    def _parse_json_data(self, json_file: str) -> Dict[str, List[Tuple[datetime, float]]]:
        """Parse JSON performance data from PowerShell Get-Counter"""
        try:
            performance_data = {}
            
            # Read with utf-8-sig to handle BOM
            with open(json_file, 'r', encoding='utf-8-sig') as f:
                content = f.read().strip()
            
            # Handle incomplete JSON (might happen if process was terminated early)
            if not content.endswith(']'):
                content += ']'
            
            data = json.loads(content)
            
            self.logger.info(f"Raw JSON data has {len(data)} sample sets")
            
            # PowerShell Get-Counter JSON structure:
            # [
            #   {
            #     "Timestamp": "2025-10-28T14:09:59.8469394+01:00",
            #     "CounterSamples": [
            #       {
            #         "Path": "\\\\Computer\\Processor(_Total)\\% Processor Time",
            #         "InstanceName": "_Total",
            #         "CookedValue": 12.345
            #       },
            #       ...
            #     ]
            #   },
            #   ...
            # ]
            
            for sample_set in data:
                if not isinstance(sample_set, dict) or 'Timestamp' not in sample_set or 'CounterSamples' not in sample_set:
                    self.logger.debug(f"Skipping invalid sample set: {sample_set}")
                    continue
                
                try:
                    # Parse timestamp - handle ISO format with timezone
                    timestamp_str = sample_set['Timestamp']
                    # Remove timezone info for simpler parsing (Python datetime handling)
                    if '+' in timestamp_str:
                        timestamp_str = timestamp_str.split('+')[0]
                    elif 'Z' in timestamp_str:
                        timestamp_str = timestamp_str.replace('Z', '')
                    
                    # Parse the timestamp - handle microseconds that might be too long
                    if '.' in timestamp_str:
                        date_part, microsec_part = timestamp_str.split('.')
                        # Truncate microseconds to 6 digits if longer
                        if len(microsec_part) > 6:
                            microsec_part = microsec_part[:6]
                        timestamp_str = f"{date_part}.{microsec_part}"
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
                    
                    # Process counter samples
                    for counter_sample in sample_set['CounterSamples']:
                        if not isinstance(counter_sample, dict):
                            continue
                        
                        path = counter_sample.get('Path', '')
                        cooked_value = counter_sample.get('CookedValue')
                        
                        self.logger.debug(f"Processing counter: Path='{path}', Value={cooked_value}")
                        
                        if path and cooked_value is not None:
                            # Clean up the counter path for consistency
                            clean_path = path.replace('\\\\', '\\').strip('\\')
                            
                            self.logger.debug(f"Clean path: '{clean_path}'")
                            
                            if clean_path not in performance_data:
                                performance_data[clean_path] = []
                            
                            try:
                                value = float(cooked_value)
                                performance_data[clean_path].append((timestamp, value))
                                self.logger.debug(f"Added sample for {clean_path}: {value}")
                            except (ValueError, TypeError) as e:
                                # Skip invalid values
                                self.logger.debug(f"Skipping invalid value for {clean_path}: {cooked_value} - {e}")
                                pass
                        else:
                            self.logger.debug(f"Skipping counter - missing path or value: Path='{path}', Value={cooked_value}")
                
                except Exception as e:
                    self.logger.debug(f"Error parsing sample set: {e}")
                    continue
            
            self.logger.info(f"Parsed JSON data: {len(performance_data)} counters, total samples: {sum(len(data) for data in performance_data.values())}")
            
            # Debug: print first few samples for each counter
            for counter_name, samples in list(performance_data.items())[:3]:
                self.logger.info(f"Counter '{counter_name}': {len(samples)} samples, first value: {samples[0][1] if samples else 'N/A'}")
            
            return performance_data
            
        except Exception as e:
            self.logger.error(f"Error parsing JSON data: {e}")
            return {}
    
    def _parse_csv_data(self, csv_file: str) -> Dict[str, List[Tuple[datetime, float]]]:
        """Parse CSV performance data"""
        try:
            performance_data = {}
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            if len(lines) < 2:
                return {}
            
            # Parse header to get counter names
            header = lines[0].strip().split(',')
            counter_names = [name.strip('"') for name in header[1:]]  # Skip timestamp column
            
            # Parse data lines
            for line in lines[1:]:
                if not line.strip():
                    continue
                
                try:
                    values = line.strip().split(',')
                    timestamp_str = values[0].strip('"')
                    timestamp = datetime.strptime(timestamp_str, '%m/%d/%Y %H:%M:%S.%f')
                    
                    for i, counter_name in enumerate(counter_names):
                        if counter_name not in performance_data:
                            performance_data[counter_name] = []
                        
                        try:
                            value = float(values[i + 1])
                            performance_data[counter_name].append((timestamp, value))
                        except (ValueError, IndexError):
                            # Skip invalid values
                            pass
                            
                except Exception as e:
                    self.logger.debug(f"Error parsing line: {line.strip()}: {e}")
                    continue
            
            return performance_data
            
        except Exception as e:
            self.logger.error(f"Error parsing CSV data: {e}")
            return {}
    
    def _generate_summary(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
        """Generate summary of performance data"""
        if not performance_data:
            return {}
        
        # Calculate collection period
        all_timestamps = []
        for counter_data in performance_data.values():
            if counter_data:
                all_timestamps.extend([ts for ts, _ in counter_data])
        
        if not all_timestamps:
            return {}
        
        start_time = min(all_timestamps)
        end_time = max(all_timestamps)
        duration = end_time - start_time
        
        return {
            'collection_start': start_time.isoformat(),
            'collection_end': end_time.isoformat(),
            'duration_minutes': duration.total_seconds() / 60,
            'total_counters': len(performance_data),
            'total_samples': sum(len(data) for data in performance_data.values())
        }
    
    def _analyze_cpu_performance(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
        """Analyze CPU performance metrics"""
        cpu_analysis = {
            'status': 'OK',
            'findings': [],
            'metrics': {}
        }
        
        # Find CPU related counters
        cpu_counters = {
            'processor_time': None,
            'privileged_time': None,
            'user_time': None,
            'processor_queue_length': None
        }
        
        for counter_name, data in performance_data.items():
            if 'processor' in counter_name.lower() and '% processor time' in counter_name.lower() and '_total' in counter_name.lower():
                cpu_counters['processor_time'] = data
            elif 'processor queue length' in counter_name.lower():
                cpu_counters['processor_queue_length'] = data
        
        # Analyze processor time
        if cpu_counters['processor_time']:
            values = [value for _, value in cpu_counters['processor_time']]
            avg_cpu = sum(values) / len(values)
            max_cpu = max(values)
            
            cpu_analysis['metrics']['avg_processor_time'] = round(avg_cpu, 2)
            cpu_analysis['metrics']['max_processor_time'] = round(max_cpu, 2)
            
            if avg_cpu > self.thresholds['cpu_critical']:
                cpu_analysis['status'] = 'CRITICAL'
                cpu_analysis['findings'].append(f"Critical CPU usage: {avg_cpu:.1f}% average")
            elif avg_cpu > self.thresholds['cpu_high']:
                cpu_analysis['status'] = 'WARNING'
                cpu_analysis['findings'].append(f"High CPU usage: {avg_cpu:.1f}% average")
        
        # Analyze processor queue length
        if cpu_counters['processor_queue_length']:
            values = [value for _, value in cpu_counters['processor_queue_length']]
            avg_queue = sum(values) / len(values)
            max_queue = max(values)
            
            cpu_analysis['metrics']['avg_processor_queue'] = round(avg_queue, 2)
            cpu_analysis['metrics']['max_processor_queue'] = round(max_queue, 2)
            
            if avg_queue > 2.0:
                if cpu_analysis['status'] == 'OK':
                    cpu_analysis['status'] = 'WARNING'
                cpu_analysis['findings'].append(f"High processor queue length: {avg_queue:.1f} average")
        
        return cpu_analysis
    
    def _analyze_memory_performance(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
        """Analyze memory performance metrics"""
        memory_analysis = {
            'status': 'OK',
            'findings': [],
            'metrics': {}
        }
        
        # Find memory related counters
        memory_counters = {
            'available_mbytes': None,
            'pages_per_sec': None,
            'page_life_expectancy': None,
            'buffer_cache_hit_ratio': None
        }
        
        for counter_name, data in performance_data.items():
            if 'available mbytes' in counter_name.lower():
                memory_counters['available_mbytes'] = data
            elif 'pages/sec' in counter_name.lower():
                memory_counters['pages_per_sec'] = data
            elif 'page life expectancy' in counter_name.lower():
                memory_counters['page_life_expectancy'] = data
            elif 'buffer cache hit ratio' in counter_name.lower():
                memory_counters['buffer_cache_hit_ratio'] = data
        
        # Analyze available memory
        if memory_counters['available_mbytes']:
            values = [value for _, value in memory_counters['available_mbytes']]
            avg_available = sum(values) / len(values)
            min_available = min(values)
            
            memory_analysis['metrics']['avg_available_mb'] = round(avg_available, 0)
            memory_analysis['metrics']['min_available_mb'] = round(min_available, 0)
            
            if min_available < self.thresholds['memory_available_critical']:
                memory_analysis['status'] = 'CRITICAL'
                memory_analysis['findings'].append(f"Critical memory shortage: {min_available:.0f} MB minimum available")
            elif avg_available < self.thresholds['memory_available_low']:
                memory_analysis['status'] = 'WARNING'
                memory_analysis['findings'].append(f"Low available memory: {avg_available:.0f} MB average")
        
        # Analyze page life expectancy
        if memory_counters['page_life_expectancy']:
            values = [value for _, value in memory_counters['page_life_expectancy']]
            avg_ple = sum(values) / len(values)
            min_ple = min(values)
            
            memory_analysis['metrics']['avg_page_life_expectancy'] = round(avg_ple, 0)
            memory_analysis['metrics']['min_page_life_expectancy'] = round(min_ple, 0)
            
            if min_ple < self.thresholds['page_life_expectancy_critical']:
                memory_analysis['status'] = 'CRITICAL'
                memory_analysis['findings'].append(f"Critical page life expectancy: {min_ple:.0f} seconds minimum")
            elif avg_ple < self.thresholds['page_life_expectancy_low']:
                if memory_analysis['status'] == 'OK':
                    memory_analysis['status'] = 'WARNING'
                memory_analysis['findings'].append(f"Low page life expectancy: {avg_ple:.0f} seconds average")
        
        return memory_analysis
    
    def _analyze_disk_performance(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
        """Analyze disk performance metrics"""
        disk_analysis = {
            'status': 'OK',
            'findings': [],
            'metrics': {}
        }
        
        # Find disk related counters
        disk_metrics = {}
        
        for counter_name, data in performance_data.items():
            if 'disk queue length' in counter_name.lower():
                disk_metrics.setdefault('queue_length', []).extend(data)
            elif 'avg. disk sec/read' in counter_name.lower():
                disk_metrics.setdefault('sec_per_read', []).extend(data)
            elif 'avg. disk sec/write' in counter_name.lower():
                disk_metrics.setdefault('sec_per_write', []).extend(data)
        
        # Analyze disk queue length
        if 'queue_length' in disk_metrics:
            values = [value for _, value in disk_metrics['queue_length']]
            avg_queue = sum(values) / len(values)
            max_queue = max(values)
            
            disk_analysis['metrics']['avg_disk_queue_length'] = round(avg_queue, 2)
            disk_analysis['metrics']['max_disk_queue_length'] = round(max_queue, 2)
            
            if max_queue > self.thresholds['disk_queue_length_critical']:
                disk_analysis['status'] = 'CRITICAL'
                disk_analysis['findings'].append(f"Critical disk queue length: {max_queue:.1f} maximum")
            elif avg_queue > self.thresholds['disk_queue_length_high']:
                disk_analysis['status'] = 'WARNING'
                disk_analysis['findings'].append(f"High disk queue length: {avg_queue:.2f} average")
        
        # Analyze disk response times
        if 'sec_per_read' in disk_metrics:
            values = [value for _, value in disk_metrics['sec_per_read']]
            avg_read_time = sum(values) / len(values)
            max_read_time = max(values)
            
            disk_analysis['metrics']['avg_disk_read_ms'] = round(avg_read_time * 1000, 1)
            disk_analysis['metrics']['max_disk_read_ms'] = round(max_read_time * 1000, 1)
            
            if max_read_time > self.thresholds['disk_sec_per_read_critical']:
                disk_analysis['status'] = 'CRITICAL'
                disk_analysis['findings'].append(f"Critical disk read latency: {max_read_time*1000:.1f}ms maximum")
            elif avg_read_time > self.thresholds['disk_sec_per_read_slow']:
                if disk_analysis['status'] == 'OK':
                    disk_analysis['status'] = 'WARNING'
                disk_analysis['findings'].append(f"Slow disk reads: {avg_read_time*1000:.1f}ms average")
        
        return disk_analysis
    
    def _analyze_sql_server_performance(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
        """Analyze SQL Server specific performance metrics"""
        sql_analysis = {
            'status': 'OK',
            'findings': [],
            'metrics': {}
        }
        
        # Find SQL Server counters
        sql_counters = {}
        
        for counter_name, data in performance_data.items():
            if 'batch requests/sec' in counter_name.lower():
                sql_counters['batch_requests'] = data
            elif 'sql compilations/sec' in counter_name.lower():
                sql_counters['compilations'] = data
            elif 'sql re-compilations/sec' in counter_name.lower():
                sql_counters['recompilations'] = data
            elif 'lock waits/sec' in counter_name.lower():
                sql_counters['lock_waits'] = data
            elif 'number of deadlocks/sec' in counter_name.lower():
                sql_counters['deadlocks'] = data
            elif 'buffer cache hit ratio' in counter_name.lower():
                sql_counters['buffer_hit_ratio'] = data
        
        # Analyze batch requests
        if 'batch_requests' in sql_counters:
            values = [value for _, value in sql_counters['batch_requests']]
            avg_batches = sum(values) / len(values)
            sql_analysis['metrics']['avg_batch_requests_per_sec'] = round(avg_batches, 1)
            
            if avg_batches > self.thresholds['batch_requests_per_sec_high']:
                sql_analysis['findings'].append(f"High batch request rate: {avg_batches:.1f}/sec")
        
        # Analyze compilations
        if 'compilations' in sql_counters:
            values = [value for _, value in sql_counters['compilations']]
            avg_compilations = sum(values) / len(values)
            sql_analysis['metrics']['avg_compilations_per_sec'] = round(avg_compilations, 2)
            
            if avg_compilations > self.thresholds['compilations_per_sec_high']:
                sql_analysis['status'] = 'WARNING'
                sql_analysis['findings'].append(f"High compilation rate: {avg_compilations:.1f}/sec")
        
        # Analyze lock waits
        if 'lock_waits' in sql_counters:
            values = [value for _, value in sql_counters['lock_waits']]
            avg_lock_waits = sum(values) / len(values)
            sql_analysis['metrics']['avg_lock_waits_per_sec'] = round(avg_lock_waits, 2)
            
            if avg_lock_waits > self.thresholds['lock_waits_per_sec_high']:
                sql_analysis['status'] = 'WARNING'
                sql_analysis['findings'].append(f"High lock wait rate: {avg_lock_waits:.2f}/sec")
        
        return sql_analysis
    
    def _analyze_network_performance(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
        """Analyze network performance metrics"""
        network_analysis = {
            'status': 'OK',
            'findings': [],
            'metrics': {}
        }
        
        # Find network counters
        network_counters = {}
        
        for counter_name, data in performance_data.items():
            if 'bytes total/sec' in counter_name.lower():
                network_counters.setdefault('bytes_total', []).extend(data)
            elif 'current bandwidth' in counter_name.lower():
                network_counters.setdefault('bandwidth', []).extend(data)
        
        # Calculate network utilization if we have both metrics
        if 'bytes_total' in network_counters and 'bandwidth' in network_counters:
            # This is a simplified calculation - would need more sophisticated analysis for production
            total_bytes = [value for _, value in network_counters['bytes_total']]
            bandwidth_values = [value for _, value in network_counters['bandwidth']]
            
            if total_bytes and bandwidth_values:
                avg_bytes_per_sec = sum(total_bytes) / len(total_bytes)
                avg_bandwidth = sum(bandwidth_values) / len(bandwidth_values)
                
                if avg_bandwidth > 0:
                    utilization_pct = (avg_bytes_per_sec * 8 * 100) / avg_bandwidth  # Convert to percentage
                    network_analysis['metrics']['avg_utilization_percent'] = round(utilization_pct, 1)
                    
                    if utilization_pct > self.thresholds['network_utilization_critical']:
                        network_analysis['status'] = 'CRITICAL'
                        network_analysis['findings'].append(f"Critical network utilization: {utilization_pct:.1f}%")
                    elif utilization_pct > self.thresholds['network_utilization_high']:
                        network_analysis['status'] = 'WARNING'
                        network_analysis['findings'].append(f"High network utilization: {utilization_pct:.1f}%")
        
        return network_analysis
    
    def _identify_bottlenecks(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> List[Dict[str, Any]]:
        """Identify performance bottlenecks from analysis"""
        bottlenecks = []
        
        # Run all analysis functions to gather findings
        analyses = [
            ('CPU', self._analyze_cpu_performance(performance_data)),
            ('Memory', self._analyze_memory_performance(performance_data)),
            ('Disk', self._analyze_disk_performance(performance_data)),
            ('SQL Server', self._analyze_sql_server_performance(performance_data)),
            ('Network', self._analyze_network_performance(performance_data))
        ]
        
        for category, analysis in analyses:
            if analysis['status'] in ['WARNING', 'CRITICAL']:
                for finding in analysis['findings']:
                    bottlenecks.append({
                        'category': category,
                        'severity': analysis['status'],
                        'description': finding,
                        'metrics': analysis.get('metrics', {})
                    })
        
        # Sort by severity (CRITICAL first)
        bottlenecks.sort(key=lambda x: 0 if x['severity'] == 'CRITICAL' else 1)
        
        return bottlenecks
    
    def _generate_recommendations(self, performance_data: Dict[str, List[Tuple[datetime, float]]]) -> List[str]:
        """Generate performance recommendations based on analysis"""
        recommendations = []
        bottlenecks = self._identify_bottlenecks(performance_data)
        
        cpu_issues = [b for b in bottlenecks if b['category'] == 'CPU']
        memory_issues = [b for b in bottlenecks if b['category'] == 'Memory']
        disk_issues = [b for b in bottlenecks if b['category'] == 'Disk']
        sql_issues = [b for b in bottlenecks if b['category'] == 'SQL Server']
        
        if cpu_issues:
            recommendations.extend([
                "Investigate CPU-intensive queries using SQL Server profiler",
                "Consider query optimization and index tuning",
                "Check for parallel execution issues (MAXDOP settings)",
                "Consider CPU upgrade if consistently high"
            ])
        
        if memory_issues:
            recommendations.extend([
                "Review SQL Server max memory configuration",
                "Investigate memory-intensive queries and procedures",
                "Consider adding more RAM to the server",
                "Check for memory leaks in applications"
            ])
        
        if disk_issues:
            recommendations.extend([
                "Consider moving to faster storage (SSD)",
                "Separate data, log, and tempdb files across different drives",
                "Optimize database file growth settings",
                "Review database maintenance operations timing"
            ])
        
        if sql_issues:
            recommendations.extend([
                "Implement query plan caching strategies",
                "Review and optimize frequently executed queries",
                "Consider partitioning for large tables",
                "Optimize index maintenance schedules"
            ])
        
        return recommendations
    
    def _get_data_samples(self, performance_data: Dict[str, List[Tuple[datetime, float]]], sample_count: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Get sample data points for visualization"""
        samples = {}
        
        for counter_name, data in performance_data.items():
            if len(data) > 0:
                # Take evenly spaced samples
                step = max(1, len(data) // sample_count)
                sampled_data = data[::step][-sample_count:]
                
                samples[counter_name] = [
                    {
                        'timestamp': timestamp.isoformat(),
                        'value': value
                    }
                    for timestamp, value in sampled_data
                ]
        
        return samples