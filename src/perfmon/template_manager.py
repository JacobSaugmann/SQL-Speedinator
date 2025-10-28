"""
Performance Monitor (PerfMon) Template Manager
Manages PerfMon templates, data collection setup, and analysis
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any, Optional
import subprocess
import time
from datetime import datetime, timedelta

class PerfMonTemplateManager:
    """Manages PerfMon templates and data collection with smart collection management"""
    
    def __init__(self, config):
        """Initialize PerfMon template manager
        
        Args:
            config: ConfigManager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.perfmon_dir = Path(__file__).parent.parent / 'perfmon'
        self.templates_dir = self.perfmon_dir / 'templates'
        self.data_dir = self.perfmon_dir / 'data'
        
        # Collection management
        self.sqlspeedinator_prefix = "SQLSpeedinator"
        self.managed_collections = set()  # Track collections we created
        
        # Ensure directories exist
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def parse_template(self, template_path: Path) -> Dict[str, Any]:
        """Parse PerfMon XML template and extract counters
        
        Args:
            template_path: Path to XML template file
            
        Returns:
            Dictionary containing parsed template information
        """
        try:
            tree = ET.parse(template_path)
            root = tree.getroot()
            
            # Extract basic information
            display_name_elem = root.find('DisplayName')
            description_elem = root.find('Description')
            
            template_info = {
                'name': display_name_elem.text if display_name_elem is not None and display_name_elem.text else 'Unknown',
                'description': description_elem.text if description_elem is not None and description_elem.text else '',
                'sample_interval': 15,  # Default
                'counters': [],
                'output_location': str(self.data_dir),
                'max_size_mb': 1024,  # 1GB default
                'duration_hours': 4  # 4 hours default
            }
            
            # Extract performance counter data collector info
            perf_collector = root.find('PerformanceCounterDataCollector')
            if perf_collector is not None:
                # Sample interval
                sample_interval = perf_collector.find('SampleInterval')
                if sample_interval is not None and sample_interval.text:
                    template_info['sample_interval'] = int(sample_interval.text)
                
                # Extract all counters
                counters = perf_collector.findall('Counter')
                for counter in counters:
                    counter_path = counter.text
                    if counter_path:
                        template_info['counters'].append({
                            'path': counter_path,
                            'category': self._extract_counter_category(counter_path),
                            'name': self._extract_counter_name(counter_path),
                            'instance': self._extract_counter_instance(counter_path)
                        })
            
            self.logger.info(f"Parsed template: {template_info['name']} with {len(template_info['counters'])} counters")
            return template_info
            
        except Exception as e:
            self.logger.error(f"Error parsing template {template_path}: {e}")
            return {}
    
    def _extract_counter_category(self, counter_path: str) -> str:
        """Extract category from counter path like \\SQLServer:Buffer Manager\\Page life expectancy"""
        if not counter_path.startswith('\\'):
            return 'Unknown'
        
        parts = counter_path.split('\\')
        if len(parts) >= 2:
            return parts[1].split('(')[0]  # Remove instance part if present
        return 'Unknown'
    
    def _extract_counter_name(self, counter_path: str) -> str:
        """Extract counter name from counter path"""
        if not counter_path.startswith('\\'):
            return counter_path
        
        parts = counter_path.split('\\')
        if len(parts) >= 3:
            return parts[2]
        return 'Unknown'
    
    def _extract_counter_instance(self, counter_path: str) -> Optional[str]:
        """Extract instance from counter path like \\SQLServer:Buffer Manager(*)\\Page life expectancy"""
        if '(' in counter_path and ')' in counter_path:
            start = counter_path.find('(')
            end = counter_path.find(')')
            instance = counter_path[start+1:end]
            return instance if instance != '*' else None
        return None
    
    def create_data_collector_set(self, template_info: Dict[str, Any], collection_name: str) -> str:
        """Create Windows Performance Data Collector Set
        
        Args:
            template_info: Parsed template information
            collection_name: Name for the data collector set
            
        Returns:
            Path to created data collector set XML file
        """
        try:
            # Generate timestamp for unique naming
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dcs_name = f"SQLSpeedinator_{collection_name}_{timestamp}"
            
            # Create data output directory
            output_dir = self.data_dir / timestamp
            output_dir.mkdir(exist_ok=True)
            
            # Create XML for data collector set
            dcs_xml = self._generate_dcs_xml(template_info, dcs_name, str(output_dir))
            
            # Save XML file
            dcs_file = self.templates_dir / f"{dcs_name}.xml"
            with open(dcs_file, 'w', encoding='utf-16') as f:
                f.write(dcs_xml)
            
            self.logger.info(f"Created data collector set: {dcs_name}")
            return str(dcs_file)
            
        except Exception as e:
            self.logger.error(f"Error creating data collector set: {e}")
            return ""
    
    def _generate_dcs_xml(self, template_info: Dict[str, Any], dcs_name: str, output_dir: str) -> str:
        """Generate XML for Windows Data Collector Set"""
        # Calculate max size in bytes (input is MB)
        max_size_bytes = template_info.get('max_size_mb', 1024) * 1024 * 1024
        
        # Generate counter XML elements
        counter_elements = ""
        for counter in template_info['counters']:
            counter_elements += f"\t\t<Counter>{counter['path']}</Counter>\n"
        
        xml_template = f'''<?xml version="1.0" encoding="UTF-16"?>
<DataCollectorSet>
    <Status>1</Status>
    <Duration>0</Duration>
    <Description>SQL Speedinator Performance Collection</Description>
    <DisplayName>{dcs_name}</DisplayName>
    <Name>{dcs_name}</Name>
    <OutputLocation>{output_dir}</OutputLocation>
    <RootPath>{output_dir}</RootPath>
    <SegmentMaxSize>{max_size_bytes}</SegmentMaxSize>
    <UserAccount>SYSTEM</UserAccount>
    <PerformanceCounterDataCollector>
        <DataCollectorType>0</DataCollectorType>
        <Name>{dcs_name}_Collector</Name>
        <FileName>{dcs_name}_</FileName>
        <FileNameFormat>3</FileNameFormat>
        <FileNameFormatPattern>yyyyMMddHHmm</FileNameFormatPattern>
        <SampleInterval>{template_info.get('sample_interval', 15)}</SampleInterval>
        <LogFileFormat>3</LogFileFormat>
{counter_elements}
    </PerformanceCounterDataCollector>
</DataCollectorSet>'''
        
        return xml_template
    
    def start_data_collection(self, dcs_file: str, duration_hours: float = 4.0) -> Dict[str, Any]:
        """Start PerfMon data collection using PowerShell Get-Counter
        
        Args:
            dcs_file: Path to data collector set XML file (used for naming)
            duration_hours: How long to collect data (default 4 hours)
            
        Returns:
            Dictionary with collection information
        """
        try:
            dcs_name = Path(dcs_file).stem
            
            # Create output directory
            output_dir = self.data_dir / datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir.mkdir(exist_ok=True, parents=True)
            
            # Define essential counters for SQL Server performance monitoring
            counters = [
                "\\Processor(_Total)\\% Processor Time",
                "\\System\\Processor Queue Length", 
                "\\Memory\\Available MBytes",
                "\\Memory\\Pages/sec",
                "\\LogicalDisk(*)\\% Idle Time",
                "\\LogicalDisk(*)\\Avg. Disk Queue Length",
                "\\LogicalDisk(*)\\Avg. Disk sec/Read",
                "\\LogicalDisk(*)\\Avg. Disk sec/Write"
            ]
            
            # Output file path
            output_file = output_dir / f"{dcs_name}.json"
            
            # Calculate how many samples to collect (1 per second)
            duration_seconds = int(duration_hours * 3600)
            max_samples = duration_seconds  # Collect for the exact duration specified
            
            # Create PowerShell script that uses WMI (fallback for systems with disabled perfmon)
            ps_script = f'''
            $outputFile = "{output_file}"
            
            # Initialize JSON array
            Write-Output "[" | Out-File -FilePath $outputFile -Encoding UTF8 -NoNewline
            
            $samples = 0
            $maxSamples = {max_samples}
            
            while ($samples -lt $maxSamples) {{
                try {{
                    # Get performance data using WMI
                    $timestamp = Get-Date
                    $cpu = Get-WmiObject -Class Win32_Processor | Measure-Object -Property LoadPercentage -Average -ErrorAction SilentlyContinue
                    $memory = Get-WmiObject -Class Win32_OperatingSystem -ErrorAction SilentlyContinue
                    $processes = Get-WmiObject -Class Win32_Process -ErrorAction SilentlyContinue | Measure-Object | Select-Object -ExpandProperty Count
                    
                    # Get disk performance data
                    $disks = Get-WmiObject -Class Win32_LogicalDisk -Filter "DriveType=3" -ErrorAction SilentlyContinue
                    $diskPerf = Get-WmiObject -Class Win32_PerfRawData_PerfDisk_LogicalDisk -ErrorAction SilentlyContinue
                    
                    if ($memory) {{
                        $availableMemoryMB = [math]::Round($memory.FreePhysicalMemory / 1024, 2)
                        $totalMemoryMB = [math]::Round($memory.TotalVisibleMemorySize / 1024, 2)
                        $usedMemoryPercent = [math]::Round((($totalMemoryMB - $availableMemoryMB) / $totalMemoryMB) * 100, 2)
                    }} else {{
                        $availableMemoryMB = 0
                        $usedMemoryPercent = 0
                    }}
                    
                    $cpuPercent = if ($cpu -and $cpu.Average) {{ $cpu.Average }} else {{ 0 }}
                    
                    # Initialize counter samples array
                    $counterSamples = @(
                        @{{
                            Path = "\\Computer\\Processor(_Total)\\% Processor Time"
                            InstanceName = "_Total"
                            CookedValue = $cpuPercent
                        }},
                        @{{
                            Path = "\\Computer\\Memory\\Available MBytes"
                            InstanceName = ""
                            CookedValue = $availableMemoryMB
                        }},
                        @{{
                            Path = "\\Computer\\Memory\\% Memory Used"
                            InstanceName = ""
                            CookedValue = $usedMemoryPercent
                        }},
                        @{{
                            Path = "\\Computer\\System\\Process Count"
                            InstanceName = ""
                            CookedValue = $processes
                        }}
                    )
                    
                    # Add disk performance counters
                    if ($disks) {{
                        foreach ($disk in $disks) {{
                            $driveLetter = $disk.DeviceID
                            $freeSpaceGB = [math]::Round($disk.FreeSpace / 1GB, 2)
                            $totalSpaceGB = [math]::Round($disk.Size / 1GB, 2)
                            $usedSpacePercent = if ($totalSpaceGB -gt 0) {{ [math]::Round((($totalSpaceGB - $freeSpaceGB) / $totalSpaceGB) * 100, 2) }} else {{ 0 }}
                            
                            $counterSamples += @(
                                @{{
                                    Path = "\\Computer\\LogicalDisk($driveLetter)\\Free Megabytes"
                                    InstanceName = $driveLetter
                                    CookedValue = $freeSpaceGB * 1024
                                }},
                                @{{
                                    Path = "\\Computer\\LogicalDisk($driveLetter)\\% Disk Space Used"
                                    InstanceName = $driveLetter
                                    CookedValue = $usedSpacePercent
                                }}
                            )
                        }}
                    }}
                    
                    # Add disk performance metrics from performance counters if available
                    if ($diskPerf) {{
                        foreach ($perf in $diskPerf) {{
                            if ($perf.Name -ne "_Total" -and $perf.Name -match "^[A-Z]:$") {{
                                $diskQueueLength = if ($perf.CurrentDiskQueueLength) {{ $perf.CurrentDiskQueueLength }} else {{ 0 }}
                                
                                # Convert raw performance counter values to meaningful metrics
                                # These are cumulative counters, so we need to calculate rates or use current values
                                $diskReadTime = if ($perf.AvgDiskSecPerRead -and $perf.AvgDiskSecPerRead_Base -and $perf.AvgDiskSecPerRead_Base -gt 0) {{ 
                                    # Convert from 100ns units to milliseconds
                                    [math]::Round(($perf.AvgDiskSecPerRead / $perf.AvgDiskSecPerRead_Base) * 100 / 1000000, 3) 
                                }} else {{ 0 }}
                                $diskWriteTime = if ($perf.AvgDiskSecPerWrite -and $perf.AvgDiskSecPerWrite_Base -and $perf.AvgDiskSecPerWrite_Base -gt 0) {{ 
                                    # Convert from 100ns units to milliseconds  
                                    [math]::Round(($perf.AvgDiskSecPerWrite / $perf.AvgDiskSecPerWrite_Base) * 100 / 1000000, 3) 
                                }} else {{ 0 }}
                                
                                # Use disk busy time as percentage
                                $diskBusyPercent = if ($perf.PercentDiskTime -and $perf.PercentDiskTime_Base -and $perf.PercentDiskTime_Base -gt 0) {{
                                    [math]::Round(($perf.PercentDiskTime / $perf.PercentDiskTime_Base) * 100, 2)
                                }} else {{ 0 }}
                                
                                $counterSamples += @(
                                    @{{
                                        Path = "\\Computer\\LogicalDisk($($perf.Name))\\Current Disk Queue Length"
                                        InstanceName = $perf.Name
                                        CookedValue = $diskQueueLength
                                    }},
                                    @{{
                                        Path = "\\Computer\\LogicalDisk($($perf.Name))\\Avg. Disk sec/Read"
                                        InstanceName = $perf.Name
                                        CookedValue = $diskReadTime
                                    }},
                                    @{{
                                        Path = "\\Computer\\LogicalDisk($($perf.Name))\\Avg. Disk sec/Write"
                                        InstanceName = $perf.Name
                                        CookedValue = $diskWriteTime
                                    }},
                                    @{{
                                        Path = "\\Computer\\LogicalDisk($($perf.Name))\\% Disk Time"
                                        InstanceName = $perf.Name
                                        CookedValue = $diskBusyPercent
                                    }}
                                )
                            }}
                        }}
                    }}
                    
                    $sampleData = @{{
                        Timestamp = $timestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffffffK")
                        CounterSamples = $counterSamples
                    }}
                    
                    $json = $sampleData | ConvertTo-Json -Depth 3 -Compress
                    if ($samples -gt 0) {{
                        Write-Output "," | Out-File -FilePath $outputFile -Append -Encoding UTF8 -NoNewline
                    }}
                    Write-Output $json | Out-File -FilePath $outputFile -Append -Encoding UTF8 -NoNewline
                    
                    $samples++
                    Start-Sleep -Seconds 1
                }} catch {{
                    # On error, create a minimal sample with timestamp
                    $timestamp = Get-Date
                    $sampleData = @{{
                        Timestamp = $timestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffffffK")
                        CounterSamples = @(
                            @{{
                                Path = "\\Computer\\System\\Error"
                                InstanceName = ""
                                CookedValue = 1
                            }}
                        )
                    }}
                    
                    $json = $sampleData | ConvertTo-Json -Depth 3 -Compress
                    if ($samples -gt 0) {{
                        Write-Output "," | Out-File -FilePath $outputFile -Append -Encoding UTF8 -NoNewline
                    }}
                    Write-Output $json | Out-File -FilePath $outputFile -Append -Encoding UTF8 -NoNewline
                    
                    $samples++
                    Start-Sleep -Seconds 1
                }}
            }}
            
            # Close JSON array
            Write-Output "]" | Out-File -FilePath $outputFile -Append -Encoding UTF8
            '''
            
            # Save PowerShell script
            ps_script_file = output_dir / "collect.ps1"
            with open(ps_script_file, 'w', encoding='utf-8') as f:
                f.write(ps_script)
            
            self.logger.info(f"Starting PowerShell data collection for {duration_hours} hours")
            self.logger.info(f"Output file: {output_file}")
            
            # Start PowerShell script in background
            powershell_cmd = [
                'powershell.exe',
                '-ExecutionPolicy', 'Bypass',
                '-File', str(ps_script_file)
            ]
            
            process = subprocess.Popen(
                powershell_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False
            )
            
            # Store process info for stopping later
            collection_info = {
                'success': True,
                'dcs_name': dcs_name,
                'start_time': datetime.now().isoformat(),
                'end_time': (datetime.now() + timedelta(hours=duration_hours)).isoformat(),
                'duration_hours': duration_hours,
                'status': 'running',
                'output_file': str(output_file),
                'process': process,
                'pid': process.pid
            }
            
            self.logger.info(f"Started PowerShell data collection (PID: {process.pid})")
            return collection_info
            
        except Exception as e:
            self.logger.error(f"Error starting data collection: {e}")
            return {'success': False, 'error': str(e)}
    
    def stop_data_collection(self, collection_info: Dict[str, Any]) -> bool:
        """Stop PerfMon data collection
        
        Args:
            collection_info: Collection information from start_data_collection
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if isinstance(collection_info, str):
                # Old API compatibility - collection_info is just the name
                dcs_name = collection_info
                # Try to stop with logman (old method)
                stop_cmd = ['logman', 'stop', dcs_name]
                result = subprocess.run(stop_cmd, capture_output=True, text=True)
                return result.returncode == 0
            
            # New API - collection_info is a dictionary
            process = collection_info.get('process')
            dcs_name = collection_info.get('dcs_name', 'Unknown')
            
            if process and hasattr(process, 'terminate'):
                try:
                    # Terminate the typeperf process
                    process.terminate()
                    process.wait(timeout=5)  # Wait up to 5 seconds
                    self.logger.info(f"Stopped data collection: {dcs_name}")
                    return True
                except subprocess.TimeoutExpired:
                    # Force kill if terminate doesn't work
                    process.kill()
                    process.wait()
                    self.logger.warning(f"Force killed data collection process: {dcs_name}")
                    return True
                except Exception as e:
                    self.logger.error(f"Error stopping process: {e}")
                    return False
            else:
                self.logger.warning("No process information available to stop")
                return False
            
        except Exception as e:
            self.logger.error(f"Error stopping data collection: {e}")
            return False
    
    def get_available_templates(self) -> List[Dict[str, Any]]:
        """Get list of available PerfMon templates
        
        Returns:
            List of template information dictionaries
        """
        templates = []
        
        for template_file in self.templates_dir.glob('*.xml'):
            try:
                template_info = self.parse_template(template_file)
                if template_info:
                    template_info['file_path'] = str(template_file)
                    templates.append(template_info)
            except Exception as e:
                self.logger.warning(f"Could not parse template {template_file}: {e}")
        
        return templates
    
    def get_default_template_path(self) -> Path:
        """Get path to default SQL performance template"""
        return self.templates_dir / 'sql_performance_template.xml'
    
    def list_existing_collections(self) -> List[Dict[str, str]]:
        """List all existing Performance Monitor Data Collector Sets"""
        try:
            cmd = 'logman query -n'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            collections = []
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                for line in lines[2:]:  # Skip header lines
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 2:
                            name = parts[0].strip()
                            status = parts[1].strip() if len(parts) > 1 else 'Unknown'
                            collections.append({
                                'name': name,
                                'status': status
                            })
            
            return collections
            
        except Exception as e:
            self.logger.error(f"Error listing existing collections: {e}")
            return []
    
    def find_matching_collection(self, template_counters: List[str]) -> Optional[str]:
        """Find existing collection that matches our template counters
        
        Args:
            template_counters: List of counter paths from template
            
        Returns:
            Name of matching collection or None
        """
        try:
            existing_collections = self.list_existing_collections()
            
            for collection in existing_collections:
                collection_name = collection['name']
                
                # Skip if not our managed collection
                if not collection_name.startswith(self.sqlspeedinator_prefix):
                    continue
                
                # Get collection details
                try:
                    cmd = f'logman query "{collection_name}" -v'
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        # Parse counters from output
                        collection_counters = self._extract_counters_from_logman_output(result.stdout)
                        
                        # Check if counters match (allow for subset match)
                        matching_counters = set(template_counters) & set(collection_counters)
                        match_percentage = len(matching_counters) / len(template_counters) if template_counters else 0
                        
                        # If 80%+ match, consider it a match
                        if match_percentage >= 0.8:
                            self.logger.info(f"Found matching collection '{collection_name}' with {match_percentage:.1%} counter match")
                            return collection_name
                            
                except Exception as e:
                    self.logger.debug(f"Error checking collection {collection_name}: {e}")
                    continue
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding matching collection: {e}")
            return None
    
    def _extract_counters_from_logman_output(self, logman_output: str) -> List[str]:
        """Extract counter paths from logman query output"""
        counters = []
        
        try:
            lines = logman_output.split('\n')
            in_counters_section = False
            
            for line in lines:
                line = line.strip()
                
                if 'Counter:' in line or 'Counters:' in line:
                    in_counters_section = True
                    continue
                elif in_counters_section and line.startswith('\\'):
                    counters.append(line)
                elif in_counters_section and not line:
                    break
                    
        except Exception as e:
            self.logger.debug(f"Error extracting counters from logman output: {e}")
        
        return counters
    
    def create_or_reuse_collection(self, template_info: Dict[str, Any], collection_name: Optional[str] = None) -> Optional[str]:
        """Create new collection or reuse existing matching collection
        
        Args:
            template_info: Parsed template information
            collection_name: Preferred collection name (auto-generated if None)
            
        Returns:
            Collection name that was created or reused, or None if failed
        """
        try:
            template_counters = [counter['path'] for counter in template_info.get('counters', [])]
            
            # Try to find existing matching collection
            existing_collection = self.find_matching_collection(template_counters)
            
            if existing_collection:
                self.logger.info(f"♻️  Reusing existing collection: {existing_collection}")
                return existing_collection
            
            # Create new collection
            if not collection_name:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                collection_name = f"{self.sqlspeedinator_prefix}_{timestamp}"
            
            self.logger.info(f"📊 Creating new collection: {collection_name}")
            
            # Use existing create_data_collector_set method
            dcs_file = self.create_data_collector_set(template_info, collection_name)
            
            if dcs_file:
                # Track that we created this collection
                self.managed_collections.add(collection_name)
                self.logger.info(f"✅ Successfully created collection: {collection_name}")
                return collection_name
            else:
                self.logger.error(f"❌ Failed to create collection: {collection_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating or reusing collection: {e}")
            return None
    
    def cleanup_managed_collections(self):
        """Clean up all collections that were created by this instance"""
        if not self.managed_collections:
            return
        
        self.logger.info(f"🧹 Cleaning up {len(self.managed_collections)} managed collections...")
        
        for collection_name in list(self.managed_collections):
            try:
                # Stop collection if running
                stop_cmd = f'logman stop "{collection_name}"'
                subprocess.run(stop_cmd, shell=True, capture_output=True)
                
                # Delete collection
                delete_cmd = f'logman delete "{collection_name}"'
                result = subprocess.run(delete_cmd, shell=True, capture_output=True, text=True)
                
                if result.returncode == 0:
                    self.logger.info(f"🗑️  Cleaned up collection: {collection_name}")
                    self.managed_collections.remove(collection_name)
                else:
                    self.logger.warning(f"Failed to delete collection {collection_name}: {result.stderr}")
                    
            except Exception as e:
                self.logger.error(f"Error cleaning up collection {collection_name}: {e}")
        
        if not self.managed_collections:
            self.logger.info("✅ All managed collections cleaned up successfully")
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        try:
            self.cleanup_managed_collections()
        except:
            pass  # Ignore errors in destructor