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
    """Manages PerfMon templates and data collection"""
    
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
            template_info = {
                'name': root.find('DisplayName').text if root.find('DisplayName') is not None else 'Unknown',
                'description': root.find('Description').text if root.find('Description') is not None else '',
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
                if sample_interval is not None:
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
    
    def start_data_collection(self, dcs_file: str, duration_hours: int = 4) -> Dict[str, Any]:
        """Start PerfMon data collection
        
        Args:
            dcs_file: Path to data collector set XML file
            duration_hours: How long to collect data (default 4 hours)
            
        Returns:
            Dictionary with collection information
        """
        try:
            dcs_name = Path(dcs_file).stem
            
            # Import the data collector set
            import_cmd = f'logman import -n "{dcs_name}" -xml "{dcs_file}"'
            result = subprocess.run(import_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.error(f"Failed to import data collector set: {result.stderr}")
                return {'success': False, 'error': result.stderr}
            
            # Start the data collection
            start_cmd = f'logman start "{dcs_name}"'
            result = subprocess.run(start_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.error(f"Failed to start data collection: {result.stderr}")
                return {'success': False, 'error': result.stderr}
            
            # Calculate end time
            start_time = datetime.now()
            end_time = start_time + timedelta(hours=duration_hours)
            
            collection_info = {
                'success': True,
                'dcs_name': dcs_name,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_hours': duration_hours,
                'status': 'running'
            }
            
            self.logger.info(f"Started data collection: {dcs_name} for {duration_hours} hours")
            return collection_info
            
        except Exception as e:
            self.logger.error(f"Error starting data collection: {e}")
            return {'success': False, 'error': str(e)}
    
    def stop_data_collection(self, dcs_name: str) -> bool:
        """Stop PerfMon data collection
        
        Args:
            dcs_name: Name of data collector set to stop
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Stop the data collection
            stop_cmd = f'logman stop "{dcs_name}"'
            result = subprocess.run(stop_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.warning(f"Error stopping data collection: {result.stderr}")
                return False
            
            # Delete the data collector set
            delete_cmd = f'logman delete "{dcs_name}"'
            result = subprocess.run(delete_cmd, shell=True, capture_output=True, text=True)
            
            self.logger.info(f"Stopped and cleaned up data collection: {dcs_name}")
            return True
            
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