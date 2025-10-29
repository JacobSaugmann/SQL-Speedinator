"""
Simple Server Analyzer for broader SQL Server compatibility
"""
import logging
from typing import Dict, Any, List, Optional
from ..core.sql_connection import SQLServerConnection
from ..core.config_manager import ConfigManager

class SimpleServerAnalyzer:
    """Simplified server analyzer that works with older SQL Server versions"""
    
    def __init__(self, connection: SQLServerConnection, config: ConfigManager):
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Perform simplified server analysis using basic queries"""
        try:
            result = {
                'server_instance_info': self._get_basic_server_info(),
                'database_overview': self._get_basic_database_info(),
                'memory_info': self._get_basic_memory_info(),
                'database_files': self._get_basic_file_info(),
                'server_configuration': [],
                'cpu_info': {},
                'security_info': {},
                'backup_info': []
            }
            
            self.logger.info("Simple server analysis completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Error during simple server analysis: {e}", exc_info=True)
            return {}
    
    def _get_basic_server_info(self) -> Dict[str, Any]:
        """Get basic server information using compatible queries"""
        try:
            query = """
            SELECT 
                @@SERVERNAME as server_name,
                @@VERSION as version_full,
                @@LANGUAGE as language_setting,
                @@LOCK_TIMEOUT as lock_timeout,
                @@MAX_CONNECTIONS as max_connections,
                @@SPID as current_spid,
                GETDATE() as analysis_time
            """
            
            result = self.connection.execute_query(query)
            if result:
                info = result[0]
                
                # Extract version info from @@VERSION
                version_full = info.get('version_full', '')
                if 'SQL Server' in version_full:
                    # Try to extract basic version info
                    lines = version_full.split('\n')
                    if lines:
                        first_line = lines[0]
                        if 'Microsoft SQL Server' in first_line:
                            parts = first_line.split(' ')
                            for i, part in enumerate(parts):
                                if part.startswith('2'):  # Version year like 2019, 2017, etc.
                                    info['product_version'] = part
                                    break
                
                # Add some basic properties
                info['edition'] = 'Unknown'
                info['machine_name'] = 'Unknown'
                info['instance_name'] = 'DEFAULT'
                info['collation'] = 'Unknown'
                info['is_clustered'] = False
                info['is_hadr_enabled'] = False
                
                return info
                
        except Exception as e:
            self.logger.error(f"Error getting basic server info: {e}")
            
        return {}
    
    def _get_basic_database_info(self) -> List[Dict[str, Any]]:
        """Get basic database information"""
        try:
            query = """
            SELECT 
                name as database_name,
                database_id,
                create_date,
                state_desc as state,
                user_access_desc as user_access
            FROM sys.databases
            WHERE database_id > 4  -- Exclude system databases
            ORDER BY name
            """
            
            result = self.connection.execute_query(query)
            if result:
                for db in result:
                    db['recovery_model'] = 'Unknown'
                    db['compatibility_level'] = 'Unknown'
                    db['configuration_issues'] = 'OK'
                    
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting basic database info: {e}")
            
        return []
    
    def _get_basic_memory_info(self) -> Dict[str, Any]:
        """Get basic memory information"""
        try:
            query = """
            SELECT
                physical_memory_kb / 1024.0 / 1024 as total_physical_memory_gb,
                virtual_memory_kb / 1024.0 / 1024 as total_virtual_memory_gb,
                committed_kb / 1024.0 / 1024 as committed_memory_gb,
                committed_target_kb / 1024.0 / 1024 as committed_target_gb,
                max_workers_count,
                scheduler_count
            FROM sys.dm_os_sys_info
            """
            
            result = self.connection.execute_query(query)
            if result:
                memory_info = result[0]
                
                # Add calculated fields
                total_gb = float(memory_info.get('total_physical_memory_gb', 0))
                committed_gb = float(memory_info.get('committed_memory_gb', 0))
                
                if total_gb > 0:
                    memory_info['memory_usage_percentage'] = round((committed_gb / total_gb * 100), 2)
                    memory_info['memory_pressure'] = 'HIGH' if committed_gb > total_gb * 0.9 else 'NORMAL' if committed_gb > total_gb * 0.7 else 'LOW'
                else:
                    memory_info['memory_usage_percentage'] = 0
                    memory_info['memory_pressure'] = 'Unknown'
                
                return memory_info
                
        except Exception as e:
            self.logger.error(f"Error getting basic memory info: {e}")
            
        return {}
    
    def _get_basic_file_info(self) -> List[Dict[str, Any]]:
        """Get basic database file information"""
        try:
            query = """
            SELECT 
                DB_NAME(database_id) as database_name,
                name as logical_name,
                physical_name,
                type_desc as file_type,
                state_desc as state,
                CAST(CAST(size AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB' as size_mb,
                CASE 
                    WHEN max_size = -1 THEN 'UNLIMITED'
                    WHEN max_size = 0 THEN 'NO GROWTH'
                    ELSE 'LIMITED'
                END as max_size_desc,
                CASE
                    WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR) + '%'
                    ELSE 'FIXED'
                END as growth_desc,
                CASE 
                    WHEN growth = 0 THEN 'WARNING: No auto-growth configured'
                    ELSE 'OK'
                END as growth_issues
            FROM sys.master_files
            WHERE database_id > 4  -- Exclude system databases
            ORDER BY database_id, type_desc, name
            """
            
            result = self.connection.execute_query(query)
            return result if result else []
            
        except Exception as e:
            self.logger.error(f"Error getting basic file info: {e}")
            
        return []