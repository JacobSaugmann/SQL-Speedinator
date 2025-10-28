"""
SQL Server Version Compatibility Manager
Handles different SQL Server versions and provides compatible queries
"""

import logging
from typing import Dict, Optional, Tuple
from src.core.sql_connection import SQLServerConnection

class SQLVersionManager:
    """Manages SQL Server version detection and compatibility"""
    
    def __init__(self, connection: SQLServerConnection):
        self.connection = connection
        self.logger = logging.getLogger(__name__)
        self._version_info = None
        self._capabilities = None
    
    def detect_version(self) -> Dict:
        """Detect SQL Server version and capabilities"""
        if self._version_info is not None:
            return self._version_info
        
        try:
            # Basic version detection that works across all versions
            version_query = """
            SELECT 
                @@VERSION as version_string,
                @@SERVERNAME as server_name,
                CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) as major_version,
                CAST(SERVERPROPERTY('ProductMinorVersion') AS INT) as minor_version,
                CAST(SERVERPROPERTY('EngineEdition') AS INT) as engine_edition
            """
            
            result = self.connection.execute_query(version_query)
            
            if result and len(result) > 0:
                row = result[0]
                version_string = row.get('version_string', '')
                major_version = row.get('major_version', 0)
                
                # Parse version from string if SERVERPROPERTY doesn't work
                if not major_version and version_string:
                    if 'SQL Server 2012' in version_string or '11.0' in version_string:
                        major_version = 11
                    elif 'SQL Server 2014' in version_string or '12.0' in version_string:
                        major_version = 12
                    elif 'SQL Server 2016' in version_string or '13.0' in version_string:
                        major_version = 13
                    elif 'SQL Server 2017' in version_string or '14.0' in version_string:
                        major_version = 14
                    elif 'SQL Server 2019' in version_string or '15.0' in version_string:
                        major_version = 15
                    elif 'SQL Server 2022' in version_string or '16.0' in version_string:
                        major_version = 16
                    elif 'SQL Server 2025' in version_string or '17.0' in version_string:
                        major_version = 17
                
                self._version_info = {
                    'version_string': version_string,
                    'server_name': row.get('server_name', ''),
                    'major_version': int(major_version) if major_version else 0,
                    'minor_version': int(row.get('minor_version', 0)) if row.get('minor_version') else 0,
                    'build_number': int(row.get('build_number', 0)) if row.get('build_number') else 0,
                    'engine_edition': int(row.get('engine_edition', 0)) if row.get('engine_edition') else 0
                }
                
                self.logger.info(f"Detected SQL Server version: {major_version}.{self._version_info['minor_version']}")
                return self._version_info
            
        except Exception as e:
            self.logger.warning(f"Failed to detect SQL Server version: {e}")
            # Default to SQL Server 2012 for maximum compatibility
            self._version_info = {
                'version_string': 'Unknown',
                'server_name': 'Unknown',
                'major_version': 11,  # SQL Server 2012
                'minor_version': 0,
                'build_number': 0,
                'engine_edition': 1
            }
        
        return self._version_info
    
    def get_capabilities(self) -> Dict:
        """Get version-specific capabilities"""
        if self._capabilities is not None:
            return self._capabilities
        
        version_info = self.detect_version()
        major_version = version_info.get('major_version', 11)
        
        # Define capabilities by version
        self._capabilities = {
            'has_physical_cpu_count': major_version >= 13,  # SQL 2016+
            'has_socket_count': major_version >= 13,        # SQL 2016+
            'has_cores_per_socket': major_version >= 13,    # SQL 2016+
            'has_advanced_analytics': major_version >= 13,  # SQL 2016+
            'has_pages_in_use_kb': major_version >= 12,     # SQL 2014+
            'supports_nvarchar_cast': major_version < 17,   # Issues in SQL 2025
            'has_performance_counter_name': major_version < 17,  # Column missing in SQL 2025
            'supports_query_plan_cross_apply': major_version >= 11,  # SQL 2012+
            'supports_extended_events': major_version >= 11,         # SQL 2012+
        }
        
        self.logger.info(f"SQL Server capabilities detected for version {major_version}")
        return self._capabilities
    
    def get_compatible_server_info_query(self) -> str:
        """Get version-compatible server info query"""
        capabilities = self.get_capabilities()
        
        base_query = """
        SELECT
            @@SERVERNAME as server_name,
            @@VERSION as version
        """
        
        if capabilities['supports_nvarchar_cast']:
            # For older versions, use CAST
            extended_query = """,
            CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)) as product_version,
            CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR(50)) as product_level,
            CAST(SERVERPROPERTY('Edition') AS VARCHAR(200)) as edition,
            CAST(SERVERPROPERTY('EngineEdition') AS INT) as engine_edition,
            CAST(SERVERPROPERTY('MachineName') AS VARCHAR(100)) as machine_name,
            CAST(SERVERPROPERTY('InstanceName') AS VARCHAR(100)) as instance_name,
            CAST(SERVERPROPERTY('Collation') AS VARCHAR(100)) as collation
            """
        else:
            # For SQL 2025, avoid CAST and use direct conversion
            extended_query = """,
            CONVERT(VARCHAR(50), SERVERPROPERTY('ProductVersion')) as product_version,
            CONVERT(VARCHAR(50), SERVERPROPERTY('ProductLevel')) as product_level,
            CONVERT(VARCHAR(200), SERVERPROPERTY('Edition')) as edition,
            CONVERT(INT, SERVERPROPERTY('EngineEdition')) as engine_edition,
            CONVERT(VARCHAR(100), SERVERPROPERTY('MachineName')) as machine_name,
            CONVERT(VARCHAR(100), SERVERPROPERTY('InstanceName')) as instance_name,
            CONVERT(VARCHAR(100), SERVERPROPERTY('Collation')) as collation
            """
        
        return base_query + extended_query
    
    def get_compatible_configuration_query(self) -> str:
        """Get version-compatible configuration query"""
        capabilities = self.get_capabilities()
        
        if capabilities['supports_nvarchar_cast']:
            return """
            SELECT
                configuration_id,
                CONVERT(VARCHAR(100), name) as name,
                CONVERT(VARCHAR(20), value) as value,
                CONVERT(VARCHAR(20), minimum) as minimum,
                CONVERT(VARCHAR(20), maximum) as maximum,
                CONVERT(VARCHAR(20), value_in_use) as value_in_use,
                CONVERT(VARCHAR(500), description) as description,
                is_dynamic,
                is_advanced
            FROM sys.configurations
            ORDER BY name
            """
        else:
            return """
            SELECT
                configuration_id,
                CONVERT(VARCHAR(100), name) as name,
                CONVERT(VARCHAR(20), value) as value,
                CONVERT(VARCHAR(20), minimum) as minimum,
                CONVERT(VARCHAR(20), maximum) as maximum,
                CONVERT(VARCHAR(20), value_in_use) as value_in_use,
                CONVERT(VARCHAR(500), description) as description,
                is_dynamic,
                is_advanced
            FROM sys.configurations
            ORDER BY name
            """
    
    def get_compatible_cpu_info_query(self) -> str:
        """Get version-compatible CPU info query"""
        capabilities = self.get_capabilities()
        
        base_query = """
        SELECT
            cpu_count,
            hyperthread_ratio,
            cpu_count / CASE WHEN hyperthread_ratio > 0 THEN hyperthread_ratio ELSE 1 END as physical_cpu_count
        """
        
        if capabilities['has_physical_cpu_count']:
            # SQL 2016+ has these columns, but check if they exist in this version
            extended_query = """,
            CASE
                WHEN COLUMNPROPERTY(OBJECT_ID('sys.dm_os_sys_info'), 'physical_cpu_count', 'ColumnId') IS NOT NULL
                THEN (SELECT TOP 1 
                      CASE WHEN c.hyperthread_ratio > 0 
                           THEN c.cpu_count / c.hyperthread_ratio 
                           ELSE c.cpu_count 
                      END 
                      FROM sys.dm_os_sys_info c)
                ELSE cpu_count / CASE WHEN hyperthread_ratio > 0 THEN hyperthread_ratio ELSE 1 END
            END as actual_physical_cpu_count,
            CASE
                WHEN COLUMNPROPERTY(OBJECT_ID('sys.dm_os_sys_info'), 'socket_count', 'ColumnId') IS NOT NULL
                THEN (SELECT TOP 1 ISNULL(socket_count, 1) FROM sys.dm_os_sys_info)
                ELSE 1
            END as socket_count,
            CASE
                WHEN COLUMNPROPERTY(OBJECT_ID('sys.dm_os_sys_info'), 'cores_per_socket', 'ColumnId') IS NOT NULL
                THEN (SELECT TOP 1 ISNULL(cores_per_socket, cpu_count) FROM sys.dm_os_sys_info)
                ELSE cpu_count
            END as cores_per_socket
            """
        else:
            # SQL 2012-2014 fallback
            extended_query = """,
            cpu_count / hyperthread_ratio as actual_physical_cpu_count,
            1 as socket_count,
            cpu_count as cores_per_socket
            """
        
        return base_query + extended_query + "\nFROM sys.dm_os_sys_info"
    
    def get_compatible_performance_counters_query(self) -> str:
        """Get version-compatible performance counters query"""
        capabilities = self.get_capabilities()
        
        if capabilities['has_performance_counter_name']:
            return """
            SELECT
                instance_name as name,
                counter_name,
                cntr_value
            FROM sys.dm_os_performance_counters
            WHERE object_name LIKE '%Plan Cache%'
            AND counter_name IN ('Cache Hit Ratio', 'Cache Object Counts', 'Cache Objects in use')
            """
        else:
            # SQL 2025 doesn't have 'name' column
            return """
            SELECT
                instance_name,
                counter_name,
                cntr_value
            FROM sys.dm_os_performance_counters
            WHERE object_name LIKE '%Plan Cache%'
            AND counter_name IN ('Cache Hit Ratio', 'Cache Object Counts', 'Cache Objects in use')
            """
    
    def get_compatible_query_stats_query(self) -> str:
        """Get version-compatible query stats query"""
        # Simplify the query to avoid SUBSTRING issues with varbinary
        return """
        SELECT TOP 20
            qs.execution_count,
            qs.total_worker_time,
            qs.total_elapsed_time,
            qs.total_logical_reads,
            qs.total_logical_writes,
            qs.total_physical_reads,
            qs.total_worker_time / qs.execution_count AS avg_cpu_time,
            qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
            qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
            qs.creation_time,
            qs.last_execution_time,
            LEFT(st.text, 100) AS query_text_sample
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        WHERE qs.last_execution_time > DATEADD(HOUR, -24, GETDATE())
        ORDER BY qs.total_worker_time DESC
        """
    
    def get_compatible_time_query(self) -> str:
        """Get version-compatible time query"""
        # Avoid reserved keyword issues
        return "SELECT GETDATE() AS query_datetime"
    
    def test_connection_compatibility(self) -> Tuple[bool, str]:
        """Test connection and return compatibility info"""
        try:
            version_info = self.detect_version()
            capabilities = self.get_capabilities()
            
            major_version = version_info.get('major_version', 0)
            version_string = version_info.get('version_string', 'Unknown')
            
            if major_version >= 11:  # SQL Server 2012+
                compatibility_level = "SUPPORTED"
                message = f"SQL Server version {major_version} is supported"
            else:
                compatibility_level = "LIMITED"
                message = f"SQL Server version {major_version} has limited support"
            
            return True, f"{compatibility_level}: {message}"
            
        except Exception as e:
            return False, f"Compatibility test failed: {e}"