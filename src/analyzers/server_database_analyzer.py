"""
Server and Database Information Analyzer for SQL Speedinator
Collects comprehensive information about SQL Server instance and databases
"""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal

class ServerDatabaseAnalyzer:
    """Analyzes SQL Server instance and database information"""
    
    def __init__(self, connection, config):
        """Initialize server database analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete server and database analysis
        
        Returns:
            Dictionary containing server and database analysis results
        """
        try:
            results = {
                'server_instance_info': self._get_server_instance_info(),
                'server_configuration': self._get_server_configuration(),
                'memory_info': self._get_memory_info(),
                'cpu_info': self._get_cpu_info(),
                'database_overview': self._get_database_overview(),
                'database_files': self._get_database_files_info(),
                'security_info': self._get_security_info(),
                'backup_info': self._get_backup_info()
            }
            
            self.logger.info("Server and database analysis completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Error during server/database analysis: {str(e)}")
            return {
                'error': str(e),
                'server_instance_info': {},
                'server_configuration': {},
                'memory_info': {},
                'cpu_info': {},
                'database_overview': [],
                'database_files': [],
                'security_info': {},
                'backup_info': []
            }
    
    def _get_server_instance_info(self) -> Dict[str, Any]:
        """Get comprehensive server instance information"""
        try:
            # Try advanced query first
            query = """
        SELECT 
            @@SERVERNAME as server_name,
            @@VERSION as version_full,
            CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)) as product_version,
            CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR(50)) as product_level,
            CAST(SERVERPROPERTY('Edition') AS VARCHAR(100)) as edition,
            CAST(SERVERPROPERTY('EngineEdition') AS VARCHAR(20)) as engine_edition,
            CAST(SERVERPROPERTY('MachineName') AS VARCHAR(128)) as machine_name,
            ISNULL(CAST(SERVERPROPERTY('InstanceName') AS VARCHAR(128)), 'DEFAULT') as instance_name,
            CAST(SERVERPROPERTY('Collation') AS VARCHAR(128)) as collation,
            CAST(SERVERPROPERTY('IsClustered') AS BIT) as is_clustered,
            CAST(SERVERPROPERTY('IsHadrEnabled') AS BIT) as is_hadr_enabled,
            CAST(SERVERPROPERTY('IsAdvancedAnalyticsInstalled') AS BIT) as advanced_analytics_installed,
            CAST(SERVERPROPERTY('IsFullTextInstalled') AS BIT) as fulltext_installed,
            CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS BIT) as windows_auth_only,
            GETDATE() as analysis_time,
            @@LANGUAGE as language_setting,
            @@LOCK_TIMEOUT as lock_timeout,
            @@MAX_CONNECTIONS as max_connections,
            @@SPID as current_spid
            """
            
            result = self.connection.execute_query(query)
            if result and result[0]:
                return result[0]
        except Exception as e:
            self.logger.warning(f"Advanced server info query failed, trying simple version: {e}")
        
        # Fallback to simple query if advanced one fails
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
            if result and result[0]:
                info = result[0]
                
                # Add default values for missing fields
                info['product_version'] = 'Unknown'
                info['product_level'] = 'Unknown'
                info['edition'] = 'Unknown'
                info['engine_edition'] = 'Unknown'
                info['machine_name'] = 'Unknown'
                info['instance_name'] = 'DEFAULT'
                info['collation'] = 'Unknown'
                info['is_clustered'] = False
                info['is_hadr_enabled'] = False
                info['advanced_analytics_installed'] = False
                info['fulltext_installed'] = False
                info['windows_auth_only'] = True
                
                # Try to extract version info from @@VERSION
                version_full = info.get('version_full', '')
                if 'SQL Server' in version_full:
                    lines = version_full.split('\n')
                    if lines:
                        first_line = lines[0]
                        if 'Microsoft SQL Server' in first_line:
                            parts = first_line.split(' ')
                            for part in parts:
                                if part.startswith('2'):  # Version year like 2019, 2017, etc.
                                    info['product_version'] = part
                                    break
                
                return info
        except Exception as e:
            self.logger.error(f"All server info queries failed: {e}")
        
        return {}
    
    def _get_server_configuration(self) -> List[Dict[str, Any]]:
        """Get server configuration settings with best practice analysis"""
        try:
            # Try main query first
            query = """
            SELECT
                name,
                CAST(value AS BIGINT) as value,
                CAST(value_in_use AS BIGINT) as value_in_use,
                CAST(minimum AS BIGINT) as minimum,
                CAST(maximum AS BIGINT) as maximum,
                CAST(description AS VARCHAR(255)) as description,
                is_dynamic,
                is_advanced,
                CASE 
                    -- Memory settings
                    WHEN name = 'max server memory (MB)' AND value = 2147483647 THEN 'WARNING: Max server memory not configured'
                    WHEN name = 'min server memory (MB)' AND value > 0 AND value >= (SELECT value FROM sys.configurations WHERE name = 'max server memory (MB)') THEN 'WARNING: Min memory >= Max memory'
                    
                    -- Parallelism settings
                    WHEN name = 'max degree of parallelism' AND value = 0 THEN 'INFO: MAXDOP set to auto (0)'
                    WHEN name = 'max degree of parallelism' AND value = 1 THEN 'WARNING: MAXDOP set to 1 (no parallelism)'
                    WHEN name = 'cost threshold for parallelism' AND value = 5 THEN 'WARNING: Cost threshold still at default (5)'
                    
                    -- Security settings
                    WHEN name = 'xp_cmdshell' AND value = 1 THEN 'WARNING: xp_cmdshell is enabled (security risk)'
                    WHEN name = 'Ad Hoc Distributed Queries' AND value = 1 THEN 'WARNING: Ad Hoc Distributed Queries enabled'
                    WHEN name = 'Ole Automation Procedures' AND value = 1 THEN 'WARNING: OLE Automation enabled'
                    
                    ELSE 'OK'
                END as best_practice_status
            FROM sys.configurations
            WHERE name IN (
                'max server memory (MB)',
                'min server memory (MB)',
                'max degree of parallelism',
                'cost threshold for parallelism',
                'backup compression default',
                'optimize for ad hoc workloads',
                'xp_cmdshell',
                'Ad Hoc Distributed Queries',
                'Ole Automation Procedures',
                'Database Mail XPs',
                'remote access',
                'remote admin connections'
            )
            ORDER BY name
            """
            
            result = self.connection.execute_query(query)
            if result:
                return result
        except Exception as e:
            self.logger.warning(f"Main server configuration query failed, trying fallback: {e}")
        
        # Fallback to simple query without CAST operations
        try:
            query = """
            SELECT
                name,
                value,
                value_in_use,
                minimum,
                maximum,
                description,
                is_dynamic,
                is_advanced,
                'WARNING: Unable to analyze best practices due to compatibility' as best_practice_status
            FROM sys.configurations
            WHERE name IN (
                'max server memory (MB)',
                'min server memory (MB)',
                'max degree of parallelism',
                'cost threshold for parallelism',
                'backup compression default',
                'optimize for ad hoc workloads',
                'xp_cmdshell',
                'Ad Hoc Distributed Queries',
                'Ole Automation Procedures',
                'Database Mail XPs',
                'remote access',
                'remote admin connections'
            )
            ORDER BY name
            """
            
            result = self.connection.execute_query(query)
            if result:
                # Apply basic best practice analysis in Python
                analyzed_configs = []
                for config in result:
                    config_dict = dict(config)
                    name = config_dict.get('name', '')
                    value = config_dict.get('value', 0)
                    
                    # Apply basic best practice analysis
                    if name == 'max server memory (MB)' and value == 2147483647:
                        config_dict['best_practice_status'] = 'WARNING: Max server memory not configured'
                    elif name == 'max degree of parallelism' and value == 0:
                        config_dict['best_practice_status'] = 'INFO: MAXDOP set to auto (0)'
                    elif name == 'max degree of parallelism' and value == 1:
                        config_dict['best_practice_status'] = 'WARNING: MAXDOP set to 1 (no parallelism)'
                    elif name == 'cost threshold for parallelism' and value == 5:
                        config_dict['best_practice_status'] = 'WARNING: Cost threshold still at default (5)'
                    elif name == 'xp_cmdshell' and value == 1:
                        config_dict['best_practice_status'] = 'WARNING: xp_cmdshell is enabled (security risk)'
                    elif name == 'Ad Hoc Distributed Queries' and value == 1:
                        config_dict['best_practice_status'] = 'WARNING: Ad Hoc Distributed Queries enabled'
                    elif name == 'Ole Automation Procedures' and value == 1:
                        config_dict['best_practice_status'] = 'WARNING: OLE Automation enabled'
                    else:
                        config_dict['best_practice_status'] = 'OK'
                    
                    analyzed_configs.append(config_dict)
                
                return analyzed_configs
        except Exception as e:
            self.logger.error(f"Fallback server configuration query also failed: {e}")
        
        return []
    
    def _get_memory_info(self) -> Dict[str, Any]:
        """Get memory information and analysis"""
        query = """
        SELECT
            CAST(physical_memory_kb / 1024.0 / 1024 AS DECIMAL(10,2)) as total_physical_memory_gb,
            CAST(virtual_memory_kb / 1024.0 / 1024 AS DECIMAL(10,2)) as total_virtual_memory_gb,
            CAST(committed_kb / 1024.0 / 1024 AS DECIMAL(10,2)) as committed_memory_gb,
            CAST(committed_target_kb / 1024.0 / 1024 AS DECIMAL(10,2)) as committed_target_gb,
            CAST(visible_target_kb / 1024.0 / 1024 AS DECIMAL(10,2)) as visible_target_gb,
            stack_size_in_bytes,
            os_quantum,
            os_error_mode,
            os_priority_class,
            max_workers_count,
            scheduler_count,
            scheduler_total_count,
            deadlock_monitor_serial_number
        FROM sys.dm_os_sys_info
        """
        
        result = self.connection.execute_query(query)
        if result:
            memory_info = result[0]
            
            # Add memory analysis
            total_gb = float(memory_info.get('total_physical_memory_gb', 0))
            committed_gb = float(memory_info.get('committed_memory_gb', 0))
            
            memory_info['memory_pressure'] = 'HIGH' if committed_gb > total_gb * 0.9 else 'NORMAL' if committed_gb > total_gb * 0.7 else 'LOW'
            memory_info['memory_usage_percentage'] = round((committed_gb / total_gb * 100), 2) if total_gb > 0 else 0
            
            return memory_info
        
        return {}
    
    def _get_cpu_info(self) -> Dict[str, Any]:
        """Get CPU information"""
        query = """
        SELECT
            cpu_count,
            hyperthread_ratio,
            CASE 
                WHEN hyperthread_ratio > cpu_count THEN cpu_count
                ELSE cpu_count / hyperthread_ratio
            END as physical_cpu_count,
            scheduler_count,
            scheduler_total_count
        FROM sys.dm_os_sys_info
        """
        
        result = self.connection.execute_query(query)
        return result[0] if result else {}
    
    def _get_database_overview(self) -> List[Dict[str, Any]]:
        """Get overview of all databases"""
        try:
            # Try comprehensive query first
            query = """
        SELECT 
            d.name as database_name,
            d.database_id,
            d.create_date,
            d.collation_name,
            d.state_desc as state,
            d.user_access_desc as user_access,
            d.is_read_only,
            d.is_auto_close_on,
            d.is_auto_shrink_on,
            d.is_auto_create_stats_on,
            d.is_auto_update_stats_on,
            d.is_trustworthy_on,
            d.recovery_model_desc as recovery_model,
            d.compatibility_level,
            d.page_verify_option_desc as page_verify_option,
            CASE 
                WHEN d.is_auto_shrink_on = 1 THEN 'WARNING: Auto-shrink enabled'
                WHEN d.is_auto_close_on = 1 THEN 'WARNING: Auto-close enabled'
                WHEN d.is_trustworthy_on = 1 AND d.name != 'msdb' THEN 'WARNING: Trustworthy bit set'
                WHEN d.compatibility_level < 130 THEN 'WARNING: Old compatibility level'
                WHEN d.page_verify_option_desc = 'NONE' THEN 'WARNING: Page verify disabled'
                ELSE 'OK'
            END as configuration_issues
        FROM sys.databases d
        WHERE d.database_id > 4  -- Exclude system databases
        ORDER BY d.name
            """
            
            result = self.connection.execute_query(query)
            if result:
                return result
        except Exception as e:
            self.logger.warning(f"Comprehensive database overview failed, trying simple version: {e}")
        
        # Fallback to simple query
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
                # Add default values for missing fields
                for db in result:
                    db['collation_name'] = 'Unknown'
                    db['is_read_only'] = False
                    db['is_auto_close_on'] = False
                    db['is_auto_shrink_on'] = False
                    db['is_auto_create_stats_on'] = True
                    db['is_auto_update_stats_on'] = True
                    db['is_trustworthy_on'] = False
                    db['recovery_model'] = 'Unknown'
                    db['compatibility_level'] = 'Unknown'
                    db['page_verify_option'] = 'Unknown'
                    db['configuration_issues'] = 'OK'
                
                return result
        except Exception as e:
            self.logger.error(f"All database overview queries failed: {e}")
        
        return []
    
    def _get_database_files_info(self) -> List[Dict[str, Any]]:
        """Get detailed database files information"""
        query = """
        SELECT 
            DB_NAME(mf.database_id) as database_name,
            mf.name as logical_name,
            mf.physical_name,
            mf.type_desc as file_type,
            mf.state_desc as state,
            CAST(mf.size AS BIGINT) * 8 / 1024 AS size_mb,
            CASE 
                WHEN mf.max_size = -1 THEN 'UNLIMITED'
                WHEN mf.max_size = 0 THEN 'NO GROWTH'
                ELSE CAST(CAST(mf.max_size AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
            END as max_size_desc,
            CASE
                WHEN mf.is_percent_growth = 1 THEN CAST(mf.growth AS VARCHAR) + '%'
                ELSE CAST(CAST(mf.growth AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
            END as growth_desc,
            mf.is_percent_growth,
            CASE 
                WHEN vs.size_on_disk_bytes IS NOT NULL THEN CAST(vs.size_on_disk_bytes / 1024.0 / 1024 AS DECIMAL(10,2))
                ELSE NULL
            END as actual_size_mb,
            CASE 
                WHEN mf.growth = 0 THEN 'WARNING: No auto-growth configured'
                WHEN mf.is_percent_growth = 1 AND mf.growth > 50 THEN 'WARNING: High percentage growth'
                WHEN mf.is_percent_growth = 0 AND mf.growth * 8 / 1024 < 64 THEN 'WARNING: Small fixed growth'
                ELSE 'OK'
            END as growth_issues
        FROM sys.master_files mf
        LEFT JOIN sys.dm_io_virtual_file_stats(NULL, NULL) vs ON mf.database_id = vs.database_id AND mf.file_id = vs.file_id
        WHERE mf.database_id > 4  -- Exclude system databases
        ORDER BY mf.database_id, mf.type_desc, mf.name
        """
        
        result = self.connection.execute_query(query)
        return result if result else []
    
    def _get_security_info(self) -> Dict[str, Any]:
        """Get security configuration information"""
        query = """
        SELECT 
            CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS BIT) as windows_auth_only,
            COUNT(*) as sql_login_count
        FROM sys.sql_logins
        WHERE is_disabled = 0
        """
        
        result = self.connection.execute_query(query)
        return result[0] if result else {}
    
    def _get_backup_info(self) -> List[Dict[str, Any]]:
        """Get backup information for user databases"""
        query = """
        SELECT 
            d.name as database_name,
            bs_full.backup_finish_date as last_full_backup,
            bs_diff.backup_finish_date as last_diff_backup,
            bs_log.backup_finish_date as last_log_backup,
            CASE 
                WHEN bs_full.backup_finish_date IS NULL THEN 'CRITICAL: No full backup found'
                WHEN bs_full.backup_finish_date < DATEADD(DAY, -7, GETDATE()) THEN 'WARNING: Full backup older than 7 days'
                WHEN d.recovery_model_desc = 'FULL' AND bs_log.backup_finish_date < DATEADD(HOUR, -24, GETDATE()) THEN 'WARNING: Log backup older than 24 hours'
                ELSE 'OK'
            END as backup_status
        FROM sys.databases d
        LEFT JOIN (
            SELECT database_name, MAX(backup_finish_date) as backup_finish_date
            FROM msdb.dbo.backupset 
            WHERE type = 'D'  -- Full backup
            GROUP BY database_name
        ) bs_full ON d.name = bs_full.database_name
        LEFT JOIN (
            SELECT database_name, MAX(backup_finish_date) as backup_finish_date
            FROM msdb.dbo.backupset 
            WHERE type = 'I'  -- Differential backup
            GROUP BY database_name
        ) bs_diff ON d.name = bs_diff.database_name
        LEFT JOIN (
            SELECT database_name, MAX(backup_finish_date) as backup_finish_date
            FROM msdb.dbo.backupset 
            WHERE type = 'L'  -- Log backup
            GROUP BY database_name
        ) bs_log ON d.name = bs_log.database_name
        WHERE d.database_id > 4  -- Exclude system databases
        ORDER BY d.name
        """
        
        result = self.connection.execute_query(query)
        return result if result else []