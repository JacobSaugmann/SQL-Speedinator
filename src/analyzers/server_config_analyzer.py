"""
Server Configuration Analyzer
Analyzes SQL Server configuration settings against best practices
Inspired by the great ones and SQL Server community recommendations
"""

import logging
from typing import Dict, Any, List, Optional

class ServerConfigAnalyzer:
    """Analyzes SQL Server configuration for best practices compliance"""
    
    def __init__(self, connection, config):
        """Initialize server config analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete server configuration analysis
        
        Returns:
            Dictionary containing configuration analysis results
        """
        try:
            results = {
                'server_info': self._get_server_info(),
                'configuration_settings': self._get_configuration_settings(),
                'memory_configuration': self._analyze_memory_configuration(),
                'parallelism_settings': self._analyze_parallelism_settings(),
                'database_settings': self._analyze_database_settings(),
                'security_settings': self._analyze_security_settings(),
                'issues': self._identify_configuration_issues(),
                'recommendations': self._generate_config_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in server config analysis: {e}")
            return {'error': str(e)}
    
    def _get_server_info(self) -> Optional[List[Dict[str, Any]]]:
        """Get basic server information and version details"""
        query = """
        SELECT 
            @@SERVERNAME as server_name,
            @@VERSION as version_info,
            SERVERPROPERTY('ProductVersion') as product_version,
            SERVERPROPERTY('ProductLevel') as product_level,
            SERVERPROPERTY('Edition') as edition,
            SERVERPROPERTY('EngineEdition') as engine_edition,
            SERVERPROPERTY('MachineName') as machine_name,
            SERVERPROPERTY('InstanceName') as instance_name,
            SERVERPROPERTY('Collation') as collation,
            SERVERPROPERTY('IsAdvancedAnalyticsInstalled') as advanced_analytics,
            SERVERPROPERTY('IsFullTextInstalled') as fulltext_installed,
            SERVERPROPERTY('IsIntegratedSecurityOnly') as windows_auth_only,
            SERVERPROPERTY('IsClustered') as is_clustered,
            SERVERPROPERTY('IsHadrEnabled') as is_hadr_enabled,
            GETDATE() as analysis_time,
            @@LANGUAGE as language_setting,
            @@LOCK_TIMEOUT as lock_timeout,
            @@MAX_CONNECTIONS as max_connections,
            @@SPID as current_spid
        """
        
        return self.connection.execute_query(query)
    
    def _get_configuration_settings(self) -> Optional[List[Dict[str, Any]]]:
        """Get all SQL Server configuration settings"""
        query = """
        SELECT 
            configuration_id,
            name,
            value,
            minimum,
            maximum,
            value_in_use,
            description,
            is_dynamic,
            is_advanced
        FROM sys.configurations
        ORDER BY name
        """
        
        return self.connection.execute_query(query)
    
    def _analyze_memory_configuration(self) -> Dict[str, Any]:
        """Analyze memory-related configuration settings"""
        try:
            # Get memory settings
            memory_query = """
            SELECT 
                c.name,
                c.value,
                c.value_in_use,
                c.description
            FROM sys.configurations c
            WHERE c.name IN (
                'max server memory (MB)',
                'min server memory (MB)', 
                'index create memory (KB)',
                'min memory per query (KB)'
            )
            """
            
            memory_settings = self.connection.execute_query(memory_query)
            
            # Get current memory usage
            memory_usage_query = """
            SELECT 
                (physical_memory_kb / 1024) AS total_physical_memory_mb,
                (committed_kb / 1024) AS committed_memory_mb,
                (committed_target_kb / 1024) AS committed_target_mb,
                (visible_target_kb / 1024) AS visible_target_mb
            FROM sys.dm_os_sys_info
            """
            
            memory_usage = self.connection.execute_query(memory_usage_query)
            
            analysis = {
                'settings': memory_settings,
                'usage': memory_usage,
                'issues': []
            }
            
            # Analyze memory configuration issues
            if memory_settings and memory_usage:
                total_physical = memory_usage[0].get('total_physical_memory_mb', 0)
                
                for setting in memory_settings:
                    name = setting.get('name')
                    value = setting.get('value_in_use', 0)
                    
                    if name == 'max server memory (MB)':
                        if value == 2147483647:  # Default unlimited value
                            analysis['issues'].append({
                                'setting': name,
                                'issue': 'Max server memory not configured (unlimited)',
                                'severity': 'HIGH',
                                'recommendation': f'Set to approximately {int(total_physical * 0.8)} MB (80% of total RAM)'
                            })
                        elif value > total_physical * 0.9:
                            analysis['issues'].append({
                                'setting': name,
                                'issue': 'Max server memory too high',
                                'severity': 'MEDIUM', 
                                'recommendation': 'Leave memory for OS and other applications'
                            })
                    
                    elif name == 'min server memory (MB)':
                        if value == 0:
                            analysis['issues'].append({
                                'setting': name,
                                'issue': 'Min server memory not set',
                                'severity': 'LOW',
                                'recommendation': 'Consider setting minimum memory to prevent memory starvation'
                            })
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing memory configuration: {e}")
            return {'error': str(e)}
    
    def _analyze_parallelism_settings(self) -> Dict[str, Any]:
        """Analyze parallelism-related settings"""
        try:
            parallelism_query = """
            SELECT 
                c.name,
                c.value,
                c.value_in_use,
                c.description
            FROM sys.configurations c
            WHERE c.name IN (
                'max degree of parallelism',
                'cost threshold for parallelism'
            )
            """
            
            settings = self.connection.execute_query(parallelism_query)
            
            # Get CPU information
            cpu_query = """
            SELECT 
                cpu_count,
                hyperthread_ratio,
                physical_cpu_count,
                socket_count,
                cores_per_socket
            FROM sys.dm_os_sys_info
            """
            
            cpu_info = self.connection.execute_query(cpu_query)
            
            analysis = {
                'settings': settings,
                'cpu_info': cpu_info,
                'issues': []
            }
            
            if settings and cpu_info:
                cpu_count = cpu_info[0].get('cpu_count', 0)
                
                for setting in settings:
                    name = setting.get('name')
                    value = setting.get('value_in_use', 0)
                    
                    if name == 'max degree of parallelism':
                        if value == 0:  # Automatic
                            analysis['issues'].append({
                                'setting': name,
                                'issue': 'MAXDOP set to 0 (automatic)',
                                'severity': 'MEDIUM',
                                'recommendation': f'Consider setting to {min(8, cpu_count)} based on CPU count'
                            })
                        elif value > cpu_count:
                            analysis['issues'].append({
                                'setting': name,
                                'issue': 'MAXDOP higher than CPU count',
                                'severity': 'LOW',
                                'recommendation': 'MAXDOP should not exceed CPU count'
                            })
                    
                    elif name == 'cost threshold for parallelism':
                        if value == 5:  # Default value
                            analysis['issues'].append({
                                'setting': name,
                                'issue': 'Cost threshold at default value (5)',
                                'severity': 'LOW',
                                'recommendation': 'Consider increasing to 50-100 for modern hardware'
                            })
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing parallelism settings: {e}")
            return {'error': str(e)}
    
    def _analyze_database_settings(self) -> Dict[str, Any]:
        """Analyze database-level settings"""
        try:
            database_query = """
            SELECT 
                d.name AS database_name,
                d.database_id,
                d.collation_name,
                d.state_desc,
                d.recovery_model_desc,
                d.page_verify_option_desc,
                d.is_auto_close_on,
                d.is_auto_shrink_on,
                d.is_auto_create_stats_on,
                d.is_auto_update_stats_on,
                d.is_auto_update_stats_async_on,
                d.is_parameterization_forced,
                d.is_read_committed_snapshot_on,
                d.is_read_only,
                d.is_trustworthy_on,
                d.compatibility_level
            FROM sys.databases d
            WHERE d.database_id > 4  -- Exclude system databases
            ORDER BY d.name
            """
            
            databases = self.connection.execute_query(database_query)
            
            analysis = {
                'databases': databases,
                'issues': []
            }
            
            if databases:
                for db in databases:
                    db_name = db.get('database_name')
                    
                    # Check for problematic settings
                    if db.get('is_auto_close_on'):
                        analysis['issues'].append({
                            'database': db_name,
                            'setting': 'AUTO_CLOSE',
                            'issue': 'AUTO_CLOSE enabled',
                            'severity': 'HIGH',
                            'recommendation': 'Disable AUTO_CLOSE to prevent performance issues'
                        })
                    
                    if db.get('is_auto_shrink_on'):
                        analysis['issues'].append({
                            'database': db_name,
                            'setting': 'AUTO_SHRINK',
                            'issue': 'AUTO_SHRINK enabled',
                            'severity': 'HIGH',
                            'recommendation': 'Disable AUTO_SHRINK to prevent fragmentation'
                        })
                    
                    if not db.get('is_auto_create_stats_on'):
                        analysis['issues'].append({
                            'database': db_name,
                            'setting': 'AUTO_CREATE_STATISTICS',
                            'issue': 'Auto create statistics disabled',
                            'severity': 'MEDIUM',
                            'recommendation': 'Enable auto create statistics for better performance'
                        })
                    
                    if not db.get('is_auto_update_stats_on'):
                        analysis['issues'].append({
                            'database': db_name,
                            'setting': 'AUTO_UPDATE_STATISTICS',
                            'issue': 'Auto update statistics disabled',
                            'severity': 'MEDIUM',
                            'recommendation': 'Enable auto update statistics'
                        })
                    
                    if db.get('page_verify_option_desc') != 'CHECKSUM':
                        analysis['issues'].append({
                            'database': db_name,
                            'setting': 'PAGE_VERIFY',
                            'issue': f"Page verify set to {db.get('page_verify_option_desc')}",
                            'severity': 'LOW',
                            'recommendation': 'Set PAGE_VERIFY to CHECKSUM for corruption detection'
                        })
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing database settings: {e}")
            return {'error': str(e)}
    
    def _analyze_security_settings(self) -> Dict[str, Any]:
        """Analyze security-related configuration"""
        try:
            security_query = """
            SELECT 
                c.name,
                c.value,
                c.value_in_use,
                c.description
            FROM sys.configurations c
            WHERE c.name IN (
                'remote access',
                'remote admin connections',
                'Ad Hoc Distributed Queries',
                'xp_cmdshell',
                'Database Mail XPs',
                'Ole Automation Procedures',
                'SQL Mail XPs'
            )
            """
            
            settings = self.connection.execute_query(security_query)
            
            analysis = {
                'settings': settings,
                'issues': []
            }
            
            # Define potentially risky settings
            risky_settings = {
                'xp_cmdshell': 'HIGH',
                'Ad Hoc Distributed Queries': 'MEDIUM',
                'Ole Automation Procedures': 'MEDIUM',
                'SQL Mail XPs': 'LOW'
            }
            
            if settings:
                for setting in settings:
                    name = setting.get('name')
                    value = setting.get('value_in_use', 0)
                    
                    if name in risky_settings and value == 1:
                        analysis['issues'].append({
                            'setting': name,
                            'issue': f'{name} is enabled',
                            'severity': risky_settings[name],
                            'recommendation': f'Disable {name} unless specifically required'
                        })
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing security settings: {e}")
            return {'error': str(e)}
    
    def _identify_configuration_issues(self) -> List[Dict[str, Any]]:
        """Compile all configuration issues found"""
        all_issues = []
        
        # Collect issues from all analysis areas
        memory_analysis = self._analyze_memory_configuration()
        if 'issues' in memory_analysis:
            all_issues.extend(memory_analysis['issues'])
        
        parallelism_analysis = self._analyze_parallelism_settings()
        if 'issues' in parallelism_analysis:
            all_issues.extend(parallelism_analysis['issues'])
        
        database_analysis = self._analyze_database_settings()
        if 'issues' in database_analysis:
            all_issues.extend(database_analysis['issues'])
        
        security_analysis = self._analyze_security_settings()
        if 'issues' in security_analysis:
            all_issues.extend(security_analysis['issues'])
        
        # Sort by severity
        severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        all_issues.sort(key=lambda x: severity_order.get(x.get('severity', 'LOW'), 2))
        
        return all_issues
    
    def _generate_config_recommendations(self) -> List[Dict[str, Any]]:
        """Generate configuration recommendations"""
        recommendations = []
        issues = self._identify_configuration_issues()
        
        # Group issues by severity
        high_issues = [i for i in issues if i.get('severity') == 'HIGH']
        medium_issues = [i for i in issues if i.get('severity') == 'MEDIUM']
        
        if high_issues:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Critical Configuration',
                'issue': f'{len(high_issues)} critical configuration issues found',
                'recommendations': [
                    'Address high-severity configuration issues immediately',
                    'Test changes in development environment first',
                    'Document all configuration changes',
                    'Monitor performance after changes'
                ]
            })
        
        if medium_issues:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Configuration Optimization',
                'issue': f'{len(medium_issues)} configuration optimizations available',
                'recommendations': [
                    'Review and implement medium-priority changes',
                    'Schedule configuration review during maintenance window',
                    'Validate settings against workload requirements'
                ]
            })
        
        # Add general best practice recommendations
        recommendations.append({
            'priority': 'LOW',
            'category': 'Best Practices',
            'issue': 'General configuration best practices',
            'recommendations': [
                'Regularly review SQL Server configuration',
                'Follow vendor best practices for your workload',
                'Document all non-standard settings',
                'Implement configuration management process',
                'Monitor configuration drift over time'
            ]
        })
        
        return recommendations