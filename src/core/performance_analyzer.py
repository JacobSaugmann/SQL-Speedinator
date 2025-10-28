"""
SQL Server Performance Analyzer
Main analyzer class that coordinates all performance checks based on best practices
inspired by the great ones and SQL Server community
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

from ..analyzers.disk_analyzer import DiskAnalyzer
from ..analyzers.index_analyzer import IndexAnalyzer
from ..analyzers.advanced_index_analyzer import AdvancedIndexAnalyzer, IndexAnalysisSettings
from ..analyzers.server_config_analyzer import ServerConfigAnalyzer
from ..analyzers.tempdb_analyzer import TempDBAnalyzer
from ..analyzers.plan_cache_analyzer import PlanCacheAnalyzer
from ..analyzers.wait_stats_analyzer import WaitStatsAnalyzer
from ..analyzers.missing_index_analyzer import MissingIndexAnalyzer
from ..analyzers.ai_analyzer import AIAnalyzer
from ..analyzers.server_database_analyzer import ServerDatabaseAnalyzer
from ..analyzers.intelligent_recommendations import IntelligentRecommendationsEngine

class PerformanceAnalyzer:
    """Main class for coordinating SQL Server performance analysis"""
    
    def __init__(self, connection, config, night_mode=False):
        """Initialize performance analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
            night_mode (bool): Whether to run in night mode with delays
        """
        self.connection = connection
        self.config = config
        self.night_mode = night_mode
        self.logger = logging.getLogger(__name__)
        
        # Initialize analyzers
        self.disk_analyzer = DiskAnalyzer(connection, config)
        self.index_analyzer = IndexAnalyzer(connection, config)
        self.advanced_index_analyzer = AdvancedIndexAnalyzer(connection)
        self.server_config_analyzer = ServerConfigAnalyzer(connection, config)
        self.tempdb_analyzer = TempDBAnalyzer(connection, config)
        self.plan_cache_analyzer = PlanCacheAnalyzer(connection, config)
        self.wait_stats_analyzer = WaitStatsAnalyzer(connection, config)
        self.missing_index_analyzer = MissingIndexAnalyzer(connection, config)
        self.server_database_analyzer = ServerDatabaseAnalyzer(connection, config)
        self.ai_analyzer = AIAnalyzer(config)
        self.intelligent_recommendations = IntelligentRecommendationsEngine(config)
        self.ai_analyzer = AIAnalyzer(config)
        self.server_database_analyzer = ServerDatabaseAnalyzer(connection, config)
        
        self.analysis_results = {}
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """Run complete performance analysis
        
        Returns:
            Dictionary containing all analysis results
        """
        self.logger.info("Starting comprehensive SQL Server performance analysis")
        
        analysis_start = datetime.now()
        
        # Get server information first
        self.analysis_results['server_info'] = self._get_server_info()
        self.analysis_results['analysis_metadata'] = {
            'start_time': analysis_start,
            'night_mode': self.night_mode,
            'analyzer_version': '1.0.0'
        }
        
        # Define analysis steps
        analysis_steps = [
            ('server_database_info', 'Server and Database Information', self.server_database_analyzer.analyze),
            ('wait_stats', 'Wait Statistics Analysis', self.wait_stats_analyzer.analyze),
            ('disk_performance', 'Disk Performance Analysis', self.disk_analyzer.analyze),
            ('index_analysis', 'Index Analysis', self.index_analyzer.analyze),
            ('advanced_index_analysis', 'Advanced Index Analysis', self._analyze_advanced_indexes),
            ('missing_indexes', 'Missing Index Analysis', self.missing_index_analyzer.analyze),
            ('server_config', 'Server Configuration Analysis', self.server_config_analyzer.analyze),
            ('tempdb_analysis', 'TempDB Analysis', self.tempdb_analyzer.analyze),
            ('plan_cache', 'Plan Cache Analysis', self.plan_cache_analyzer.analyze),
        ]
        
        # Execute analysis steps
        for step_key, step_name, analyzer_func in analysis_steps:
            try:
                self.logger.info(f"Running {step_name}...")
                step_start = datetime.now()
                
                result = analyzer_func()
                
                step_duration = (datetime.now() - step_start).total_seconds()
                self.logger.info(f"{step_name} completed in {step_duration:.2f} seconds")
                
                self.analysis_results[step_key] = {
                    'data': result,
                    'duration_seconds': step_duration,
                    'timestamp': step_start
                }
                
                # Add delay in night mode to reduce server load
                if self.night_mode:
                    delay = self.config.night_mode_delay
                    self.logger.info(f"Night mode: waiting {delay} seconds before next analysis...")
                    time.sleep(delay)
                    
            except Exception as e:
                self.logger.error(f"Error during {step_name}: {e}", exc_info=True)
                self.analysis_results[step_key] = {
                    'error': str(e),
                    'timestamp': datetime.now()
                }
        
        # Calculate total analysis time
        analysis_duration = (datetime.now() - analysis_start).total_seconds()
        self.analysis_results['analysis_metadata']['end_time'] = datetime.now()
        self.analysis_results['analysis_metadata']['total_duration_seconds'] = analysis_duration
        
        self.logger.info(f"Complete analysis finished in {analysis_duration:.2f} seconds")
        
        # Generate summary and recommendations
        self.analysis_results['summary'] = self._generate_summary()
        self.analysis_results['recommendations'] = self._generate_recommendations()
        
        # Generate intelligent correlation analysis and enhanced recommendations
        try:
            self.logger.info("Running intelligent correlation analysis...")
            correlation_start = datetime.now()
            correlation_results = self.intelligent_recommendations.analyze_correlations(self.analysis_results)
            correlation_duration = (datetime.now() - correlation_start).total_seconds()
            
            if correlation_results:
                self.analysis_results['intelligent_correlations'] = correlation_results
                self.logger.info(f"Intelligent correlation analysis completed in {correlation_duration:.2f} seconds")
                
                # Add correlated recommendations to existing recommendations
                if 'recommendations' in correlation_results:
                    if 'recommendations' not in self.analysis_results:
                        self.analysis_results['recommendations'] = []
                    
                    # Add intelligent recommendations with priority markers
                    for rec in correlation_results['recommendations']:
                        rec['source'] = 'intelligent_correlation'
                        self.analysis_results['recommendations'].append(rec)
                    
                    self.logger.info(f"Added {len(correlation_results['recommendations'])} intelligent recommendations")
            else:
                self.logger.warning("Intelligent correlation analysis returned no results")
                
        except Exception as e:
            self.logger.error(f"Error during intelligent correlation analysis: {e}", exc_info=True)
            self.analysis_results['intelligent_correlations'] = {
                'error': str(e),
                'timestamp': datetime.now()
            }
        
        # Update summary with recommendations count
        if 'recommendations' in self.analysis_results:
            self.analysis_results['summary']['recommendations_count'] = len(self.analysis_results['recommendations'])
        
        # Run AI analysis if enabled
        if self.config.be_my_copilot:
            try:
                self.logger.info("Running AI Copilot analysis...")
                ai_start = datetime.now()
                ai_result = self.ai_analyzer.analyze(self.analysis_results)
                ai_duration = (datetime.now() - ai_start).total_seconds()
                
                if ai_result:
                    self.analysis_results['ai_analysis'] = ai_result
                    self.logger.info(f"AI analysis completed in {ai_duration:.2f} seconds")
                else:
                    self.logger.warning("AI analysis returned no results")
                    
            except Exception as e:
                self.logger.error(f"AI analysis failed: {e}", exc_info=True)
                self.analysis_results['ai_analysis'] = {
                    'ai_enabled': True,
                    'analysis': {'error': f'AI analysis failed: {str(e)}'}
                }
        
        return self.analysis_results
    
    def _get_server_info(self) -> Dict[str, Any]:
        """Get basic server information"""
        try:
            server_info = self.connection.get_server_info()
            if server_info and len(server_info) > 0:
                return server_info[0]
            else:
                return {'error': 'Could not retrieve server information'}
        except Exception as e:
            self.logger.error(f"Error getting server info: {e}")
            return {'error': str(e)}
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate executive summary of findings"""
        summary = {
            'critical_issues': [],
            'warnings': [],
            'recommendations_count': 0,
            'overall_health_score': 0,
            'total_databases': 0,
            'total_issues': 0
        }
        
        try:
            # Count total databases from ServerDatabaseAnalyzer
            if 'server_database_info' in self.analysis_results and 'data' in self.analysis_results['server_database_info']:
                server_db_data = self.analysis_results['server_database_info']['data']
                if server_db_data and 'database_overview' in server_db_data:
                    summary['total_databases'] = len(server_db_data['database_overview'])
            
            # Analyze results for critical issues
            health_score = 100
            
            # Check wait stats for blocking issues
            if 'wait_stats' in self.analysis_results and 'data' in self.analysis_results['wait_stats']:
                wait_data = self.analysis_results['wait_stats']['data']
                if wait_data and 'high_waits' in wait_data:
                    for wait in wait_data['high_waits']:
                        if wait.get('wait_type') in ['PAGEIOLATCH_SH', 'PAGEIOLATCH_EX', 'WRITELOG']:
                            summary['critical_issues'].append(f"High {wait['wait_type']} waits detected")
                            health_score -= 15
            
            # Check disk performance
            if 'disk_performance' in self.analysis_results and 'data' in self.analysis_results['disk_performance']:
                disk_data = self.analysis_results['disk_performance']['data']
                if disk_data and 'slow_disks' in disk_data:
                    for disk in disk_data['slow_disks']:
                        summary['critical_issues'].append(f"Slow disk performance on {disk['drive']}")
                        health_score -= 10
            
            # Check index fragmentation
            if 'index_analysis' in self.analysis_results and 'data' in self.analysis_results['index_analysis']:
                index_data = self.analysis_results['index_analysis']['data']
                if index_data and 'fragmented_indexes' in index_data:
                    fragmented_count = len(index_data['fragmented_indexes'])
                    if fragmented_count > 20:
                        summary['warnings'].append(f"{fragmented_count} highly fragmented indexes found")
                        health_score -= 5
            
            # Check missing indexes
            if 'missing_indexes' in self.analysis_results and 'data' in self.analysis_results['missing_indexes']:
                missing_data = self.analysis_results['missing_indexes']['data']
                if missing_data and 'high_impact_indexes' in missing_data:
                    high_impact_count = len(missing_data['high_impact_indexes'])
                    if high_impact_count > 5:
                        summary['warnings'].append(f"{high_impact_count} high-impact missing indexes found")
                        health_score -= 5
            
            # Check server configuration issues from ServerDatabaseAnalyzer
            if 'server_database_info' in self.analysis_results and 'data' in self.analysis_results['server_database_info']:
                server_db_data = self.analysis_results['server_database_info']['data']
                if server_db_data and 'server_configuration' in server_db_data:
                    for config in server_db_data['server_configuration']:
                        status = config.get('best_practice_status', 'OK')
                        if status.startswith('WARNING'):
                            summary['warnings'].append(status)
                            health_score -= 3
                        elif status.startswith('CRITICAL'):
                            summary['critical_issues'].append(status)
                            health_score -= 10
            
            # Calculate total issues
            summary['total_issues'] = len(summary['critical_issues']) + len(summary['warnings'])
            summary['overall_health_score'] = max(0, health_score)
            
        except Exception as e:
            self.logger.error(f"Error generating summary: {e}")
            summary['error'] = str(e)
        
        return summary
    
    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """Generate prioritized recommendations based on analysis results"""
        recommendations = []
        
        try:
            # High priority recommendations
            if 'wait_stats' in self.analysis_results and 'data' in self.analysis_results['wait_stats']:
                wait_data = self.analysis_results['wait_stats']['data']
                if wait_data and 'high_waits' in wait_data:
                    for wait in wait_data['high_waits']:
                        if wait.get('wait_percentage', 0) > 10:
                            recommendations.append({
                                'priority': 'HIGH',
                                'category': 'Wait Stats',
                                'issue': f"High {wait['wait_type']} waits",
                                'recommendation': self._get_wait_recommendation(wait['wait_type']),
                                'impact': 'Performance degradation affecting user experience'
                            })
            
            # Index recommendations
            if 'missing_indexes' in self.analysis_results and 'data' in self.analysis_results['missing_indexes']:
                missing_data = self.analysis_results['missing_indexes']['data']
                if missing_data and 'high_impact_indexes' in missing_data:
                    for idx in missing_data['high_impact_indexes'][:5]:  # Top 5
                        recommendations.append({
                            'priority': 'MEDIUM',
                            'category': 'Missing Indexes',
                            'issue': f"Missing index on {idx.get('table_name')}",
                            'recommendation': f"CREATE INDEX IX_{idx.get('table_name')}_{idx.get('column_names', 'unnamed')} ON {idx.get('table_name')} ({idx.get('equality_columns', '')}{idx.get('inequality_columns', '')})",
                            'impact': f"Estimated improvement: {idx.get('avg_user_impact', 0):.1f}%"
                        })
            
            # Configuration recommendations from ServerDatabaseAnalyzer
            if 'server_database_info' in self.analysis_results and 'data' in self.analysis_results['server_database_info']:
                server_db_data = self.analysis_results['server_database_info']['data']
                if server_db_data and 'server_configuration' in server_db_data:
                    for config in server_db_data['server_configuration']:
                        status = config.get('best_practice_status', 'OK')
                        name = config.get('name', 'Unknown Setting')
                        value = config.get('value_in_use', config.get('value', ''))
                        
                        if status.startswith('WARNING') or status.startswith('CRITICAL'):
                            priority = 'HIGH' if status.startswith('CRITICAL') else 'MEDIUM'
                            recommendations.append({
                                'priority': priority,
                                'category': 'Server Configuration',
                                'issue': f"{name} configuration issue",
                                'recommendation': status,
                                'impact': f"Current value: {value}"
                            })
            
            # Configuration recommendations from ServerConfigAnalyzer
            if 'server_config' in self.analysis_results and 'data' in self.analysis_results['server_config']:
                config_data = self.analysis_results['server_config']['data']
                if config_data and 'issues' in config_data:
                    for issue in config_data['issues']:
                        recommendations.append({
                            'priority': issue.get('severity', 'LOW'),
                            'category': 'Configuration',
                            'issue': issue.get('description'),
                            'recommendation': issue.get('recommendation'),
                            'impact': issue.get('impact', 'Configuration optimization')
                        })
            
        except Exception as e:
            self.logger.error(f"Error generating recommendations: {e}")
            recommendations.append({
                'priority': 'ERROR',
                'category': 'System',
                'issue': 'Error generating recommendations',
                'recommendation': f'Review logs for details: {str(e)}',
                'impact': 'Analysis incomplete'
            })
        
        return sorted(recommendations, key=lambda x: {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}.get(x['priority'], 3))
    
    def _get_wait_recommendation(self, wait_type: str) -> str:
        """Get specific recommendations for wait types"""
        recommendations = {
            'PAGEIOLATCH_SH': 'Consider adding more memory, check for index fragmentation, or improve disk I/O subsystem',
            'PAGEIOLATCH_EX': 'Check for TempDB contention, consider multiple TempDB files, improve disk performance',
            'WRITELOG': 'Move transaction log to faster storage, check log file growth settings',
            'LCK_M_S': 'Review query plans for missing indexes, consider READ_COMMITTED_SNAPSHOT',
            'LCK_M_X': 'Check for blocking queries, review transaction isolation levels',
            'CXPACKET': 'Review MAXDOP and cost threshold for parallelism settings',
            'SOS_SCHEDULER_YIELD': 'Check for CPU pressure, review expensive queries',
            'THREADPOOL': 'Monitor connection pooling, check for excessive concurrent requests'
        }
        
        return recommendations.get(wait_type, 'Review SQL Server documentation for this wait type')
    
    def _analyze_advanced_indexes(self):
        """Run advanced index analysis with configurable settings"""
        try:
            # Create settings from config
            settings = IndexAnalysisSettings(
                min_advantage=self.config.index_min_advantage,
                get_selectability=self.config.index_calculate_selectability,
                only_index_analysis=self.config.index_only_analysis,
                limit_to_tablename=self.config.index_limit_to_table,
                limit_to_indexname=self.config.index_limit_to_index
            )
            
            # Run analysis
            results = self.advanced_index_analyzer.analyze_indexes(settings)
            
            # Convert to dict format for consistency
            analysis_data = {
                'missing_indexes_count': len(results.missing_indexes),
                'existing_indexes_count': len(results.existing_indexes),
                'overlapping_indexes_count': len(results.overlapping_indexes),
                'unused_indexes_count': len(results.unused_indexes),
                'total_wasted_space_mb': results.total_wasted_space_mb,
                'metadata_age_days': results.metadata_age_days,
                'warnings': results.warnings,
                'top_recommendations': self.advanced_index_analyzer.get_index_recommendations(results, 10),
                'maintenance_recommendations': self.advanced_index_analyzer.get_maintenance_recommendations(results),
                'missing_indexes': [
                    {
                        'table_name': idx.table_name,
                        'equality_columns': idx.equality_columns,
                        'inequality_columns': idx.inequality_columns,
                        'included_columns': idx.included_columns,
                        'advantage_score': idx.create_index_advantage,
                        'user_impact': idx.avg_user_impact,
                        'user_cost': idx.avg_total_user_cost,
                        'user_scans': idx.user_scans,
                        'user_seeks': idx.user_seeks,
                        'create_statement': idx.create_index_statement
                    } for idx in results.missing_indexes[:10]  # Top 10
                ],
                'unused_indexes': [
                    {
                        'table_name': idx.table_name,
                        'index_name': idx.index_name,
                        'drop_statement': idx.drop_statement,
                        'disable_statement': idx.disable_statement,
                        'usage_stats': {
                            'lookups': idx.user_lookups,
                            'scans': idx.user_scans,
                            'seeks': idx.user_seeks,
                            'updates': idx.user_updates
                        }
                    } for idx in results.unused_indexes
                ],
                'overlapping_indexes': [
                    {
                        'table_name': idx.table_name,
                        'index_name': idx.index_name,
                        'columns': idx.index_columns,
                        'overlap_type': idx.overlap_type,
                        'disable_statement': idx.disable_statement
                    } for idx in results.overlapping_indexes
                ]
            }
            
            return analysis_data
            
        except Exception as e:
            self.logger.error(f"Advanced index analysis failed: {e}")
            return {'error': str(e)}