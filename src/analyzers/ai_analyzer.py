"""
AI Analyzer for SQL Server Performance Analysis
Collects and summarizes performance data for AI analysis
"""

import logging
from typing import Dict, Any, List
from datetime import datetime
try:
    from ..services.ai_service import AIService
except ImportError:
    from services.ai_service import AIService

class AIAnalyzer:
    """Analyzes performance data and generates AI-powered insights"""
    
    def __init__(self, config):
        """Initialize AI analyzer
        
        Args:
            config: Configuration manager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.ai_service = AIService(config)
    
    def analyze(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Perform AI analysis on collected performance data
        
        Args:
            analysis_results: Complete performance analysis results
            
        Returns:
            Dict containing AI analysis results
        """
        if not self.ai_service.is_enabled():
            return {
                'ai_enabled': False,
                'analysis': {'message': 'AI Copilot not enabled'}
            }
        
        try:
            # Create performance summary optimized for AI analysis
            performance_summary = self._create_performance_summary(analysis_results)
            
            # Get AI analysis
            ai_result = self.ai_service.analyze_performance_summary(performance_summary)
            
            if ai_result:
                ai_result['generated_at'] = datetime.now().isoformat()
                self.logger.info(f"AI analysis completed. Tokens used: {ai_result.get('tokens_used', 0)}")
            
            return ai_result or {
                'ai_enabled': True,
                'analysis': {'error': 'AI analysis failed'}
            }
            
        except Exception as e:
            self.logger.error(f"AI analysis error: {e}")
            return {
                'ai_enabled': True,
                'analysis': {'error': f'AI analysis error: {str(e)}'}
            }
    
    def _create_performance_summary(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create optimized performance summary for AI analysis
        
        Args:
            analysis_results: Complete analysis results
            
        Returns:
            Condensed summary optimized for token efficiency
        """
        summary = {}
        
        # Server info (minimal essential data)
        server_info = analysis_results.get('server_info', {}).get('data', {})
        if server_info and not server_info.get('error'):
            summary['server_info'] = {
                'edition': server_info.get('edition', ''),
                'version': server_info.get('sql_version', '')[:20],  # Truncate version
                'cpu_count': server_info.get('cpu_count', 0),
                'total_memory_mb': server_info.get('total_memory_mb', 0)
            }
        
        # Wait stats (top 5 waits only)
        wait_stats = analysis_results.get('wait_stats', {}).get('data', {})
        if wait_stats and not wait_stats.get('error'):
            wait_types = wait_stats.get('wait_types', [])
            if wait_types:
                top_waits = sorted(wait_types, key=lambda x: x.get('percentage', 0), reverse=True)[:5]
                summary['wait_stats'] = {
                    'top_waits': [
                        {
                            'wait_type': wait.get('wait_type', ''),
                            'percentage': wait.get('percentage', 0)
                        }
                        for wait in top_waits if wait.get('percentage', 0) > 1  # Only significant waits
                    ]
                }
        
        # Disk issues (high impact only)
        disk_analysis = analysis_results.get('disk_analysis', {}).get('data', {})
        if disk_analysis and not disk_analysis.get('error'):
            disk_issues = []
            
            # Check I/O stalls
            io_stats = disk_analysis.get('disk_io_stats', [])
            for stat in io_stats[:5]:  # Top 5 databases
                read_latency = stat.get('io_stall_read_ms', 0) / max(stat.get('num_of_reads', 1), 1)
                write_latency = stat.get('io_stall_write_ms', 0) / max(stat.get('num_of_writes', 1), 1)
                
                if read_latency > 15:  # > 15ms is concerning
                    disk_issues.append({
                        'database': stat.get('database_name', ''),
                        'issue': f'High read latency {read_latency:.1f}ms',
                        'severity': 'HIGH' if read_latency > 50 else 'MEDIUM'
                    })
                
                if write_latency > 20:  # > 20ms is concerning
                    disk_issues.append({
                        'database': stat.get('database_name', ''),
                        'issue': f'High write latency {write_latency:.1f}ms',
                        'severity': 'HIGH' if write_latency > 100 else 'MEDIUM'
                    })
            
            if disk_issues:
                summary['disk_issues'] = disk_issues[:5]  # Top 5 issues
        
        # Index issues (counts only for efficiency)
        index_analysis = analysis_results.get('index_analysis', {}).get('data', {})
        if index_analysis and not index_analysis.get('error'):
            index_issues = {}
            
            # High fragmentation count
            fragmentation = index_analysis.get('fragmentation', [])
            high_frag_count = len([f for f in fragmentation if f.get('fragmentation_pct', 0) > 30])
            if high_frag_count > 0:
                index_issues['high_fragmentation_count'] = high_frag_count
            
            # Unused indexes count
            unused = index_analysis.get('unused_indexes', [])
            if unused:
                index_issues['unused_count'] = len(unused)
            
            # Duplicate indexes count
            duplicates = index_analysis.get('duplicate_indexes', [])
            if duplicates:
                index_issues['duplicate_count'] = len(duplicates)
            
            if index_issues:
                summary['index_issues'] = index_issues
        
        # Missing indexes (high impact only)
        missing_indexes = analysis_results.get('missing_indexes', {}).get('data', {})
        if missing_indexes and not missing_indexes.get('error'):
            high_impact = missing_indexes.get('high_impact_indexes', [])
            if high_impact:
                summary['index_issues'] = summary.get('index_issues', {})
                summary['index_issues']['missing_high_impact'] = len(high_impact)
        
        # Config issues (critical only)
        config_analysis = analysis_results.get('config_analysis', {}).get('data', {})
        if config_analysis and not config_analysis.get('error'):
            issues = config_analysis.get('issues', [])
            critical_issues = [
                {
                    'setting': issue.get('setting', ''),
                    'issue': issue.get('issue', '')[:50],  # Truncate long descriptions
                    'severity': issue.get('severity', '')
                }
                for issue in issues if issue.get('severity') == 'HIGH'
            ]
            if critical_issues:
                summary['config_issues'] = critical_issues[:3]  # Top 3 critical
        
        # TempDB issues (critical only)
        tempdb_analysis = analysis_results.get('tempdb_analysis', {}).get('data', {})
        if tempdb_analysis and not tempdb_analysis.get('error'):
            issues = tempdb_analysis.get('configuration_issues', [])
            critical_tempdb = [
                {
                    'description': issue.get('description', '')[:60],  # Truncate
                    'severity': issue.get('severity', '')
                }
                for issue in issues if issue.get('severity') == 'HIGH'
            ]
            if critical_tempdb:
                summary['tempdb_issues'] = critical_tempdb[:2]  # Top 2
        
        # Plan cache efficiency
        plan_cache = analysis_results.get('plan_cache', {}).get('data', {})
        if plan_cache and not plan_cache.get('error'):
            overview = plan_cache.get('cache_overview', [])
            if overview:
                cache_info = overview[0]
                single_use_pct = cache_info.get('single_use_percentage', 0)
                if single_use_pct > 10:  # Only if concerning
                    summary['plan_cache'] = {
                        'single_use_pct': single_use_pct,
                        'total_plans': cache_info.get('total_plans', 0)
                    }
        
        return summary