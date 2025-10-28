"""
Plan Cache Analyzer
Analyzes SQL Server plan cache for performance issues and optimization opportunities
Inspired by the great ones and SQL Server community best practices
"""

import logging
from typing import Dict, Any, List, Optional

class PlanCacheAnalyzer:
    """Analyzes SQL Server plan cache for performance bottlenecks"""
    
    def __init__(self, connection, config):
        """Initialize plan cache analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete plan cache analysis
        
        Returns:
            Dictionary containing plan cache analysis results
        """
        try:
            results = {
                'cache_overview': self._get_cache_overview(),
                'expensive_queries': self._get_expensive_queries(),
                'frequently_executed': self._get_frequently_executed_queries(),
                'poor_performing_queries': self._get_poor_performing_queries(),
                'plan_reuse_analysis': self._analyze_plan_reuse(),
                'memory_pressure': self._analyze_memory_pressure(),
                'recommendations': self._generate_plan_cache_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in plan cache analysis: {e}")
            return {'error': str(e)}
    
    def _get_cache_overview(self) -> Optional[List[Dict[str, Any]]]:
        """Get overview of plan cache usage and statistics"""
        query = """
        SELECT 
            COUNT(*) AS total_plans,
            SUM(size_in_bytes) / 1024 / 1024 AS total_size_mb,
            AVG(size_in_bytes) / 1024 AS avg_plan_size_kb,
            SUM(usecounts) AS total_use_count,
            AVG(usecounts) AS avg_use_count,
            COUNT(CASE WHEN usecounts = 1 THEN 1 END) AS single_use_plans,
            COUNT(CASE WHEN usecounts > 100 THEN 1 END) AS highly_reused_plans,
            CAST(COUNT(CASE WHEN usecounts = 1 THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS single_use_percentage
        FROM sys.dm_exec_cached_plans
        """
        
        return self.connection.execute_query(query)
    
    def _get_expensive_queries(self) -> Optional[List[Dict[str, Any]]]:
        """Get most expensive queries by various metrics"""
        query = """
        SELECT TOP 20
            qs.sql_handle,
            qs.plan_handle,
            qs.total_worker_time,
            qs.total_elapsed_time,
            qs.total_logical_reads,
            qs.total_logical_writes,
            qs.total_physical_reads,
            qs.execution_count,
            qs.total_worker_time / qs.execution_count AS avg_cpu_time,
            qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
            qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
            qs.creation_time,
            qs.last_execution_time,
            SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 
                CASE 
                    WHEN qs.statement_end_offset = -1 THEN LEN(CONVERT(nvarchar(max), st.text)) * 2 
                    ELSE qs.statement_end_offset 
                END - qs.statement_start_offset)/2 AS query_text,
            qp.query_plan
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE qs.last_execution_time > DATEADD(HOUR, -""" + str(self.config.plan_cache_analysis_hours) + """, GETDATE())
        ORDER BY qs.total_worker_time DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_frequently_executed_queries(self) -> Optional[List[Dict[str, Any]]]:
        """Get most frequently executed queries"""
        query = """
        SELECT TOP 20
            qs.sql_handle,
            qs.plan_handle,
            qs.execution_count,
            qs.total_worker_time,
            qs.total_elapsed_time,
            qs.total_logical_reads,
            qs.total_worker_time / qs.execution_count AS avg_cpu_time,
            qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
            qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
            qs.creation_time,
            qs.last_execution_time,
            SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 
                CASE 
                    WHEN qs.statement_end_offset = -1 THEN LEN(CONVERT(nvarchar(max), st.text)) * 2 
                    ELSE qs.statement_end_offset 
                END - qs.statement_start_offset)/2 AS query_text
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        WHERE qs.last_execution_time > DATEADD(HOUR, -""" + str(self.config.plan_cache_analysis_hours) + """, GETDATE())
        ORDER BY qs.execution_count DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_poor_performing_queries(self) -> Optional[List[Dict[str, Any]]]:
        """Get queries with poor performance characteristics"""
        query = """
        WITH PoorPerformers AS (
            SELECT 
                qs.sql_handle,
                qs.plan_handle,
                qs.execution_count,
                qs.total_worker_time,
                qs.total_elapsed_time,
                qs.total_logical_reads,
                qs.total_logical_writes,
                qs.total_physical_reads,
                qs.total_worker_time / qs.execution_count AS avg_cpu_time,
                qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
                qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
                qs.total_physical_reads / qs.execution_count AS avg_physical_reads,
                qs.creation_time,
                qs.last_execution_time,
                CASE 
                    WHEN qs.total_physical_reads / qs.execution_count > 1000 THEN 'HIGH_PHYSICAL_READS'
                    WHEN qs.total_logical_reads / qs.execution_count > 10000 THEN 'HIGH_LOGICAL_READS'
                    WHEN qs.total_worker_time / qs.execution_count > 5000000 THEN 'HIGH_CPU'  -- 5 seconds
                    WHEN qs.total_elapsed_time / qs.execution_count > 10000000 THEN 'HIGH_DURATION'  -- 10 seconds
                    ELSE 'OTHER'
                END AS performance_issue
            FROM sys.dm_exec_query_stats qs
            WHERE qs.last_execution_time > DATEADD(HOUR, -""" + str(self.config.plan_cache_analysis_hours) + """, GETDATE())
            AND (
                qs.total_physical_reads / qs.execution_count > 1000 OR
                qs.total_logical_reads / qs.execution_count > 10000 OR
                qs.total_worker_time / qs.execution_count > 5000000 OR
                qs.total_elapsed_time / qs.execution_count > 10000000
            )
        )
        SELECT TOP 20
            pp.*,
            SUBSTRING(st.text, (pp.sql_handle), 500) AS query_text_sample
        FROM PoorPerformers pp
        CROSS APPLY sys.dm_exec_sql_text(pp.sql_handle) st
        ORDER BY pp.avg_cpu_time DESC
        """
        
        return self.connection.execute_query(query)
    
    def _analyze_plan_reuse(self) -> Dict[str, Any]:
        """Analyze plan reuse patterns"""
        try:
            # Get plan reuse statistics
            reuse_query = """
            SELECT 
                objtype,
                COUNT(*) AS plan_count,
                SUM(usecounts) AS total_executions,
                AVG(usecounts) AS avg_reuse,
                COUNT(CASE WHEN usecounts = 1 THEN 1 END) AS single_use_plans,
                COUNT(CASE WHEN usecounts > 10 THEN 1 END) AS well_reused_plans,
                SUM(size_in_bytes) / 1024 / 1024 AS total_size_mb
            FROM sys.dm_exec_cached_plans
            GROUP BY objtype
            ORDER BY plan_count DESC
            """
            
            reuse_stats = self.connection.execute_query(reuse_query)
            
            # Get single-use plan analysis
            single_use_query = """
            SELECT TOP 10
                cp.objtype,
                cp.size_in_bytes / 1024 AS size_kb,
                SUBSTRING(st.text, 1, 200) AS query_sample,
                cp.cacheobjtype
            FROM sys.dm_exec_cached_plans cp
            CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) st
            WHERE cp.usecounts = 1
            AND cp.size_in_bytes > 50000  -- Plans larger than 50KB
            ORDER BY cp.size_in_bytes DESC
            """
            
            single_use_plans = self.connection.execute_query(single_use_query)
            
            # Calculate plan reuse efficiency
            analysis = {
                'reuse_stats': reuse_stats,
                'large_single_use_plans': single_use_plans,
                'reuse_efficiency': 'UNKNOWN'
            }
            
            if reuse_stats:
                total_plans = sum(stat.get('plan_count', 0) for stat in reuse_stats)
                single_use_total = sum(stat.get('single_use_plans', 0) for stat in reuse_stats)
                
                if total_plans > 0:
                    single_use_percentage = (single_use_total / total_plans) * 100
                    
                    if single_use_percentage > 80:
                        analysis['reuse_efficiency'] = 'POOR'
                    elif single_use_percentage > 60:
                        analysis['reuse_efficiency'] = 'FAIR'
                    elif single_use_percentage > 40:
                        analysis['reuse_efficiency'] = 'GOOD'
                    else:
                        analysis['reuse_efficiency'] = 'EXCELLENT'
                    
                    analysis['single_use_percentage'] = single_use_percentage
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing plan reuse: {e}")
            return {'error': str(e)}
    
    def _analyze_memory_pressure(self) -> Dict[str, Any]:
        """Analyze plan cache memory pressure"""
        try:
            # Get memory clerks information
            memory_query = """
            SELECT 
                type,
                SUM(pages_kb) / 1024 AS size_mb,
                SUM(pages_in_use_kb) / 1024 AS pages_in_use_mb
            FROM sys.dm_os_memory_clerks
            WHERE type IN ('CACHESTORE_SQLCP', 'CACHESTORE_OBJCP', 'CACHESTORE_PHDR')
            GROUP BY type
            ORDER BY size_mb DESC
            """
            
            memory_clerks = self.connection.execute_query(memory_query)
            
            # Get plan cache eviction information
            eviction_query = """
            SELECT 
                name,
                counter_name,
                cntr_value
            FROM sys.dm_os_performance_counters
            WHERE object_name LIKE '%Plan Cache%'
            AND counter_name IN ('Cache Hit Ratio', 'Cache Object Counts', 'Cache Objects in use')
            """
            
            eviction_stats = self.connection.execute_query(eviction_query)
            
            # Check for memory pressure indicators
            pressure_indicators = []
            
            if memory_clerks:
                total_plan_cache_mb = sum(clerk.get('size_mb', 0) for clerk in memory_clerks)
                
                if total_plan_cache_mb > 1000:  # More than 1GB
                    pressure_indicators.append({
                        'type': 'HIGH_PLAN_CACHE_USAGE',
                        'description': f'Plan cache using {total_plan_cache_mb:.1f} MB',
                        'severity': 'MEDIUM'
                    })
            
            # Analyze cache overview for pressure signs
            cache_overview = self._get_cache_overview()
            if cache_overview and len(cache_overview) > 0:
                overview = cache_overview[0]
                single_use_pct = overview.get('single_use_percentage', 0)
                
                if single_use_pct > 70:
                    pressure_indicators.append({
                        'type': 'HIGH_SINGLE_USE_PLANS',
                        'description': f'{single_use_pct:.1f}% of plans used only once',
                        'severity': 'HIGH'
                    })
            
            return {
                'memory_clerks': memory_clerks,
                'eviction_stats': eviction_stats,
                'pressure_indicators': pressure_indicators,
                'memory_pressure_level': 'HIGH' if len(pressure_indicators) > 2 else 'MEDIUM' if len(pressure_indicators) > 0 else 'LOW'
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing memory pressure: {e}")
            return {'error': str(e)}
    
    def _generate_plan_cache_recommendations(self) -> List[Dict[str, Any]]:
        """Generate plan cache optimization recommendations"""
        recommendations = []
        
        # Plan reuse analysis
        reuse_analysis = self._analyze_plan_reuse()
        reuse_efficiency = reuse_analysis.get('reuse_efficiency')
        
        if reuse_efficiency == 'POOR':
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Plan Reuse',
                'issue': 'Poor plan reuse efficiency detected',
                'recommendations': [
                    'Review queries for parameterization opportunities',
                    'Consider forced parameterization for appropriate databases',
                    'Check for ad-hoc queries with literal values',
                    'Implement stored procedures for frequently executed code'
                ]
            })
        elif reuse_efficiency == 'FAIR':
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Plan Reuse',
                'issue': 'Plan reuse could be improved',
                'recommendations': [
                    'Audit queries for parameterization potential',
                    'Review application code for dynamic SQL usage',
                    'Consider query templates and prepared statements'
                ]
            })
        
        # Memory pressure analysis
        memory_analysis = self._analyze_memory_pressure()
        pressure_level = memory_analysis.get('memory_pressure_level')
        pressure_indicators = memory_analysis.get('pressure_indicators', [])
        
        if pressure_level == 'HIGH':
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Memory Pressure',
                'issue': 'High plan cache memory pressure detected',
                'recommendations': [
                    'Clear plan cache during maintenance windows if needed',
                    'Optimize queries causing single-use plans',
                    'Consider increasing server memory',
                    'Review optimize for ad hoc workloads setting'
                ]
            })
        
        # Expensive queries analysis
        expensive_queries = self._get_expensive_queries()
        if expensive_queries and len(expensive_queries) > 0:
            top_cpu_query = expensive_queries[0]
            avg_cpu_time = top_cpu_query.get('avg_cpu_time', 0)
            
            if avg_cpu_time > 5000000:  # More than 5 seconds average CPU
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Query Performance',
                    'issue': f'Found queries with very high CPU usage (avg: {avg_cpu_time/1000000:.2f}s)',
                    'recommendations': [
                        'Review and optimize high CPU queries',
                        'Check for missing indexes',
                        'Consider query plan optimization',
                        'Review execution frequency vs. optimization cost'
                    ]
                })
        
        # Poor performing queries
        poor_queries = self._get_poor_performing_queries()
        if poor_queries and len(poor_queries) > 5:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Query Optimization',
                'issue': f'Found {len(poor_queries)} queries with performance issues',
                'recommendations': [
                    'Prioritize optimization of poor performing queries',
                    'Review execution plans for inefficiencies',
                    'Check for parameter sniffing issues',
                    'Consider query hints or plan guides if appropriate'
                ]
            })
        
        # General plan cache recommendations
        cache_overview = self._get_cache_overview()
        if cache_overview and len(cache_overview) > 0:
            overview = cache_overview[0]
            total_plans = overview.get('total_plans', 0)
            
            if total_plans > 100000:
                recommendations.append({
                    'priority': 'LOW',
                    'category': 'Plan Cache Management',
                    'issue': f'Large number of cached plans ({total_plans:,})',
                    'recommendations': [
                        'Monitor plan cache growth over time',
                        'Consider periodic plan cache analysis',
                        'Review if optimize for ad hoc workloads is appropriate',
                        'Implement plan cache maintenance procedures'
                    ]
                })
        
        # Add general best practices
        recommendations.append({
            'priority': 'INFO',
            'category': 'Best Practices',
            'issue': 'Plan cache best practices',
            'recommendations': [
                'Regularly monitor plan cache performance',
                'Use parameterized queries when possible',
                'Avoid unnecessary plan cache flushes',
                'Monitor for plan regression after changes',
                'Consider Query Store for plan management'
            ]
        })
        
        return recommendations