"""
Missing Index Analyzer
Identifies missing indexes that could improve query performance
Based on SQL Server DMVs and best practices from SQL experts
"""

import logging
from typing import Dict, Any, List, Optional

class MissingIndexAnalyzer:
    """Analyzes missing indexes using SQL Server DMVs"""
    
    def __init__(self, connection, config):
        """Initialize missing index analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete missing index analysis
        
        Returns:
            Dictionary containing missing index analysis results
        """
        try:
            results = {
                'missing_indexes': self._get_missing_indexes(),
                'high_impact_indexes': self._get_high_impact_indexes(),
                'group_stats': self._get_missing_index_group_stats(),
                'recommendations': self._generate_missing_index_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in missing index analysis: {e}")
            return {'error': str(e)}
    
    def _get_missing_indexes(self) -> Optional[List[Dict[str, Any]]]:
        """Get missing index suggestions from SQL Server DMVs"""
        query = """
        SELECT 
            migs.group_handle,
            migs.unique_compiles,
            migs.user_seeks,
            migs.user_scans,
            migs.last_user_seek,
            migs.last_user_scan,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
            migs.system_seeks,
            migs.system_scans,
            migs.last_system_seek,
            migs.last_system_scan,
            migs.avg_total_system_cost,
            migs.avg_system_impact,
            mid.database_id,
            DB_NAME(mid.database_id) AS database_name,
            OBJECT_SCHEMA_NAME(mid.object_id, mid.database_id) AS schema_name,
            OBJECT_NAME(mid.object_id, mid.database_id) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            mid.statement as table_statement,
            -- Calculate impact score
            (migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) AS impact_score,
            -- Generate index creation statement
            'CREATE NONCLUSTERED INDEX IX_' 
                + OBJECT_NAME(mid.object_id, mid.database_id) + '_' 
                + CAST(migs.group_handle AS VARCHAR(20))
                + ' ON ' + mid.statement 
                + ' (' + ISNULL(mid.equality_columns, '') 
                + CASE WHEN mid.inequality_columns IS NOT NULL AND mid.equality_columns IS NOT NULL 
                       THEN ', ' ELSE '' END 
                + ISNULL(mid.inequality_columns, '') + ')'
                + CASE WHEN mid.included_columns IS NOT NULL 
                       THEN ' INCLUDE (' + mid.included_columns + ')' ELSE '' END
                + ';' AS create_statement
        FROM sys.dm_db_missing_index_groups mig
        INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
        INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
        WHERE mid.database_id = DB_ID()
        AND migs.avg_user_impact > 10  -- Only indexes with significant impact
        ORDER BY impact_score DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_high_impact_indexes(self) -> List[Dict[str, Any]]:
        """Get missing indexes with high performance impact"""
        missing_indexes = self._get_missing_indexes()
        if not missing_indexes:
            return []
        
        min_impact = self.config.min_missing_index_impact
        
        high_impact = []
        for idx in missing_indexes:
            impact_score = idx.get('impact_score', 0)
            avg_user_impact = idx.get('avg_user_impact', 0)
            
            if impact_score > min_impact and avg_user_impact > 25:
                # Add additional analysis
                idx['priority'] = self._calculate_index_priority(idx)
                idx['estimated_size_impact'] = self._estimate_index_size_impact(idx)
                high_impact.append(idx)
        
        return sorted(high_impact, key=lambda x: x.get('impact_score', 0), reverse=True)[:10]
    
    def _get_missing_index_group_stats(self) -> Optional[List[Dict[str, Any]]]:
        """Get aggregated statistics about missing index groups"""
        query = """
        SELECT 
            COUNT(*) AS total_missing_indexes,
            SUM(migs.user_seeks + migs.user_scans) AS total_user_operations,
            AVG(migs.avg_user_impact) AS avg_impact,
            MAX(migs.avg_user_impact) AS max_impact,
            MIN(migs.avg_user_impact) AS min_impact,
            SUM(migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) AS total_impact_score,
            COUNT(CASE WHEN migs.avg_user_impact > 50 THEN 1 END) AS high_impact_count,
            COUNT(CASE WHEN migs.avg_user_impact BETWEEN 25 AND 50 THEN 1 END) AS medium_impact_count,
            COUNT(CASE WHEN migs.avg_user_impact < 25 THEN 1 END) AS low_impact_count
        FROM sys.dm_db_missing_index_groups mig
        INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
        INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
        WHERE mid.database_id = DB_ID()
        """
        
        return self.connection.execute_query(query)
    
    def _calculate_index_priority(self, index_info: Dict[str, Any]) -> str:
        """Calculate priority level for a missing index"""
        impact_score = index_info.get('impact_score', 0)
        avg_user_impact = index_info.get('avg_user_impact', 0)
        user_seeks = index_info.get('user_seeks', 0)
        user_scans = index_info.get('user_scans', 0)
        
        # High priority criteria
        if (impact_score > 100000 and avg_user_impact > 50) or \
           (user_seeks + user_scans > 1000 and avg_user_impact > 40):
            return 'HIGH'
        
        # Medium priority criteria
        elif (impact_score > 10000 and avg_user_impact > 30) or \
             (user_seeks + user_scans > 100 and avg_user_impact > 25):
            return 'MEDIUM'
        
        # Low priority
        else:
            return 'LOW'
    
    def _estimate_index_size_impact(self, index_info: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate the storage and maintenance impact of creating the index"""
        # Get table size information
        table_name = index_info.get('table_name')
        schema_name = index_info.get('schema_name')
        
        if not table_name or not schema_name:
            return {'error': 'Missing table information'}
        
        size_query = f"""
        SELECT 
            p.rows AS table_rows,
            SUM(a.total_pages) * 8 / 1024 AS table_size_mb,
            COUNT(i.index_id) AS existing_indexes
        FROM sys.tables t
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        LEFT JOIN sys.indexes i ON t.object_id = i.object_id AND i.type > 0
        WHERE t.name = '{table_name}'
        AND SCHEMA_NAME(t.schema_id) = '{schema_name}'
        GROUP BY p.rows
        """
        
        try:
            size_info = self.connection.execute_query(size_query)
            if size_info and len(size_info) > 0:
                table_info = size_info[0]
                
                # Estimate index size (rough calculation)
                table_rows = table_info.get('table_rows', 0)
                table_size_mb = table_info.get('table_size_mb', 0)
                existing_indexes = table_info.get('existing_indexes', 0)
                
                # Rough estimation: index size is typically 20-40% of table size
                # depending on columns included
                equality_cols = len(index_info.get('equality_columns', '').split(',')) if index_info.get('equality_columns') else 0
                inequality_cols = len(index_info.get('inequality_columns', '').split(',')) if index_info.get('inequality_columns') else 0
                included_cols = len(index_info.get('included_columns', '').split(',')) if index_info.get('included_columns') else 0
                
                total_columns = equality_cols + inequality_cols + included_cols
                estimated_size_mb = table_size_mb * (0.15 + (total_columns * 0.05))
                
                return {
                    'estimated_size_mb': round(estimated_size_mb, 2),
                    'table_rows': table_rows,
                    'table_size_mb': table_size_mb,
                    'existing_indexes': existing_indexes,
                    'maintenance_overhead': 'LOW' if total_columns <= 3 else 'MEDIUM' if total_columns <= 6 else 'HIGH'
                }
            else:
                return {'error': 'Could not retrieve table size information'}
                
        except Exception as e:
            self.logger.error(f"Error estimating index size impact: {e}")
            return {'error': str(e)}
    
    def _generate_missing_index_recommendations(self) -> List[Dict[str, Any]]:
        """Generate recommendations for missing indexes"""
        recommendations = []
        
        high_impact_indexes = self._get_high_impact_indexes()
        group_stats = self._get_missing_index_group_stats()
        
        # High impact index recommendations
        for idx in high_impact_indexes[:5]:  # Top 5 recommendations
            priority = idx.get('priority', 'MEDIUM')
            impact_score = idx.get('impact_score', 0)
            avg_user_impact = idx.get('avg_user_impact', 0)
            
            recommendations.append({
                'priority': priority,
                'category': 'Missing Indexes',
                'issue': f"Missing index on {idx.get('schema_name')}.{idx.get('table_name')}",
                'impact_score': impact_score,
                'estimated_improvement': f"{avg_user_impact:.1f}%",
                'sql_statement': idx.get('create_statement'),
                'recommendations': [
                    'Review query plans using this table',
                    'Test index in development environment',
                    'Monitor performance after implementation',
                    'Consider maintenance overhead vs performance gain'
                ]
            })
        
        # Overall missing index analysis
        if group_stats and len(group_stats) > 0:
            stats = group_stats[0]
            total_missing = stats.get('total_missing_indexes', 0)
            high_impact_count = stats.get('high_impact_count', 0)
            
            if total_missing > 50:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'Index Strategy',
                    'issue': f"High number of missing index suggestions ({total_missing})",
                    'recommendations': [
                        'Review overall indexing strategy',
                        'Prioritize high-impact indexes first',
                        'Consider query optimization alongside indexing',
                        'Implement systematic index review process'
                    ]
                })
            
            if high_impact_count > 10:
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Performance Optimization',
                    'issue': f"Multiple high-impact missing indexes ({high_impact_count})",
                    'recommendations': [
                        'Urgent review of critical missing indexes',
                        'Implement top 5-10 indexes immediately',
                        'Schedule comprehensive index analysis',
                        'Monitor query performance improvements'
                    ]
                })
        
        # DMV limitations warning
        recommendations.append({
            'priority': 'INFO',
            'category': 'Analysis Notes',
            'issue': 'Missing index DMVs have limitations',
            'recommendations': [
                'DMVs reset on SQL Server restart',
                'Consider multiple column sort orders',
                'Review actual query execution plans',
                'Test all recommendations in development first',
                'Some suggested indexes may be redundant'
            ]
        })
        
        return recommendations