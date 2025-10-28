"""
Index Analysis Module  
Analyzes index fragmentation, usage, and identifies unused indexes
Inspired by the great ones and SQL Server community methodologies
"""

import logging
from typing import Dict, Any, List, Optional

class IndexAnalyzer:
    """Analyzes SQL Server indexes for fragmentation and usage patterns"""
    
    def __init__(self, connection, config):
        """Initialize index analyzer
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
        """
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def _get_user_databases(self) -> List[str]:
        """Get list of user databases (excluding system databases)"""
        query = """
        SELECT name 
        FROM sys.databases 
        WHERE database_id > 4 
        AND state = 0  -- Online only
        AND is_read_only = 0  -- Exclude read-only databases
        ORDER BY name
        """
        
        result = self.connection.execute_query(query)
        return [row['name'] for row in result] if result else []
    
    def analyze(self) -> Dict[str, Any]:
        """Run complete index analysis
        
        Returns:
            Dictionary containing index analysis results
        """
        try:
            results = {
                'fragmented_indexes': self._get_fragmented_indexes(),
                'unused_indexes': self._get_unused_indexes(),
                'duplicate_indexes': self._find_duplicate_indexes(),
                'index_usage_stats': self._get_index_usage_stats(),
                'fragmentation_usage_analysis': self._get_fragmentation_usage_analysis(),
                'maintenance_recommendations': self._get_index_maintenance_recommendations(),
                'recommendations': self._generate_index_recommendations()
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in index analysis: {e}")
            return {'error': str(e)}
    
    def _get_fragmented_indexes(self) -> Optional[List[Dict[str, Any]]]:
        """Get highly fragmented indexes that need attention"""
        min_size_mb = self.config.min_index_size_mb
        frag_threshold = self.config.max_fragmentation_threshold
        
        query = """
        SELECT 
            DB_NAME(ps.database_id) AS database_name,
            OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id) AS schema_name,
            OBJECT_NAME(ps.object_id, ps.database_id) AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            ps.index_id,
            ps.partition_number,
            ps.avg_fragmentation_in_percent,
            ps.fragment_count,
            ps.page_count,
            ps.avg_page_space_used_in_percent,
            ps.record_count,
            ps.ghost_record_count,
            ps.forwarded_record_count,
            CAST(ps.page_count * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb,
            CASE 
                WHEN ps.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
                WHEN ps.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
                ELSE 'OK'
            END AS recommended_action
        FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'LIMITED') ps
        INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
        WHERE ps.avg_fragmentation_in_percent > 10
        AND ps.page_count > (""" + str(min_size_mb) + """ * 128)  -- Minimum size filter
        AND i.is_disabled = 0
        AND i.is_hypothetical = 0
        ORDER BY ps.avg_fragmentation_in_percent DESC, ps.page_count DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_unused_indexes(self) -> Optional[List[Dict[str, Any]]]:
        """Find indexes that are never used for seeks, scans, or lookups"""
        
        unused_indexes = []
        user_databases = self._get_user_databases()
        
        if not user_databases:
            self.logger.warning("No user databases found for unused index analysis")
            return None
        
        # Save current database context (likely master)
        original_db_query = "SELECT DB_NAME() as current_db"
        original_db_result = self.connection.execute_query(original_db_query)
        original_db = original_db_result[0]['current_db'] if original_db_result else 'master'
        
        try:
            for db_name in user_databases:
                self.logger.debug(f"Analyzing unused indexes in database: {db_name}")
                
                # Change to user database
                if not self.connection.change_database(db_name):
                    self.logger.warning(f"Could not access database {db_name}, skipping...")
                    continue
                
                query = """
        SELECT 
            DB_NAME() AS database_name,
            OBJECT_SCHEMA_NAME(i.object_id) AS schema_name,
            OBJECT_NAME(i.object_id) AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            i.is_unique_constraint,
            ISNULL(us.user_seeks, 0) AS user_seeks,
            ISNULL(us.user_scans, 0) AS user_scans,
            ISNULL(us.user_lookups, 0) AS user_lookups,
            ISNULL(us.user_updates, 0) AS user_updates,
            us.last_user_seek,
            us.last_user_scan,
            us.last_user_lookup,
            us.last_user_update,
            p.rows AS table_rows,
            CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id AND us.database_id = DB_ID()
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE i.type > 0  -- Exclude heaps
        AND i.is_primary_key = 0  -- Exclude primary keys
        AND i.is_unique_constraint = 0  -- Exclude unique constraints
        AND i.is_disabled = 0
        AND i.is_hypothetical = 0
        AND OBJECT_SCHEMA_NAME(i.object_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
        GROUP BY i.object_id, i.index_id, i.name, i.type_desc, i.is_unique, 
                 i.is_primary_key, i.is_unique_constraint, us.user_seeks, 
                 us.user_scans, us.user_lookups, us.user_updates, 
                 us.last_user_seek, us.last_user_scan, us.last_user_lookup, 
                 us.last_user_update, p.rows
        HAVING (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) = 0
        OR (us.last_user_seek IS NULL AND us.last_user_scan IS NULL AND us.last_user_lookup IS NULL)
        ORDER BY size_mb DESC
                """
                
                db_result = self.connection.execute_query(query)
                if db_result:
                    unused_indexes.extend(db_result)
                    
        except Exception as e:
            self.logger.error(f"Error during unused index analysis: {str(e)}")
        finally:
            # Restore original database context
            self.connection.change_database(original_db)
        
        return unused_indexes if unused_indexes else None
    
    def _find_duplicate_indexes(self) -> Optional[List[Dict[str, Any]]]:
        """Find potentially duplicate or overlapping indexes"""
        query = """
        WITH IndexColumns AS (
            SELECT 
                i.object_id,
                i.index_id,
                i.name AS index_name,
                i.type_desc,
                i.is_unique,
                i.is_primary_key,
                STUFF((
                    SELECT ', ' + c.name + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                    FROM sys.index_columns ic
                    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = i.object_id 
                    AND ic.index_id = i.index_id
                    AND ic.is_included_column = 0
                    ORDER BY ic.key_ordinal
                    FOR XML PATH('')
                ), 1, 2, '') AS key_columns,
                STUFF((
                    SELECT ', ' + c.name
                    FROM sys.index_columns ic
                    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = i.object_id 
                    AND ic.index_id = i.index_id
                    AND ic.is_included_column = 1
                    ORDER BY ic.key_ordinal
                    FOR XML PATH('')
                ), 1, 2, '') AS included_columns
            FROM sys.indexes i
            WHERE i.type > 0
            AND i.is_disabled = 0
            AND i.is_hypothetical = 0
        )
        SELECT 
            DB_NAME() AS database_name,
            OBJECT_SCHEMA_NAME(ic1.object_id) AS schema_name,
            OBJECT_NAME(ic1.object_id) AS table_name,
            ic1.index_name AS index1_name,
            ic1.type_desc AS index1_type,
            ic1.key_columns AS index1_key_columns,
            ic1.included_columns AS index1_included_columns,
            ic2.index_name AS index2_name,
            ic2.type_desc AS index2_type,
            ic2.key_columns AS index2_key_columns,
            ic2.included_columns AS index2_included_columns,
            CASE 
                WHEN ic1.key_columns = ic2.key_columns THEN 'EXACT_DUPLICATE'
                WHEN ic1.key_columns LIKE ic2.key_columns + ',%' THEN 'OVERLAPPING'
                WHEN ic2.key_columns LIKE ic1.key_columns + ',%' THEN 'OVERLAPPING'
                ELSE 'SIMILAR'
            END AS duplicate_type
        FROM IndexColumns ic1
        INNER JOIN IndexColumns ic2 ON ic1.object_id = ic2.object_id
        WHERE ic1.index_id < ic2.index_id  -- Avoid duplicates
        AND (
            ic1.key_columns = ic2.key_columns  -- Exact key match
            OR ic1.key_columns LIKE ic2.key_columns + ',%'  -- ic2 is prefix of ic1
            OR ic2.key_columns LIKE ic1.key_columns + ',%'  -- ic1 is prefix of ic2
        )
        AND NOT (ic1.is_primary_key = 1 OR ic2.is_primary_key = 1)  -- Exclude primary keys
        ORDER BY OBJECT_NAME(ic1.object_id), ic1.index_name
        """
        
        return self.connection.execute_query(query)
    
    def _get_index_usage_stats(self) -> Optional[List[Dict[str, Any]]]:
        """Get comprehensive index usage statistics"""
        query = """
        SELECT 
            DB_NAME() AS database_name,
            OBJECT_SCHEMA_NAME(i.object_id) AS schema_name,
            OBJECT_NAME(i.object_id) AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            ISNULL(us.user_seeks, 0) AS user_seeks,
            ISNULL(us.user_scans, 0) AS user_scans,
            ISNULL(us.user_lookups, 0) AS user_lookups,
            ISNULL(us.user_updates, 0) AS user_updates,
            ISNULL(us.system_seeks, 0) AS system_seeks,
            ISNULL(us.system_scans, 0) AS system_scans,
            ISNULL(us.system_lookups, 0) AS system_lookups,
            ISNULL(us.system_updates, 0) AS system_updates,
            us.last_user_seek,
            us.last_user_scan,
            us.last_user_lookup,
            us.last_user_update,
            p.rows AS table_rows,
            CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb,
            CASE 
                WHEN (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) = 0 THEN 'UNUSED'
                WHEN us.user_updates > (us.user_seeks + us.user_scans + us.user_lookups) * 5 THEN 'OVER_UPDATED'
                WHEN us.user_seeks > 1000 THEN 'HIGHLY_USED'
                WHEN us.user_seeks > 100 THEN 'MODERATELY_USED'
                ELSE 'LIGHTLY_USED'
            END AS usage_pattern
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE i.type > 0  -- Exclude heaps
        AND i.is_disabled = 0
        AND i.is_hypothetical = 0
        AND OBJECT_SCHEMA_NAME(i.object_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
        GROUP BY i.object_id, i.index_id, i.name, i.type_desc, i.is_unique, 
                 i.is_primary_key, us.user_seeks, us.user_scans, us.user_lookups, 
                 us.user_updates, us.system_seeks, us.system_scans, us.system_lookups,
                 us.system_updates, us.last_user_seek, us.last_user_scan, 
                 us.last_user_lookup, us.last_user_update, p.rows
        ORDER BY (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) DESC
        """
        
        return self.connection.execute_query(query)
    
    def _get_fragmentation_usage_analysis(self) -> Optional[List[Dict[str, Any]]]:
        """Analyze index fragmentation combined with usage patterns for smart maintenance decisions
        Based on Jacob Saugmann's fragmentation and usage analysis script"""
        
        fragmentation_results = []
        user_databases = self._get_user_databases()
        
        if not user_databases:
            self.logger.warning("No user databases found for fragmentation usage analysis")
            return None
        
        # Save current database context
        original_db_query = "SELECT DB_NAME() as current_db"
        original_db_result = self.connection.execute_query(original_db_query)
        original_db = original_db_result[0]['current_db'] if original_db_result else 'master'
        
        try:
            for db_name in user_databases:
                self.logger.debug(f"Analyzing index fragmentation and usage in database: {db_name}")
                
                # Change to user database
                if not self.connection.change_database(db_name):
                    self.logger.warning(f"Could not access database {db_name}, skipping...")
                    continue
                
                query = """
                WITH IndexStats AS (
                    SELECT
                        s.name AS SchemaName,
                        t.name AS TableName,
                        i.name AS IndexName,
                        ips.avg_fragmentation_in_percent,
                        ips.page_count,
                        ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) AS TotalReads,
                        ISNULL(us.user_updates, 0) AS TotalWrites,
                        us.last_user_seek,
                        us.last_user_scan,
                        us.last_user_lookup,
                        us.last_user_update,
                        CAST(ips.page_count * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb
                    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
                    INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
                    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
                    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                    LEFT JOIN sys.dm_db_index_usage_stats AS us ON i.object_id = us.object_id 
                        AND i.index_id = us.index_id AND us.database_id = DB_ID()
                    WHERE ips.avg_fragmentation_in_percent > 30
                      AND ips.page_count > 500  -- Only large indexes (>4MB)
                      AND i.is_disabled = 0
                      AND i.is_hypothetical = 0
                )
                SELECT
                    DB_NAME() AS database_name,
                    SchemaName,
                    TableName,
                    ISNULL(IndexName, '(ALL)') AS IndexName,
                    avg_fragmentation_in_percent AS FragmentationPct,
                    page_count AS PageCount,
                    size_mb,
                    TotalReads,
                    TotalWrites,
                    last_user_seek,
                    last_user_scan,
                    last_user_lookup,
                    last_user_update,
                    CASE
                        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 'REBUILD'
                        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 'REORGANIZE'
                        ELSE 'IGNORE'
                    END AS ActionRecommendation,
                    CASE
                        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN
                            CASE WHEN IndexName IS NULL OR IndexName = '(ALL)'
                                THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF);'
                                ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF);'
                            END
                        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN
                            CASE WHEN IndexName IS NULL OR IndexName = '(ALL)'
                                THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
                                ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
                            END
                        ELSE '-- IGNORE: Low usage or low fragmentation'
                    END AS SQLCommand,
                    CASE
                        WHEN TotalReads = 0 THEN 'UNUSED'
                        WHEN TotalReads > 10000 THEN 'VERY_HIGH_USAGE'
                        WHEN TotalReads > 1000 THEN 'HIGH_USAGE'
                        WHEN TotalReads > 100 THEN 'MODERATE_USAGE'
                        ELSE 'LOW_USAGE'
                    END AS UsageCategory,
                    CASE
                        WHEN TotalWrites > TotalReads * 2 THEN 'WRITE_HEAVY'
                        WHEN TotalReads > TotalWrites * 2 THEN 'READ_HEAVY'
                        ELSE 'BALANCED'
                    END AS WorkloadPattern
                FROM IndexStats
                ORDER BY
                    CASE
                        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 1
                        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 2
                        ELSE 3
                    END,
                    avg_fragmentation_in_percent DESC,
                    TotalReads DESC
                """
                
                db_result = self.connection.execute_query(query)
                if db_result:
                    fragmentation_results.extend(db_result)
                    
        except Exception as e:
            self.logger.error(f"Error during fragmentation usage analysis: {str(e)}")
        finally:
            # Restore original database context
            self.connection.change_database(original_db)
        
        return fragmentation_results if fragmentation_results else None
    
    def _get_index_maintenance_recommendations(self) -> List[Dict[str, Any]]:
        """Get index maintenance recommendations with specific actions (REBUILD/REORGANIZE)"""
        query = """
        WITH IndexStats AS (
            SELECT
                s.name AS SchemaName,
                t.name AS TableName,
                i.name AS IndexName,
                ips.avg_fragmentation_in_percent,
                ips.page_count,
                ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) AS TotalReads
            FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'LIMITED') AS ips
            INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
            INNER JOIN sys.tables AS t ON i.object_id = t.object_id
            INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            LEFT JOIN sys.dm_db_index_usage_stats AS us ON i.object_id = us.object_id AND i.index_id = us.index_id AND us.database_id = ips.database_id
            WHERE ips.avg_fragmentation_in_percent > 30
              AND ips.page_count > 500  -- Only large indexes
              AND ips.database_id > 4  -- Exclude system databases
        )
        SELECT
            SchemaName,
            TableName,
            ISNULL(IndexName, '(ALL)') AS IndexName,
            avg_fragmentation_in_percent AS FragmentationPct,
            page_count AS PageCount,
            TotalReads,
            CASE
                WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 'REBUILD'
                WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 'REORGANIZE'
                ELSE 'IGNORE'
            END AS ActionRecommendation,
            CASE
                WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN
                    CASE WHEN IndexName IS NULL
                        THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF);'
                        ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF);'
                    END
                WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN
                    CASE WHEN IndexName IS NULL
                        THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
                        ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
                    END
                ELSE '-- IGNORE: Low usage or low fragmentation'
            END AS SQLCommand
        FROM IndexStats
        ORDER BY
            CASE
                WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 1
                WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 2
                ELSE 3
            END,
            avg_fragmentation_in_percent DESC,
            TotalReads DESC
        """
        
        return self.connection.execute_query(query)
    
    def _generate_index_recommendations(self) -> List[Dict[str, Any]]:
        """Generate specific index maintenance recommendations"""
        recommendations = []
        
        # Get maintenance recommendations
        maintenance_items = self._get_index_maintenance_recommendations()
        if maintenance_items:
            rebuild_count = sum(1 for item in maintenance_items if item.get('ActionRecommendation') == 'REBUILD')
            reorg_count = sum(1 for item in maintenance_items if item.get('ActionRecommendation') == 'REORGANIZE')
            
            if rebuild_count > 0:
                recommendations.append({
                    'category': 'Index Maintenance',
                    'severity': 'HIGH',
                    'finding': f'{rebuild_count} indexes require REBUILD due to high fragmentation (>80%) and usage',
                    'recommendation': 'Execute REBUILD operations during maintenance window',
                    'details': [item for item in maintenance_items if item.get('ActionRecommendation') == 'REBUILD']
                })
            
            if reorg_count > 0:
                recommendations.append({
                    'category': 'Index Maintenance',
                    'severity': 'MEDIUM',
                    'finding': f'{reorg_count} indexes require REORGANIZE due to moderate fragmentation (30-80%)',
                    'recommendation': 'Execute REORGANIZE operations - can run online',
                    'details': [item for item in maintenance_items if item.get('ActionRecommendation') == 'REORGANIZE']
                })
        
        # Fragmented indexes recommendations
        fragmented = self._get_fragmented_indexes()
        if fragmented:
            rebuild_count = sum(1 for idx in fragmented if idx.get('recommended_action') == 'REBUILD')
            reorganize_count = sum(1 for idx in fragmented if idx.get('recommended_action') == 'REORGANIZE')
            
            if rebuild_count > 0:
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Index Maintenance',
                    'issue': f'{rebuild_count} indexes need rebuilding (>30% fragmentation)',
                    'recommendations': [
                        'Schedule index rebuilds during maintenance window',
                        'Consider online index rebuilds for Enterprise Edition',
                        'Monitor index fragmentation regularly',
                        'Use REBUILD for fragmentation > 30%'
                    ]
                })
            
            if reorganize_count > 0:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'Index Maintenance',
                    'issue': f'{reorganize_count} indexes need reorganizing (10-30% fragmentation)',
                    'recommendations': [
                        'Schedule index reorganizations',
                        'Use REORGANIZE for fragmentation 10-30%',
                        'Can be performed online',
                        'Consider automated maintenance plans'
                    ]
                })
        
        # Unused indexes recommendations
        unused = self._get_unused_indexes()
        if unused:
            total_unused_size = sum(idx.get('size_mb', 0) for idx in unused)
            
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Index Cleanup',
                'issue': f'{len(unused)} unused indexes found ({total_unused_size:.1f} MB)',
                'recommendations': [
                    'Review unused indexes for potential removal',
                    'Verify indexes are truly unused in production',
                    'Consider dropping after extended monitoring period',
                    'Document business reasons for keeping if necessary'
                ]
            })
        
        # Duplicate indexes recommendations
        duplicates = self._find_duplicate_indexes()
        if duplicates:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Index Optimization',
                'issue': f'{len(duplicates)} duplicate or overlapping indexes found',
                'recommendations': [
                    'Review duplicate indexes for consolidation',
                    'Keep the most comprehensive index',
                    'Consider covering indexes vs multiple indexes',
                    'Test performance impact before removal'
                ]
            })
        
        # Usage pattern recommendations
        usage_stats = self._get_index_usage_stats()
        if usage_stats:
            over_updated = [idx for idx in usage_stats if idx.get('usage_pattern') == 'OVER_UPDATED']
            if over_updated:
                recommendations.append({
                    'priority': 'LOW',
                    'category': 'Index Performance',
                    'issue': f'{len(over_updated)} indexes with high update to read ratio',
                    'recommendations': [
                        'Review indexes with high update overhead',
                        'Consider if all columns are necessary',
                        'Evaluate impact on insert/update performance',
                        'Monitor for write-heavy workloads'
                    ]
                })
        
        # Fragmentation Usage Analysis - Smart Maintenance Recommendations
        frag_usage_analysis = self._get_fragmentation_usage_analysis()
        if frag_usage_analysis:
            rebuild_candidates = [idx for idx in frag_usage_analysis if idx.get('ActionRecommendation') == 'REBUILD']
            reorganize_candidates = [idx for idx in frag_usage_analysis if idx.get('ActionRecommendation') == 'REORGANIZE']
            
            # High priority rebuild recommendations
            if rebuild_candidates:
                high_usage_rebuilds = [idx for idx in rebuild_candidates if idx.get('UsageCategory') in ['VERY_HIGH_USAGE', 'HIGH_USAGE']]
                if high_usage_rebuilds:
                    total_size = sum(idx.get('size_mb', 0) for idx in high_usage_rebuilds)
                    recommendations.append({
                        'priority': 'CRITICAL',
                        'category': 'Smart Index Maintenance',
                        'issue': f'{len(high_usage_rebuilds)} heavily used indexes need REBUILD (>80% fragmentation, {total_size:.1f} MB)',
                        'recommendations': [
                            'Priority rebuild - these indexes impact performance significantly',
                            'Schedule during maintenance window for best results',
                            'Consider Enterprise Edition online rebuilds if available',
                            'Monitor performance improvement after rebuild'
                        ],
                        'details': high_usage_rebuilds
                    })
                
                # Low usage rebuild recommendations
                low_usage_rebuilds = [idx for idx in rebuild_candidates if idx.get('UsageCategory') in ['LOW_USAGE', 'MODERATE_USAGE']]
                if low_usage_rebuilds:
                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'Smart Index Maintenance',
                        'issue': f'{len(low_usage_rebuilds)} moderately used indexes need REBUILD',
                        'recommendations': [
                            'Lower priority rebuilds - evaluate if worth the maintenance time',
                            'Consider if these indexes are actually needed',
                            'May combine with other maintenance activities'
                        ]
                    })
            
            # Reorganize recommendations
            if reorganize_candidates:
                total_reorg_size = sum(idx.get('size_mb', 0) for idx in reorganize_candidates)
                recommendations.append({
                    'priority': 'LOW',
                    'category': 'Smart Index Maintenance',
                    'issue': f'{len(reorganize_candidates)} indexes need REORGANIZE (30-80% fragmentation, {total_reorg_size:.1f} MB)',
                    'recommendations': [
                        'REORGANIZE operations can run online',
                        'Less disruptive than rebuilds',
                        'Good for routine maintenance',
                        'Consider automated maintenance plans'
                    ],
                    'details': reorganize_candidates
                })
            
            # Workload pattern insights
            write_heavy_indexes = [idx for idx in frag_usage_analysis if idx.get('WorkloadPattern') == 'WRITE_HEAVY']
            if write_heavy_indexes:
                recommendations.append({
                    'priority': 'INFO',
                    'category': 'Index Workload Analysis',
                    'issue': f'{len(write_heavy_indexes)} indexes show write-heavy patterns',
                    'recommendations': [
                        'Write-heavy indexes fragment faster',
                        'Consider more frequent maintenance cycles',
                        'Evaluate if all columns in index are necessary',
                        'Monitor fill factor settings'
                    ]
                })
        
        return recommendations