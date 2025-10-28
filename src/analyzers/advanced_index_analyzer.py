"""
Advanced Index Analyzer for SQL Speedinator
Implements comprehensive index analysis based on Jacob Saugmann's advanced SQL script
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

@dataclass
class IndexAnalysisSettings:
    """Settings for advanced index analysis"""
    db_id: str = "DB_ID()"
    min_advantage: int = 80
    get_selectability: bool = False
    drop_tmp_table: bool = True
    meta_age: int = -1
    only_index_analysis: bool = True
    limit_to_tablename: str = ""
    limit_to_indexname: str = ""

@dataclass
class MissingIndexInfo:
    """Missing index information"""
    table_name: str
    equality_columns: Optional[str]
    inequality_columns: Optional[str]
    included_columns: Optional[str]
    avg_total_user_cost: float
    avg_user_impact: float
    user_scans: int
    user_seeks: int
    create_index_statement: str
    create_index_advantage: float
    meta_data_age: int

@dataclass
class ExistingIndexInfo:
    """Existing index information"""
    table_name: str
    index_name: str
    index_type: str
    is_unique: bool
    is_primary_key: bool
    has_filter: bool
    index_columns: str
    included_columns: Optional[str]
    record_count: int
    fragmentation_percent: float
    user_lookups: int
    user_scans: int
    user_seeks: int
    user_updates: int
    meta_data_age: int

@dataclass
class OverlappingIndexInfo:
    """Overlapping index information"""
    table_name: str
    index_name: str
    index_columns: str
    included_columns: Optional[str]
    overlap_type: str
    disable_statement: str

@dataclass
class UnusedIndexInfo:
    """Unused index information"""
    table_name: str
    index_name: str
    drop_statement: str
    disable_statement: str
    user_lookups: int
    user_scans: int
    user_seeks: int
    user_updates: int
    space_mb: Optional[float]

@dataclass
class IndexAnalysisResults:
    """Complete index analysis results"""
    missing_indexes: List[MissingIndexInfo]
    existing_indexes: List[ExistingIndexInfo]
    overlapping_indexes: List[OverlappingIndexInfo]
    unused_indexes: List[UnusedIndexInfo]
    total_wasted_space_mb: float
    metadata_age_days: int
    warnings: List[str]

class AdvancedIndexAnalyzer:
    """Advanced Index Analyzer based on Jacob Saugmann's comprehensive script"""
    
    def __init__(self, connection):
        self.connection = connection
        self.logger = logging.getLogger(__name__)
        
    def analyze_indexes(self, settings: IndexAnalysisSettings = None) -> IndexAnalysisResults:
        """
        Perform comprehensive index analysis
        
        Args:
            settings: Analysis settings with switches
            
        Returns:
            Complete index analysis results
        """
        if settings is None:
            settings = IndexAnalysisSettings()
            
        try:
            self.logger.info("Starting advanced index analysis...")
            
            # Get index analysis data
            index_data = self._execute_advanced_analysis(settings)
            
            # Parse results into structured data
            results = self._parse_analysis_results(index_data, settings)
            
            self.logger.info(f"Advanced index analysis completed - Found {len(results.missing_indexes)} missing indexes, {len(results.unused_indexes)} unused indexes")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Advanced index analysis failed: {str(e)}")
            raise
    
    def _execute_advanced_analysis(self, settings: IndexAnalysisSettings) -> Dict[str, List[Dict]]:
        """Execute the advanced index analysis SQL script"""
        
        # For now, execute a simplified version to test functionality
        # We'll build up the full script step by step
        
        sql_query = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
        SET NOCOUNT ON;
        
        DECLARE @db_id SMALLINT = DB_ID()
        DECLARE @min_advantage TINYINT = {settings.min_advantage}
        DECLARE @limit_to_tablename VARCHAR(512) = '{settings.limit_to_tablename}'
        
        -- Missing indexes analysis
        SELECT md.statement AS table_name,
               'Missing_Index' AS table_type_desc,
               md.equality_columns,
               md.inequality_columns,
               md.included_columns,
               mgs.avg_total_user_cost,
               mgs.avg_user_impact,
               mgs.user_scans,
               mgs.user_seeks,
               mgs.last_user_scan,
               mgs.last_user_seek,
               DATEDIFF(DAY, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE()) AS meta_data_age,
               CONCAT('CREATE NONCLUSTERED INDEX idx_',
                      REPLACE(REPLACE(REPLACE(SUBSTRING(md.statement, 
                                                      CHARINDEX('.', md.statement, CHARINDEX('.', md.statement)+1)+1, 
                                                      LEN(md.statement)- LEN(CHARINDEX('.', md.statement,CHARINDEX('.', md.statement)+1))), 
                                             '[',''), ']',''),'.','_'),
                      '_',FORMAT(GETDATE(), 'yyyyMMdd'),
                      ' ON ',md.statement, ' (',
                      IIF(md.equality_columns IS NOT NULL, md.equality_columns, md.inequality_columns),
                      ')', 
                      IIF(md.included_columns IS NOT NULL,CONCAT(CHAR(13),'INCLUDE(' +md.included_columns+ ')'), ''),
                      CHAR(13) ,'WITH (DATA_COMPRESSION=PAGE);') AS create_ix_stmt,
               CAST((mgs.avg_total_user_cost * (mgs.avg_user_impact /100.0) * mgs.user_seeks) / 
                    GREATEST(DATEDIFF(DAY, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE()), 1) AS DECIMAL(18,2)) AS create_index_adv
        FROM sys.dm_db_missing_index_details md               
            INNER JOIN sys.dm_db_missing_index_groups mg      
                    ON mg.index_handle = md.index_handle
            INNER JOIN sys.dm_db_missing_index_group_stats mgs
                    ON mg.index_group_handle = mgs.group_handle
        WHERE md.database_id = @db_id
            AND (mgs.avg_user_impact > @min_advantage OR mgs.avg_total_user_cost > 50)
            AND md.statement LIKE '%'+@limit_to_tablename+'%'
        ORDER BY mgs.avg_user_impact DESC
        """
        
        cursor = self.connection.connection.cursor()
        results = {}
        
        try:
            cursor.execute(sql_query)
            
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                results['index_info'] = [dict(zip(columns, row)) for row in rows]
            
        finally:
            cursor.close()
        
        return results
    
    def _split_sql_script(self, sql_script: str) -> List[str]:
        """Split SQL script into individual queries"""
        # Simple split on GO statements and major sections
        sections = []
        current_section = []
        
        for line in sql_script.split('\n'):
            line = line.strip()
            
            if line.upper() == 'GO' or line.startswith('--Missing index') or line.startswith(';WITH') or line.startswith('/* Get'):
                if current_section:
                    sections.append('\n'.join(current_section))
                    current_section = []
            
            if line and not line.upper() == 'GO':
                current_section.append(line)
        
        if current_section:
            sections.append('\n'.join(current_section))
        
        return sections
    
    def _parse_analysis_results(self, data: Dict[str, List[Dict]], settings: IndexAnalysisSettings) -> IndexAnalysisResults:
        """Parse raw analysis results into structured objects"""
        
        missing_indexes = []
        existing_indexes = []
        overlapping_indexes = []
        unused_indexes = []
        warnings = []
        metadata_age = 0
        wasted_space_mb = 0.0
        
        # Parse index info (contains both missing and existing indexes)
        if 'index_info' in data:
            for row in data['index_info']:
                metadata_age = row.get('meta_data_age', 0)
                
                if row.get('table_type_desc') == 'Missing_Index':
                    missing_indexes.append(MissingIndexInfo(
                        table_name=row.get('table_name', ''),
                        equality_columns=row.get('index_columns_names'),
                        inequality_columns=row.get('inequality_columns'),
                        included_columns=row.get('included_columns'),
                        avg_total_user_cost=float(row.get('avg_total_user_cost', 0)),
                        avg_user_impact=float(row.get('avg_user_impact', 0)),
                        user_scans=int(row.get('user_scans', 0)),
                        user_seeks=int(row.get('user_seeks', 0)),
                        create_index_statement=row.get('create_index_statement', ''),
                        create_index_advantage=float(row.get('create_index_adv', 0)),
                        meta_data_age=metadata_age
                    ))
                else:
                    existing_indexes.append(ExistingIndexInfo(
                        table_name=row.get('table_name', ''),
                        index_name=row.get('index_name', ''),
                        index_type=row.get('index_type_desc', ''),
                        is_unique=bool(row.get('is_unique', False)),
                        is_primary_key=bool(row.get('is_primary_key', False)),
                        has_filter=bool(row.get('has_filter', False)),
                        index_columns=row.get('index_columns_names', ''),
                        included_columns=row.get('included_columns'),
                        record_count=int(row.get('record_count', 0)),
                        fragmentation_percent=float(row.get('avg_fragmentation_in_percent', 0)),
                        user_lookups=int(row.get('user_lookups', 0)),
                        user_scans=int(row.get('user_scans', 0)),
                        user_seeks=int(row.get('user_seeks', 0)),
                        user_updates=int(row.get('user_updates', 0)),
                        meta_data_age=metadata_age
                    ))
        
        # Parse overlapping indexes
        if 'overlapping_indexes' in data:
            for row in data['overlapping_indexes']:
                if row.get('msg'):  # Only include rows with warnings
                    overlapping_indexes.append(OverlappingIndexInfo(
                        table_name=row.get('table_name', ''),
                        index_name=row.get('index_name', ''),
                        index_columns=row.get('index_columns_names', ''),
                        included_columns=row.get('included_columns_names'),
                        overlap_type=row.get('msg', ''),
                        disable_statement=row.get('disable_stmt', '')
                    ))
        
        # Parse unused indexes
        if 'unused_indexes' in data:
            for row in data['unused_indexes']:
                unused_indexes.append(UnusedIndexInfo(
                    table_name=row.get('table_name', ''),
                    index_name=row.get('index_name', ''),
                    drop_statement=row.get('drop_statement', ''),
                    disable_statement=row.get('disable_statement', ''),
                    user_lookups=int(row.get('user_lookups', 0)),
                    user_scans=int(row.get('user_scans', 0)),
                    user_seeks=int(row.get('user_seeks', 0)),
                    user_updates=int(row.get('user_updates', 0)),
                    space_mb=None  # Will be calculated separately
                ))
        
        # Parse wasted space
        if 'wasted_space' in data and data['wasted_space']:
            comment = data['wasted_space'][0].get('comment', '')
            if 'MB' in comment:
                try:
                    wasted_space_mb = float(comment.split('MB')[0].split()[-1])
                except:
                    wasted_space_mb = 0.0
        
        # Add warnings based on metadata age
        if metadata_age < 14:
            warnings.append(f"⚠️ Metadata is only {metadata_age} days old. For more precise results, data should be at least 14 days old.")
        
        if metadata_age < 100:
            warnings.append(f"⚠️ Metadata is only {metadata_age} days old. Some indexes may only be used every 3-6 months but can have significant performance impact.")
        
        return IndexAnalysisResults(
            missing_indexes=missing_indexes,
            existing_indexes=existing_indexes,
            overlapping_indexes=overlapping_indexes,
            unused_indexes=unused_indexes,
            total_wasted_space_mb=wasted_space_mb,
            metadata_age_days=metadata_age,
            warnings=warnings
        )
    
    def get_index_recommendations(self, results: IndexAnalysisResults, top_n: int = 10) -> List[Dict[str, Any]]:
        """Get top index recommendations based on advantage score"""
        
        recommendations = []
        
        # Sort missing indexes by advantage
        sorted_missing = sorted(results.missing_indexes, 
                              key=lambda x: x.create_index_advantage, 
                              reverse=True)
        
        for i, idx in enumerate(sorted_missing[:top_n]):
            recommendations.append({
                'rank': i + 1,
                'type': 'Missing Index',
                'table_name': idx.table_name,
                'columns': idx.equality_columns or idx.inequality_columns,
                'included_columns': idx.included_columns,
                'advantage_score': idx.create_index_advantage,
                'user_impact': idx.avg_user_impact,
                'user_cost': idx.avg_total_user_cost,
                'usage': f"Scans: {idx.user_scans}, Seeks: {idx.user_seeks}",
                'create_statement': idx.create_index_statement,
                'priority': 'High' if idx.create_index_advantage > 1000 else 'Medium' if idx.create_index_advantage > 100 else 'Low'
            })
        
        return recommendations
    
    def get_maintenance_recommendations(self, results: IndexAnalysisResults) -> List[Dict[str, Any]]:
        """Get index maintenance recommendations"""
        
        maintenance = []
        
        # Unused indexes
        for idx in results.unused_indexes:
            maintenance.append({
                'type': 'Drop Unused Index',
                'table_name': idx.table_name,
                'index_name': idx.index_name,
                'recommendation': idx.drop_statement,
                'alternative': idx.disable_statement,
                'reason': f"Very low usage - Lookups: {idx.user_lookups}, Scans: {idx.user_scans}, Seeks: {idx.user_seeks}, Updates: {idx.user_updates}",
                'priority': 'High'
            })
        
        # Overlapping indexes
        for idx in results.overlapping_indexes:
            maintenance.append({
                'type': 'Remove Overlapping Index',
                'table_name': idx.table_name,
                'index_name': idx.index_name,
                'recommendation': idx.disable_statement,
                'reason': idx.overlap_type,
                'columns': idx.index_columns,
                'priority': 'Medium' if 'WARNING' in idx.overlap_type else 'Low'
            })
        
        # Fragmented indexes
        for idx in results.existing_indexes:
            if idx.fragmentation_percent > 30:
                maintenance.append({
                    'type': 'Rebuild Fragmented Index',
                    'table_name': idx.table_name,
                    'index_name': idx.index_name,
                    'recommendation': f"ALTER INDEX [{idx.index_name}] ON {idx.table_name} REBUILD WITH (DATA_COMPRESSION=PAGE);",
                    'reason': f"High fragmentation: {idx.fragmentation_percent:.1f}%",
                    'priority': 'High' if idx.fragmentation_percent > 50 else 'Medium'
                })
            elif idx.fragmentation_percent > 10:
                maintenance.append({
                    'type': 'Reorganize Fragmented Index',
                    'table_name': idx.table_name,
                    'index_name': idx.index_name,
                    'recommendation': f"ALTER INDEX [{idx.index_name}] ON {idx.table_name} REORGANIZE;",
                    'reason': f"Moderate fragmentation: {idx.fragmentation_percent:.1f}%",
                    'priority': 'Low'
                })
        
        return sorted(maintenance, key=lambda x: {'High': 3, 'Medium': 2, 'Low': 1}[x['priority']], reverse=True)