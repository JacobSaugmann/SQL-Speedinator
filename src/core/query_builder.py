"""
QueryBuilder - Centralized SQL query management
Consolidates common SQL Server queries used across analyzers
"""

import logging
from typing import Dict, Any, Optional


class QueryBuilder:
    """Centralized SQL query builder for SQL Server analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("QueryBuilder initialized")
    
    # ===== Wait Statistics Queries =====
    
    def get_wait_stats_query(self) -> str:
        """Get wait statistics from DMV"""
        return """
        SELECT 
            wait_type,
            wait_time_ms,
            signal_wait_time_ms,
            waiting_tasks_count,
            CAST(100.0 * wait_time_ms / (SELECT SUM(wait_time_ms) FROM sys.dm_os_wait_stats) AS NUMERIC(5,2)) AS pct
        FROM sys.dm_os_wait_stats
        WHERE wait_type NOT IN (
            'DISPATCHER_QUEUE_SEMAPHORE',
            'TRACEWRITE',
            'DBMIRROR_DBM_EVENT',
            'ONDEMAND_TASK_QUEUE',
            'REQUEST_FOR_DEADLOCK_SEARCH',
            'LOGMGR_QUEUE',
            'CHECKPOINT_QUEUE',
            'CLR_AUTO_EVENT',
            'DIRTY_PAGE_TABLE_LOCK'
        )
        ORDER BY wait_time_ms DESC
        """
    
    # ===== Index Queries =====
    
    def get_index_fragmentation_query(self) -> str:
        """Get index fragmentation statistics"""
        return """
        SELECT 
            DB_NAME() as database_name,
            OBJECT_NAME(ips.object_id) as table_name,
            i.name as index_name,
            ps.avg_fragmentation_in_percent as fragmentation_percentage,
            ps.page_count,
            CASE 
                WHEN ps.avg_fragmentation_in_percent < 10 THEN 'Healthy'
                WHEN ps.avg_fragmentation_in_percent BETWEEN 10 AND 30 THEN 'Rebuild'
                ELSE 'Reorganize'
            END as recommended_action
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps
        INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
        WHERE ps.avg_fragmentation_in_percent > 5
        AND ps.page_count > 1000
        ORDER BY ps.avg_fragmentation_in_percent DESC
        """
    
    def get_unused_indexes_query(self) -> str:
        """Get unused indexes"""
        return """
        SELECT 
            OBJECT_NAME(i.object_id) as table_name,
            i.name as index_name,
            ISNULL(s.user_seeks, 0) as seeks,
            ISNULL(s.user_scans, 0) as scans,
            ISNULL(s.user_lookups, 0) as lookups,
            ISNULL(s.user_updates, 0) as updates
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
        WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
        AND i.index_id > 0  -- Exclude heaps
        AND (
            ISNULL(s.user_seeks, 0) = 0 AND
            ISNULL(s.user_scans, 0) = 0 AND
            ISNULL(s.user_lookups, 0) = 0
        )
        ORDER BY ISNULL(s.user_updates, 0) DESC
        """
    
    def get_missing_indexes_query(self) -> str:
        """Get missing index recommendations"""
        return """
        SELECT 
            CONVERT(DECIMAL(18, 2), migs.user_seeks * migs.avg_total_user_cost * (migs.avg_user_impact * 0.01)) AS improvement_measure,
            mid.equality_columns,
            mid.included_columns,
            mid.database_id,
            migs.user_seeks,
            migs.avg_user_impact
        FROM sys.dm_db_missing_index_details mid
        INNER JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
        INNER JOIN sys.dm_db_missing_index_groups_stats migs ON mig.index_group_handle = migs.index_group_handle
        WHERE mid.database_id = DB_ID()
        ORDER BY improvement_measure DESC
        """
    
    # ===== Disk Performance Queries =====
    
    def get_disk_latency_query(self) -> str:
        """Get disk I/O latency statistics"""
        return """
        SELECT 
            mf.physical_name,
            CAST((IOPS.io_stall_read_ms + IOPS.io_stall_write_ms) / (IOPS.num_of_reads + IOPS.num_of_writes) AS NUMERIC(10, 1)) AS AVGLatency,
            CAST(IOPS.num_of_reads AS NUMERIC(10, 0)) AS NumReads,
            CAST(IOPS.num_of_writes AS NUMERIC(10, 0)) AS NumWrites,
            CAST((IOPS.num_of_bytes_written + IOPS.num_of_bytes_read) / 1024.0 / 1024.0 / 1024.0 AS NUMERIC(10, 2)) AS TotalDataTransferredGB
        FROM sys.dm_io_virtual_file_stats(NULL, NULL) IOPS
        INNER JOIN sys.master_files mf ON IOPS.database_id = mf.database_id AND IOPS.file_id = mf.file_id
        ORDER BY AVGLatency DESC
        """
    
    def get_tempdb_size_query(self) -> str:
        """Get tempdb size information"""
        return """
        SELECT 
            mf.name as file_name,
            CAST((mf.size * 8) / 1024.0 / 1024.0 AS NUMERIC(10, 2)) AS size_gb,
            CAST((FILEPROPERTY(mf.name, 'SpaceUsed') * 8) / 1024.0 / 1024.0 AS NUMERIC(10, 2)) AS used_gb
        FROM tempdb.sys.database_files mf
        WHERE mf.type = 0
        """
    
    # ===== Server Configuration Queries =====
    
    def get_server_memory_query(self) -> str:
        """Get server memory configuration"""
        return """
        SELECT 
            CAST(c.value AS INT) as max_server_memory_mb,
            CAST((SELECT SUM(pages_kb) / 1024 FROM sys.dm_os_memory_allocators) AS INT) as current_used_memory_mb
        FROM sys.configurations c
        WHERE c.name = 'max server memory (MB)'
        """
    
    def get_server_info_query(self) -> str:
        """Get basic server information"""
        return """
        SELECT 
            SERVERPROPERTY('ServerName') as server_name,
            SERVERPROPERTY('Edition') as edition,
            SERVERPROPERTY('ProductVersion') as version,
            @@version as version_string
        """
    
    # ===== Database Queries =====
    
    def get_database_list_query(self) -> str:
        """Get list of user databases"""
        return """
        SELECT 
            name,
            state_desc,
            create_date,
            recovery_model_desc,
            CAST(SUM(size) * 8 / 1024.0 / 1024.0 AS NUMERIC(10, 2)) as size_gb
        FROM sys.databases d
        LEFT JOIN sys.master_files mf ON d.database_id = mf.database_id
        WHERE d.database_id > 4  -- Exclude system databases
        AND d.name NOT LIKE 'ReportServer%'
        GROUP BY d.name, d.state_desc, d.create_date, d.recovery_model_desc, d.database_id
        ORDER BY d.name
        """
    
    def get_database_growth_query(self, database_name: str) -> str:
        """Get database growth trend"""
        return f"""
        SELECT 
            CONVERT(DATE, GETDATE()) as snapshot_date,
            SUM(size) * 8 / 1024.0 / 1024.0 as size_gb
        FROM sys.master_files
        WHERE database_id = DB_ID('{database_name}')
        """
    
    # ===== Connection Queries =====
    
    def get_active_connections_query(self) -> str:
        """Get active database connections"""
        return """
        SELECT 
            DB_NAME(database_id) as database_name,
            COUNT(*) as connection_count
        FROM sys.dm_exec_sessions
        WHERE database_id IS NOT NULL AND database_id > 4
        GROUP BY database_id
        ORDER BY connection_count DESC
        """
    
    # ===== Error Log Queries =====
    
    def get_error_log_query(self, hours_back: int = 24) -> str:
        """Get error log entries from SQL Server"""
        return f"""
        EXEC xp_readerrorlog 0, 1
        """  # This requires xp_readerrorlog; handled specially
    
    # ===== Query Performance Queries =====
    
    def get_slow_queries_query(self, min_duration_ms: int = 1000) -> str:
        """Get slow running queries"""
        return f"""
        SELECT TOP 20
            CONVERT(NUMERIC(10, 2), qs.total_elapsed_time / 1000.0 / 1000.0) as total_duration_seconds,
            CONVERT(NUMERIC(10, 2), qs.total_elapsed_time / qs.execution_count / 1000.0 / 1000.0) as avg_duration_seconds,
            qs.execution_count,
            SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 
                ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) 
                  ELSE qs.statement_end_offset END - qs.statement_start_offset) / 2) + 1) AS statement_text
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        WHERE qs.total_elapsed_time / qs.execution_count > {min_duration_ms} * 1000
        ORDER BY qs.total_elapsed_time DESC
        """
    
    # ===== Blocking Queries =====
    
    def get_blocking_query(self) -> str:
        """Get current blocking sessions"""
        return """
        SELECT 
            s1.session_id as blocking_session_id,
            s2.session_id as blocked_session_id,
            DB_NAME(s2.database_id) as database_name,
            r2.command,
            r2.status,
            DATEDIFF(SECOND, s2.login_time, GETDATE()) as session_duration_seconds
        FROM sys.dm_exec_sessions s1
        INNER JOIN sys.dm_exec_sessions s2 ON s1.session_id = s2.blocking_session_id
        LEFT JOIN sys.dm_exec_requests r2 ON s2.session_id = r2.session_id
        """
    
    def validate_query(self, query: str) -> bool:
        """Validate query syntax"""
        # Basic validation
        if not query or len(query.strip()) == 0:
            return False
        return True
    
    def log_query(self, query_name: str, query: str) -> None:
        """Log query for debugging"""
        self.logger.debug(f"Executing query: {query_name}")
        self.logger.debug(f"Query: {query[:100]}...")
