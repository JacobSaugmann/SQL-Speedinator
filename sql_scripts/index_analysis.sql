-- Index Analysis Script
-- Analyzes index fragmentation, usage, and identifies optimization opportunities
-- Inspired by the great ones and SQL Server community methodologies

-- Index fragmentation analysis
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
    END AS recommended_action,
    CASE 
        WHEN ps.avg_fragmentation_in_percent > 30 THEN 'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id) + '].[' + OBJECT_NAME(ps.object_id, ps.database_id) + '] REBUILD;'
        WHEN ps.avg_fragmentation_in_percent > 10 THEN 'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id) + '].[' + OBJECT_NAME(ps.object_id, ps.database_id) + '] REORGANIZE;'
        ELSE NULL
    END AS maintenance_command
FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'LIMITED') ps
INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
WHERE ps.avg_fragmentation_in_percent > 10
AND ps.page_count > 128  -- Minimum 1MB size
AND i.is_disabled = 0
AND i.is_hypothetical = 0
AND ps.database_id = DB_ID()
ORDER BY ps.avg_fragmentation_in_percent DESC, ps.page_count DESC;

-- Index usage statistics
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
    CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb,
    CASE 
        WHEN (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) = 0 THEN 'UNUSED'
        WHEN us.user_updates > (us.user_seeks + us.user_scans + us.user_lookups) * 5 THEN 'OVER_UPDATED'
        WHEN us.user_seeks > 1000 THEN 'HIGHLY_USED'
        WHEN us.user_seeks > 100 THEN 'MODERATELY_USED'
        ELSE 'LIGHTLY_USED'
    END AS usage_pattern,
    -- Columns in index
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
LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id AND us.database_id = DB_ID()
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE i.type > 0  -- Exclude heaps
AND i.is_disabled = 0
AND i.is_hypothetical = 0
AND OBJECT_SCHEMA_NAME(i.object_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
GROUP BY i.object_id, i.index_id, i.name, i.type_desc, i.is_unique, 
         i.is_primary_key, i.is_unique_constraint, us.user_seeks, 
         us.user_scans, us.user_lookups, us.user_updates, 
         us.last_user_seek, us.last_user_scan, us.last_user_lookup, 
         us.last_user_update, p.rows
ORDER BY (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) DESC;

-- Duplicate/overlapping indexes
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
ORDER BY OBJECT_NAME(ic1.object_id), ic1.index_name;