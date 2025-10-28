-- Index Maintenance Recommendations Script
-- Inspired by the great ones and SQL Server community methodologies
-- Provides specific REBUILD/REORGANIZE recommendations with SQL commands

WITH IndexStats AS (
    SELECT
        s.name AS SchemaName,
        t.name AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent,
        ips.page_count,
        ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) AS TotalReads,
        CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) AS SizeMB
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
    INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    LEFT JOIN sys.dm_db_index_usage_stats AS us ON i.object_id = us.object_id 
        AND i.index_id = us.index_id 
        AND us.database_id = DB_ID()
    WHERE ips.avg_fragmentation_in_percent > 30
      AND ips.page_count > 500  -- Only large indexes (>4MB)
      AND i.is_disabled = 0
      AND i.is_hypothetical = 0
    GROUP BY s.name, t.name, i.name, ips.avg_fragmentation_in_percent, 
             ips.page_count, us.user_seeks, us.user_scans, us.user_lookups
)
SELECT
    SchemaName,
    TableName,
    ISNULL(IndexName, '(ALL)') AS IndexName,
    avg_fragmentation_in_percent AS FragmentationPct,
    page_count AS PageCount,
    SizeMB,
    TotalReads,
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 'REBUILD'
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 'REORGANIZE'
        ELSE 'IGNORE'
    END AS ActionRecommendation,
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 'HIGH'
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS Priority,
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN
            CASE WHEN IndexName IS NULL
                THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF, MAXDOP = 1);'
                ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF, MAXDOP = 1);'
            END
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN
            CASE WHEN IndexName IS NULL
                THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
                ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
            END
        ELSE '-- IGNORE: Low usage (' + CAST(TotalReads AS VARCHAR(10)) + ' reads) or acceptable fragmentation'
    END AS SQLCommand,
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 
            'Execute during maintenance window. High fragmentation with heavy usage impacts performance significantly.'
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 
            'Can be executed online during business hours. Moderate fragmentation with regular usage.'
        ELSE 
            'No action needed. Either low usage or acceptable fragmentation level.'
    END AS MaintenanceNotes
FROM IndexStats
ORDER BY
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 1
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 2
        ELSE 3
    END,
    avg_fragmentation_in_percent DESC,
    TotalReads DESC,
    SizeMB DESC;

-- Summary statistics
SELECT 
    COUNT(*) AS TotalIndexes,
    SUM(CASE WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 1 ELSE 0 END) AS NeedRebuild,
    SUM(CASE WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 1 ELSE 0 END) AS NeedReorganize,
    SUM(CASE WHEN avg_fragmentation_in_percent <= 30 OR TotalReads <= 100 THEN 1 ELSE 0 END) AS NoActionNeeded,
    CAST(AVG(avg_fragmentation_in_percent) AS DECIMAL(5,2)) AS AvgFragmentation,
    CAST(SUM(SizeMB) AS DECIMAL(10,2)) AS TotalSizeMB
FROM IndexStats;

-- Generate maintenance script for high priority items
PRINT '-- =================================================';
PRINT '-- HIGH PRIORITY INDEX MAINTENANCE SCRIPT';
PRINT '-- Execute during maintenance window';
PRINT '-- =================================================';
PRINT '';

DECLARE @sql NVARCHAR(MAX);
DECLARE maintenance_cursor CURSOR FOR
SELECT SQLCommand
FROM (
    SELECT 
        CASE
            WHEN ips.avg_fragmentation_in_percent > 80 AND 
                 (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) > 1000 THEN
                CASE WHEN i.name IS NULL
                    THEN 'ALTER INDEX ALL ON [' + s.name + '].[' + t.name + '] REBUILD WITH (ONLINE = OFF, MAXDOP = 1);'
                    ELSE 'ALTER INDEX [' + i.name + '] ON [' + s.name + '].[' + t.name + '] REBUILD WITH (ONLINE = OFF, MAXDOP = 1);'
                END
        END AS SQLCommand
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
    INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN sys.dm_db_index_usage_stats AS us ON i.object_id = us.object_id 
        AND i.index_id = us.index_id 
        AND us.database_id = DB_ID()
    WHERE ips.avg_fragmentation_in_percent > 80
      AND ips.page_count > 500
      AND (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) > 1000
      AND i.is_disabled = 0
      AND i.is_hypothetical = 0
) AS MaintenanceCommands
WHERE SQLCommand IS NOT NULL
ORDER BY SQLCommand;

OPEN maintenance_cursor;
FETCH NEXT FROM maintenance_cursor INTO @sql;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT @sql;
    FETCH NEXT FROM maintenance_cursor INTO @sql;
END;

CLOSE maintenance_cursor;
DEALLOCATE maintenance_cursor;