-- Disk I/O Performance Analysis
-- Inspired by the great ones and SQL Server community methodologies
-- Analyzes disk performance from SQL Server perspective

-- File I/O statistics
SELECT 
    DB_NAME(vfs.database_id) AS database_name,
    mf.physical_name,
    mf.type_desc as file_type,
    vfs.sample_ms,
    vfs.num_of_reads,
    vfs.num_of_bytes_read,
    vfs.io_stall_read_ms,
    vfs.num_of_writes, 
    vfs.num_of_bytes_written,
    vfs.io_stall_write_ms,
    vfs.io_stall,
    vfs.size_on_disk_bytes / 1024 / 1024 AS size_on_disk_mb,
    CASE 
        WHEN vfs.num_of_reads = 0 THEN 0 
        ELSE CAST(vfs.io_stall_read_ms AS FLOAT) / vfs.num_of_reads 
    END AS avg_read_latency_ms,
    CASE 
        WHEN vfs.num_of_writes = 0 THEN 0 
        ELSE CAST(vfs.io_stall_write_ms AS FLOAT) / vfs.num_of_writes 
    END AS avg_write_latency_ms,
    CASE 
        WHEN (vfs.num_of_reads + vfs.num_of_writes) = 0 THEN 0 
        ELSE CAST(vfs.io_stall AS FLOAT) / (vfs.num_of_reads + vfs.num_of_writes) 
    END AS avg_io_latency_ms,
    LEFT(mf.physical_name, 1) AS drive_letter,
    -- Performance assessment
    CASE 
        WHEN mf.type_desc = 'LOG' AND CAST(vfs.io_stall_write_ms AS FLOAT) / NULLIF(vfs.num_of_writes, 0) > 5 
            THEN 'LOG_WRITE_SLOW'
        WHEN CAST(vfs.io_stall_read_ms AS FLOAT) / NULLIF(vfs.num_of_reads, 0) > 20 
            THEN 'READ_SLOW'
        WHEN CAST(vfs.io_stall_write_ms AS FLOAT) / NULLIF(vfs.num_of_writes, 0) > 10 
            THEN 'WRITE_SLOW'
        ELSE 'OK'
    END AS performance_status
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
INNER JOIN sys.master_files mf ON vfs.database_id = mf.database_id 
    AND vfs.file_id = mf.file_id
WHERE vfs.num_of_reads > 0 OR vfs.num_of_writes > 0
ORDER BY vfs.io_stall DESC;

-- Database file configuration
SELECT 
    DB_NAME(database_id) AS database_name,
    name AS file_name,
    physical_name,
    type_desc,
    state_desc,
    size * 8 / 1024 AS size_mb,
    max_size,
    growth,
    is_percent_growth,
    CASE 
        WHEN max_size = -1 THEN 'UNLIMITED'
        WHEN max_size = 0 THEN 'NO GROWTH'
        ELSE CAST(max_size * 8 / 1024 AS VARCHAR) + ' MB'
    END AS max_size_desc,
    CASE
        WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR) + '%'
        ELSE CAST(growth * 8 / 1024 AS VARCHAR) + ' MB'
    END AS growth_desc,
    -- Configuration issues
    CASE 
        WHEN is_percent_growth = 1 THEN 'Consider fixed MB growth instead of percentage'
        WHEN max_size = -1 THEN 'Consider setting maximum size limit'
        WHEN growth = 0 THEN 'Growth is disabled'
        ELSE 'OK'
    END AS configuration_recommendation
FROM sys.master_files
WHERE database_id > 4  -- Exclude system databases
ORDER BY database_id, type_desc, name;

-- Pending I/O requests
SELECT 
    r.session_id,
    r.request_id,
    r.command,
    r.wait_type,
    r.wait_time,
    r.wait_resource,
    r.blocking_session_id,
    DB_NAME(r.database_id) AS database_name,
    s.login_name,
    s.host_name,
    s.program_name
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
WHERE r.wait_type LIKE 'PAGEIOLATCH%'
   OR r.wait_type LIKE 'WRITELOG%'
   OR r.wait_type LIKE 'LOGMGR%'
ORDER BY r.wait_time DESC;