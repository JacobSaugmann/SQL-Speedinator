-- Wait Statistics Analysis
-- Inspired by the great ones and SQL Server community methodologies
-- This script captures current wait statistics and identifies performance bottlenecks

-- Clear wait stats (only run if needed for baseline)
-- DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR);

-- Top waits by total wait time
WITH Waits AS (
    SELECT 
        wait_type,
        wait_time_ms,
        waiting_tasks_count,
        signal_wait_time_ms,
        max_wait_time_ms,
        CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS wait_percentage
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        -- Filter out irrelevant waits
        'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
        'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
        'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
        'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT',
        'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
        'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        'SP_SERVER_DIAGNOSTICS_SLEEP', 'PREEMPTIVE_XE_GETTARGETSTATE',
        'PREEMPTIVE_XE_DISPATCHER', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION'
    )
    AND wait_time_ms > 0
)
SELECT TOP 20
    wait_type,
    wait_time_ms,
    waiting_tasks_count,
    signal_wait_time_ms,
    max_wait_time_ms,
    wait_percentage,
    CASE 
        WHEN wait_percentage > 10 THEN 'CRITICAL'
        WHEN wait_percentage > 5 THEN 'HIGH'
        WHEN wait_percentage > 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CASE wait_type
        WHEN 'PAGEIOLATCH_SH' THEN 'Data page reads from disk - consider more memory or faster storage'
        WHEN 'PAGEIOLATCH_EX' THEN 'Data page writes - check TempDB contention or storage performance'
        WHEN 'WRITELOG' THEN 'Transaction log writes - move log to faster storage'
        WHEN 'LCK_M_S' THEN 'Shared lock waits - check for blocking or missing indexes'
        WHEN 'LCK_M_X' THEN 'Exclusive lock waits - review blocking and isolation levels'
        WHEN 'CXPACKET' THEN 'Parallel query coordination - review MAXDOP and cost threshold'
        WHEN 'SOS_SCHEDULER_YIELD' THEN 'CPU pressure - check for expensive queries'
        WHEN 'THREADPOOL' THEN 'Worker thread shortage - review connection pooling'
        ELSE 'Review SQL Server documentation for this wait type'
    END as recommendation
FROM Waits
WHERE wait_percentage > 1
ORDER BY wait_time_ms DESC;