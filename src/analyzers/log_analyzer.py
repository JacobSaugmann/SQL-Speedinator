"""
SQL Server and Windows Event Log Analysis Module

This module analyzes SQL Server error logs and Windows event logs to identify
performance-impacting issues over the last 7 days.

Key Features:
- SQL Server Error Log analysis for critical errors (Severity ≥ 16)
- Identification of performance issues (I/O errors, deadlocks, autogrow events)
- Windows Event Log analysis for SQL-related performance problems
- Categorization and severity assessment of log entries
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import re
import subprocess
import json
from pathlib import Path


class LogAnalyzer:
    """Analyzes SQL Server error logs and Windows event logs for performance issues"""
    
    def __init__(self, sql_connection, config):
        """Initialize log analyzer
        
        Args:
            sql_connection: Active SQL Server connection
            config: Configuration object
        """
        self.connection = sql_connection
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Performance-related keywords to search for
        self.performance_keywords = {
            'io_errors': [
                'I/O request', 'disk', 'read error', 'write error', 'timeout',
                'The operating system returned error', 'device error'
            ],
            'deadlocks': [
                'deadlock', 'victim', 'lock timeout', 'blocking'
            ],
            'autogrow': [
                'autogrow', 'growing', 'file growth', 'database growth',
                'log file auto', 'data file auto'
            ],
            'memory': [
                'memory', 'out of memory', 'insufficient memory', 'page fault'
            ],
            'connectivity': [
                'connection failed', 'login failed', 'timeout expired',
                'network error', 'communication link'
            ],
            'corruption': [
                'corruption', 'checksum', 'torn page', 'consistency error'
            ]
        }
        
        # SQL Server severity levels (16+ are critical)
        self.severity_levels = {
            16: 'GENERAL ERROR',
            17: 'INSUFFICIENT RESOURCES',
            18: 'NONFATAL INTERNAL ERROR',
            19: 'FATAL ERROR IN RESOURCE',
            20: 'FATAL ERROR IN CURRENT PROCESS',
            21: 'FATAL ERROR IN DATABASE PROCESS',
            22: 'FATAL ERROR: TABLE INTEGRITY',
            23: 'FATAL ERROR: DATABASE INTEGRITY',
            24: 'FATAL ERROR: HARDWARE ERROR',
            25: 'FATAL ERROR: SYSTEM ERROR'
        }

    def analyze_logs(self) -> Dict[str, Any]:
        """Perform comprehensive log analysis
        
        Returns:
            Dictionary containing log analysis results
        """
        self.logger.info("Starting comprehensive log analysis...")
        
        results = {
            'sql_server_errors': {},
            'windows_events': {},
            'summary': {},
            'recommendations': []
        }
        
        try:
            # Analyze SQL Server Error Log
            self.logger.info("Analyzing SQL Server error logs...")
            results['sql_server_errors'] = self._analyze_sql_server_logs()
            
            # Analyze Windows Event Logs
            self.logger.info("Analyzing Windows event logs...")
            results['windows_events'] = self._analyze_windows_event_logs()
            
            # Generate summary and recommendations
            results['summary'] = self._generate_summary(results)
            results['recommendations'] = self._generate_recommendations(results)
            
            self.logger.info("Log analysis completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Error during log analysis: {str(e)}")
            return {
                'error': str(e),
                'sql_server_errors': {},
                'windows_events': {},
                'summary': {'total_issues': 0},
                'recommendations': []
            }

    def _analyze_sql_server_logs(self) -> Dict[str, Any]:
        """Analyze SQL Server error logs for the last 7 days
        
        Returns:
            Dictionary containing SQL Server log analysis results
        """
        try:
            # Calculate date range (last 7 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            # Read error log entries
            error_entries = self._read_sql_server_error_log(start_date, end_date)
            
            if not error_entries:
                return {
                    'total_entries': 0,
                    'critical_errors': [],
                    'performance_issues': {},
                    'severity_breakdown': {}
                }
            
            # Filter critical errors (Severity >= 16)
            critical_errors = [entry for entry in error_entries if entry.get('severity', 0) >= 16]
            
            # Categorize performance issues
            performance_issues = self._categorize_performance_issues(error_entries)
            
            # Generate severity breakdown
            severity_breakdown = self._generate_severity_breakdown(error_entries)
            
            return {
                'total_entries': len(error_entries),
                'critical_errors': critical_errors,
                'performance_issues': performance_issues,
                'severity_breakdown': severity_breakdown,
                'analysis_period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing SQL Server logs: {str(e)}")
            return {'error': str(e)}

    def _read_sql_server_error_log(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Read SQL Server error log entries for the specified date range
        
        Args:
            start_date: Start date for log analysis
            end_date: End date for log analysis
            
        Returns:
            List of error log entries
        """
        query = """
        DECLARE @StartDate DATETIME = ?
        DECLARE @EndDate DATETIME = ?
        
        CREATE TABLE #ErrorLog (
            LogDate DATETIME,
            ProcessInfo NVARCHAR(50),
            Text NVARCHAR(MAX)
        )
        
        -- Read current error log
        INSERT INTO #ErrorLog
        EXEC xp_readerrorlog 0, 1, NULL, NULL, @StartDate, @EndDate
        
        -- Read previous error log files (up to 6 files back)
        INSERT INTO #ErrorLog
        EXEC xp_readerrorlog 1, 1, NULL, NULL, @StartDate, @EndDate
        
        INSERT INTO #ErrorLog
        EXEC xp_readerrorlog 2, 1, NULL, NULL, @StartDate, @EndDate
        
        INSERT INTO #ErrorLog
        EXEC xp_readerrorlog 3, 1, NULL, NULL, @StartDate, @EndDate
        
        -- Extract severity and error information
        SELECT 
            LogDate,
            ProcessInfo,
            Text,
            -- Extract severity level from text (format: "Error: 1234, Severity: 16, State: 1")
            CASE 
                WHEN Text LIKE '%Severity: %' THEN
                    CAST(SUBSTRING(Text, CHARINDEX('Severity: ', Text) + 10, 2) AS INT)
                ELSE 0
            END AS Severity,
            -- Extract error number
            CASE 
                WHEN Text LIKE '%Error: %' THEN
                    CAST(SUBSTRING(Text, CHARINDEX('Error: ', Text) + 7, 
                         CHARINDEX(',', Text, CHARINDEX('Error: ', Text)) - CHARINDEX('Error: ', Text) - 7) AS INT)
                ELSE 0
            END AS ErrorNumber
        FROM #ErrorLog
        WHERE LogDate BETWEEN @StartDate AND @EndDate
            AND Text NOT LIKE '%Backup%' -- Exclude routine backup messages
            AND Text NOT LIKE '%Log was backed up%'
            AND Text NOT LIKE '%Database backed up%'
        ORDER BY LogDate DESC
        
        DROP TABLE #ErrorLog
        """
        
        try:
            cursor = self.connection.connection.cursor()
            cursor.execute(query, (start_date, end_date))
            
            entries = []
            for row in cursor.fetchall():
                entries.append({
                    'log_date': row.LogDate,
                    'process_info': row.ProcessInfo or '',
                    'text': row.Text or '',
                    'severity': row.Severity or 0,
                    'error_number': row.ErrorNumber or 0
                })
            
            cursor.close()
            return entries
            
        except Exception as e:
            self.logger.error(f"Error reading SQL Server error log: {str(e)}")
            return []

    def _categorize_performance_issues(self, log_entries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Categorize log entries by performance issue type
        
        Args:
            log_entries: List of log entries to categorize
            
        Returns:
            Dictionary with performance issues categorized by type
        """
        categorized = {category: [] for category in self.performance_keywords.keys()}
        
        for entry in log_entries:
            text = entry.get('text', '').lower()
            
            for category, keywords in self.performance_keywords.items():
                if any(keyword.lower() in text for keyword in keywords):
                    categorized[category].append({
                        'log_date': entry['log_date'],
                        'severity': entry['severity'],
                        'error_number': entry['error_number'],
                        'text': entry['text'][:500],  # Truncate for readability
                        'category': category
                    })
                    break  # Assign to first matching category only
        
        # Remove empty categories
        return {k: v for k, v in categorized.items() if v}

    def _generate_severity_breakdown(self, log_entries: List[Dict[str, Any]]) -> Dict[str, int]:
        """Generate breakdown of entries by severity level
        
        Args:
            log_entries: List of log entries
            
        Returns:
            Dictionary with severity breakdown
        """
        breakdown = {}
        
        for entry in log_entries:
            severity = entry.get('severity', 0)
            if severity >= 16:  # Only count critical severities
                severity_name = self.severity_levels.get(severity, f'UNKNOWN ({severity})')
                breakdown[severity_name] = breakdown.get(severity_name, 0) + 1
        
        return breakdown

    def _analyze_windows_event_logs(self) -> Dict[str, Any]:
        """Analyze Windows Event Logs for SQL-related performance issues
        
        Returns:
            Dictionary containing Windows event log analysis results
        """
        try:
            # Calculate date range (last 7 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            # PowerShell script to read Windows Event Logs
            ps_script = f'''
            $StartDate = (Get-Date).AddDays(-7)
            $EndDate = Get-Date
            
            # SQL Server related event IDs and sources
            $SqlSources = @(
                "MSSQLSERVER", "SQLSERVERAGENT", "SQLBrowser",
                "Disk", "ntfs", "volsnap", "storport"
            )
            
            $Events = @()
            
            # System Log - Disk and storage errors
            $SystemEvents = Get-WinEvent -FilterHashtable @{{
                LogName = 'System'
                StartTime = $StartDate
                EndTime = $EndDate
                Level = 1,2,3  # Critical, Error, Warning
            }} -ErrorAction SilentlyContinue | Where-Object {{
                $_.ProviderName -in $SqlSources -or
                $_.Message -match "SQL|Database|Disk|Storage|I/O|timeout"
            }}
            
            # Application Log - SQL Server events
            $AppEvents = Get-WinEvent -FilterHashtable @{{
                LogName = 'Application'
                StartTime = $StartDate
                EndTime = $EndDate
                Level = 1,2,3  # Critical, Error, Warning
            }} -ErrorAction SilentlyContinue | Where-Object {{
                $_.ProviderName -match "SQL|MSSQL" -or
                $_.Message -match "SQL Server|Database|Performance|Timeout"
            }}
            
            $AllEvents = @($SystemEvents) + @($AppEvents)
            
            # Convert to JSON for easier parsing
            $EventsJson = @()
            foreach ($Event in $AllEvents) {{
                $EventsJson += @{{
                    TimeCreated = $Event.TimeCreated.ToString("yyyy-MM-ddTHH:mm:ss")
                    Id = $Event.Id
                    Level = $Event.Level
                    LevelDisplayName = $Event.LevelDisplayName
                    ProviderName = $Event.ProviderName
                    LogName = $Event.LogName
                    Message = $Event.Message.Substring(0, [Math]::Min(1000, $Event.Message.Length))
                }}
            }}
            
            $EventsJson | ConvertTo-Json -Depth 3
            '''
            
            # Execute PowerShell script
            result = subprocess.run(
                ["powershell", "-Command", ps_script],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore',
                timeout=60
            )
            
            if result.returncode != 0:
                self.logger.warning(f"PowerShell event log query failed: {result.stderr}")
                return {'error': 'Failed to read Windows Event Logs', 'events': []}
            
            # Parse JSON results
            if result.stdout.strip():
                events = json.loads(result.stdout)
                if not isinstance(events, list):
                    events = [events] if events else []
            else:
                events = []
            
            # Categorize events by type and impact
            categorized_events = self._categorize_windows_events(events)
            
            return {
                'total_events': len(events),
                'categorized_events': categorized_events,
                'analysis_period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing Windows event logs: {str(e)}")
            return {'error': str(e), 'events': []}

    def _categorize_windows_events(self, events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Categorize Windows events by type and performance impact
        
        Args:
            events: List of Windows events
            
        Returns:
            Dictionary with events categorized by type
        """
        categories = {
            'disk_errors': [],
            'storage_warnings': [],
            'sql_service_issues': [],
            'performance_warnings': [],
            'other': []
        }
        
        for event in events:
            message = event.get('Message', '').lower()
            provider = event.get('ProviderName', '').lower()
            event_id = event.get('Id', 0)
            
            # Categorize based on content
            if any(keyword in message for keyword in ['disk', 'storage', 'i/o', 'read error', 'write error']):
                categories['disk_errors'].append(event)
            elif any(keyword in message for keyword in ['timeout', 'slow', 'latency', 'performance']):
                categories['performance_warnings'].append(event)
            elif any(keyword in provider for keyword in ['sql', 'mssql']):
                categories['sql_service_issues'].append(event)
            elif any(keyword in message for keyword in ['warning', 'caution']):
                categories['storage_warnings'].append(event)
            else:
                categories['other'].append(event)
        
        # Remove empty categories
        return {k: v for k, v in categories.items() if v}

    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics for log analysis
        
        Args:
            results: Log analysis results
            
        Returns:
            Summary statistics
        """
        sql_errors = results.get('sql_server_errors', {})
        windows_events = results.get('windows_events', {})
        
        return {
            'total_sql_entries': sql_errors.get('total_entries', 0),
            'critical_sql_errors': len(sql_errors.get('critical_errors', [])),
            'performance_issue_categories': len(sql_errors.get('performance_issues', {})),
            'total_windows_events': windows_events.get('total_events', 0),
            'windows_categories': len(windows_events.get('categorized_events', {})),
            'analysis_period_days': 7
        }

    def _generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on log analysis
        
        Args:
            results: Log analysis results
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        sql_errors = results.get('sql_server_errors', {})
        windows_events = results.get('windows_events', {})
        
        # SQL Server recommendations
        critical_count = len(sql_errors.get('critical_errors', []))
        if critical_count > 0:
            recommendations.append(f"🔴 Found {critical_count} critical SQL Server errors (Severity ≥ 16). Immediate investigation required.")
        
        performance_issues = sql_errors.get('performance_issues', {})
        
        if 'deadlocks' in performance_issues:
            count = len(performance_issues['deadlocks'])
            recommendations.append(f"⚠️ Detected {count} deadlock-related entries. Review application logic and indexing strategy.")
        
        if 'io_errors' in performance_issues:
            count = len(performance_issues['io_errors'])
            recommendations.append(f"💾 Found {count} I/O error entries. Check disk subsystem health and storage performance.")
        
        if 'autogrow' in performance_issues:
            count = len(performance_issues['autogrow'])
            recommendations.append(f"📈 Detected {count} autogrow events. Consider pre-sizing databases to avoid performance impact.")
        
        # Windows Event recommendations
        categorized = windows_events.get('categorized_events', {})
        
        if 'disk_errors' in categorized:
            count = len(categorized['disk_errors'])
            recommendations.append(f"🚨 Found {count} disk/storage errors in Windows Event Log. Check hardware health immediately.")
        
        if 'performance_warnings' in categorized:
            count = len(categorized['performance_warnings'])
            recommendations.append(f"⏱️ Detected {count} performance warnings. Monitor system resources and workload patterns.")
        
        # General recommendations
        if not recommendations:
            recommendations.append("✅ No critical performance issues detected in recent logs. Continue regular monitoring.")
        else:
            recommendations.insert(0, "📋 Log Analysis Summary - Actions Required:")
        
        return recommendations