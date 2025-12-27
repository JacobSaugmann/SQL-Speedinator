"""
Azure OpenAI Service for SQL Server Performance Analysis
Provides AI-powered analysis and recommendations using Azure OpenAI GPT models
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from openai import AzureOpenAI
try:
    from ..core.config_manager import ConfigManager
    from ..reports.text_formatter import TextFormatter
except ImportError:
    from core.config_manager import ConfigManager
    from reports.text_formatter import TextFormatter

class AIService:
    """Azure OpenAI service for performance analysis insights"""
    
    def __init__(self, config: ConfigManager):
        """Initialize AI service with configuration
        
        Args:
            config: Configuration manager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.text_formatter = TextFormatter()
        self.client = None
        
        if config.be_my_copilot:
            if not config.validate_ai_config():
                self.logger.warning("Invalid AI configuration. Disabling AI analysis.")
                self.client = None
                return
            
            try:
                # Import here to catch any import errors
                import httpx
                
                # Clear all proxy-related environment variables
                import os
                proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'REQUESTS_CA_BUNDLE']
                saved_vars = {}
                for var in proxy_vars:
                    if var in os.environ:
                        saved_vars[var] = os.environ[var]
                        del os.environ[var]
                
                try:
                    # Try basic initialization first
                    self.client = AzureOpenAI(
                        api_key=config.azure_openai_api_key,
                        api_version=config.azure_openai_api_version,
                        azure_endpoint=config.azure_openai_endpoint
                    )
                    self.logger.info("Azure OpenAI client initialized")
                    
                except Exception as e:
                    self.logger.debug(f"Basic initialization failed ({str(e)}), trying with custom HTTP client...")
                    
                    # Create custom HTTP client without proxy settings
                    custom_client = httpx.Client(
                        timeout=httpx.Timeout(60.0),
                        limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
                    )
                    
                    # Initialize with custom client
                    self.client = AzureOpenAI(
                        api_key=config.azure_openai_api_key,
                        api_version=config.azure_openai_api_version,
                        azure_endpoint=config.azure_openai_endpoint,
                        http_client=custom_client
                    )
                    self.logger.info("Azure OpenAI client initialized with custom HTTP client")
                
                # Restore environment variables
                for var, value in saved_vars.items():
                    os.environ[var] = value
                    
            except Exception as e:
                self.logger.error(f"Failed to initialize Azure OpenAI client: {str(e)}")
                self.client = None
    
    def _clean_html_tags(self, text: str) -> str:
        """Clean malformed HTML tags like '>green>' from AI responses
        
        Delegates to TextFormatter for centralized HTML tag cleaning.
        """
        return self.text_formatter.clean_html_tags(text)
    
    def _clean_dict_recursively(self, obj):
        """Recursively clean HTML tags in dictionary/list structures
        
        Delegates to TextFormatter for centralized recursive cleaning.
        """
        return self.text_formatter.clean_dict_recursively(obj)
    
    def is_enabled(self) -> bool:
        """Check if AI service is enabled and configured"""
        return self.config.be_my_copilot and self.client is not None
    
    def analyze_performance_summary(self, performance_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze performance data and provide AI recommendations
        
        Args:
            performance_summary: Condensed performance analysis data
            
        Returns:
            Dict containing AI analysis and recommendations, or None if disabled/failed
        """
        if not self.is_enabled():
            self.logger.info("AI Copilot not enabled or not configured")
            return None
        
        try:
            # Create efficient prompt to minimize tokens
            prompt = self._create_analysis_prompt(performance_summary)
            
            self.logger.info("Sending performance data to Azure OpenAI for analysis")
            
            response = self.client.chat.completions.create(
                model=self.config.azure_openai_deployment,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert SQL Server performance analyst. Analyze the provided performance data and identify the TOP 3 bottlenecks with specific, actionable recommendations. Be concise and focus on highest impact issues. Format your response as JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=self.config.ai_max_tokens,
                temperature=self.config.ai_temperature,
                response_format={"type": "json_object"}
            )
            
            ai_response = response.choices[0].message.content
            self.logger.info("Received AI analysis response")
            
            # Parse and structure the response
            analysis_result = json.loads(ai_response)
            
            # Clean malformed HTML tags in the analysis result
            analysis_result = self._clean_dict_recursively(analysis_result)
            
            return {
                'ai_enabled': True,
                'model_used': self.config.azure_openai_model,
                'analysis': analysis_result,
                'tokens_used': response.usage.total_tokens if response.usage else 0,
                'generated_at': None  # Will be set by caller
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse AI response as JSON: {e}")
            # Fallback to text response
            return {
                'ai_enabled': True,
                'model_used': self.config.azure_openai_model,
                'analysis': {'recommendations': [ai_response]},
                'tokens_used': response.usage.total_tokens if response.usage else 0,
                'generated_at': None
            }
            
        except Exception as e:
            self.logger.error(f"AI analysis failed: {e}")
            return {
                'ai_enabled': True,
                'model_used': self.config.azure_openai_model,
                'analysis': {'error': f"AI analysis failed: {str(e)}"},
                'tokens_used': 0,
                'generated_at': None
            }
    
    def _create_analysis_prompt(self, performance_summary: Dict[str, Any]) -> str:
        """Create an efficient prompt for AI analysis to minimize token usage
        
        Args:
            performance_summary: Performance data summary
            
        Returns:
            Formatted prompt string
        """
        prompt_parts = []
        
        # Build sections of the prompt
        prompt_parts.append(self._format_server_info_section(performance_summary))
        prompt_parts.append(self._format_wait_stats_section(performance_summary))
        prompt_parts.append(self._format_disk_and_index_section(performance_summary))
        prompt_parts.append(self._format_config_and_cache_section(performance_summary))
        
        # Filter out empty sections
        prompt_parts = [p for p in prompt_parts if p]
        
        if not prompt_parts:
            prompt_parts.append("No significant performance issues detected in summary data")
        
        prompt = "SQL Server Performance Analysis:\n" + "\n".join(prompt_parts)
        prompt += "\n\nProvide JSON response with: {'bottlenecks': [{'issue': 'description', 'impact': 'HIGH/MEDIUM/LOW', 'recommendation': 'specific action'}], 'summary': 'overall assessment'}"
        
        return prompt
    
    def _format_server_info_section(self, performance_summary: Dict[str, Any]) -> str:
        """Format server information section of prompt"""
        server_info = performance_summary.get('server_info', {})
        if not server_info:
            return ""
        return f"Server: {server_info.get('edition', 'Unknown')} {server_info.get('version', '')}, {server_info.get('cpu_count', 'N/A')} CPUs, {server_info.get('total_memory_mb', 'N/A')}MB RAM"
    
    def _format_wait_stats_section(self, performance_summary: Dict[str, Any]) -> str:
        """Format wait statistics section of prompt"""
        wait_stats = performance_summary.get('wait_stats', {})
        if not wait_stats or not wait_stats.get('top_waits'):
            return ""
        top_waits = wait_stats['top_waits'][:5]
        wait_list = [f"{w['wait_type']}({w['percentage']:.1f}%)" for w in top_waits]
        return f"Top waits: {', '.join(wait_list)}"
    
    def _format_disk_and_index_section(self, performance_summary: Dict[str, Any]) -> str:
        """Format disk performance and index issues section"""
        sections = []
        
        # Disk issues
        disk_issues = performance_summary.get('disk_issues', [])
        if disk_issues:
            critical = [i for i in disk_issues if i.get('severity') == 'HIGH'][:3]
            if critical:
                disk_list = [f"{issue['database']}({issue['issue']})" for issue in critical]
                sections.append(f"Disk issues: {', '.join(disk_list)}")
        
        # Index problems
        index_issues = performance_summary.get('index_issues', {})
        if index_issues:
            if index_issues.get('high_fragmentation_count', 0) > 0:
                sections.append(f"High fragmentation: {index_issues['high_fragmentation_count']} indexes")
            if index_issues.get('unused_count', 0) > 0:
                sections.append(f"Unused indexes: {index_issues['unused_count']}")
            if index_issues.get('missing_high_impact', 0) > 0:
                sections.append(f"Missing high-impact indexes: {index_issues['missing_high_impact']}")
        
        return "\n".join(sections)
    
    def _format_config_and_cache_section(self, performance_summary: Dict[str, Any]) -> str:
        """Format configuration and plan cache section"""
        sections = []
        
        # Config issues
        config_issues = performance_summary.get('config_issues', [])
        if config_issues:
            critical = [i for i in config_issues if i.get('severity') == 'HIGH'][:3]
            if critical:
                config_list = [f"{issue['setting']}({issue['issue']})" for issue in critical]
                sections.append(f"Config issues: {', '.join(config_list)}")
        
        # TempDB issues
        tempdb_issues = performance_summary.get('tempdb_issues', [])
        if tempdb_issues:
            critical = [i for i in tempdb_issues if i.get('severity') == 'HIGH'][:2]
            if critical:
                tempdb_list = [issue['description'] for issue in critical]
                sections.append(f"TempDB: {', '.join(tempdb_list)}")
        
        # Plan cache efficiency
        plan_cache = performance_summary.get('plan_cache', {})
        if plan_cache and plan_cache.get('single_use_pct', 0) > 10:
            sections.append(f"Plan cache: {plan_cache['single_use_pct']:.1f}% single-use plans")
        
        return "\n".join(sections)
    
    def analyze_perfmon_bottlenecks(self, perfmon_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze Performance Monitor data and provide AI-powered bottleneck insights
        
        Args:
            perfmon_data: Performance Monitor analysis results
            
        Returns:
            Dict containing AI analysis of Performance Monitor bottlenecks, or None if disabled/failed
        """
        if not self.is_enabled():
            self.logger.info("AI Copilot not enabled or not configured")
            return None
        
        try:
            # Create PerfMon-specific analysis prompt
            prompt = self._create_perfmon_analysis_prompt(perfmon_data)
            
            self.logger.info("Sending Performance Monitor data to Azure OpenAI for bottleneck analysis")
            
            response = self.client.chat.completions.create(
                model=self.config.azure_openai_deployment,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert Windows Performance Monitor and SQL Server performance analyst. Analyze the provided performance counter data to identify system bottlenecks and their root causes. Focus on CPU, memory, disk I/O, and SQL Server specific metrics. Provide specific, actionable recommendations. Format your response as JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=1500,
                temperature=0.1
            )
            
            # Parse the AI response
            ai_content = response.choices[0].message.content.strip()
            self.logger.info("Received AI analysis for Performance Monitor data")
            
            # Try to parse as JSON, fallback to text if needed
            try:
                ai_analysis = json.loads(ai_content)
                # Clean malformed HTML tags in the analysis result
                ai_analysis = self._clean_dict_recursively(ai_analysis)
            except json.JSONDecodeError:
                # If JSON parsing fails, create structured response from text
                cleaned_content = self._clean_html_tags(ai_content)
                ai_analysis = {
                    "analysis_type": "perfmon_bottlenecks",
                    "raw_response": cleaned_content,
                    "summary": "AI analysis completed - see raw response for details",
                    "bottlenecks": [],
                    "recommendations": cleaned_content.split('\n') if '\n' in cleaned_content else [cleaned_content]
                }
            
            ai_analysis["analysis_type"] = "perfmon_bottlenecks"
            from datetime import datetime
            ai_analysis["timestamp"] = datetime.now().isoformat()
            
            return ai_analysis
            
        except Exception as e:
            self.logger.error(f"Error in AI Performance Monitor analysis: {str(e)}")
            return None
    
    def _create_perfmon_analysis_prompt(self, perfmon_data: Dict[str, Any]) -> str:
        """Create focused prompt for Performance Monitor analysis"""
        prompt_parts = []
        
        # Build sections
        prompt_parts.append(self._format_perfmon_collection_section(perfmon_data))
        prompt_parts.append(self._format_perfmon_resource_sections(perfmon_data))
        prompt_parts.append(self._format_perfmon_bottlenecks_section(perfmon_data))
        
        # Filter empty sections
        prompt_parts = [p for p in prompt_parts if p]
        
        if not prompt_parts:
            prompt_parts.append("No significant performance metrics available in Performance Monitor data")
        
        prompt = "Windows Performance Monitor Analysis:\n" + "\n".join(prompt_parts)
        prompt += "\n\nAnalyze these Performance Monitor metrics and identify:\n"
        prompt += "1. Root cause analysis of performance bottlenecks\n"
        prompt += "2. Correlation between different metrics (CPU, Memory, Disk, SQL Server)\n"
        prompt += "3. Specific recommendations for each bottleneck\n"
        prompt += "4. Priority order for addressing issues\n\n"
        prompt += "Provide JSON response with: {'bottlenecks': [{'component': 'CPU/Memory/Disk/SQL', 'severity': 'CRITICAL/WARNING/INFO', 'root_cause': 'analysis', 'recommendation': 'specific action', 'priority': 1-10}], 'correlation_analysis': 'cross-component analysis', 'summary': 'overall assessment'}"
        
        return prompt
    
    def _format_perfmon_collection_section(self, perfmon_data: Dict[str, Any]) -> str:
        """Format collection metadata section of PerfMon prompt"""
        summary = perfmon_data.get('summary', {})
        if not summary:
            return ""
        return f"Collection: {summary.get('duration_minutes', 0):.1f} minutes, {summary.get('total_counters', 0)} counters"
    
    def _format_perfmon_resource_sections(self, perfmon_data: Dict[str, Any]) -> str:
        """Format CPU, memory, disk, and SQL Server resource sections"""
        sections = []
        
        # CPU metrics
        cpu = perfmon_data.get('cpu_analysis', {})
        if 'metrics' in cpu:
            metrics = cpu['metrics']
            status = cpu.get('status', 'OK')
            if 'avg_processor_time' in metrics:
                sections.append(f"CPU: {metrics['avg_processor_time']}% avg usage ({status})")
            if 'avg_processor_queue' in metrics:
                sections.append(f"CPU Queue: {metrics['avg_processor_queue']} avg length")
        
        # Memory metrics
        memory = perfmon_data.get('memory_analysis', {})
        if 'metrics' in memory:
            metrics = memory['metrics']
            status = memory.get('status', 'OK')
            if 'avg_available_mb' in metrics:
                sections.append(f"Memory: {metrics['avg_available_mb']:,.0f} MB avg available ({status})")
            if 'avg_page_life_expectancy' in metrics:
                sections.append(f"Page Life Expectancy: {metrics['avg_page_life_expectancy']:,.0f} seconds")
        
        # Disk metrics
        disk = perfmon_data.get('disk_analysis', {})
        if 'metrics' in disk:
            metrics = disk['metrics']
            status = disk.get('status', 'OK')
            if 'avg_disk_queue_length' in metrics:
                sections.append(f"Disk Queue: {metrics['avg_disk_queue_length']} avg length ({status})")
            if 'avg_disk_read_ms' in metrics:
                sections.append(f"Disk Latency: {metrics['avg_disk_read_ms']} ms avg read")
        
        # SQL Server metrics
        sql = perfmon_data.get('sql_server_analysis', {})
        if 'metrics' in sql:
            metrics = sql['metrics']
            status = sql.get('status', 'OK')
            if 'avg_batch_requests_per_sec' in metrics:
                sections.append(f"SQL Batches/sec: {metrics['avg_batch_requests_per_sec']} ({status})")
            if 'avg_compilations_per_sec' in metrics:
                sections.append(f"SQL Compilations/sec: {metrics['avg_compilations_per_sec']}")
            if 'avg_lock_waits_per_sec' in metrics:
                sections.append(f"Lock Waits/sec: {metrics['avg_lock_waits_per_sec']}")
        
        return "\n".join(sections)
    
    def _format_perfmon_bottlenecks_section(self, perfmon_data: Dict[str, Any]) -> str:
        """Format detected bottlenecks section of PerfMon prompt"""
        if not perfmon_data.get('bottlenecks'):
            return ""
        bottleneck_list = []
        for bottleneck in perfmon_data['bottlenecks']:
            severity = bottleneck.get('severity', 'UNKNOWN')
            category = bottleneck.get('category', 'Unknown')
            description = bottleneck.get('description', 'No description')
            bottleneck_list.append(f"{severity} {category}: {description}")
        return f"Detected Bottlenecks: {'; '.join(bottleneck_list)}"

    def analyze_log_entries(self, log_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze log entries using Azure OpenAI
        
        Args:
            log_data: Log analysis results from LogAnalyzer
            
        Returns:
            AI analysis of log entries or None if analysis fails
        """
        try:
            self.logger.info("Sending log data to Azure OpenAI for analysis")
            
            # Create focused prompt for log analysis
            prompt = self._create_log_analysis_prompt(log_data)
            
            response = self.client.chat.completions.create(
                model=self.config.azure_openai_deployment,
                messages=[
                    {
                        "role": "system", 
                        "content": """You are a SQL Server performance expert analyzing log files. 
                        Focus on identifying performance bottlenecks, security issues, and operational problems 
                        from SQL Server error logs and Windows event logs. Provide actionable recommendations
                        with priority levels and impact assessment."""
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500,
                temperature=0.3
            )
            
            content = response.choices[0].message.content
            self.logger.info("Received AI analysis for log data")
            
            # Try to parse as JSON, fallback to text analysis
            try:
                import json
                return json.loads(content)
            except:
                # If JSON parsing fails, create structured response
                return {
                    'analysis': content,
                    'recommendations': self._extract_recommendations_from_text(content or ''),
                    'summary': 'AI analysis completed - see detailed analysis for findings'
                }
                
        except Exception as e:
            self.logger.error(f"Error in AI log analysis: {str(e)}")
            return None

    def _create_log_analysis_prompt(self, log_data: Dict[str, Any]) -> str:
        """Create focused prompt for log analysis
        
        Args:
            log_data: Log analysis results
            
        Returns:
            Formatted prompt for AI analysis
        """
        prompt_parts = []
        
        # Build sections
        prompt_parts.append(self._format_log_summary_section(log_data))
        prompt_parts.append(self._format_sql_server_errors_section(log_data))
        prompt_parts.append(self._format_windows_events_section(log_data))
        prompt_parts.append(self._format_log_recommendations_section(log_data))
        
        # Filter empty sections
        prompt_parts = [p for p in prompt_parts if p]
        
        if not prompt_parts:
            prompt_parts.append("No significant log issues detected in the analysis period")
        
        prompt = "SQL Server and Windows Log Analysis:\n" + "\n".join(prompt_parts)
        prompt += "\n\nAnalyze these log findings and provide:\n"
        prompt += "1. Root cause analysis of critical errors and performance issues\n"
        prompt += "2. Risk assessment for each category of issues\n"
        prompt += "3. Specific remediation steps with priority levels (1-10)\n"
        prompt += "4. Preventive measures to avoid future occurrences\n"
        prompt += "5. Correlation between SQL Server and Windows events\n\n"
        prompt += "Provide JSON response with: {'critical_findings': [{'issue': 'description', 'severity': 'CRITICAL/HIGH/MEDIUM/LOW', 'root_cause': 'analysis', 'remediation': 'specific steps', 'priority': 1-10, 'prevention': 'preventive measures'}], 'correlation_analysis': 'relationship between different log entries', 'risk_assessment': 'overall risk evaluation', 'summary': 'executive summary'}"
        
        return prompt
    
    def _format_log_summary_section(self, log_data: Dict[str, Any]) -> str:
        """Format analysis period and scope summary section"""
        summary = log_data.get('summary', {})
        if not summary:
            return ""
        sections = [
            f"Analysis Period: {summary.get('analysis_period_days', 7)} days",
            f"SQL Server Entries: {summary.get('total_sql_entries', 0):,}",
            f"Critical SQL Errors: {summary.get('critical_sql_errors', 0):,}",
            f"Windows Events: {summary.get('total_windows_events', 0):,}"
        ]
        return "\n".join(sections)
    
    def _format_sql_server_errors_section(self, log_data: Dict[str, Any]) -> str:
        """Format SQL Server error analysis section"""
        sql_errors = log_data.get('sql_server_errors', {})
        if not sql_errors:
            return ""
        sections = []
        
        # Critical errors
        critical_errors = sql_errors.get('critical_errors', [])
        if critical_errors:
            sections.append(f"\nCritical SQL Server Errors ({len(critical_errors)} found):")
            for error in critical_errors[:5]:
                severity = error.get('severity', 0)
                error_num = error.get('error_number', 0)
                text = error.get('text', '')[:200]
                sections.append(f"- Severity {severity}, Error {error_num}: {text}")
        
        # Performance issues
        performance_issues = sql_errors.get('performance_issues', {})
        if performance_issues:
            sections.append("\nPerformance Issues Detected:")
            for issue_type, issues in performance_issues.items():
                if issues:
                    sections.append(f"- {issue_type.replace('_', ' ').title()}: {len(issues)} occurrences")
                    latest = max(issues, key=lambda x: x.get('log_date', datetime.min))
                    sample_text = latest.get('text', '')[:150]
                    sections.append(f"  Sample: {sample_text}")
        
        # Severity breakdown
        severity_breakdown = sql_errors.get('severity_breakdown', {})
        if severity_breakdown:
            sections.append("\nSeverity Breakdown:")
            for severity, count in severity_breakdown.items():
                sections.append(f"- {severity}: {count} occurrences")
        
        return "\n".join(sections)
    
    def _format_windows_events_section(self, log_data: Dict[str, Any]) -> str:
        """Format Windows event log analysis section"""
        windows_events = log_data.get('windows_events', {})
        categorized = windows_events.get('categorized_events', {})
        if not categorized:
            return ""
        sections = ["\nWindows Event Log Issues:"]
        for category, events in categorized.items():
            if events:
                sections.append(f"- {category.replace('_', ' ').title()}: {len(events)} events")
                sample_event = events[0]
                sample_message = sample_event.get('Message', '')[:100]
                sections.append(f"  Sample: {sample_message}")
        return "\n".join(sections)
    
    def _format_log_recommendations_section(self, log_data: Dict[str, Any]) -> str:
        """Format existing recommendations section"""
        recommendations = log_data.get('recommendations', [])
        if not recommendations:
            return ""
        sections = [f"\nCurrent Recommendations ({len(recommendations)}):"]
        for rec in recommendations[:3]:
            sections.append(f"- {rec}")
        return "\n".join(sections)

    def _extract_recommendations_from_text(self, text: str) -> List[str]:
        """Extract recommendations from AI response text
        
        Args:
            text: AI response text
            
        Returns:
            List of extracted recommendations
        """
        recommendations = []
        
        # Look for numbered lists or bullet points
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if line:
                # Check if line looks like a recommendation
                if any(keyword in line.lower() for keyword in ['recommend', 'should', 'consider', 'implement', 'configure', 'monitor', 'check', 'review', 'upgrade']):
                    # Clean up the line
                    if line.startswith(('•', '-', '*', '1.', '2.', '3.', '4.', '5.')):
                        line = line[2:].strip()
                    elif line.startswith(tuple(f'{i}.' for i in range(1, 21))):
                        line = line[line.find('.') + 1:].strip()
                    
                    if line and len(line) > 10:  # Only meaningful recommendations
                        recommendations.append(line)
        
        # If no recommendations found, look for action items
        if not recommendations:
            for line in lines:
                line = line.strip()
                if line and ('action' in line.lower() or 'step' in line.lower()):
                    recommendations.append(line)
        
        return recommendations[:10]  # Limit to 10 recommendations