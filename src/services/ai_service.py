"""
Azure OpenAI Service for SQL Server Performance Analysis
Provides AI-powered analysis and recommendations using Azure OpenAI GPT models
"""

import logging
import json
import re
from typing import Dict, Any, Optional
from openai import AzureOpenAI
try:
    from ..core.config_manager import ConfigManager
except ImportError:
    from core.config_manager import ConfigManager

class AIService:
    """Azure OpenAI service for performance analysis insights"""
    
    def __init__(self, config: ConfigManager):
        """Initialize AI service with configuration
        
        Args:
            config: Configuration manager instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
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
                    self.logger.warning(f"Basic initialization failed ({str(e)}), trying with custom HTTP client...")
                    
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
        """Clean malformed HTML tags like '>green>' from AI responses"""
        if not isinstance(text, str):
            return text
            
        # Fix malformed tags like '>green>', '>red>', '>orange>'
        text = re.sub(r'>green>', '<font color="green">OK</font>', text)
        text = re.sub(r'>red>', '<font color="red">CRITICAL</font>', text) 
        text = re.sub(r'>orange>', '<font color="orange">WARNING</font>', text)
        text = re.sub(r'>yellow>', '<font color="orange">CAUTION</font>', text)
        
        # Remove any other malformed > tags
        text = re.sub(r'>(\w+)>', r'\1', text)
        
        return text
    
    def _clean_dict_recursively(self, obj):
        """Recursively clean HTML tags in dictionary/list structures"""
        if isinstance(obj, dict):
            return {k: self._clean_dict_recursively(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_dict_recursively(item) for item in obj]
        elif isinstance(obj, str):
            return self._clean_html_tags(obj)
        else:
            return obj
    
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
        
        # Server info (minimal)
        server_info = performance_summary.get('server_info', {})
        if server_info:
            prompt_parts.append(f"Server: {server_info.get('edition', 'Unknown')} {server_info.get('version', '')}, {server_info.get('cpu_count', 'N/A')} CPUs, {server_info.get('total_memory_mb', 'N/A')}MB RAM")
        
        # Wait stats (top 5 only)
        wait_stats = performance_summary.get('wait_stats', {})
        if wait_stats and wait_stats.get('top_waits'):
            top_waits = wait_stats['top_waits'][:5]
            wait_list = [f"{w['wait_type']}({w['percentage']:.1f}%)" for w in top_waits]
            prompt_parts.append(f"Top waits: {', '.join(wait_list)}")
        
        # Disk issues (critical only)
        disk_issues = performance_summary.get('disk_issues', [])
        if disk_issues:
            critical_issues = [issue for issue in disk_issues if issue.get('severity') == 'HIGH'][:3]
            if critical_issues:
                disk_list = [f"{issue['database']}({issue['issue']})" for issue in critical_issues]
                prompt_parts.append(f"Disk issues: {', '.join(disk_list)}")
        
        # Index problems (high impact only)
        index_issues = performance_summary.get('index_issues', {})
        if index_issues:
            if index_issues.get('high_fragmentation_count', 0) > 0:
                prompt_parts.append(f"High fragmentation: {index_issues['high_fragmentation_count']} indexes")
            if index_issues.get('unused_count', 0) > 0:
                prompt_parts.append(f"Unused indexes: {index_issues['unused_count']}")
            if index_issues.get('missing_high_impact', 0) > 0:
                prompt_parts.append(f"Missing high-impact indexes: {index_issues['missing_high_impact']}")
        
        # Config issues (critical only)
        config_issues = performance_summary.get('config_issues', [])
        if config_issues:
            critical_config = [issue for issue in config_issues if issue.get('severity') == 'HIGH'][:3]
            if critical_config:
                config_list = [f"{issue['setting']}({issue['issue']})" for issue in critical_config]
                prompt_parts.append(f"Config issues: {', '.join(config_list)}")
        
        # TempDB issues
        tempdb_issues = performance_summary.get('tempdb_issues', [])
        if tempdb_issues:
            critical_tempdb = [issue for issue in tempdb_issues if issue.get('severity') == 'HIGH'][:2]
            if critical_tempdb:
                tempdb_list = [issue['description'] for issue in critical_tempdb]
                prompt_parts.append(f"TempDB: {', '.join(tempdb_list)}")
        
        # Plan cache efficiency
        plan_cache = performance_summary.get('plan_cache', {})
        if plan_cache and plan_cache.get('single_use_pct', 0) > 10:
            prompt_parts.append(f"Plan cache: {plan_cache['single_use_pct']:.1f}% single-use plans")
        
        if not prompt_parts:
            prompt_parts.append("No significant performance issues detected in summary data")
        
        prompt = "SQL Server Performance Analysis:\n" + "\n".join(prompt_parts)
        prompt += "\n\nProvide JSON response with: {'bottlenecks': [{'issue': 'description', 'impact': 'HIGH/MEDIUM/LOW', 'recommendation': 'specific action'}], 'summary': 'overall assessment'}"
        
        return prompt
    
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
        
        # Collection summary
        if 'summary' in perfmon_data:
            summary = perfmon_data['summary']
            prompt_parts.append(f"Collection: {summary.get('duration_minutes', 0):.1f} minutes, {summary.get('total_counters', 0)} counters")
        
        # CPU metrics
        if 'cpu_analysis' in perfmon_data:
            cpu = perfmon_data['cpu_analysis']
            if 'metrics' in cpu:
                metrics = cpu['metrics']
                status = cpu.get('status', 'OK')
                if 'avg_processor_time' in metrics:
                    prompt_parts.append(f"CPU: {metrics['avg_processor_time']}% avg usage ({status})")
                if 'avg_processor_queue' in metrics:
                    prompt_parts.append(f"CPU Queue: {metrics['avg_processor_queue']} avg length")
        
        # Memory metrics
        if 'memory_analysis' in perfmon_data:
            memory = perfmon_data['memory_analysis']
            if 'metrics' in memory:
                metrics = memory['metrics']
                status = memory.get('status', 'OK')
                if 'avg_available_mb' in metrics:
                    prompt_parts.append(f"Memory: {metrics['avg_available_mb']:,.0f} MB avg available ({status})")
                if 'avg_page_life_expectancy' in metrics:
                    prompt_parts.append(f"Page Life Expectancy: {metrics['avg_page_life_expectancy']:,.0f} seconds")
        
        # Disk metrics
        if 'disk_analysis' in perfmon_data:
            disk = perfmon_data['disk_analysis']
            if 'metrics' in disk:
                metrics = disk['metrics']
                status = disk.get('status', 'OK')
                if 'avg_disk_queue_length' in metrics:
                    prompt_parts.append(f"Disk Queue: {metrics['avg_disk_queue_length']} avg length ({status})")
                if 'avg_disk_read_ms' in metrics:
                    prompt_parts.append(f"Disk Latency: {metrics['avg_disk_read_ms']} ms avg read")
        
        # SQL Server metrics
        if 'sql_server_analysis' in perfmon_data:
            sql = perfmon_data['sql_server_analysis']
            if 'metrics' in sql:
                metrics = sql['metrics']
                status = sql.get('status', 'OK')
                if 'avg_batch_requests_per_sec' in metrics:
                    prompt_parts.append(f"SQL Batches/sec: {metrics['avg_batch_requests_per_sec']} ({status})")
                if 'avg_compilations_per_sec' in metrics:
                    prompt_parts.append(f"SQL Compilations/sec: {metrics['avg_compilations_per_sec']}")
                if 'avg_lock_waits_per_sec' in metrics:
                    prompt_parts.append(f"Lock Waits/sec: {metrics['avg_lock_waits_per_sec']}")
        
        # Existing bottlenecks
        if 'bottlenecks' in perfmon_data and perfmon_data['bottlenecks']:
            bottleneck_list = []
            for bottleneck in perfmon_data['bottlenecks']:
                severity = bottleneck.get('severity', 'UNKNOWN')
                category = bottleneck.get('category', 'Unknown')
                description = bottleneck.get('description', 'No description')
                bottleneck_list.append(f"{severity} {category}: {description}")
            prompt_parts.append(f"Detected Bottlenecks: {'; '.join(bottleneck_list)}")
        
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