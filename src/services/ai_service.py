"""
Azure OpenAI Service for SQL Server Performance Analysis
Provides AI-powered analysis and recommendations using Azure OpenAI GPT models
"""

import logging
import json
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
                raise ValueError("Invalid AI configuration. Check required Azure OpenAI settings.")
            
            try:
                self.client = AzureOpenAI(
                    api_key=config.azure_openai_api_key,
                    api_version=config.azure_openai_api_version,
                    azure_endpoint=config.azure_openai_endpoint
                )
                self.logger.info("Azure OpenAI client initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Azure OpenAI client: {e}")
                raise
    
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