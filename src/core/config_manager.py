"""
Configuration Manager for SQL Speedinator
Handles loading and management of configuration from environment files
"""

from typing import Union, Any, List
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import configparser

class ConfigManager:
    """Manages configuration settings from environment files and config files"""
    
    def __init__(self, config_file=".env"):
        """Initialize configuration manager
        
        Args:
            config_file (str): Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config_file = Path(config_file)
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file"""
        try:
            if self.config_file.exists():
                load_dotenv(self.config_file)
                self.logger.info(f"Loaded configuration from {self.config_file}")
            else:
                self.logger.warning(f"Configuration file {self.config_file} not found, using defaults")
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise
    
    def get(self, key: str, default: Any = None, convert_type: type = str) -> Any:
        """Get configuration value
        
        Args:
            key (str): Configuration key
            default: Default value if key not found
            convert_type: Type to convert value to
            
        Returns:
            Configuration value
        """
        value = os.getenv(key, default)
        
        if value is None:
            return None
            
        try:
            if convert_type == bool:
                if isinstance(value, str):
                    return value.lower() in ('true', 'yes', '1', 'on')
                return bool(value)
            elif convert_type == int:
                return int(value)
            elif convert_type == float:
                return float(value)
            else:
                return str(value)
        except (ValueError, AttributeError) as e:
            self.logger.warning(f"Error converting {key}={value} to {convert_type.__name__}: {e}")
            return default
    
    # SQL Server Connection Settings
    @property
    def use_windows_auth(self):
        return self.get('USE_WINDOWS_AUTH', True, bool)
    
    @property
    def sql_username(self):
        return self.get('SQL_USERNAME')
    
    @property
    def sql_password(self):
        return self.get('SQL_PASSWORD')
    
    @property
    def sql_trusted_connection(self):
        return self.get('SQL_TRUSTED_CONNECTION', 'yes', bool)
    
    # Advanced Index Analysis Settings
    @property
    def index_min_advantage(self):
        """Get minimum advantage threshold for missing indexes"""
        return self.get('INDEX_MIN_ADVANTAGE', 80, int)
    
    @property
    def index_calculate_selectability(self):
        """Get selectability calculation setting"""
        return self.get('INDEX_CALCULATE_SELECTABILITY', False, bool)
    
    @property
    def index_only_analysis(self):
        """Get index only analysis setting"""
        return self.get('INDEX_ONLY_ANALYSIS', True, bool)
    
    @property
    def index_limit_to_table(self):
        """Get table name filter for index analysis"""
        return self.get('INDEX_LIMIT_TO_TABLE', '')
    
    @property
    def index_limit_to_index(self):
        """Get index name filter for index analysis"""
        return self.get('INDEX_LIMIT_TO_INDEX', '')
    
    @property
    def sql_driver(self):
        return self.get('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    @property
    def connection_timeout(self):
        return self.get('CONNECTION_TIMEOUT', 30, int)
    
    @property
    def query_timeout(self):
        return self.get('QUERY_TIMEOUT', 300, int)
    
    # Performance Settings
    @property
    def night_mode_delay(self):
        return self.get('NIGHT_MODE_DELAY', 30, int)
    
    @property
    def max_parallel_queries(self):
        return self.get('MAX_PARALLEL_QUERIES', 2, int)
    
    # Report Settings
    @property
    def output_directory(self):
        return self.get('OUTPUT_DIRECTORY', './reports')
    
    @property
    def include_charts(self):
        return self.get('INCLUDE_CHARTS', True, bool)
    
    @property
    def chart_dpi(self):
        return self.get('CHART_DPI', 300, int)
    
    # Analysis Settings
    @property
    def min_index_size_mb(self):
        return self.get('MIN_INDEX_SIZE_MB', 100, int)
    
    @property
    def max_fragmentation_threshold(self):
        return self.get('MAX_FRAGMENTATION_THRESHOLD', 30, int)
    
    @property
    def min_missing_index_impact(self):
        return self.get('MIN_MISSING_INDEX_IMPACT', 10000, int)
    
    @property
    def plan_cache_analysis_hours(self):
        return self.get('PLAN_CACHE_ANALYSIS_HOURS', 24, int)
    
    # AI Copilot Settings
    @property
    def be_my_copilot(self):
        return self.get('AI_ANALYSIS_ENABLED', False, bool)
    
    @property
    def azure_openai_endpoint(self):
        return self.get('AZURE_OPENAI_ENDPOINT', '')
    
    @property
    def azure_openai_api_key(self):
        return self.get('AZURE_OPENAI_API_KEY', '')
    
    @property
    def azure_openai_deployment(self):
        return self.get('AZURE_OPENAI_DEPLOYMENT', 'gpt-4')
    
    @property
    def azure_openai_api_version(self):
        return self.get('AZURE_OPENAI_API_VERSION', '2024-02-15-preview')
    
    @property
    def azure_openai_model(self):
        return self.get('AZURE_OPENAI_MODEL', 'gpt-4')
    
    @property
    def ai_max_tokens(self):
        return self.get('AI_MAX_TOKENS', 1000, int)
    
    @property
    def ai_temperature(self):
        return self.get('AI_TEMPERATURE', 0.3, float)
    
    def validate_ai_config(self) -> bool:
        """Validate AI configuration settings"""
        if not self.be_my_copilot:
            return True
        
        required_fields = [
            self.azure_openai_endpoint,
            self.azure_openai_api_key,
            self.azure_openai_deployment
        ]
        
        missing_fields = []
        if not self.azure_openai_endpoint:
            missing_fields.append('AZURE_OPENAI_ENDPOINT')
        if not self.azure_openai_api_key:
            missing_fields.append('AZURE_OPENAI_API_KEY')
        if not self.azure_openai_deployment:
            missing_fields.append('AZURE_OPENAI_DEPLOYMENT')
        
        if missing_fields:
            self.logger.error(f"AI Copilot enabled but missing required config: {', '.join(missing_fields)}")
            return False
        
        return True
    
    def validate_sql_config(self) -> bool:
        """Validate SQL Server configuration settings"""
        if self.use_windows_auth:
            # Windows Authentication - no additional validation needed
            return True
        else:
            # SQL Server Authentication - require username and password
            if not self.sql_username or not self.sql_password:
                self.logger.error(
                    "SQL Server Authentication selected (USE_WINDOWS_AUTH=false) but "
                    "SQL_USERNAME or SQL_PASSWORD not provided"
                )
                return False
            return True
    
    # Scheduling Settings
    @property
    def schedule_enabled(self):
        return self.get('SCHEDULE_ENABLED', False, bool)
    
    @property
    def schedule_time(self):
        return self.get('SCHEDULE_TIME', '02:00')
    
    @property
    def schedule_days(self) -> List[int]:
        days_str = self.get('SCHEDULE_DAYS', '1,2,3,4,5')
        try:
            if isinstance(days_str, str):
                return [int(d.strip()) for d in days_str.split(',')]
            else:
                return [1,2,3,4,5]  # Default to weekdays
        except:
            return [1,2,3,4,5]  # Default to weekdays