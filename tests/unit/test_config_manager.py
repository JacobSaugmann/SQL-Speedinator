"""
Unit tests for Config Manager
"""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from src.core.config_manager import ConfigManager


class TestConfigManager:
    """Test cases for ConfigManager class"""

    def test_init_default_config_file(self):
        """Test initialization with default config file"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                assert config.config_file == Path(".env")

    def test_init_custom_config_file(self):
        """Test initialization with custom config file"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager("custom.env")
                assert config.config_file == Path("custom.env")

    @patch('src.core.config_manager.load_dotenv')
    @patch('pathlib.Path.exists')
    def test_load_config_file_exists(self, mock_exists, mock_load_dotenv):
        """Test loading configuration when file exists"""
        mock_exists.return_value = True
        
        config = ConfigManager(".env")
        
        mock_load_dotenv.assert_called_once_with(Path(".env"))

    @patch('src.core.config_manager.load_dotenv')
    @patch('pathlib.Path.exists')
    def test_load_config_file_not_exists(self, mock_exists, mock_load_dotenv):
        """Test loading configuration when file doesn't exist"""
        mock_exists.return_value = False
        
        config = ConfigManager(".env")
        
        mock_load_dotenv.assert_not_called()

    @patch('src.core.config_manager.load_dotenv')
    @patch('pathlib.Path.exists')
    def test_load_config_error(self, mock_exists, mock_load_dotenv):
        """Test loading configuration with error"""
        mock_exists.return_value = True
        mock_load_dotenv.side_effect = Exception("Load error")
        
        with pytest.raises(Exception) as exc_info:
            ConfigManager(".env")
        
        assert "Load error" in str(exc_info.value)

    @patch.dict(os.environ, {'TEST_KEY': 'test_value'})
    def test_get_existing_key(self):
        """Test getting existing configuration key"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('TEST_KEY')
                assert result == 'test_value'

    def test_get_non_existing_key_with_default(self):
        """Test getting non-existing key with default value"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('NON_EXISTING_KEY', 'default_value')
                assert result == 'default_value'

    def test_get_non_existing_key_without_default(self):
        """Test getting non-existing key without default value"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('NON_EXISTING_KEY')
                assert result is None

    @patch.dict(os.environ, {'INT_KEY': '123'})
    def test_get_with_type_conversion_int(self):
        """Test getting value with integer type conversion"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('INT_KEY', convert_type=int)
                assert result == 123
                assert isinstance(result, int)

    @patch.dict(os.environ, {'FLOAT_KEY': '123.45'})
    def test_get_with_type_conversion_float(self):
        """Test getting value with float type conversion"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('FLOAT_KEY', convert_type=float)
                assert result == 123.45
                assert isinstance(result, float)

    @patch.dict(os.environ, {'BOOL_KEY': 'true'})
    def test_get_with_type_conversion_bool_true(self):
        """Test getting value with boolean type conversion (true)"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('BOOL_KEY', convert_type=bool)
                assert result is True

    @patch.dict(os.environ, {'BOOL_KEY': 'false'})
    def test_get_with_type_conversion_bool_false(self):
        """Test getting value with boolean type conversion (false)"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('BOOL_KEY', convert_type=bool)
                assert result is False

    @patch.dict(os.environ, {'LIST_KEY': 'item1,item2,item3'})
    def test_get_with_type_conversion_list(self):
        """Test getting value with list type conversion"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('LIST_KEY', convert_type=list)
                assert result == ['item1', 'item2', 'item3']
                assert isinstance(result, list)

    def test_get_with_invalid_type_conversion(self):
        """Test getting value with invalid type conversion"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                with patch.dict(os.environ, {'INVALID_KEY': 'not_a_number'}):
                    config = ConfigManager()
                    
                    with pytest.raises(ValueError):
                        config.get('INVALID_KEY', convert_type=int)

    @pytest.mark.parametrize("env_value,expected", [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("1", True),
        ("yes", True),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        ("0", False),
        ("no", False),
    ])
    def test_boolean_conversion_values(self, env_value, expected):
        """Test various boolean conversion values"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                with patch.dict(os.environ, {'BOOL_TEST': env_value}):
                    config = ConfigManager()
                    result = config.get('BOOL_TEST', convert_type=bool)
                    assert result is expected

    def test_get_with_empty_string_default(self):
        """Test getting value with empty string as default"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('NON_EXISTING_KEY', '')
                assert result == ''

    def test_get_with_none_default_explicit(self):
        """Test getting value with explicit None as default"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('NON_EXISTING_KEY', None)
                assert result is None

    @patch.dict(os.environ, {'WHITESPACE_KEY': '  value with spaces  '})
    def test_get_value_with_whitespace(self):
        """Test getting value that contains whitespace"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config = ConfigManager()
                result = config.get('WHITESPACE_KEY')
                assert result == '  value with spaces  '

    def test_multiple_config_instances(self):
        """Test multiple config manager instances"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                config1 = ConfigManager("config1.env")
                config2 = ConfigManager("config2.env")
                
                assert config1.config_file != config2.config_file
                assert config1.config_file == Path("config1.env")
                assert config2.config_file == Path("config2.env")

    def test_reload_config(self):
        """Test reloading configuration"""
        with patch('pathlib.Path.exists', return_value=True):
            with patch('src.core.config_manager.load_dotenv') as mock_load:
                config = ConfigManager()
                
                # Clear the call from initialization
                mock_load.reset_mock()
                
                # Call _load_config again
                config._load_config()
                
                mock_load.assert_called_once_with(Path(".env"))

    def test_config_file_path_handling(self):
        """Test configuration file path handling"""
        with patch('pathlib.Path.exists', return_value=False):
            with patch('src.core.config_manager.load_dotenv'):
                # Test with string path
                config1 = ConfigManager("test.env")
                assert isinstance(config1.config_file, Path)
                
                # Test with Path object
                config2 = ConfigManager(Path("test2.env"))
                assert isinstance(config2.config_file, Path)