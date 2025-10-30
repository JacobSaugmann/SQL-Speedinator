"""
Unit tests for SQL Server connection module
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pyodbc
from src.core.sql_connection import SQLServerConnection


class TestSQLServerConnection:
    """Test cases for SQLServerConnection class"""

    def test_init_with_config(self, mock_config):
        """Test connection initialization with config"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        assert conn.server_name == "localhost"
        assert conn.config == mock_config

    @patch('pyodbc.connect')
    def test_connect_success_windows_auth(self, mock_connect, mock_config):
        """Test successful connection with Windows authentication"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        conn = SQLServerConnection("localhost", mock_config)
        result = conn.connect()
        
        assert result is True
        assert conn.connection is not None
        mock_connect.assert_called_once()
        
        # Verify connection string contains Windows auth
        call_args = mock_connect.call_args[0][0]
        assert "Trusted_Connection=yes" in call_args

    @patch('pyodbc.connect')
    def test_connect_success_sql_auth(self, mock_connect, mock_config):
        """Test successful connection with SQL authentication"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = False
        mock_config.sql_username = "testuser"
        mock_config.sql_password = "testpass"
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        conn = SQLServerConnection("localhost", mock_config)
        result = conn.connect()
        
        assert result is True
        assert conn.connection is not None
        
        # Verify connection string contains SQL auth
        call_args = mock_connect.call_args[0][0]
        assert "UID=testuser" in call_args
        assert "PWD=testpass" in call_args

    @patch('pyodbc.connect')
    def test_connect_failure(self, mock_connect, mock_config):
        """Test connection failure handling"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        mock_connect.side_effect = pyodbc.Error("Connection failed")
        
        conn = SQLServerConnection("localhost", mock_config)
        result = conn.connect()
        
        assert result is False
        assert conn.connection is None

    def test_disconnect_when_connected(self, mock_config):
        """Test disconnection when connected"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        mock_connection = Mock()
        conn.connection = mock_connection
        
        conn.disconnect()
        
        mock_connection.close.assert_called_once()
        assert conn.connection is None

    def test_disconnect_when_not_connected(self, mock_config):
        """Test disconnection when not connected"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        conn.connection = None
        
        # Should not raise exception
        conn.disconnect()
        assert conn.connection is None

    def test_test_connection_success(self, mock_config):
        """Test successful connection test"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value = mock_cursor
        conn.connection = mock_connection
        
        result = conn.test_connection()
        
        assert result is True
        mock_cursor.execute.assert_called_with("SELECT 1")

    def test_test_connection_no_connection(self, mock_config):
        """Test connection test when not connected"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        conn.connection = None
        
        result = conn.test_connection()
        assert result is False

    def test_connection_string_building_windows_auth(self, mock_config):
        """Test connection string building for Windows auth"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        conn_str = conn._build_connection_string()
        
        assert "SERVER=localhost" in conn_str
        assert "Trusted_Connection=yes" in conn_str
        assert "UID=" not in conn_str
        assert "PWD=" not in conn_str

    def test_connection_string_building_sql_auth(self, mock_config):
        """Test connection string building for SQL auth"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = False
        mock_config.sql_username = "testuser"
        mock_config.sql_password = "testpass"
        
        conn = SQLServerConnection("localhost", mock_config)
        conn_str = conn._build_connection_string()
        
        assert "SERVER=localhost" in conn_str
        assert "UID=testuser" in conn_str
        assert "PWD=testpass" in conn_str
        assert "Trusted_Connection" not in conn_str

    def test_sql_auth_missing_credentials(self, mock_config):
        """Test SQL auth with missing credentials raises error"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = False
        mock_config.sql_username = None
        mock_config.sql_password = None
        
        with pytest.raises(ValueError) as exc_info:
            SQLServerConnection("localhost", mock_config)
        
        assert "SQL Server Authentication selected" in str(exc_info.value)

    @pytest.mark.parametrize("timeout_value", [10, 30, 60])
    def test_different_timeout_values(self, mock_config, timeout_value):
        """Test connection with different timeout values"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = timeout_value
        mock_config.query_timeout = timeout_value
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection("localhost", mock_config)
        conn_str = conn._build_connection_string()
        
        assert f"Connection Timeout={timeout_value}" in conn_str
        assert f"CommandTimeout={timeout_value}" in conn_str

    @pytest.mark.parametrize("server_name", ["localhost", "server1", "server1\\instance"])
    def test_different_server_names(self, mock_config, server_name):
        """Test connection with different server names"""
        mock_config.sql_driver = "ODBC Driver 17 for SQL Server"
        mock_config.connection_timeout = 30
        mock_config.query_timeout = 30
        mock_config.use_windows_auth = True
        
        conn = SQLServerConnection(server_name, mock_config)
        assert conn.server_name == server_name
        
        conn_str = conn._build_connection_string()
        assert f"SERVER={server_name}" in conn_str