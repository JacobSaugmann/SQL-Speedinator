"""
Unit tests for Index Analyzer
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.analyzers.index_analyzer import IndexAnalyzer


class TestIndexAnalyzer:
    """Test cases for IndexAnalyzer class"""

    def test_init(self, mock_sql_connection, mock_config):
        """Test analyzer initialization"""
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        
        assert analyzer.connection == mock_sql_connection
        assert analyzer.config == mock_config.analysis
        assert analyzer.logger is not None

    def test_get_user_databases_success(self, mock_sql_connection, mock_config):
        """Test successful retrieval of user databases"""
        mock_sql_connection.execute_query.return_value = [
            {'name': 'AdventureWorks2022'},
            {'name': 'TestDB'},
            {'name': 'UserDB'}
        ]
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        databases = analyzer._get_user_databases()
        
        assert databases == ['AdventureWorks2022', 'TestDB', 'UserDB']
        mock_sql_connection.execute_query.assert_called_once()

    def test_get_user_databases_empty(self, mock_sql_connection, mock_config):
        """Test user databases retrieval when none found"""
        mock_sql_connection.execute_query.return_value = []
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        databases = analyzer._get_user_databases()
        
        assert databases == []

    def test_get_user_databases_failure(self, mock_sql_connection, mock_config):
        """Test user databases retrieval failure"""
        mock_sql_connection.execute_query.return_value = None
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        databases = analyzer._get_user_databases()
        
        assert databases == []

    def test_analyze_success(self, mock_sql_connection, mock_config, sample_index_data):
        """Test successful complete analysis"""
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        
        # Mock all the individual analysis methods
        with patch.object(analyzer, '_get_fragmented_indexes', return_value=sample_index_data['fragmented_indexes']):
            with patch.object(analyzer, '_get_unused_indexes', return_value=sample_index_data['unused_indexes']):
                with patch.object(analyzer, '_find_duplicate_indexes', return_value=sample_index_data['duplicate_indexes']):
                    with patch.object(analyzer, '_get_index_usage_stats', return_value=[]):
                        with patch.object(analyzer, '_get_fragmentation_usage_analysis', return_value=[]):
                            with patch.object(analyzer, '_get_index_maintenance_recommendations', return_value=[]):
                                with patch.object(analyzer, '_generate_index_recommendations', return_value=[]):
                                    
                                    result = analyzer.analyze()
                                    
                                    assert 'fragmented_indexes' in result
                                    assert 'unused_indexes' in result
                                    assert 'duplicate_indexes' in result
                                    assert result['fragmented_indexes'] == sample_index_data['fragmented_indexes']
                                    assert result['unused_indexes'] == sample_index_data['unused_indexes']
                                    assert result['duplicate_indexes'] == sample_index_data['duplicate_indexes']

    def test_analyze_failure(self, mock_sql_connection, mock_config):
        """Test analysis failure handling"""
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        
        # Mock method to raise exception
        with patch.object(analyzer, '_get_fragmented_indexes', side_effect=Exception("Test error")):
            result = analyzer.analyze()
            
            assert 'error' in result
            assert "Test error" in result['error']

    def test_get_fragmented_indexes_success(self, mock_sql_connection, mock_config):
        """Test successful fragmented indexes retrieval"""
        mock_data = [
            {
                'database_name': 'TestDB',
                'schema_name': 'dbo',
                'table_name': 'TestTable',
                'index_name': 'IX_Test',
                'avg_fragmentation_in_percent': 85.5,
                'page_count': 1000,
                'size_mb': 8.0,
                'recommended_action': 'REBUILD'
            }
        ]
        mock_sql_connection.execute_query.return_value = mock_data
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_fragmented_indexes()
        
        assert result == mock_data
        mock_sql_connection.execute_query.assert_called_once()

    def test_get_fragmented_indexes_empty(self, mock_sql_connection, mock_config):
        """Test fragmented indexes retrieval when none found"""
        mock_sql_connection.execute_query.return_value = []
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_fragmented_indexes()
        
        assert result == []

    def test_get_unused_indexes_success(self, mock_sql_connection, mock_config):
        """Test successful unused indexes analysis"""
        # Mock user databases
        mock_sql_connection.execute_query.side_effect = [
            [{'name': 'TestDB'}],  # User databases query
            [{'current_db': 'master'}],  # Original database query
            [  # Unused indexes data
                {
                    'database_name': 'TestDB',
                    'schema_name': 'dbo',
                    'table_name': 'UnusedTable',
                    'index_name': 'IX_Unused',
                    'user_seeks': 0,
                    'user_scans': 0,
                    'user_lookups': 0,
                    'user_updates': 100,
                    'size_mb': 5.0
                }
            ]
        ]
        mock_sql_connection.change_database.return_value = True
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_unused_indexes()
        
        assert result is not None
        assert len(result) == 1
        assert result[0]['index_name'] == 'IX_Unused'
        assert mock_sql_connection.change_database.call_count == 2  # Switch to TestDB and back

    def test_get_unused_indexes_no_databases(self, mock_sql_connection, mock_config):
        """Test unused indexes analysis when no user databases found"""
        mock_sql_connection.execute_query.return_value = []
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_unused_indexes()
        
        assert result is None

    def test_get_unused_indexes_database_access_failure(self, mock_sql_connection, mock_config):
        """Test unused indexes analysis when database access fails"""
        mock_sql_connection.execute_query.side_effect = [
            [{'name': 'TestDB'}],  # User databases query
            [{'current_db': 'master'}]  # Original database query
        ]
        mock_sql_connection.change_database.return_value = False  # Access fails
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_unused_indexes()
        
        assert result is None

    def test_find_duplicate_indexes_success(self, mock_sql_connection, mock_config):
        """Test successful duplicate indexes detection"""
        # Mock user databases and duplicate index data
        mock_sql_connection.execute_query.side_effect = [
            [{'name': 'TestDB'}],  # User databases query
            [{'current_db': 'master'}],  # Original database query
            [  # Duplicate indexes data
                {
                    'database_name': 'TestDB',
                    'schema_name': 'dbo',
                    'table_name': 'TestTable',
                    'index1_name': 'IX_FirstName',
                    'index1_key_columns': 'FirstName ASC',
                    'index2_name': 'IX_FirstName_LastName',
                    'index2_key_columns': 'FirstName ASC, LastName ASC',
                    'duplicate_type': 'OVERLAPPING'
                }
            ]
        ]
        mock_sql_connection.change_database.return_value = True
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._find_duplicate_indexes()
        
        assert result is not None
        assert len(result) == 1
        assert result[0]['duplicate_type'] == 'OVERLAPPING'
        assert result[0]['index1_name'] == 'IX_FirstName'

    def test_find_duplicate_indexes_no_databases(self, mock_sql_connection, mock_config):
        """Test duplicate indexes detection when no user databases found"""
        mock_sql_connection.execute_query.return_value = []
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._find_duplicate_indexes()
        
        assert result is None

    def test_get_index_usage_stats_success(self, mock_sql_connection, mock_config):
        """Test successful index usage statistics retrieval"""
        mock_sql_connection.execute_query.side_effect = [
            [{'name': 'TestDB'}],  # User databases query
            [{'current_db': 'master'}],  # Original database query
            [  # Usage stats data
                {
                    'database_name': 'TestDB',
                    'schema_name': 'dbo',
                    'table_name': 'TestTable',
                    'index_name': 'IX_Test',
                    'user_seeks': 1000,
                    'user_scans': 500,
                    'user_lookups': 200,
                    'user_updates': 100,
                    'usage_pattern': 'HIGHLY_USED'
                }
            ]
        ]
        mock_sql_connection.change_database.return_value = True
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_index_usage_stats()
        
        assert result is not None
        assert len(result) == 1
        assert result[0]['usage_pattern'] == 'HIGHLY_USED'

    def test_generate_index_recommendations_with_data(self, mock_sql_connection, mock_config, sample_index_data):
        """Test index recommendations generation with data"""
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        
        # Mock the maintenance recommendations method
        with patch.object(analyzer, '_get_index_maintenance_recommendations', return_value=[]):
            with patch.object(analyzer, '_get_fragmented_indexes', return_value=sample_index_data['fragmented_indexes']):
                recommendations = analyzer._generate_index_recommendations(
                    unused_indexes=sample_index_data['unused_indexes'],
                    duplicate_indexes=sample_index_data['duplicate_indexes'],
                    index_usage_stats=[]
                )
                
                # Should have recommendations for unused and duplicate indexes
                assert len(recommendations) >= 2
                
                # Check for unused indexes recommendation
                unused_rec = next((r for r in recommendations if 'unused' in r.get('issue', '').lower()), None)
                assert unused_rec is not None
                assert unused_rec['category'] == 'Index Cleanup'
                
                # Check for duplicate indexes recommendation
                duplicate_rec = next((r for r in recommendations if 'duplicate' in r.get('issue', '').lower()), None)
                assert duplicate_rec is not None
                assert duplicate_rec['category'] == 'Index Optimization'

    def test_generate_index_recommendations_no_data(self, mock_sql_connection, mock_config):
        """Test index recommendations generation with no data"""
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        
        with patch.object(analyzer, '_get_index_maintenance_recommendations', return_value=[]):
            with patch.object(analyzer, '_get_fragmented_indexes', return_value=[]):
                recommendations = analyzer._generate_index_recommendations(
                    unused_indexes=[],
                    duplicate_indexes=[],
                    index_usage_stats=[]
                )
                
                # Should have no recommendations when no issues found
                assert len(recommendations) == 0

    @pytest.mark.parametrize("fragmentation_percent,expected_action", [
        (85.0, "REBUILD"),
        (25.0, "REORGANIZE"),
        (5.0, "OK")
    ])
    def test_fragmentation_action_logic(self, mock_sql_connection, mock_config, fragmentation_percent, expected_action):
        """Test fragmentation action logic with different percentages"""
        mock_data = [
            {
                'avg_fragmentation_in_percent': fragmentation_percent,
                'recommended_action': expected_action
            }
        ]
        mock_sql_connection.execute_query.return_value = mock_data
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_fragmented_indexes()
        
        if result:
            assert result[0]['recommended_action'] == expected_action

    def test_error_handling_in_analysis_methods(self, mock_sql_connection, mock_config):
        """Test error handling in various analysis methods"""
        # Test connection error during unused index analysis
        mock_sql_connection.execute_query.side_effect = [
            [{'name': 'TestDB'}],  # User databases query succeeds
            Exception("Connection error")  # Subsequent query fails
        ]
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        result = analyzer._get_unused_indexes()
        
        # Should return None on error, not crash
        assert result is None

    def test_database_context_restoration(self, mock_sql_connection, mock_config):
        """Test that database context is properly restored after analysis"""
        mock_sql_connection.execute_query.side_effect = [
            [{'name': 'TestDB1'}, {'name': 'TestDB2'}],  # User databases
            [{'current_db': 'master'}],  # Original database
            [],  # TestDB1 results
            []   # TestDB2 results
        ]
        mock_sql_connection.change_database.return_value = True
        
        analyzer = IndexAnalyzer(mock_sql_connection, mock_config.analysis)
        analyzer._get_unused_indexes()
        
        # Should call change_database to restore original context
        change_calls = mock_sql_connection.change_database.call_args_list
        assert len(change_calls) >= 3  # TestDB1, TestDB2, back to master
        assert change_calls[-1][0][0] == 'master'  # Last call should restore to master