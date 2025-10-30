"""
Unit tests for AdvancedIndexAnalyzer
Tests comprehensive index analysis functionality with proper mocking
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.analyzers.advanced_index_analyzer import (
    AdvancedIndexAnalyzer, 
    IndexAnalysisSettings,
    IndexAnalysisResults,
    MissingIndexInfo,
    ExistingIndexInfo,
    OverlappingIndexInfo,
    UnusedIndexInfo
)


class TestAdvancedIndexAnalyzer:
    """Test cases for AdvancedIndexAnalyzer"""

    @pytest.fixture
    def mock_connection(self):
        """Mock SQL connection for tests"""
        mock_conn = Mock()
        mock_conn.execute_query.return_value = []
        return mock_conn

    @pytest.fixture
    def analyzer(self, mock_connection):
        """Create analyzer instance with mocked connection"""
        return AdvancedIndexAnalyzer(mock_connection)

    def test_init(self, mock_connection):
        """Test analyzer initialization"""
        analyzer = AdvancedIndexAnalyzer(mock_connection)
        
        assert analyzer.connection == mock_connection
        assert analyzer.logger is not None

    def test_analyze_indexes_with_default_settings(self, analyzer):
        """Test index analysis with default settings"""
        # Mock the internal methods
        analyzer._execute_advanced_analysis = Mock(return_value={
            'missing_indexes': [],
            'existing_indexes': [],
            'overlapping_indexes': [],
            'unused_indexes': []
        })
        analyzer._parse_analysis_results = Mock(return_value=IndexAnalysisResults(
            missing_indexes=[],
            existing_indexes=[],
            overlapping_indexes=[],
            unused_indexes=[],
            total_wasted_space_mb=0.0,
            metadata_age_days=0,
            warnings=[]
        ))
        
        result = analyzer.analyze_indexes()
        
        assert isinstance(result, IndexAnalysisResults)
        analyzer._execute_advanced_analysis.assert_called_once()
        analyzer._parse_analysis_results.assert_called_once()

    def test_analyze_indexes_with_custom_settings(self, analyzer):
        """Test index analysis with custom settings"""
        settings = IndexAnalysisSettings(
            min_advantage=90,
            get_selectability=True,
            limit_to_tablename="TestTable"
        )
        
        # Mock the internal methods
        analyzer._execute_advanced_analysis = Mock(return_value={})
        analyzer._parse_analysis_results = Mock(return_value=IndexAnalysisResults(
            missing_indexes=[],
            existing_indexes=[],
            overlapping_indexes=[],
            unused_indexes=[],
            total_wasted_space_mb=0.0,
            metadata_age_days=0,
            warnings=[]
        ))
        
        result = analyzer.analyze_indexes(settings)
        
        assert isinstance(result, IndexAnalysisResults)
        # Verify settings were passed through
        analyzer._execute_advanced_analysis.assert_called_once_with(settings)

    def test_analyze_indexes_exception_handling(self, analyzer):
        """Test exception handling in analyze_indexes"""
        analyzer._execute_advanced_analysis = Mock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception) as exc_info:
            analyzer.analyze_indexes()
        
        assert "Database error" in str(exc_info.value)

    def test_split_sql_script_simple(self, analyzer):
        """Test SQL script splitting with simple GO statements"""
        sql_script = """
        SELECT 1
        GO
        SELECT 2
        GO
        SELECT 3
        """
        
        result = analyzer._split_sql_script(sql_script)
        
        assert len(result) == 3
        assert "SELECT 1" in result[0].strip()
        assert "SELECT 2" in result[1].strip()
        assert "SELECT 3" in result[2].strip()

    def test_split_sql_script_with_comments(self, analyzer):
        """Test SQL script splitting with comments and GO statements"""
        sql_script = """
        -- This is a comment
        SELECT 1
        GO
        /* Multi-line
           comment */
        SELECT 2
        GO
        """
        
        result = analyzer._split_sql_script(sql_script)
        
        assert len(result) == 2
        assert "SELECT 1" in result[0]
        assert "SELECT 2" in result[1]

    def test_split_sql_script_no_go_statements(self, analyzer):
        """Test SQL script splitting when no GO statements present"""
        sql_script = "SELECT 1; SELECT 2;"
        
        result = analyzer._split_sql_script(sql_script)
        
        assert len(result) == 1
        assert result[0].strip() == sql_script

    def test_execute_advanced_analysis_success(self, analyzer, mock_connection):
        """Test successful execution of advanced analysis"""
        settings = IndexAnalysisSettings()
        
        # Mock the connection.connection.cursor() to avoid the cursor mocking issue
        mock_cursor = Mock()
        mock_cursor.description = [['column1'], ['column2']]
        mock_cursor.fetchall.return_value = [
            {'table_name': 'Table1', 'missing_cols': 'col1,col2'}
        ]
        mock_connection.connection.cursor.return_value = mock_cursor
        
        # Mock the SQL script that gets executed
        with patch.object(analyzer, '_split_sql_script') as mock_split:
            mock_split.return_value = ['SELECT 1']  # Simple query
            
            result = analyzer._execute_advanced_analysis(settings)
            
            assert isinstance(result, dict)
            # The method should call cursor operations
            mock_cursor.execute.assert_called()

    def test_execute_advanced_analysis_with_limit_to_table(self, analyzer, mock_connection):
        """Test analysis with table name limitation"""
        settings = IndexAnalysisSettings(limit_to_tablename="SpecificTable")
        
        # Mock the connection.connection.cursor() 
        mock_cursor = Mock()
        mock_cursor.description = [['column1']]
        mock_cursor.fetchall.return_value = []
        mock_connection.connection.cursor.return_value = mock_cursor
        
        analyzer._execute_advanced_analysis(settings)
        
        # Verify that the method was called (table name filter is in SQL)
        mock_cursor.execute.assert_called()
        # Verify the SQL contains the table name filter
        call_args = mock_cursor.execute.call_args[0][0]
        assert "SpecificTable" in call_args

    def test_parse_analysis_results_with_data(self, analyzer):
        """Test parsing of analysis results with sample data"""
        data = {
            'index_info': [
                {
                    'table_name': 'Users',
                    'table_type_desc': 'Missing_Index',
                    'index_columns_names': 'user_id',
                    'inequality_columns': 'created_date',
                    'included_columns': 'name,email',
                    'avg_total_user_cost': 100.5,
                    'avg_user_impact': 85.2,
                    'user_scans': 1000,
                    'user_seeks': 500,
                    'create_index_statement': 'CREATE INDEX...',
                    'create_index_adv': 90.0,
                    'meta_data_age': 7
                },
                {
                    'table_name': 'Users',
                    'table_type_desc': 'USER_TABLE',
                    'index_name': 'IX_Users_Email',
                    'index_type_desc': 'NONCLUSTERED',
                    'is_unique': False,
                    'is_primary_key': False,
                    'has_filter': False,
                    'index_columns_names': 'email',
                    'included_columns': None,
                    'record_count': 10000,
                    'avg_fragmentation_in_percent': 15.5,
                    'user_lookups': 100,
                    'user_scans': 50,
                    'user_seeks': 200,
                    'user_updates': 10,
                    'meta_data_age': 7
                }
            ]
        }
        
        settings = IndexAnalysisSettings()
        result = analyzer._parse_analysis_results(data, settings)
        
        assert isinstance(result, IndexAnalysisResults)
        assert len(result.missing_indexes) == 1
        assert len(result.existing_indexes) == 1
        
        # Test missing index details
        missing_idx = result.missing_indexes[0]
        assert missing_idx.table_name == 'Users'
        assert missing_idx.equality_columns == 'user_id'
        assert missing_idx.avg_user_impact == 85.2
        
        # Test existing index details
        existing_idx = result.existing_indexes[0]
        assert existing_idx.table_name == 'Users'
        assert existing_idx.index_name == 'IX_Users_Email'
        assert existing_idx.fragmentation_percent == 15.5

    def test_parse_analysis_results_empty_data(self, analyzer):
        """Test parsing with empty data"""
        data = {
            'index_info': []
        }
        
        settings = IndexAnalysisSettings()
        result = analyzer._parse_analysis_results(data, settings)
        
        assert isinstance(result, IndexAnalysisResults)
        assert len(result.missing_indexes) == 0
        assert len(result.existing_indexes) == 0
        assert len(result.overlapping_indexes) == 0
        assert len(result.unused_indexes) == 0

    def test_get_index_recommendations_with_data(self, analyzer):
        """Test getting index recommendations from results"""
        results = IndexAnalysisResults(
            missing_indexes=[
                MissingIndexInfo(
                    table_name="Users",
                    equality_columns="user_id",
                    inequality_columns="created_date",
                    included_columns="name",
                    avg_total_user_cost=100.0,
                    avg_user_impact=90.0,
                    user_scans=1000,
                    user_seeks=500,
                    create_index_statement="CREATE INDEX IX_Users_Optimal ON Users (user_id, created_date) INCLUDE (name)",
                    create_index_advantage=90.0,
                    meta_data_age=7
                ),
                MissingIndexInfo(
                    table_name="Orders",
                    equality_columns="customer_id",
                    inequality_columns=None,
                    included_columns="order_date",
                    avg_total_user_cost=50.0,
                    avg_user_impact=70.0,
                    user_scans=500,
                    user_seeks=200,
                    create_index_statement="CREATE INDEX IX_Orders_Customer ON Orders (customer_id) INCLUDE (order_date)",
                    create_index_advantage=70.0,
                    meta_data_age=7
                )
            ],
            existing_indexes=[],
            overlapping_indexes=[],
            unused_indexes=[],
            total_wasted_space_mb=0.0,
            metadata_age_days=7,
            warnings=[]
        )
        
        recommendations = analyzer.get_index_recommendations(results, top_n=5)
        
        assert len(recommendations) <= 5
        assert len(recommendations) == 2  # Both indexes should be returned
        
        # Verify ordering by advantage_score (highest first)
        assert recommendations[0]['advantage_score'] >= recommendations[1]['advantage_score']
        assert recommendations[0]['table_name'] == 'Users'
        assert 'create_statement' in recommendations[0]

    def test_get_index_recommendations_limited_results(self, analyzer):
        """Test getting limited number of recommendations"""
        results = IndexAnalysisResults(
            missing_indexes=[
                MissingIndexInfo(
                    table_name=f"Table{i}",
                    equality_columns="col1",
                    inequality_columns=None,
                    included_columns=None,
                    avg_total_user_cost=100.0,
                    avg_user_impact=float(90 - i),
                    user_scans=100,
                    user_seeks=50,
                    create_index_statement=f"CREATE INDEX IX_Table{i}...",
                    create_index_advantage=float(90 - i),
                    meta_data_age=7
                ) for i in range(15)  # Create 15 missing indexes
            ],
            existing_indexes=[],
            overlapping_indexes=[],
            unused_indexes=[],
            total_wasted_space_mb=0.0,
            metadata_age_days=7,
            warnings=[]
        )
        
        recommendations = analyzer.get_index_recommendations(results, top_n=3)
        
        assert len(recommendations) == 3
        # Verify highest impact indexes are returned
        assert recommendations[0]['advantage_score'] == 90.0
        assert recommendations[1]['advantage_score'] == 89.0
        assert recommendations[2]['advantage_score'] == 88.0

    def test_get_maintenance_recommendations_with_fragmented_indexes(self, analyzer):
        """Test maintenance recommendations for fragmented indexes"""
        results = IndexAnalysisResults(
            missing_indexes=[],
            existing_indexes=[
                ExistingIndexInfo(
                    table_name="Users",
                    index_name="IX_Users_Email",
                    index_type="NONCLUSTERED",
                    is_unique=False,
                    is_primary_key=False,
                    has_filter=False,
                    index_columns="email",
                    included_columns=None,
                    record_count=100000,
                    fragmentation_percent=85.5,  # High fragmentation
                    user_lookups=1000,
                    user_scans=500,
                    user_seeks=2000,
                    user_updates=100,
                    meta_data_age=7
                ),
                ExistingIndexInfo(
                    table_name="Orders",
                    index_name="IX_Orders_Date",
                    index_type="NONCLUSTERED",
                    is_unique=False,
                    is_primary_key=False,
                    has_filter=False,
                    index_columns="order_date",
                    included_columns=None,
                    record_count=50000,
                    fragmentation_percent=25.0,  # Medium fragmentation
                    user_lookups=500,
                    user_scans=200,
                    user_seeks=1000,
                    user_updates=50,
                    meta_data_age=7
                )
            ],
            overlapping_indexes=[],
            unused_indexes=[],
            total_wasted_space_mb=0.0,
            metadata_age_days=7,
            warnings=[]
        )
        
        recommendations = analyzer.get_maintenance_recommendations(results)
        
        assert len(recommendations) == 2
        
        # Check high fragmentation recommendation (should be REBUILD)
        high_frag_rec = next(r for r in recommendations if r['table_name'] == 'Users')
        assert 'REBUILD' in high_frag_rec['recommendation']
        assert high_frag_rec['type'] == 'Rebuild Fragmented Index'
        
        # Check medium fragmentation recommendation (should be REORGANIZE)
        med_frag_rec = next(r for r in recommendations if r['table_name'] == 'Orders')
        assert 'REORGANIZE' in med_frag_rec['recommendation']
        assert med_frag_rec['type'] == 'Reorganize Fragmented Index'

    def test_get_maintenance_recommendations_no_maintenance_needed(self, analyzer):
        """Test maintenance recommendations when no maintenance is needed"""
        results = IndexAnalysisResults(
            missing_indexes=[],
            existing_indexes=[
                ExistingIndexInfo(
                    table_name="Users",
                    index_name="IX_Users_Email",
                    index_type="NONCLUSTERED",
                    is_unique=False,
                    is_primary_key=False,
                    has_filter=False,
                    index_columns="email",
                    included_columns=None,
                    record_count=100000,
                    fragmentation_percent=5.0,  # Low fragmentation
                    user_lookups=1000,
                    user_scans=500,
                    user_seeks=2000,
                    user_updates=100,
                    meta_data_age=7
                )
            ],
            overlapping_indexes=[],
            unused_indexes=[],
            total_wasted_space_mb=0.0,
            metadata_age_days=7,
            warnings=[]
        )
        
        recommendations = analyzer.get_maintenance_recommendations(results)
        
        # No maintenance recommendations for low fragmentation
        assert len(recommendations) == 0

    def test_index_analysis_settings_defaults(self):
        """Test IndexAnalysisSettings default values"""
        settings = IndexAnalysisSettings()
        
        assert settings.db_id == "DB_ID()"
        assert settings.min_advantage == 80
        assert settings.get_selectability == False
        assert settings.drop_tmp_table == True
        assert settings.meta_age == -1
        assert settings.only_index_analysis == True
        assert settings.limit_to_tablename == ""
        assert settings.limit_to_indexname == ""

    def test_index_analysis_settings_custom(self):
        """Test IndexAnalysisSettings with custom values"""
        settings = IndexAnalysisSettings(
            min_advantage=90,
            get_selectability=True,
            limit_to_tablename="TestTable",
            meta_age=30
        )
        
        assert settings.min_advantage == 90
        assert settings.get_selectability == True
        assert settings.limit_to_tablename == "TestTable"
        assert settings.meta_age == 30
        # Defaults should still apply
        assert settings.db_id == "DB_ID()"
        assert settings.drop_tmp_table == True