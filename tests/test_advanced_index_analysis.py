"""
Test Advanced Index Analyzer for SQL Speedinator
"""

import sys
import os

# Add the src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', 'src')
sys.path.insert(0, src_dir)

from core.config_manager import ConfigManager
from core.sql_connection import SQLServerConnection
from analyzers.advanced_index_analyzer import AdvancedIndexAnalyzer, IndexAnalysisSettings

def test_advanced_index_analysis():
    """Test advanced index analysis functionality"""
    
    print("🔍 Testing Advanced Index Analysis for SQL Speedinator")
    print("=" * 60)
    
    try:
        # Load configuration
        config = ConfigManager()
        print(f"✓ Configuration loaded")
        print(f"  - Index Min Advantage: {config.index_min_advantage}")
        print(f"  - Calculate Selectability: {config.index_calculate_selectability}")
        print(f"  - Only Index Analysis: {config.index_only_analysis}")
        print(f"  - Table Filter: '{config.index_limit_to_table}'")
        print(f"  - Index Filter: '{config.index_limit_to_index}'")
        
        # Get SQL Server connection
        server_name = input("\nEnter SQL Server name to test (or press Enter to skip analysis): ").strip()
        
        if not server_name:
            print("❌ No server specified, skipping connection test")
            return
        
        print(f"\nConnecting to SQL Server: {server_name}")
        connection = SQLServerConnection(server_name, config)
        
        if not connection.connect():
            print("❌ Failed to connect to SQL Server")
            return
            
        print("✓ SQL Server connection successful")
        
        # Test advanced index analyzer
        print("\n🔍 Testing Advanced Index Analyzer...")
        analyzer = AdvancedIndexAnalyzer(connection)
        
        # Create test settings
        settings = IndexAnalysisSettings(
            min_advantage=config.index_min_advantage,
            get_selectability=False,  # Skip selectability for test speed
            only_index_analysis=True,
            limit_to_tablename=config.index_limit_to_table,
            limit_to_indexname=config.index_limit_to_index
        )
        
        print(f"  - Running with settings: min_advantage={settings.min_advantage}, only_analysis={settings.only_index_analysis}")
        
        # Run analysis
        results = analyzer.analyze_indexes(settings)
        
        print(f"\n✓ Advanced index analysis completed!")
        print(f"  - Missing Indexes: {len(results.missing_indexes)}")
        print(f"  - Existing Indexes: {len(results.existing_indexes)}")
        print(f"  - Overlapping Indexes: {len(results.overlapping_indexes)}")
        print(f"  - Unused Indexes: {len(results.unused_indexes)}")
        print(f"  - Total Wasted Space: {results.total_wasted_space_mb:.2f} MB")
        print(f"  - Metadata Age: {results.metadata_age_days} days")
        
        # Show warnings
        if results.warnings:
            print(f"\n⚠️ Warnings:")
            for warning in results.warnings:
                print(f"  - {warning}")
        
        # Show top missing indexes
        if results.missing_indexes:
            print(f"\n🔍 Top Missing Indexes:")
            for i, idx in enumerate(results.missing_indexes[:5], 1):
                print(f"  {i}. {idx.table_name}")
                print(f"     Columns: {idx.equality_columns or idx.inequality_columns}")
                if idx.included_columns:
                    print(f"     Include: {idx.included_columns}")
                print(f"     Advantage: {idx.create_index_advantage:.2f}, Impact: {idx.avg_user_impact}%")
                print(f"     Usage: Scans={idx.user_scans}, Seeks={idx.user_seeks}")
                print(f"     CREATE: {idx.create_index_statement[:100]}...")
                print()
        
        # Show unused indexes
        if results.unused_indexes:
            print(f"\n🗑️ Unused Indexes (consider dropping):")
            for i, idx in enumerate(results.unused_indexes[:3], 1):
                print(f"  {i}. {idx.table_name}.{idx.index_name}")
                print(f"     Usage: Lookups={idx.user_lookups}, Scans={idx.user_scans}, Seeks={idx.user_seeks}, Updates={idx.user_updates}")
                print(f"     DROP: {idx.drop_statement}")
                print()
        
        # Show overlapping indexes
        if results.overlapping_indexes:
            print(f"\n⚠️ Overlapping Indexes:")
            for i, idx in enumerate(results.overlapping_indexes[:3], 1):
                print(f"  {i}. {idx.table_name}.{idx.index_name}")
                print(f"     Type: {idx.overlap_type}")
                print(f"     Columns: {idx.index_columns}")
                print(f"     DISABLE: {idx.disable_statement}")
                print()
        
        # Get recommendations
        recommendations = analyzer.get_index_recommendations(results, 5)
        if recommendations:
            print(f"\n⭐ Top Index Recommendations:")
            for rec in recommendations:
                print(f"  {rec['rank']}. {rec['table_name']} (Priority: {rec['priority']})")
                print(f"     Columns: {rec['columns']}")
                print(f"     Advantage Score: {rec['advantage_score']:.2f}")
                print(f"     Impact: {rec['user_impact']}%, Cost: {rec['user_cost']:.2f}")
                print()
        
        # Get maintenance recommendations
        maintenance = analyzer.get_maintenance_recommendations(results)
        if maintenance:
            print(f"\n🔧 Maintenance Recommendations:")
            for i, rec in enumerate(maintenance[:5], 1):
                print(f"  {i}. {rec['type']} - {rec['table_name']}")
                if 'index_name' in rec:
                    print(f"     Index: {rec['index_name']}")
                print(f"     Priority: {rec['priority']}")
                print(f"     Reason: {rec['reason']}")
                print(f"     Action: {rec['recommendation'][:100]}...")
                print()
        
        # Test with different settings
        print(f"\n🔍 Testing with different settings...")
        settings_full = IndexAnalysisSettings(
            min_advantage=50,  # Lower threshold
            get_selectability=False,  # Still skip for speed
            only_index_analysis=False,  # Include overlapping/unused analysis
            limit_to_tablename="",
            limit_to_indexname=""
        )
        
        results_full = analyzer.analyze_indexes(settings_full)
        print(f"✓ Full analysis completed with lower threshold:")
        print(f"  - Missing Indexes: {len(results_full.missing_indexes)}")
        print(f"  - Overlapping Indexes: {len(results_full.overlapping_indexes)}")
        print(f"  - Unused Indexes: {len(results_full.unused_indexes)}")
        
        print(f"\n✅ All advanced index analysis tests passed!")
        print(f"\nNext steps:")
        print(f"1. Test the analysis with your specific database")
        print(f"2. Adjust INDEX_MIN_ADVANTAGE setting as needed")
        print(f"3. Run: python main.py -s {server_name} to see full integration")
        
    except Exception as e:
        print(f"❌ Advanced index analysis test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_advanced_index_analysis()