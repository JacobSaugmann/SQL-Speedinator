"""
Quick Test for SQL Speedinator - Test localhost connection and basic functionality
"""

import sys
from pathlib import Path
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_sql_connection():
    """Test basic SQL Server connection to localhost"""
    print("🔌 Testing SQL Server connection to localhost...")
    
    try:
        from src.core.config_manager import ConfigManager
        from src.core.sql_connection import SQLServerConnection
        
        config = ConfigManager()
        
        with SQLServerConnection("localhost", config) as conn:
            if conn.test_connection():
                print("   ✅ SQL Server connection successful")
                
                # Test basic query
                result = conn.execute_query("SELECT @@VERSION as version")
                if result and len(result) > 0:
                    version = result[0].get('version', 'Unknown')
                    print(f"   📊 SQL Server Version: {version[:50]}...")
                    return True
                else:
                    print("   ❌ Failed to get SQL Server version")
                    return False
            else:
                print("   ❌ Failed to connect to SQL Server")
                return False
                
    except Exception as e:
        print(f"   ❌ Connection test failed: {e}")
        return False

def test_perfmon_template():
    """Test PerfMon template parsing"""
    print("📊 Testing PerfMon template parsing...")
    
    try:
        from src.core.config_manager import ConfigManager
        from src.perfmon.template_manager import PerfMonTemplateManager
        
        config = ConfigManager()
        template_manager = PerfMonTemplateManager(config)
        template_file = Path(__file__).parent.parent / "perfmon" / "templates" / "sql_performance_template.xml"
        
        if template_file.exists():
            template_info = template_manager.parse_template(template_file)
            if template_info:
                counter_count = len(template_info.get('counters', []))
                print(f"   ✅ Template parsed successfully: {counter_count} counters")
                return True
            else:
                print("   ❌ Failed to parse template")
                return False
        else:
            print(f"   ❌ Template file not found: {template_file}")
            return False
            
    except Exception as e:
        print(f"   ❌ Template test failed: {e}")
        return False

def test_performance_analysis():
    """Test quick performance analysis"""
    print("⚡ Testing performance analysis...")
    
    try:
        from src.core.config_manager import ConfigManager
        from src.core.sql_connection import SQLServerConnection
        from src.core.performance_analyzer import PerformanceAnalyzer
        
        config = ConfigManager()
        
        with SQLServerConnection("localhost", config) as conn:
            if conn.test_connection():
                # Run quick analysis in night mode
                analyzer = PerformanceAnalyzer(conn, config, night_mode=True)
                
                start_time = time.time()
                results = analyzer.run_full_analysis()
                duration = time.time() - start_time
                
                if results:
                    print(f"   ✅ Analysis completed in {duration:.2f} seconds")
                    
                    # Check key sections
                    sections = ['server_info', 'disk_performance', 'index_analysis']
                    for section in sections:
                        if section in results and results[section]:
                            print(f"   📈 {section.replace('_', ' ').title()}: ✓")
                    
                    return True
                else:
                    print("   ❌ Analysis returned no results")
                    return False
            else:
                print("   ❌ Could not connect to SQL Server")
                return False
                
    except Exception as e:
        print(f"   ❌ Analysis test failed: {e}")
        return False

def test_enterprise_features():
    """Test enterprise features"""
    print("🏢 Testing enterprise features...")
    
    try:
        from src.core.config_manager import ConfigManager
        from src.core.analysis_status_tracker import AnalysisStatusTracker, AnalysisPhase
        
        config = ConfigManager()
        
        # Test status tracker
        tracker = AnalysisStatusTracker(config)
        tracker.start_analysis("localhost", "test")
        
        phase_status = tracker.start_phase(AnalysisPhase.INITIALIZATION, 2, "Testing")
        tracker.update_phase_progress(AnalysisPhase.INITIALIZATION, completed_steps=1)
        tracker.complete_phase(AnalysisPhase.INITIALIZATION, "completed")
        
        summary = tracker.get_status_summary()
        
        if summary and 'overall_progress' in summary:
            print("   ✅ Status Tracker: Working")
        else:
            print("   ❌ Status Tracker: Failed")
            return False
        
        # Test AI Dialog System
        from src.services.ai_dialog_system import AIDialogSystem
        dialog_system = AIDialogSystem(None, config)  # Pass None for ai_service, config for config
        
        if dialog_system:
            print("   ✅ AI Dialog System: Initialized")
        else:
            print("   ❌ AI Dialog System: Failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"   ❌ Enterprise features test failed: {e}")
        return False

def test_main_application():
    """Test main application help"""
    print("🚀 Testing main application...")
    
    try:
        import subprocess
        
        # Test help command
        result = subprocess.run([sys.executable, "main.py", "--help"], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and "SQL Speedinator" in result.stdout:
            print("   ✅ Main application: Help working")
            return True
        else:
            print("   ❌ Main application: Help failed")
            print(f"   Return code: {result.returncode}")
            if result.stderr:
                print(f"   Error: {result.stderr[:100]}")
            return False
            
    except Exception as e:
        print(f"   ❌ Main application test failed: {e}")
        return False

def run_quick_tests():
    """Run all quick tests"""
    print("🧪 SQL Speedinator - Quick Test Suite")
    print("=" * 50)
    
    tests = [
        ("SQL Connection", test_sql_connection),
        ("PerfMon Template", test_perfmon_template),
        ("Performance Analysis", test_performance_analysis),
        ("Enterprise Features", test_enterprise_features),
        ("Main Application", test_main_application)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            success = test_func()
            results.append(success)
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📋 Test Results:")
    
    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"   {test_name}: {status}")
    
    passed = sum(results)
    total = len(results)
    success_rate = (passed / total) * 100
    
    print(f"\n🎯 Summary: {passed}/{total} tests passed ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        print("🎉 System is ready for use!")
        return True
    else:
        print("⚠️  Some issues detected - check failed tests")
        return False

if __name__ == "__main__":
    success = run_quick_tests()
    sys.exit(0 if success else 1)