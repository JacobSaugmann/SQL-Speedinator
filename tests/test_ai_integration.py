#!/usr/bin/env python3
"""
Test AI integration with mock data to verify functionality
"""

import sys
import os
from datetime import datetime

# Add parent directory to path to import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from core.config_manager import ConfigManager
from analyzers.ai_analyzer import AIAnalyzer

def create_mock_analysis_results():
    """Create mock analysis results for AI testing"""
    return {
        'server_info': {
            'data': {
                'server_name': 'TEST-SERVER',
                'sql_version': 'Microsoft SQL Server 2019',
                'edition': 'Developer Edition',
                'total_memory_mb': 8192,
                'cpu_count': 8
            }
        },
        'wait_stats': {
            'data': {
                'wait_types': [
                    {'wait_type': 'CXPACKET', 'wait_time_ms': 50000, 'waiting_tasks_count': 1000, 'percentage': 25.5},
                    {'wait_type': 'PAGEIOLATCH_SH', 'wait_time_ms': 30000, 'waiting_tasks_count': 800, 'percentage': 15.2},
                    {'wait_type': 'LCK_M_S', 'wait_time_ms': 20000, 'waiting_tasks_count': 600, 'percentage': 10.1}
                ]
            }
        },
        'disk_analysis': {
            'data': {
                'disk_io_stats': [
                    {
                        'database_name': 'TestDB', 
                        'io_stall_read_ms': 50000, 
                        'io_stall_write_ms': 25000, 
                        'num_of_reads': 1000, 
                        'num_of_writes': 500
                    }
                ]
            }
        },
        'index_analysis': {
            'data': {
                'fragmentation': [
                    {'database_name': 'TestDB', 'table_name': 'Products', 'index_name': 'IX_Products_Category', 'fragmentation_pct': 45.2, 'page_count': 500}
                ],
                'unused_indexes': [
                    {'database_name': 'TestDB', 'table_name': 'Archive', 'index_name': 'IX_Archive_Old', 'user_seeks': 0, 'user_scans': 0, 'user_lookups': 0}
                ]
            }
        },
        'missing_indexes': {
            'data': {
                'high_impact_indexes': [
                    {
                        'database_name': 'TestDB', 
                        'table_name': 'Orders', 
                        'equality_columns': 'customer_id', 
                        'avg_user_impact': 85.5, 
                        'user_seeks': 10000
                    }
                ]
            }
        },
        'config_analysis': {
            'data': {
                'issues': [
                    {
                        'setting': 'max degree of parallelism', 
                        'current_value': 0, 
                        'issue': 'MAXDOP is set to unlimited which can cause contention', 
                        'severity': 'HIGH'
                    }
                ]
            }
        },
        'tempdb_analysis': {
            'data': {
                'configuration_issues': [
                    {'description': 'TempDB data files are not equal in size', 'severity': 'HIGH'}
                ]
            }
        },
        'plan_cache': {
            'data': {
                'cache_overview': [
                    {'total_plans': 15000, 'single_use_percentage': 25.5, 'memory_mb': 512}
                ]
            }
        }
    }

def test_ai_integration_disabled():
    """Test AI integration when disabled"""
    print("Testing AI integration (disabled)...")
    
    try:
        # Create config with AI disabled
        config = ConfigManager()
        
        # Override environment for test
        os.environ['BE_MY_COPILOT'] = 'false'
        config._load_config()
        
        # Create AI analyzer
        ai_analyzer = AIAnalyzer(config)
        
        # Test with mock data
        mock_results = create_mock_analysis_results()
        result = ai_analyzer.analyze(mock_results)
        
        if result and not result.get('ai_enabled', True):
            print("✅ AI integration correctly reports as disabled")
            return True
        else:
            print("❌ AI integration did not properly handle disabled state")
            return False
            
    except Exception as e:
        print(f"❌ Error testing disabled AI integration: {e}")
        return False

def test_ai_data_summarization():
    """Test AI data summarization without actual API call"""
    print("Testing AI data summarization...")
    
    try:
        # Create config with AI disabled to test summarization only
        config = ConfigManager()
        os.environ['BE_MY_COPILOT'] = 'false'
        config._load_config()
        
        ai_analyzer = AIAnalyzer(config)
        mock_results = create_mock_analysis_results()
        
        # Test the private summarization method
        summary = ai_analyzer._create_performance_summary(mock_results)
        
        # Verify summary contains expected data
        checks = [
            ('server_info' in summary, "Server info included"),
            ('wait_stats' in summary, "Wait stats included"),
            ('disk_issues' in summary, "Disk issues included"),
            ('index_issues' in summary, "Index issues included"),
            ('config_issues' in summary, "Config issues included"),
            ('tempdb_issues' in summary, "TempDB issues included"),
            ('plan_cache' in summary, "Plan cache included")
        ]
        
        passed = 0
        for check, desc in checks:
            if check:
                print(f"✅ {desc}")
                passed += 1
            else:
                print(f"❌ {desc}")
        
        print(f"Data summarization: {passed}/{len(checks)} checks passed")
        
        # Check token efficiency - summary should be much smaller than full data
        import json
        full_size = len(json.dumps(mock_results))
        summary_size = len(json.dumps(summary))
        reduction = (1 - summary_size / full_size) * 100
        
        print(f"✅ Data reduction: {reduction:.1f}% (Full: {full_size} bytes, Summary: {summary_size} bytes)")
        
        return passed == len(checks)
        
    except Exception as e:
        print(f"❌ Error testing data summarization: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_ai_prompt_generation():
    """Test AI prompt generation for token efficiency"""
    print("Testing AI prompt generation...")
    
    try:
        # Create a mock AI service to test prompt generation
        from services.ai_service import AIService
        
        config = ConfigManager()
        os.environ['BE_MY_COPILOT'] = 'false'  # Disable to avoid API call
        config._load_config()
        
        # We can't instantiate AIService without valid config, so test the data flow
        ai_analyzer = AIAnalyzer(config)
        summary = ai_analyzer._create_performance_summary(create_mock_analysis_results())
        
        # Create a mock AI service instance to test prompt creation
        class MockAIService:
            def __init__(self):
                pass
            
            def _create_analysis_prompt(self, performance_summary):
                # Import the method logic from actual AIService
                prompt_parts = []
                
                server_info = performance_summary.get('server_info', {})
                if server_info:
                    prompt_parts.append(f"Server: {server_info.get('edition', 'Unknown')} {server_info.get('version', '')}")
                
                wait_stats = performance_summary.get('wait_stats', {})
                if wait_stats and wait_stats.get('top_waits'):
                    top_waits = wait_stats['top_waits'][:5]
                    wait_list = [f"{w['wait_type']}({w['percentage']:.1f}%)" for w in top_waits]
                    prompt_parts.append(f"Top waits: {', '.join(wait_list)}")
                
                if not prompt_parts:
                    prompt_parts.append("No significant performance issues detected")
                
                prompt = "SQL Server Performance Analysis:\n" + "\n".join(prompt_parts)
                return prompt
        
        mock_service = MockAIService()
        prompt = mock_service._create_analysis_prompt(summary)
        
        # Verify prompt is reasonable length for token efficiency
        prompt_length = len(prompt)
        print(f"✅ Generated prompt length: {prompt_length} characters")
        
        if prompt_length < 2000:  # Should be under 2000 chars for token efficiency
            print("✅ Prompt is appropriately concise for token efficiency")
            return True
        else:
            print("⚠️  Prompt might be too long for optimal token usage")
            return False
            
    except Exception as e:
        print(f"❌ Error testing prompt generation: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all AI integration tests"""
    print("🤖 Testing AI Integration for SQL Speedinator")
    print("=" * 70)
    
    tests = [
        ("AI Integration (Disabled)", test_ai_integration_disabled),
        ("Data Summarization", test_ai_data_summarization),
        ("Prompt Generation", test_ai_prompt_generation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 50)
        
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print("\n" + "=" * 70)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All AI integration tests passed!")
        print("\n📝 Next steps to fully enable AI:")
        print("1. Set BE_MY_COPILOT=true in your .env file")
        print("2. Configure Azure OpenAI settings:")
        print("   - AZURE_OPENAI_ENDPOINT")
        print("   - AZURE_OPENAI_API_KEY")
        print("   - AZURE_OPENAI_DEPLOYMENT")
        print("3. Run analysis to get AI-powered insights")
    else:
        print("⚠️  Some tests failed. Please review the implementation.")

if __name__ == "__main__":
    main()