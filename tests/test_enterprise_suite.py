"""
Comprehensive Test Suite for SQL Speedinator Enterprise Edition
Tests all components: SQL Connection, PerfMon, AI Services, Enterprise Features
"""

import unittest
import time
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import logging

# Add src to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.sql_connection import SQLServerConnection
from src.core.config_manager import ConfigManager
from src.core.performance_analyzer import PerformanceAnalyzer
from src.core.analysis_status_tracker import AnalysisStatusTracker, AnalysisPhase
from src.core.server_performance_protector import ServerPerformanceProtector
from src.services.ai_dialog_system import AIDialogSystem
from src.perfmon.template_manager import PerfMonTemplateManager
from src.services.ai_service import AIService

class TestSQLConnection(unittest.TestCase):
    """Test SQL Server connection functionality"""
    
    @classmethod
    def setUpClass(cls):
        cls.config = ConfigManager()
        cls.server_name = "localhost"
    
    def test_sql_connection_basic(self):
        """Test basic SQL Server connection"""
        with SQLServerConnection(self.server_name, self.config) as conn:
            self.assertTrue(conn.test_connection(), "Should connect to localhost SQL Server")
    
    def test_sql_connection_queries(self):
        """Test SQL query execution"""
        with SQLServerConnection(self.server_name, self.config) as conn:
            if conn.test_connection():
                # Test basic query
                result = conn.execute_query("SELECT @@VERSION as version")
                self.assertIsNotNone(result, "Should return SQL Server version")
                self.assertGreater(len(result), 0, "Should have at least one row")
                
                # Test server info query
                info_result = conn.get_server_info()
                self.assertIsNotNone(info_result, "Should return server info")
                if info_result:
                    self.assertIn('server_name', info_result, "Should contain server name")
    
    def test_sql_error_handling(self):
        """Test SQL error handling"""
        with SQLServerConnection(self.server_name, self.config) as conn:
            if conn.test_connection():
                # Test invalid query
                result = conn.execute_query("SELECT * FROM nonexistent_table")
                self.assertIsNone(result, "Should return None for invalid query")

class TestPerfMonTemplateManager(unittest.TestCase):
    """Test Performance Monitor template management"""
    
    @classmethod
    def setUpClass(cls):
        cls.config = ConfigManager()
        cls.template_manager = PerfMonTemplateManager(cls.config)
        cls.template_file = Path(__file__).parent.parent / "perfmon" / "templates" / "sql_performance_template.xml"
    
    def test_template_parsing(self):
        """Test PerfMon template parsing"""
        if self.template_file.exists():
            template_info = self.template_manager.parse_template(self.template_file)
            self.assertIsNotNone(template_info, "Should parse template successfully")
            self.assertIn('name', template_info, "Should contain template name")
            self.assertIn('counters', template_info, "Should contain counters")
            self.assertGreater(len(template_info['counters']), 0, "Should have counters")
        else:
            self.skipTest("Template file not found")
    
    def test_smart_collection_detection(self):
        """Test smart collection management"""
        existing_collections = self.template_manager.list_existing_collections()
        self.assertIsInstance(existing_collections, list, "Should return list of collections")
    
    def test_collection_matching(self):
        """Test collection matching algorithm"""
        if self.template_file.exists():
            template_info = self.template_manager.parse_template(self.template_file)
            if template_info:
                test_counters = template_info['counters'][:10]  # Test with subset
                matching_collection = self.template_manager.find_matching_collection(test_counters)
                # Should work without error even if no match found
                self.assertIsInstance(matching_collection, (dict, type(None)), "Should return dict or None")

class TestEnterpriseFeatures(unittest.TestCase):
    """Test Enterprise features: Status Tracking, Performance Protection, AI Dialog"""
    
    @classmethod
    def setUpClass(cls):
        cls.config = ConfigManager()
    
    def test_status_tracker(self):
        """Test Analysis Status Tracker"""
        tracker = AnalysisStatusTracker(self.config)
        
        # Test phase management
        tracker.start_analysis("localhost", "test")
        phase_status = tracker.start_phase(AnalysisPhase.INITIALIZATION, 3, "Testing initialization")
        self.assertEqual(phase_status.phase, AnalysisPhase.INITIALIZATION)
        self.assertTrue(phase_status.is_running)
        
        # Test progress updates
        tracker.update_phase_progress(AnalysisPhase.INITIALIZATION, completed_steps=1, current_step="Step 1")
        self.assertEqual(phase_status.completed_steps, 1)
        
        # Test phase completion
        tracker.complete_phase(AnalysisPhase.INITIALIZATION, "completed", "Test completed")
        self.assertTrue(phase_status.is_completed)
        self.assertEqual(phase_status.status, "completed")
        
        # Test status summary
        summary = tracker.get_status_summary()
        self.assertIn('overall_progress', summary)
        self.assertIn('phases', summary)
    
    def test_performance_protector(self):
        """Test Server Performance Protector"""
        # Create protector with test config
        protector = ServerPerformanceProtector(
            connection=None,  # Mock connection for testing
            protection_enabled=True,
            max_cpu_percent=80.0,
            max_memory_percent=85.0,
            max_connections=100,
            max_blocking_sessions=5,
            check_interval_seconds=1
        )
        
        self.assertTrue(protector.thresholds.protection_enabled)
        self.assertEqual(protector.thresholds.max_cpu_percent, 80.0)
        
        # Test status without real monitoring
        status = protector.get_protection_status()
        self.assertIn('protection_enabled', status)
        self.assertIn('thresholds', status)
    
    def test_ai_dialog_system(self):
        """Test AI Dialog System"""
        dialog_system = AIDialogSystem(self.config)
        
        # Test context creation
        context = dialog_system._create_dialog_context("test_server", {"test": "data"})
        self.assertEqual(context.server_name, "test_server")
        self.assertIn("test", context.performance_data)
        
        # Test confidence scoring (mock)
        confidence = dialog_system._calculate_confidence_score("test response")
        self.assertIsInstance(confidence, float)
        self.assertGreaterEqual(confidence, 0.0)
        self.assertLessEqual(confidence, 1.0)

class TestAIService(unittest.TestCase):
    """Test AI Service functionality"""
    
    @classmethod
    def setUpClass(cls):
        cls.config = ConfigManager()
        cls.ai_service = AIService(cls.config)
    
    def test_ai_service_initialization(self):
        """Test AI service initialization"""
        # Should initialize without error even if no API key
        self.assertIsNotNone(self.ai_service)
        enabled = self.ai_service.is_enabled()
        self.assertIsInstance(enabled, bool)

class TestFullAnalysisWorkflow(unittest.TestCase):
    """Test complete analysis workflow"""
    
    @classmethod
    def setUpClass(cls):
        cls.config = ConfigManager()
        cls.server_name = "localhost"
    
    def test_performance_analyzer_workflow(self):
        """Test complete performance analysis workflow"""
        with SQLServerConnection(self.server_name, self.config) as conn:
            if conn.test_connection():
                # Test with night mode for minimal impact
                analyzer = PerformanceAnalyzer(conn, self.config, night_mode=True)
                
                # Run quick analysis
                start_time = time.time()
                results = analyzer.run_full_analysis()
                duration = time.time() - start_time
                
                self.assertIsNotNone(results, "Should return analysis results")
                self.assertIsInstance(results, dict, "Results should be dictionary")
                self.assertLess(duration, 30, "Analysis should complete within 30 seconds")
                
                # Check required sections
                expected_sections = ['server_info', 'disk_performance', 'index_analysis']
                for section in expected_sections:
                    if section in results:
                        self.assertIsNotNone(results[section], f"Section {section} should not be None")
            else:
                self.skipTest("Could not connect to SQL Server")

class TestPerfMonIntegration(unittest.TestCase):
    """Test PerfMon integration with proper timing"""
    
    @classmethod
    def setUpClass(cls):
        cls.config = ConfigManager()
        cls.template_manager = PerfMonTemplateManager(cls.config)
        cls.template_file = Path(__file__).parent.parent / "perfmon" / "templates" / "sql_performance_template.xml"
    
    def test_perfmon_collection_workflow(self):
        """Test PerfMon collection start/stop workflow"""
        if not self.template_file.exists():
            self.skipTest("PerfMon template not found")
        
        # Parse template
        template_info = self.template_manager.parse_template(self.template_file)
        self.assertIsNotNone(template_info, "Should parse template")
        
        if template_info:
            # Create collector set
            collection_name = f"SQLSpeedinatorTest_{int(time.time())}"
            dcs_file = self.template_manager.create_data_collector_set(template_info, collection_name)
            
            if dcs_file and dcs_file.exists():
                # Test very short collection (1 minute minimum)
                collection_result = self.template_manager.start_data_collection(dcs_file, duration_hours=1/60)  # 1 minute
                
                if collection_result.get('success'):
                    print(f"✅ Started test collection: {collection_name}")
                    
                    # Wait a few seconds to verify it's running
                    time.sleep(3)
                    
                    # Stop collection
                    stop_result = self.template_manager.stop_data_collection(collection_name)
                    self.assertTrue(stop_result, "Should stop collection successfully")
                    print(f"✅ Stopped test collection: {collection_name}")
                else:
                    self.skipTest("Could not start PerfMon collection - may need admin privileges")
            else:
                self.skipTest("Could not create data collector set")

def run_comprehensive_tests():
    """Run all tests with detailed output"""
    # Configure logging for tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 Starting SQL Speedinator Enterprise Test Suite")
    print("=" * 60)
    
    # Create test suite
    test_classes = [
        TestSQLConnection,
        TestPerfMonTemplateManager,
        TestEnterpriseFeatures,
        TestAIService,
        TestFullAnalysisWorkflow,
        TestPerfMonIntegration
    ]
    
    suite = unittest.TestSuite()
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print("🎯 Test Summary:")
    print(f"   Tests run: {result.testsRun}")
    print(f"   Failures: {len(result.failures)}")
    print(f"   Errors: {len(result.errors)}")
    print(f"   Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"   - {test}: {traceback.split('AssertionError: ')[-1].split('\n')[0]}")
    
    if result.errors:
        print("\n🚨 ERRORS:")
        for test, traceback in result.errors:
            print(f"   - {test}: {traceback.split('\n')[-2]}")
    
    success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0
    print(f"\n✅ Success Rate: {success_rate:.1f}%")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_comprehensive_tests()
    sys.exit(0 if success else 1)