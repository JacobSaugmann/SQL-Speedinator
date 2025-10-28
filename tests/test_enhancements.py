#!/usr/bin/env python3
"""
Test the enhancements to the PDF report generator
"""

import sys
import os
from datetime import datetime

# Add parent directory to path to import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from reports.pdf_report_generator import PDFReportGenerator

def create_test_data():
    """Create test data for all sections"""
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
                ],
                'current_active_waits': [
                    {'session_id': 52, 'wait_type': 'PAGEIOLATCH_SH', 'wait_time': 5000, 'resource': 'database_id:5'},
                    {'session_id': 68, 'wait_type': 'LCK_M_X', 'wait_time': 2000, 'resource': 'object_id:123456'}
                ]
            }
        },
        'disk_analysis': {
            'data': {
                'disk_io_stats': [
                    {'database_name': 'TestDB', 'io_stall_read_ms': 1500, 'io_stall_write_ms': 800, 'num_of_reads': 10000, 'num_of_writes': 5000},
                    {'database_name': 'TempDB', 'io_stall_read_ms': 300, 'io_stall_write_ms': 200, 'num_of_reads': 3000, 'num_of_writes': 2000}
                ],
                'file_config': [
                    {'database_name': 'TestDB', 'file_name': 'TestDB_Data', 'size_mb': 2048, 'growth': '10%', 'max_size': 'Unlimited'},
                    {'database_name': 'TestDB', 'file_name': 'TestDB_Log', 'size_mb': 512, 'growth': '64MB', 'max_size': '2048MB'}
                ],
                'volume_info': [
                    {'volume_mount_point': 'C:\\', 'total_bytes': 500000000000, 'available_bytes': 100000000000},
                    {'volume_mount_point': 'D:\\', 'total_bytes': 1000000000000, 'available_bytes': 500000000000}
                ]
            }
        },
        'index_analysis': {
            'data': {
                'index_usage': [
                    {'database_name': 'TestDB', 'table_name': 'Orders', 'index_name': 'IX_Orders_Date', 'user_seeks': 5000, 'user_scans': 100, 'user_lookups': 2000},
                    {'database_name': 'TestDB', 'table_name': 'Customers', 'index_name': 'IX_Customers_Name', 'user_seeks': 3000, 'user_scans': 50, 'user_lookups': 1000}
                ],
                'fragmentation': [
                    {'database_name': 'TestDB', 'table_name': 'Orders', 'index_name': 'PK_Orders', 'fragmentation_pct': 15.5, 'page_count': 1000},
                    {'database_name': 'TestDB', 'table_name': 'Products', 'index_name': 'IX_Products_Category', 'fragmentation_pct': 45.2, 'page_count': 500}
                ],
                'unused_indexes': [
                    {'database_name': 'TestDB', 'table_name': 'Archive', 'index_name': 'IX_Archive_Old', 'user_seeks': 0, 'user_scans': 0, 'user_lookups': 0}
                ],
                'duplicate_indexes': [
                    {'database_name': 'TestDB', 'table_name': 'Orders', 'index_1': 'IX_Orders_Customer', 'index_2': 'IX_Orders_CustomerID', 'columns': 'customer_id'}
                ],
                'maintenance_recommendations': [
                    {'database_name': 'TestDB', 'table_name': 'Products', 'index_name': 'IX_Products_Category', 'action': 'REBUILD', 'reason': 'High fragmentation (45.2%)'},
                    {'database_name': 'TestDB', 'table_name': 'Orders', 'index_name': 'PK_Orders', 'action': 'REORGANIZE', 'reason': 'Medium fragmentation (15.5%)'}
                ]
            }
        },
        'missing_indexes': {
            'data': {
                'high_impact_indexes': [
                    {'database_name': 'TestDB', 'table_name': 'Orders', 'equality_columns': 'customer_id', 'inequality_columns': 'order_date', 'included_columns': 'total_amount', 'avg_user_impact': 85.5, 'user_seeks': 10000, 'avg_total_user_cost': 125.5, 'priority': 'HIGH'},
                    {'database_name': 'TestDB', 'table_name': 'Products', 'equality_columns': 'category_id', 'inequality_columns': '', 'included_columns': 'product_name,price', 'avg_user_impact': 72.3, 'user_seeks': 8000, 'avg_total_user_cost': 98.2, 'priority': 'HIGH'}
                ],
                'medium_impact_indexes': [
                    {'database_name': 'TestDB', 'table_name': 'Customers', 'equality_columns': 'city', 'inequality_columns': '', 'included_columns': 'customer_name', 'avg_user_impact': 45.2, 'user_seeks': 3000, 'avg_total_user_cost': 55.1, 'priority': 'MEDIUM'}
                ],
                'low_impact_indexes': [
                    {'database_name': 'TestDB', 'table_name': 'Archive', 'equality_columns': 'status', 'inequality_columns': '', 'included_columns': '', 'avg_user_impact': 15.1, 'user_seeks': 500, 'avg_total_user_cost': 10.5, 'priority': 'LOW'}
                ]
            }
        },
        'config_analysis': {
            'data': {
                'issues': [
                    {'setting': 'max degree of parallelism', 'current_value': 0, 'issue': 'MAXDOP is set to unlimited which can cause contention', 'severity': 'HIGH', 'recommendation': 'Set to number of cores per NUMA node'},
                    {'setting': 'cost threshold for parallelism', 'current_value': 5, 'issue': 'Very low threshold may cause unnecessary parallelism', 'severity': 'MEDIUM', 'recommendation': 'Consider increasing to 25-50'}
                ],
                'all_settings': [
                    {'name': 'max degree of parallelism', 'value': 0},
                    {'name': 'cost threshold for parallelism', 'value': 5},
                    {'name': 'max server memory (MB)', 'value': 6144}
                ]
            }
        },
        'tempdb_analysis': {
            'data': {
                'tempdb_files': [
                    {'name': 'tempdev', 'type_desc': 'ROWS', 'size_mb': 1024, 'growth': '64MB', 'max_size': 'Unlimited', 'physical_name': 'C:\\Data\\tempdb.mdf'},
                    {'name': 'temp2', 'type_desc': 'ROWS', 'size_mb': 1024, 'growth': '64MB', 'max_size': 'Unlimited', 'physical_name': 'D:\\Data\\tempdb2.ndf'},
                    {'name': 'templog', 'type_desc': 'LOG', 'size_mb': 256, 'growth': '64MB', 'max_size': '2048MB', 'physical_name': 'C:\\Logs\\templog.ldf'}
                ],
                'configuration_issues': [
                    {'description': 'TempDB data files are not equal in size', 'severity': 'HIGH'},
                    {'description': 'TempDB files should be on separate drives', 'severity': 'MEDIUM'}
                ],
                'usage_stats': {
                    'used_mb': 512
                }
            }
        },
        'plan_cache': {
            'data': {
                'cache_overview': [
                    {'total_plans': 15000, 'single_use_percentage': 25.5, 'memory_mb': 512, 'cache_hit_ratio': 95.2}
                ],
                'reuse_analysis': {
                    'reuse_efficiency': 'FAIR',
                    'recommendations': [
                        'Consider using parameterized queries to improve plan reuse',
                        'Monitor single-use plans and optimize frequent queries'
                    ]
                },
                'expensive_queries': [
                    {'query_text': 'SELECT * FROM Orders o JOIN Customers c ON o.customer_id = c.id WHERE o.order_date > ?', 'execution_count': 5000, 'total_worker_time': 150000000, 'avg_elapsed_time': 25000, 'total_logical_reads': 500000},
                    {'query_text': 'UPDATE Products SET last_updated = GETDATE() WHERE category_id = ?', 'execution_count': 2000, 'total_worker_time': 80000000, 'avg_elapsed_time': 15000, 'total_logical_reads': 200000}
                ]
            }
        },
        'ai_analysis': {
            'ai_enabled': True,
            'model_used': 'gpt-4',
            'analysis': {
                'bottlenecks': [
                    {
                        'issue': 'High CXPACKET waits (25.5%) indicate excessive parallelism causing thread contention',
                        'impact': 'HIGH',
                        'recommendation': 'Review MAXDOP setting and cost threshold for parallelism. Consider setting MAXDOP to number of cores per NUMA node.'
                    },
                    {
                        'issue': 'PAGEIOLATCH_SH waits (15.2%) suggest I/O subsystem bottleneck',
                        'impact': 'HIGH',
                        'recommendation': 'Investigate disk performance, consider adding more memory, and optimize slow queries causing excessive I/O.'
                    },
                    {
                        'issue': 'High index fragmentation (45.2%) on IX_Products_Category affecting query performance',
                        'impact': 'MEDIUM',
                        'recommendation': 'Implement regular index maintenance schedule and rebuild highly fragmented indexes during maintenance windows.'
                    }
                ],
                'summary': 'This SQL Server instance shows classic signs of parallelism contention and I/O pressure. The combination of high CXPACKET and PAGEIOLATCH waits, along with significant index fragmentation, suggests immediate attention to MAXDOP configuration and index maintenance strategy. Consider implementing automated maintenance and monitoring query execution plans for optimization opportunities.'
            },
            'tokens_used': 850,
            'generated_at': '2025-10-27T15:30:00'
        }
    }

def test_pdf_generation():
    """Test PDF generation with enhanced data"""
    print("Testing enhanced PDF report generation...")
    
    try:
        # Create test data
        analysis_results = create_test_data()
        
        # Create a simple config
        from core.config_manager import ConfigManager
        config = ConfigManager()
        
        # Create PDF generator
        generator = PDFReportGenerator(config)
        
        # Generate report
        output_file = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        generator.generate_report(analysis_results, output_file, "TEST-SERVER")
        
        print(f"✅ Enhanced PDF report generated successfully: {output_file}")
        
        # Check if file exists and has content
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file)
            print(f"✅ File size: {file_size:,} bytes")
            
            if file_size > 50000:  # Should be substantial with all the enhancements
                print("✅ Report appears to contain comprehensive data")
            else:
                print("⚠️  Report file seems small - may be missing data")
        else:
            print("❌ Report file was not created")
            
    except Exception as e:
        print(f"❌ Error generating report: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pdf_generation()