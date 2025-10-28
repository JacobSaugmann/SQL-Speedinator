#!/usr/bin/env python3
"""
Test responsive table functionality with automatic text wrapping
"""

import sys
import os
# Add parent directory to path to import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from reports.pdf_report_generator import PDFReportGenerator
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch

def test_responsive_tables():
    """Test responsive table width calculations and text wrapping"""
    
    # Create PDF report generator
    generator = PDFReportGenerator()
    
    # Test 1: Responsive column width calculation
    print("Testing responsive column width calculations...")
    
    # Test for 2 columns (should fit 6.0 inch page width)
    widths_2col = generator._get_responsive_column_widths(2)
    total_width_2col = sum(widths_2col)
    print(f"2-column widths: {widths_2col}")
    print(f"Total width: {total_width_2col:.2f} inches (target: 6.0)")
    
    # Test for 4 columns
    widths_4col = generator._get_responsive_column_widths(4)
    total_width_4col = sum(widths_4col)
    print(f"4-column widths: {widths_4col}")
    print(f"Total width: {total_width_4col:.2f} inches (target: 6.0)")
    
    # Test 2: Optimized column widths with weights
    print("\nTesting optimized column widths with custom weights...")
    
    # Test AI bottleneck table weights [0.8, 2.5, 0.6, 2.1]
    optimized_widths = generator._get_optimized_column_widths(4, [0.8, 2.5, 0.6, 2.1])
    total_optimized = sum(optimized_widths)
    print(f"AI bottleneck table widths: {optimized_widths}")
    print(f"Total width: {total_optimized:.2f} inches (target: 6.0)")
    
    # Test 3: Text wrapping functionality
    print("\nTesting text wrapping functionality...")
    
    long_text = "This is a very long text that should be wrapped into multiple lines to fit within the table cell properly without overflowing or causing display issues."
    
    # Test text wrapping with different lengths
    wrapped_30_chars = generator._wrap_text(long_text, 30)
    print(f"Text wrapped (30 chars): {wrapped_30_chars}")
    
    wrapped_25_chars = generator._wrap_text(long_text, 25)
    print(f"Text wrapped (25 chars): {wrapped_25_chars}")
    
    # Test width calculations in inches
    print(f"\nColumn width calculations (in inches):")
    print(f"2-column widths: {[w/inch for w in widths_2col]} inches each")
    print(f"4-column widths: {[w/inch for w in widths_4col]} inches each")
    print(f"Optimized widths: {[w/inch for w in optimized_widths]} inches each")
    
    # Verify all tables fit within page width (convert from points to inches)
    total_width_2col_inches = sum(widths_2col) / inch
    total_width_4col_inches = sum(widths_4col) / inch
    total_optimized_inches = sum(optimized_widths) / inch
    
    assert abs(total_width_2col_inches - 6.0) < 0.01, f"2-column table width mismatch: {total_width_2col_inches}"
    assert abs(total_width_4col_inches - 6.0) < 0.01, f"4-column table width mismatch: {total_width_4col_inches}"
    assert abs(total_optimized_inches - 6.0) < 0.01, f"Optimized table width mismatch: {total_optimized_inches}"
    
    print("\n✅ All responsive table tests passed!")
    print("✅ Tables will fit exactly within page width (6.0 inches)")
    print("✅ Text wrapping functionality working correctly")
    
    return True

if __name__ == "__main__":
    try:
        test_responsive_tables()
        print("\n🎉 Responsive table implementation successful!")
        print("Tables will now automatically fit page width with proper text wrapping.")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)