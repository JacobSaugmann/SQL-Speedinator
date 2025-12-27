#!/usr/bin/env python3
"""
Systematically fix type annotation issues in SQL Speedinator codebase
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple

fixes: Dict[str, List[Tuple[str, str]]] = {
    # Wait stats analyzer - 15 errors
    'src/analyzers/wait_stats_analyzer.py': [
        # Line 177: analysis without type annotation
        (r'(\s+)analysis = \{', r'\1analysis: Dict[str, Any] = {'),
        # Lines 206-214: recommendations without type or with wrong type
        (r'(\s+)recommendations(\[\w+\]) = ', r'\1recommendations\2: List[str] = '),
    ],
    
    # Disk analyzer - 11 errors  
    'src/analyzers/disk_analyzer.py': [
        # problem_disks should be set or list
        (r'(\s+)problem_disks = (\{\}|set\(\)|list\(\))', r'\1problem_disks: set[str] = set()'),
    ],
    
    # Log analyzer - 4 errors
    'src/analyzers/log_analyzer.py': [
        (r'(\s+)results = \{\}', r'\1results: Dict[str, List[str]] = {}'),
        (r'(\s+)categorized = \{\}', r'\1categorized: Dict[str, List[Dict[str, Any]]] = {}'),
        (r'(\s+)breakdown = \{\}', r'\1breakdown: Dict[str, int] = {}'),
    ],
    
    # Template manager - 3 errors
    'src/perfmon/template_manager.py': [
        (r'(\s+)counter_list = \[\]', r'\1counter_list: List[str] = []'),
    ],
    
    # SQL connection - 2 errors (None checks)
    'src/core/sql_connection.py': [
        # Already fixed by initializing with proper type
    ],
    
    # Text formatter - 1 error
    'src/reports/text_formatter.py': [
        (r'(\s+)current_line = \[\]', r'\1current_line: List[str] = []'),
    ],
    
    # File cleanup manager - 8 errors
    'src/core/file_cleanup_manager.py': [
        (r'(\s+)removed_files = \[\]', r'\1removed_files: List[str] = []'),
        (r'(\s+)cleaned_size = 0', r'\1cleaned_size: int = 0'),
    ],
}

def apply_fixes():
    """Apply all type annotation fixes"""
    root = Path('.')
    
    for filepath_str, replacements in fixes.items():
        filepath = root / filepath_str
        
        if not filepath.exists():
            print(f"⚠️  File not found: {filepath_str}")
            continue
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            for pattern, replacement in replacements:
                content = re.sub(pattern, replacement, content)
            
            if content != original_content:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"✅ Fixed {filepath_str}")
            else:
                print(f"⏭️  No changes needed for {filepath_str}")
                
        except Exception as e:
            print(f"❌ Error fixing {filepath_str}: {e}")

if __name__ == '__main__':
    apply_fixes()
    print("\n✨ Type annotation fixes applied!")
