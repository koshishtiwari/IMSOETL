#!/usr/bin/env python3
"""Script to fix datetime.utcnow() deprecation warnings."""

import os
import re
from pathlib import Path

def fix_datetime_in_file(file_path):
    """Fix datetime.utcnow() issues in a single file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Check if datetime import needs timezone
        if 'datetime.utcnow()' in content and 'from datetime import datetime, timezone' not in content:
            # Replace the import
            if 'from datetime import datetime' in content:
                content = re.sub(
                    r'from datetime import datetime(?!\s*,\s*timezone)',
                    'from datetime import datetime, timezone',
                    content
                )
        
        # Replace datetime.utcnow() with datetime.now(timezone.utc)
        content = re.sub(r'datetime\.utcnow\(\)', 'datetime.now(timezone.utc)', content)
        
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            print(f"Fixed: {file_path}")
            return True
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function to fix all Python files."""
    src_dir = Path('src')
    fixed_count = 0
    
    for py_file in src_dir.rglob('*.py'):
        if fix_datetime_in_file(py_file):
            fixed_count += 1
    
    print(f"Fixed {fixed_count} files")

if __name__ == '__main__':
    main()
