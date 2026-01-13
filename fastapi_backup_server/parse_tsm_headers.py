#!/usr/bin/env python3
"""
Parse IBM Storage Protect header files to extract structure definitions
"""
import re
import os

def parse_struct(header_content, struct_name):
    """Extract a structure definition from header content"""
    # Pattern to match typedef struct ... } structName;
    pattern = rf'typedef\s+struct\s*{{([^}}]+)}}\s*{struct_name}\s*;'
    match = re.search(pattern, header_content, re.DOTALL)
    
    if match:
        struct_body = match.group(1)
        print(f"\nFound structure: {struct_name}")
        print("=" * 60)
        print(struct_body.strip())
        print("=" * 60)
        return struct_body
    else:
        print(f"Structure {struct_name} not found")
        return None

def find_defines(header_content, prefix):
    """Find #define constants with a given prefix"""
    pattern = rf'#define\s+({prefix}\S+)\s+(.+?)(?:\n|$)'
    matches = re.findall(pattern, header_content)
    
    if matches:
        print(f"\nFound defines with prefix '{prefix}':")
        print("=" * 60)
        for name, value in matches:
            print(f"{name} = {value.strip()}")
        print("=" * 60)
    
    return matches

def main():
    header_dir = "/opt/tivoli/tsm/client/api/bin64/sample"
    
    headers = {
        'dsmapitd.h': ['dsmApiVersionEx', 'dsmInitExIn_t', 'dsmInitExOut_t', 'dsmApiVersion'],
        'dsmrc.h': [],
        'dsmapips.h': []
    }
    
    for header_file, structs in headers.items():
        header_path = os.path.join(header_dir, header_file)
        
        if not os.path.exists(header_path):
            print(f"Header not found: {header_path}")
            continue
        
        print(f"\n{'='*60}")
        print(f"Parsing: {header_file}")
        print(f"{'='*60}")
        
        with open(header_path, 'r', errors='ignore') as f:
            content = f.read()
        
        # Parse structures
        for struct in structs:
            parse_struct(content, struct)
        
        # Find DSM return codes
        if header_file == 'dsmrc.h':
            find_defines(content, 'DSM_RC_')
        
        # Find version info
        if header_file == 'dsmapitd.h':
            find_defines(content, 'DSM_API_VERSION')

if __name__ == "__main__":
    main()