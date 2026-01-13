#!/usr/bin/env python3
"""
Check for IBM Storage Protect (TSM) API files on Linux system
"""
import os
import glob

def check_file_exists(path):
    """Check if file exists and print status"""
    if os.path.exists(path):
        print(f"✓ Found: {path}")
        return True
    else:
        print(f"✗ Missing: {path}")
        return False

def find_files(pattern):
    """Find files matching pattern"""
    files = glob.glob(pattern)
    if files:
        print(f"✓ Found {len(files)} file(s) matching '{pattern}':")
        for f in files:
            print(f"  - {f}")
        return files
    else:
        print(f"✗ No files found matching '{pattern}'")
        return []

print("=" * 60)
print("IBM Storage Protect API File Check")
print("=" * 60)

# Common installation paths
common_paths = [
    "/opt/tivoli/tsm/client/api/bin64",
    "/opt/tivoli/tsm/client/ba/bin",
    "/usr/lib",
    "/usr/local/lib",
]

print("\n1. Checking for shared libraries (.so files):")
print("-" * 60)
found_libs = []
for path in common_paths:
    lib_pattern = os.path.join(path, "libApiTSM64.so*")
    libs = find_files(lib_pattern)
    found_libs.extend(libs)
    
    lib_pattern = os.path.join(path, "libgpfs.so*")
    libs = find_files(lib_pattern)
    found_libs.extend(libs)

print("\n2. Checking for header files:")
print("-" * 60)
header_paths = [
    "/opt/tivoli/tsm/client/api/bin64/sample",
    "/usr/include",
]
found_headers = []
for path in header_paths:
    header_pattern = os.path.join(path, "dsmapi*.h")
    headers = find_files(header_pattern)
    found_headers.extend(headers)
    
    # Also check for specific important headers
    important_headers = ["dsmapitd.h", "dsmapips.h", "dsmrc.h"]
    for header in important_headers:
        header_path = os.path.join(path, header)
        if check_file_exists(header_path):
            found_headers.append(header_path)

print("\n3. Checking environment variables:")
print("-" * 60)
dsmi_dir = os.environ.get('DSMI_DIR')
dsmi_config = os.environ.get('DSMI_CONFIG')
print(f"DSMI_DIR: {dsmi_dir if dsmi_dir else '(not set)'}")
print(f"DSMI_CONFIG: {dsmi_config if dsmi_config else '(not set)'}")

print("\n" + "=" * 60)
print("Summary:")
print("=" * 60)
print(f"Shared libraries found: {len(found_libs)}")
print(f"Header files found: {len(found_headers)}")

if found_libs:
    print("\n✓ You can proceed with Python ctypes integration")
    print(f"  Primary library to use: {found_libs[0]}")
else:
    print("\n✗ IBM Storage Protect API not found")
    print("  Please install the API from IBM Storage Protect backup-archive client")