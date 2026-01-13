#!/usr/bin/env python3
"""
IBM Storage Protect API Test - WITH AUTHENTICATION
Tests dsmSetUp and dsmInitEx API calls with proper credentials
"""
import ctypes
import os
import sys
import getpass
from ctypes import *

# DSM API Return Codes
DSM_RC_SUCCESSFUL = 0
DSM_RC_UNSUCCESSFUL = 1
DSM_RC_AUTH_FAILURE = 137

# Structure version constants
apiVersionExVer = 2
dsmInitExInVersion = 5
dsmInitExOutVersion = 4

# Constants
DSM_MAX_SERVERNAME_LENGTH = 64

print("="*70)
print("IBM Storage Protect API Test - WITH AUTHENTICATION")
print("="*70)

# Step 1: Check files
print("\n[STEP 1] Checking files...")
lib_path = "/opt/tivoli/tsm/client/api/bin64/libApiTSM64.so"
config_file = "/opt/tivoli/tsm/client/ba/bin/dsm.opt"

if not os.path.exists(lib_path):
    print(f"✗ Library not found: {lib_path}")
    sys.exit(1)
print(f"✓ Library found: {lib_path}")

if not os.path.exists(config_file):
    print(f"✗ Config file not found: {config_file}")
    sys.exit(1)
print(f"✓ Config file found: {config_file}")

# Parse config file for NODENAME
print("\n[Config File Contents]")
nodename = None
try:
    with open(config_file, 'r') as f:
        for line in f:
            line_stripped = line.strip()
            if line_stripped and not line_stripped.startswith('*'):
                print(f"  {line_stripped}")
                # Extract NODENAME if present
                if line_stripped.upper().startswith('NODENAME'):
                    parts = line_stripped.split(None, 1)
                    if len(parts) > 1:
                        nodename = parts[1].strip()
except Exception as e:
    print(f"  Could not read config: {e}")

# Step 2: Get credentials
print("\n[STEP 2] Authentication Setup")
if nodename:
    print(f"  Node name from config: {nodename}")
    use_node = input(f"  Use this node name? (Y/n): ").strip().lower()
    if use_node in ['n', 'no']:
        nodename = None

if not nodename:
    nodename = input("  Enter TSM node name: ").strip()
    if not nodename:
        print("✗ Node name is required")
        sys.exit(1)

# Get password
password = getpass.getpass(f"  Enter password for '{nodename}': ")
if not password:
    print("✗ Password is required")
    sys.exit(1)

print(f"\n  Will authenticate as: {nodename}")

# Step 3: Load library
print("\n[STEP 3] Loading TSM library...")
lib_dir = os.path.dirname(lib_path)
os.environ['LD_LIBRARY_PATH'] = f"{lib_dir}:{os.environ.get('LD_LIBRARY_PATH', '')}"

try:
    lib = CDLL(lib_path)
    print("✓ Library loaded successfully")
except Exception as e:
    print(f"✗ Failed to load library: {e}")
    sys.exit(1)

# Step 4: Define structures
print("\n[STEP 4] Defining API structures...")

class dsmApiVersionEx(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("version", c_uint16),
        ("release", c_uint16),
        ("level", c_uint16),
        ("subLevel", c_uint16),
    ]

class dsmAppVersion(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("applicationVersion", c_uint16),
        ("applicationRelease", c_uint16),
        ("applicationLevel", c_uint16),
        ("applicationSubLevel", c_uint16),
    ]

class dsmInitExIn_t(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("apiVersionExP", POINTER(dsmApiVersionEx)),
        ("clientNodeNameP", c_char_p),
        ("clientOwnerNameP", c_char_p),
        ("clientPasswordP", c_char_p),
        ("userNameP", c_char_p),
        ("userPasswordP", c_char_p),
        ("applicationTypeP", c_char_p),
        ("configfile", c_char_p),
        ("options", c_char_p),
        ("dirDelimiter", c_char),
        ("useUnicode", c_ubyte),
        ("bCrossPlatform", c_ubyte),
        ("bService", c_ubyte),
        ("bEncryptKeyEnabled", c_ubyte),
        ("encryptionPasswordP", c_char_p),
        ("useTsmBuffers", c_ubyte),
        ("numTsmBuffers", c_uint8),
        ("appVersionP", POINTER(dsmAppVersion)),
    ]

class dsmInitExOut_t(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("userNameAuthorities", c_int16),
        ("infoRC", c_int16),
        ("adsmServerName", c_char * (DSM_MAX_SERVERNAME_LENGTH + 1)),
        ("serverVer", c_uint16),
        ("serverRel", c_uint16),
        ("serverLev", c_uint16),
        ("serverSubLev", c_uint16),
        ("bIsFailOverMode", c_ubyte),
        ("replServerName", c_char * (DSM_MAX_SERVERNAME_LENGTH + 1)),
        ("homeServerName", c_char * (DSM_MAX_SERVERNAME_LENGTH + 1)),
    ]

print(f"  dsmInitExIn_t size: {sizeof(dsmInitExIn_t)} bytes")
print(f"  dsmInitExOut_t size: {sizeof(dsmInitExOut_t)} bytes")

# Step 5: Test dsmSetUp
print("\n[STEP 5] Testing dsmSetUp()...")
try:
    lib.dsmSetUp.argtypes = [c_bool, c_void_p]
    lib.dsmSetUp.restype = c_int
    
    rc = lib.dsmSetUp(c_bool(True), None)
    
    if rc == DSM_RC_SUCCESSFUL:
        print(f"✓ dsmSetUp() succeeded (rc={rc})")
    else:
        print(f"✗ dsmSetUp() failed with rc={rc}")
        sys.exit(1)
except Exception as e:
    print(f"✗ Exception in dsmSetUp(): {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 6: Test dsmInitEx WITH CREDENTIALS
print("\n[STEP 6] Testing dsmInitEx() with authentication...")

# Set environment
os.environ['DSMI_DIR'] = "/opt/tivoli/tsm/client/api/bin64"
os.environ['DSMI_CONFIG'] = config_file

try:
    # Set function signature
    lib.dsmInitEx.argtypes = [
        POINTER(c_uint32),
        POINTER(dsmInitExIn_t),
        POINTER(dsmInitExOut_t)
    ]
    lib.dsmInitEx.restype = c_int
    
    # Initialize API version
    api_version = dsmApiVersionEx()
    ctypes.memset(ctypes.addressof(api_version), 0, ctypes.sizeof(api_version))
    api_version.stVersion = apiVersionExVer
    api_version.version = 8
    api_version.release = 2
    api_version.level = 0
    api_version.subLevel = 0
    
    # Initialize app version
    app_version = dsmAppVersion()
    ctypes.memset(ctypes.addressof(app_version), 0, ctypes.sizeof(app_version))
    app_version.stVersion = 1
    app_version.applicationVersion = 1
    app_version.applicationRelease = 0
    app_version.applicationLevel = 0
    app_version.applicationSubLevel = 0
    
    # Initialize input structure WITH CREDENTIALS
    init_in = dsmInitExIn_t()
    ctypes.memset(ctypes.addressof(init_in), 0, ctypes.sizeof(init_in))
    
    init_in.stVersion = dsmInitExInVersion
    init_in.apiVersionExP = ctypes.pointer(api_version)
    init_in.appVersionP = ctypes.pointer(app_version)
    
    # Set credentials - IMPORTANT: Provide node name and password
    init_in.clientNodeNameP = nodename.encode('utf-8')      # ← Provide node name
    init_in.clientOwnerNameP = None
    init_in.clientPasswordP = password.encode('utf-8')      # ← Provide password
    init_in.userNameP = None
    init_in.userPasswordP = None
    init_in.applicationTypeP = b"PythonTest"
    init_in.configfile = config_file.encode('utf-8')
    init_in.options = b""
    init_in.dirDelimiter = ord('/')
    init_in.useUnicode = 0
    init_in.bCrossPlatform = 0
    init_in.bService = 0
    init_in.bEncryptKeyEnabled = 0
    init_in.encryptionPasswordP = None
    init_in.useTsmBuffers = 0
    init_in.numTsmBuffers = 0
    
    print(f"  Authenticating as: {nodename}")
    print(f"  Config file: {config_file}")
    
    # Initialize output structure
    init_out = dsmInitExOut_t()
    ctypes.memset(ctypes.addressof(init_out), 0, ctypes.sizeof(init_out))
    init_out.stVersion = dsmInitExOutVersion
    
    # Session handle
    handle = c_uint32(0)
    
    print("\n  Calling dsmInitEx()...")
    
    rc = lib.dsmInitEx(
        ctypes.byref(handle),
        ctypes.byref(init_in),
        ctypes.byref(init_out)
    )
    
    print(f"  Return code: {rc}")
    
    if rc == DSM_RC_SUCCESSFUL:
        print("\n" + "="*70)
        print("✓✓✓ SUCCESS! Connected to TSM server! ✓✓✓")
        print("="*70)
        print(f"\n  Session Handle: {handle.value}")
        
        # Decode and display server info
        try:
            server_bytes = bytes(init_out.adsmServerName)
            null_pos = server_bytes.find(b'\x00')
            if null_pos != -1:
                server_bytes = server_bytes[:null_pos]
            server_name = server_bytes.decode('utf-8', errors='ignore')
            
            print(f"\n  SERVER INFORMATION:")
            print(f"  {'='*50}")
            print(f"  Server Name: '{server_name}'")
            print(f"  Server Version: {init_out.serverVer}.{init_out.serverRel}.{init_out.serverLev}.{init_out.serverSubLev}")
            
            if init_out.bIsFailOverMode:
                print(f"  Connection Mode: Failover")
                repl_bytes = bytes(init_out.replServerName)
                null_pos = repl_bytes.find(b'\x00')
                if null_pos != -1:
                    repl_bytes = repl_bytes[:null_pos]
                repl_name = repl_bytes.decode('utf-8', errors='ignore')
                if repl_name:
                    print(f"  Replication Server: '{repl_name}'")
            else:
                print(f"  Connection Mode: Normal (Primary Server)")
                
            home_bytes = bytes(init_out.homeServerName)
            null_pos = home_bytes.find(b'\x00')
            if null_pos != -1:
                home_bytes = home_bytes[:null_pos]
            home_name = home_bytes.decode('utf-8', errors='ignore')
            if home_name:
                print(f"  Home Server: '{home_name}'")
            
            print(f"\n  CLIENT INFORMATION:")
            print(f"  {'='*50}")
            print(f"  Node Name: {nodename}")
            print(f"  User Authorities: {init_out.userNameAuthorities}")
            print(f"  Info RC: {init_out.infoRC}")
                
        except Exception as e:
            print(f"  Warning: Error decoding server info: {e}")
        
        # Terminate session
        print(f"\n  {'='*50}")
        print("  Terminating session...")
        lib.dsmTerminate.argtypes = [c_uint32]
        lib.dsmTerminate.restype = c_int
        term_rc = lib.dsmTerminate(handle)
        
        if term_rc == DSM_RC_SUCCESSFUL:
            print("  ✓ Session terminated successfully")
        else:
            print(f"  ⚠ Terminate returned: {term_rc}")
            
    elif rc == DSM_RC_AUTH_FAILURE:
        print(f"\n✗ Authentication failed (RC {rc})")
        print(f"  Node name: {nodename}")
        print(f"  Info RC: {init_out.infoRC}")
        print("\nPossible issues:")
        print("  1. Incorrect password")
        print("  2. Node not registered on server")
        print("  3. Node is locked")
        print("  4. Password expired")
        print("\nTo check node status on server, run:")
        print(f"  dsmadmc> query node {nodename}")
        sys.exit(1)
    else:
        print(f"\n✗ dsmInitEx() returned error code: {rc}")
        print(f"  Info RC: {init_out.infoRC}")
        
        # Try to get error message
        try:
            lib.dsmRCMsg.argtypes = [c_uint32, c_int, c_char_p]
            lib.dsmRCMsg.restype = None
            error_msg = create_string_buffer(1024)
            lib.dsmRCMsg(handle, rc, error_msg)
            msg = error_msg.value.decode('utf-8', errors='ignore')
            if msg:
                print(f"  Error Message: {msg}")
        except Exception as e:
            print(f"  Could not retrieve error message: {e}")
        
        sys.exit(1)
        
except Exception as e:
    print(f"\n✗ Exception in dsmInitEx(): {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL TESTS PASSED!")
print("="*70)
print("\nYou have successfully:")
print("  ✓ Loaded the TSM API library")
print("  ✓ Called dsmSetUp() to initialize the API")
print("  ✓ Called dsmInitEx() and authenticated to 'bugsbunny' server")
print("  ✓ Terminated the session cleanly")
print("\n" + "="*70)
print("NEXT STEPS - What you can do now:")
print("="*70)
print("\n1. Query Session Info:")
print("   - Use dsmQuerySessInfo() to get session details")
print("\n2. Backup Operations:")
print("   - dsmBeginTxn() - Start a transaction")
print("   - dsmSendObj() - Send a file to backup")
print("   - dsmEndSendObj() - Finish sending")
print("   - dsmEndTxn() - Commit the transaction")
print("\n3. Restore Operations:")
print("   - dsmBeginQuery() - Query for files")
print("   - dsmGetNextQObj() - Get next object")
print("   - dsmGetObj() - Restore a file")
print("   - dsmEndQuery() - End the query")
print("\n4. Archive/Retrieve:")
print("   - Similar to backup/restore but for archive storage")
print("\n" + "="*70)