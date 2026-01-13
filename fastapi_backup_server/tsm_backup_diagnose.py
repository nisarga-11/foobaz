#!/usr/bin/env python3
"""
DEBUG VERSION - Find why RC 102 is happening
"""
import ctypes
import os
import sys
import getpass
from ctypes import *

# Constants
DSM_RC_SUCCESSFUL = 0
DSM_RC_INVALID_OPT = 102
apiVersionExVer = 2
dsmInitExInVersion = 5
dsmInitExOutVersion = 4
DSM_MAX_SERVERNAME_LENGTH = 64

print("="*70)
print("DEBUG: Finding RC 102 Issue")
print("="*70)

# Configuration
lib_path = "/opt/tivoli/tsm/client/api/bin64/libApiTSM64.so"
config_file = "/opt/tivoli/tsm/client/ba/bin/dsm.opt"

# Get credentials
print("\n[Getting credentials...]")
nodename = input("Node name: ").strip()
password = getpass.getpass("Password: ")

# Load library
print("\n[Loading library...]")
lib_dir = os.path.dirname(lib_path)
os.environ['LD_LIBRARY_PATH'] = f"{lib_dir}:{os.environ.get('LD_LIBRARY_PATH', '')}"
os.environ['DSMI_DIR'] = lib_dir
os.environ['DSMI_CONFIG'] = config_file

lib = CDLL(lib_path)
print("✓ Loaded")

# Structures
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

# dsmSetUp
print("\n[Testing dsmSetUp...]")
lib.dsmSetUp.argtypes = [c_bool, c_void_p]
lib.dsmSetUp.restype = c_int
rc = lib.dsmSetUp(c_bool(True), None)
print(f"dsmSetUp RC: {rc}")

# Try different applicationTypeP values
test_values = [
    ("NULL (None)", None),
    ("Empty string", b""),
    ("PythonTest", b"PythonTest"),
    ("PythonBackup", b"PythonBackup"),
    ("apitest", b"apitest"),
    ("backup", b"backup"),
]

lib.dsmInitEx.argtypes = [
    POINTER(c_uint32),
    POINTER(dsmInitExIn_t),
    POINTER(dsmInitExOut_t)
]
lib.dsmInitEx.restype = c_int

for test_name, app_type_value in test_values:
    print(f"\n{'='*70}")
    print(f"TESTING: applicationTypeP = {test_name}")
    print(f"{'='*70}")
    
    try:
        # API version
        api_version = dsmApiVersionEx()
        ctypes.memset(ctypes.addressof(api_version), 0, ctypes.sizeof(api_version))
        api_version.stVersion = apiVersionExVer
        api_version.version = 8
        api_version.release = 2
        api_version.level = 0
        api_version.subLevel = 0
        
        # App version
        app_version = dsmAppVersion()
        ctypes.memset(ctypes.addressof(app_version), 0, ctypes.sizeof(app_version))
        app_version.stVersion = 1
        app_version.applicationVersion = 1
        app_version.applicationRelease = 0
        app_version.applicationLevel = 0
        app_version.applicationSubLevel = 0
        
        # Init input
        init_in = dsmInitExIn_t()
        ctypes.memset(ctypes.addressof(init_in), 0, ctypes.sizeof(init_in))
        
        init_in.stVersion = dsmInitExInVersion
        init_in.apiVersionExP = ctypes.pointer(api_version)
        init_in.appVersionP = ctypes.pointer(app_version)
        
        init_in.clientNodeNameP = nodename.encode('utf-8')
        init_in.clientOwnerNameP = None
        init_in.clientPasswordP = password.encode('utf-8')
        init_in.userNameP = None
        init_in.userPasswordP = None
        
        # THIS IS WHAT WE'RE TESTING
        init_in.applicationTypeP = app_type_value
        
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
        
        # Init output
        init_out = dsmInitExOut_t()
        ctypes.memset(ctypes.addressof(init_out), 0, ctypes.sizeof(init_out))
        init_out.stVersion = dsmInitExOutVersion
        
        # Handle
        handle = c_uint32(0)
        
        # Call dsmInitEx
        print(f"  Calling dsmInitEx()...")
        rc = lib.dsmInitEx(
            ctypes.byref(handle),
            ctypes.byref(init_in),
            ctypes.byref(init_out)
        )
        
        print(f"  Return code: {rc}")
        
        if rc == DSM_RC_SUCCESSFUL:
            server_bytes = bytes(init_out.adsmServerName)
            null_pos = server_bytes.find(b'\x00')
            if null_pos != -1:
                server_bytes = server_bytes[:null_pos]
            server_name = server_bytes.decode('utf-8', errors='ignore')
            
            print(f"  ✓✓✓ SUCCESS! Connected to: {server_name}")
            print(f"  Handle: {handle.value}")
            
            # Terminate
            lib.dsmTerminate.argtypes = [c_uint32]
            lib.dsmTerminate.restype = c_int
            lib.dsmTerminate(handle.value)
            print(f"  Session terminated")
            
            print(f"\n{'='*70}")
            print(f"✓✓✓ WINNER: applicationTypeP = {test_name} works!")
            print(f"{'='*70}")
            break
        elif rc == DSM_RC_INVALID_OPT:
            print(f"  ✗ RC 102 (Invalid option)")
            print(f"  infoRC: {init_out.infoRC}")
        else:
            print(f"  ✗ RC {rc}")
            print(f"  infoRC: {init_out.infoRC}")
            
    except Exception as e:
        print(f"  ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "="*70)
print("DEBUG COMPLETE")
print("="*70)