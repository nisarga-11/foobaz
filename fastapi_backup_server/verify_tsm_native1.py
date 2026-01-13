#!/usr/bin/env python3
"""
Test IBM Storage Protect API - Basic calls to dsmSetUp and dsmInitEx
Connect to 'bugsbunny' TSM server
Based on IBM sample code structure
"""
import ctypes
import os
import sys
from ctypes import *

# DSM API Return Codes
DSM_RC_SUCCESSFUL = 0
DSM_RC_UNSUCCESSFUL = 1

# Structure version constants from dsmapitd.h
apiVersionExVer = 2
dsmInitExInVersion = 5
dsmInitExOutVersion = 3  # Correct value from header

# Constants from dsmapitd.h
DSM_MAX_SERVERNAME_LENGTH = 64  # Will verify this

# Define structures based on dsmapitd.h
class dsmAppVersion(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("applicationVersion", c_uint16),
        ("applicationRelease", c_uint16),
        ("applicationLevel", c_uint16),
        ("applicationSubLevel", c_uint16),
    ]
class dsmApiVersionEx(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("version", c_uint16),
        ("release", c_uint16),
        ("level", c_uint16),
        ("subLevel", c_uint16),
    ]

class dsmInitExIn_t(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("apiVersionExP", POINTER(dsmApiVersionEx)),
        ("clientNodeNameP", c_char_p),
        ("clientOwnerNameP", c_char_p),
        ("clientPasswordP", c_char_p),
        ("applicationTypeP", c_char_p),
        ("configfile", c_char_p),
        ("options", c_char_p),
        ("userNameP", c_char_p),
        ("userPasswordP", c_char_p),
        ("dirDelimiter", c_char),
        ("useUnicode", c_ubyte),
        ("bEncryptKeyEnabled", c_ubyte),
        ("encryptionPasswordP", c_char_p),
        ("appVersionP", POINTER(dsmAppVersion)),  # Missing field!
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
        ("bIsFailOverMode", c_ubyte),  # dsmBool_t
        ("replServerName", c_char * (DSM_MAX_SERVERNAME_LENGTH + 1)),
        ("homeServerName", c_char * (DSM_MAX_SERVERNAME_LENGTH + 1)),
    ]

def load_tsm_library():
    """Load the TSM API library"""
    lib_path = "/opt/tivoli/tsm/client/api/bin64/libApiTSM64.so"
    
    if not os.path.exists(lib_path):
        raise RuntimeError(f"Library not found: {lib_path}")
    
    print(f"Loading library: {lib_path}")
    
    # Set library path in LD_LIBRARY_PATH
    lib_dir = os.path.dirname(lib_path)
    current_ld_path = os.environ.get('LD_LIBRARY_PATH', '')
    if lib_dir not in current_ld_path:
        os.environ['LD_LIBRARY_PATH'] = f"{lib_dir}:{current_ld_path}"
    
    return CDLL(lib_path)

def test_dsm_setup(lib):
    """Test dsmSetUp API call"""
    print("\n" + "="*60)
    print("Testing dsmSetUp()")
    print("="*60)
    
    try:
        # Set up function signature
        lib.dsmSetUp.argtypes = [c_bool, c_void_p]
        lib.dsmSetUp.restype = c_int
        
        # Call dsmSetUp with mt_flag=True and NULL version pointer
        rc = lib.dsmSetUp(c_bool(True), None)
        
        print(f"Return code: {rc}")
        
        if rc == DSM_RC_SUCCESSFUL:
            print("✓ dsmSetUp() succeeded")
            return True
        else:
            print(f"✗ dsmSetUp() failed with return code: {rc}")
            return False
    except Exception as e:
        print(f"✗ Exception calling dsmSetUp(): {e}")
        import traceback
        traceback.print_exc()
        return False

def test_dsm_init_ex(lib, server_name="bugsbunny", node_name=None, password=None):
    """Test dsmInitEx API call"""
    print("\n" + "="*60)
    print(f"Testing dsmInitEx() - connecting to '{server_name}'")
    print("="*60)
    
    # Set DSMI_DIR and DSMI_CONFIG environment variables
    api_dir = "/opt/tivoli/tsm/client/api/bin64"
    config_file = "/opt/tivoli/tsm/client/ba/bin/dsm.opt"
    
    os.environ['DSMI_DIR'] = api_dir
    os.environ['DSMI_CONFIG'] = config_file
    
    print(f"Environment:")
    print(f"  DSMI_DIR: {os.environ.get('DSMI_DIR')}")
    print(f"  DSMI_CONFIG: {os.environ.get('DSMI_CONFIG')}")
    
    try:
        # Set up function signature for dsmInitEx
        lib.dsmInitEx.argtypes = [
            POINTER(c_uint32),           # handle
            POINTER(dsmInitExIn_t),      # input
            POINTER(dsmInitExOut_t)      # output
        ]
        lib.dsmInitEx.restype = c_int
        
        # Initialize API version structure - using memset approach like sample code
        print("\nInitializing structures (using memset approach)...")
        api_version = dsmApiVersionEx()
        # Zero out the structure first (like memset in C)
        ctypes.memset(ctypes.addressof(api_version), 0, ctypes.sizeof(api_version))
        
        # Use the constant from header file
        api_version.stVersion = apiVersionExVer  # Should be 2, not sizeof
        api_version.version = 8
        api_version.release = 2
        api_version.level = 0
        api_version.subLevel = 0
        
        print(f"  API Version struct size: {sizeof(dsmApiVersionEx)}")
        print(f"  API Version stVersion: {api_version.stVersion}")
        print(f"  API Version: {api_version.version}.{api_version.release}.{api_version.level}.{api_version.subLevel}")
        
        # Initialize app version structure
        app_version = dsmAppVersion()
        ctypes.memset(ctypes.addressof(app_version), 0, ctypes.sizeof(app_version))
        app_version.stVersion = 1  # appVersionVer
        app_version.applicationVersion = 8
        app_version.applicationRelease = 2
        app_version.applicationLevel = 0
        app_version.applicationSubLevel = 0
        
        # Initialize input structure
        init_in = dsmInitExIn_t()
        ctypes.memset(ctypes.addressof(init_in), 0, ctypes.sizeof(init_in))
        
        # Use the constant from header file
        init_in.stVersion = dsmInitExInVersion  # Should be 5, not sizeof
        init_in.apiVersionExP = ctypes.pointer(api_version)
        init_in.appVersionP = ctypes.pointer(app_version)  # Add app version pointer
        
        # Use node name and password if provided, otherwise None (use config file)
        if node_name:
            init_in.clientNodeNameP = node_name.encode('utf-8')
        else:
            init_in.clientNodeNameP = None
            
        init_in.clientOwnerNameP = None
        
        if password:
            init_in.clientPasswordP = password.encode('utf-8')
        else:
            init_in.clientPasswordP = None
            
        init_in.applicationTypeP = b"Python_TSM_Test"
        init_in.configfile = config_file.encode('utf-8')
        init_in.options = b""
        init_in.userNameP = None
        init_in.userPasswordP = None
        init_in.dirDelimiter = ord('/')  # Use ord() to get the byte value
        init_in.useUnicode = 0
        init_in.bEncryptKeyEnabled = 0
        init_in.encryptionPasswordP = None
        
        print(f"  Init input struct size: {sizeof(dsmInitExIn_t)}")
        print(f"  Init input stVersion: {init_in.stVersion}")
        print(f"  Config file: {config_file}")
        if node_name:
            print(f"  Node name: {node_name}")
        else:
            print(f"  Node name: (from config file)")
        
        # Initialize output structure
        init_out = dsmInitExOut_t()
        ctypes.memset(ctypes.addressof(init_out), 0, ctypes.sizeof(init_out))
        
        init_out.stVersion = dsmInitExOutVersion  # Should be 4, not sizeof
        
        print(f"  Init output struct size: {sizeof(dsmInitExOut_t)}")
        print(f"  Init output stVersion: {init_out.stVersion}")
        
        # Handle for the session
        handle = c_uint32(0)
        
        print("\nCalling dsmInitEx()...")
        
        # Call dsmInitEx
        rc = lib.dsmInitEx(
            ctypes.byref(handle),
            ctypes.byref(init_in),
            ctypes.byref(init_out)
        )
        
        print(f"Return code: {rc}")
        
        if rc == DSM_RC_SUCCESSFUL:
            print(f"✓ dsmInitEx() succeeded!")
            print(f"  Session Handle: {handle.value}")
            
            try:
                # Decode server name carefully
                server_bytes = bytes(init_out.adsmServerName)
                null_pos = server_bytes.find(b'\x00')
                if null_pos != -1:
                    server_bytes = server_bytes[:null_pos]
                server_name_str = server_bytes.decode('utf-8', errors='ignore')
                print(f"  Server Name: {server_name_str}")
                
                # Show server version info
                print(f"  Server Version: {init_out.serverVer}.{init_out.serverRel}.{init_out.serverLev}.{init_out.serverSubLev}")
                
                # Show failover info
                if init_out.bIsFailOverMode:
                    print(f"  Mode: Failover (connected to replication server)")
                    repl_bytes = bytes(init_out.replServerName)
                    null_pos = repl_bytes.find(b'\x00')
                    if null_pos != -1:
                        repl_bytes = repl_bytes[:null_pos]
                    print(f"  Replication Server: {repl_bytes.decode('utf-8', errors='ignore')}")
                else:
                    print(f"  Mode: Normal (connected to home server)")
                
                home_bytes = bytes(init_out.homeServerName)
                null_pos = home_bytes.find(b'\x00')
                if null_pos != -1:
                    home_bytes = home_bytes[:null_pos]
                print(f"  Home Server: {home_bytes.decode('utf-8', errors='ignore')}")
                
            except Exception as e:
                print(f"  Error decoding server info: {e}")
            
            print(f"  Info RC: {init_out.infoRC}")
            print(f"  User Authorities: {init_out.userNameAuthorities}")
            
            # Cleanup - terminate session
            print("\nCleaning up session...")
            try:
                lib.dsmTerminate.argtypes = [c_uint32]
                lib.dsmTerminate.restype = c_int
                term_rc = lib.dsmTerminate(handle)
                if term_rc == DSM_RC_SUCCESSFUL:
                    print("✓ Session terminated successfully")
                else:
                    print(f"⚠ Session terminate returned: {term_rc}")
            except Exception as e:
                print(f"⚠ Error terminating session: {e}")
            
            return True
        else:
            print(f"✗ dsmInitEx() failed with return code: {rc}")
            print(f"  Info RC: {init_out.infoRC}")
            
            # Try to get error message
            try:
                lib.dsmRCMsg.argtypes = [c_uint32, c_int, c_char_p]
                lib.dsmRCMsg.restype = None
                error_msg = create_string_buffer(1024)
                lib.dsmRCMsg(handle, rc, error_msg)
                msg_str = error_msg.value.decode('utf-8', errors='ignore')
                if msg_str:
                    print(f"  Error message: {msg_str}")
            except Exception as e:
                print(f"  Could not retrieve error message: {e}")
            
            return False
            
    except Exception as e:
        print(f"✗ Exception calling dsmInitEx(): {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 60)
    print("IBM Storage Protect API Test")
    print("Testing dsmSetUp() and dsmInitEx()")
    print("=" * 60)
    
    # Check for config file
    config_file = "/opt/tivoli/tsm/client/ba/bin/dsm.opt"
    if os.path.exists(config_file):
        print(f"✓ Found config file: {config_file}")
        # Show first few lines
        try:
            with open(config_file, 'r') as f:
                lines = f.readlines()[:15]
                print("  Config file contents:")
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('*'):
                        print(f"    {line}")
        except Exception as e:
            print(f"  Could not read config file: {e}")
    else:
        print(f"✗ Warning: Config file not found: {config_file}")
        return 1
    
    try:
        # Load the library
        lib = load_tsm_library()
        print("✓ Library loaded successfully\n")
        
        # Test dsmSetUp
        if not test_dsm_setup(lib):
            print("\n✗ dsmSetUp failed, cannot proceed")
            return 1
        
        # Test dsmInitEx - you can pass node_name and password as arguments
        # if not set in config file, or leave as None to use config
        if not test_dsm_init_ex(lib, "bugsbunny", node_name=None, password=None):
            print("\n✗ dsmInitEx failed")
            return 1
        
        print("\n" + "=" * 60)
        print("✓ All tests completed successfully!")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())