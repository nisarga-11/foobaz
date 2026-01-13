#!/usr/bin/env python3
"""
IBM Storage Protect API - TSM 8.2 Server Complete Backup Script
Follows: dsmBeginTxn -> dsmSendObj -> dsmSendData -> dsmEndSendObjEx -> dsmEndTxn
Management class determined by include-exclude rules in dsm.opt
"""
import ctypes
import os
import sys
import getpass
from ctypes import *
from datetime import datetime

# ==================== Constants ====================
DSM_RC_SUCCESSFUL = 0
DSM_RC_OK = 0
DSM_RC_UNSUCCESSFUL = 1
DSM_RC_AUTH_FAILURE = 137
DSM_RC_FS_ALREADY_REGED = 2062
DSM_RC_INVALID_OPT = 102

# Structure versions
apiVersionExVer = 2
dsmInitExInVersion = 5
dsmInitExOutVersion = 4
regFSDataVersion = 1
ObjAttrVersion = 3  # Version 3 for TSM 8.2

# Maximum lengths
DSM_MAX_SERVERNAME_LENGTH = 64
DSM_MAX_FSNAME_LENGTH = 1024
DSM_MAX_HL_LENGTH = 1024
DSM_MAX_LL_LENGTH = 256
DSM_MAX_OBJINFO_LENGTH = 255
DSM_MAX_MC_NAME_LENGTH = 30
DSM_MAX_PLATFORM_LENGTH = 16
DSM_MAX_OWNER_LENGTH = 64

# Object types
DSM_OBJ_FILE = 1

# Backup types
DSM_BACKUP_INCREMENTAL = 0x01
stBackup = 0x01

# Transaction vote
DSM_VOTE_COMMIT = 1

# Read buffer size
READSIZE = 32768

# ==================== Structure Definitions ====================

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

class regFSData(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("fsName", c_char_p),
        ("fsType", c_char_p),
        ("occupancy", c_int64),
        ("capacity", c_int64),
        ("fsAttr", c_ubyte),
    ]

class dsmObjName(Structure):
    _fields_ = [
        ("fs", c_char * (DSM_MAX_FSNAME_LENGTH + 1)),
        ("hl", c_char * (DSM_MAX_HL_LENGTH + 1)),
        ("ll", c_char * (DSM_MAX_LL_LENGTH + 1)),
        ("objType", c_ubyte),
    ]

class dsStruct64(Structure):
    _fields_ = [
        ("hi", c_uint32),
        ("lo", c_uint32),
    ]

class ObjAttr(Structure):
    """ObjAttr structure - version 5 for TSM 8.2 compatibility
    Let compiler handle alignment naturally (no manual padding)
    """
    _fields_ = [
        ("stVersion", c_uint16),                          # offset 0
        ("owner", c_char * (DSM_MAX_OWNER_LENGTH + 1)),   # offset 2
        ("sizeEstimate", dsStruct64),                     # compiler aligns this
        ("objCompressed", c_ubyte),                       # 1 byte
        ("objInfoLength", c_uint16),                      # 2 bytes
        ("objInfo", c_char_p),                            # 8 bytes (pointer)
        ("mcNameP", c_char_p),                            # 8 bytes (pointer)
        ("disableDeduplication", c_ubyte),                # 1 byte
    ]

class DataBlk(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("bufferLen", c_size_t),
        ("numBytes", c_size_t),
        ("bufferPtr", c_void_p),
    ]

class dsmEndSendObjExIn_t(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("dsmHandle", c_uint32),
    ]

class dsmEndSendObjExOut_t(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("totalBytesSent", c_uint64),
        ("objCompressed", c_ubyte),
        ("totalLFBytesSent", c_uint64),
        ("totalCompressedSize", c_uint64),
    ]

class mcBindKey(Structure):
    """Management class bind key"""
    _fields_ = [
        ("stVersion", c_uint16),
        ("mcName", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
        ("backup_cg_exists", c_ubyte),
        ("archive_cg_exists", c_ubyte),
        ("backup_copy_dest", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
        ("archive_copy_dest", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
    ]

# ==================== Main Script ====================

def main():
    print("=" * 70)
    print("IBM Storage Protect - TSM 8.2 Complete Backup Script")
    print("Sequence: BeginTxn -> SendObj -> SendData -> EndSendObj -> EndTxn")
    print("=" * 70)

    # Configuration
    lib_path = "/opt/tivoli/tsm/client/api/bin64/libApiTSM64.so"
    config_file = "/opt/tivoli/tsm/client/ba/bin/dsm.opt"
    
    print("\n[STEP 1] Verifying environment...")
    if not os.path.exists(lib_path):
        print(f"✗ Library not found: {lib_path}")
        sys.exit(1)
    print(f"✓ Library: {lib_path}")
    
    if not os.path.exists(config_file):
        print(f"✗ Config not found: {config_file}")
        sys.exit(1)
    print(f"✓ Config: {config_file}")

    # Check for include statement in dsm.opt
    print("\n  Checking dsm.opt for include-exclude rules...")
    has_include = False
    try:
        with open(config_file, 'r') as f:
            for line in f:
                if 'include' in line.lower() and '/python_backup' in line:
                    print(f"  ✓ Found: {line.strip()}")
                    has_include = True
                    break
        if not has_include:
            print("  ⚠ No include rule for /python_backup found")
            print("  ⚠ Add this to dsm.opt: include /python_backup/.../* STANDARD")
    except:
        pass

    # Get file to backup
    print("\n[STEP 2] Select file to backup...")
    default_file = "/tmp/test_backup.txt"
    file_to_backup = input(f"File path (default: {default_file}): ").strip()
    if not file_to_backup:
        file_to_backup = default_file
        if not os.path.exists(file_to_backup):
            print(f"  Creating test file: {file_to_backup}")
            with open(file_to_backup, 'w') as f:
                f.write(f"Test backup file - {datetime.now()}\n")
                f.write("=" * 50 + "\n")
                for i in range(10):
                    f.write(f"Line {i+1}: Sample backup data\n")
    
    if not os.path.exists(file_to_backup):
        print(f"✗ File not found: {file_to_backup}")
        sys.exit(1)
    
    file_size = os.path.getsize(file_to_backup)
    print(f"✓ File: {file_to_backup}")
    print(f"  Size: {file_size} bytes")

    # Parse config for NODENAME
    print("\n[STEP 3] Reading configuration...")
    nodename = None
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line_stripped = line.strip()
                if line_stripped and line_stripped.upper().startswith('NODENAME'):
                    parts = line_stripped.split(None, 1)
                    if len(parts) > 1:
                        nodename = parts[1].strip()
                        print(f"  Node name: {nodename}")
                        break
    except Exception as e:
        print(f"  Could not read config: {e}")

    # Get credentials
    print("\n[STEP 4] Authentication...")
    if nodename:
        use_node = input(f"  Use node '{nodename}'? (Y/n): ").strip().lower()
        if use_node in ['n', 'no']:
            nodename = None
    
    if not nodename:
        nodename = input("  Enter node name: ").strip()
    
    if not nodename:
        print("✗ Node name required")
        sys.exit(1)
    
    password = getpass.getpass(f"  Enter password for '{nodename}': ")
    
    if not password:
        print("✗ Password required")
        sys.exit(1)

    # Load library
    print("\n[STEP 5] Loading TSM library...")
    lib_dir = os.path.dirname(lib_path)
    os.environ['LD_LIBRARY_PATH'] = f"{lib_dir}:{os.environ.get('LD_LIBRARY_PATH', '')}"
    
    try:
        lib = CDLL(lib_path)
        print("✓ Library loaded")
    except Exception as e:
        print(f"✗ Failed to load library: {e}")
        sys.exit(1)

    # Initialize API
    print("\n[STEP 6] Initializing API...")
    
    os.environ['DSMI_DIR'] = "/opt/tivoli/tsm/client/api/bin64"
    os.environ['DSMI_CONFIG'] = config_file
    
    try:
        lib.dsmSetUp.argtypes = [c_bool, c_void_p]
        lib.dsmSetUp.restype = c_int
        rc = lib.dsmSetUp(c_bool(True), None)
        if rc != DSM_RC_SUCCESSFUL:
            print(f"✗ dsmSetUp failed: {rc}")
            sys.exit(1)
        print("✓ dsmSetUp succeeded")
        
        lib.dsmInitEx.argtypes = [
            POINTER(c_uint32),
            POINTER(dsmInitExIn_t),
            POINTER(dsmInitExOut_t)
        ]
        lib.dsmInitEx.restype = c_int
        
        api_version = dsmApiVersionEx()
        ctypes.memset(ctypes.addressof(api_version), 0, ctypes.sizeof(api_version))
        api_version.stVersion = apiVersionExVer
        api_version.version = 8
        api_version.release = 2
        api_version.level = 0
        api_version.subLevel = 0
        
        app_version = dsmAppVersion()
        ctypes.memset(ctypes.addressof(app_version), 0, ctypes.sizeof(app_version))
        app_version.stVersion = 1
        app_version.applicationVersion = 1
        app_version.applicationRelease = 0
        app_version.applicationLevel = 0
        app_version.applicationSubLevel = 0
        
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
        init_in.applicationTypeP = b"PythonAPI"
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
        
        init_out = dsmInitExOut_t()
        ctypes.memset(ctypes.addressof(init_out), 0, ctypes.sizeof(init_out))
        init_out.stVersion = dsmInitExOutVersion
        
        handle = c_uint32(0)
        
        rc = lib.dsmInitEx(
            ctypes.byref(handle),
            ctypes.byref(init_in),
            ctypes.byref(init_out)
        )
        
        if rc == DSM_RC_SUCCESSFUL:
            server_bytes = bytes(init_out.adsmServerName)
            null_pos = server_bytes.find(b'\x00')
            if null_pos != -1:
                server_bytes = server_bytes[:null_pos]
            server_name = server_bytes.decode('utf-8', errors='ignore')
            print(f"✓ Connected to: {server_name}")
            print(f"  Server: {init_out.serverVer}.{init_out.serverRel}.{init_out.serverLev}")
            print(f"  Handle: {handle.value}")
        else:
            print(f"✗ dsmInitEx failed: {rc}")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Initialization error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Register filespace
    print("\n[STEP 7] Registering filespace...")
    filespace_name = "/python_backup"
    
    try:
        lib.dsmRegisterFS.argtypes = [c_uint32, POINTER(regFSData)]
        lib.dsmRegisterFS.restype = c_int
        
        fs_name_bytes = create_string_buffer(filespace_name.encode('utf-8'))
        fs_type_bytes = create_string_buffer(b"API")
        
        fs_data = regFSData()
        fs_data.stVersion = regFSDataVersion
        fs_data.fsName = ctypes.cast(fs_name_bytes, c_char_p)
        fs_data.fsType = ctypes.cast(fs_type_bytes, c_char_p)
        fs_data.occupancy = 0
        fs_data.capacity = 0
        fs_data.fsAttr = 0
        
        rc = lib.dsmRegisterFS(handle.value, ctypes.byref(fs_data))
        
        if rc == DSM_RC_SUCCESSFUL:
            print(f"✓ Filespace registered")
        elif rc == DSM_RC_FS_ALREADY_REGED:
            print(f"✓ Filespace exists")
        else:
            print(f"✗ dsmRegisterFS failed: {rc}")
            sys.exit(1)
            
    except Exception as e:
        print(f"✗ Register filespace error: {e}")
        sys.exit(1)
        
    # Prepare object name
    print("\n[STEP 8] Preparing object name...")
    
    obj_name = dsmObjName()
    ctypes.memset(ctypes.addressof(obj_name), 0, ctypes.sizeof(obj_name))
    
    obj_name.fs = filespace_name.encode('utf-8')
    hl_path = os.path.dirname(file_to_backup) or "/"
    obj_name.hl = hl_path.encode('utf-8')
    ll_name = "/" + os.path.basename(file_to_backup)
    obj_name.ll = ll_name.encode('utf-8')
    obj_name.objType = DSM_OBJ_FILE
    
    print(f"  FS: {filespace_name}")
    print(f"  HL: {hl_path}")
    print(f"  LL: {ll_name}")

    # Bind management class
    print("\n[STEP 9] Binding management class...")
    
    try:
        lib.dsmBindMC.argtypes = [c_uint32, POINTER(dsmObjName), c_ubyte, POINTER(mcBindKey)]
        lib.dsmBindMC.restype = c_int
        
        mc_bind_key = mcBindKey()
        ctypes.memset(ctypes.addressof(mc_bind_key), 0, ctypes.sizeof(mc_bind_key))
        mc_bind_key.stVersion = 1
        
        rc = lib.dsmBindMC(
            handle.value,
            ctypes.byref(obj_name),
            c_ubyte(stBackup),
            ctypes.byref(mc_bind_key)
        )
        
        if rc == DSM_RC_SUCCESSFUL:
            mc_name = mc_bind_key.mcName.decode('utf-8', errors='ignore').rstrip('\x00')
            print(f"  Bound to: {mc_name}")
            
            if not mc_bind_key.backup_cg_exists:
                print(f"  ⚠ '{mc_name}' has no backup copy group")
                print(f"  → Will override with STANDARD in dsmSendObj")
            else:
                print(f"  ✓ Backup copy group exists")
        else:
            print(f"✗ dsmBindMC failed: {rc}")
            try:
                lib.dsmRCMsg.argtypes = [c_uint32, c_int, c_char_p]
                lib.dsmRCMsg.restype = None
                error_msg = create_string_buffer(1024)
                lib.dsmRCMsg(handle.value, rc, error_msg)
                msg = error_msg.value.decode('utf-8', errors='ignore')
                if msg:
                    print(f"  Error: {msg}")
            except:
                pass
            sys.exit(1)
            
    except Exception as e:
        print(f"✗ dsmBindMC error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Begin transaction
    print("\n[STEP 10] Starting transaction...")
    
    try:
        lib.dsmBeginTxn.argtypes = [c_uint32]
        lib.dsmBeginTxn.restype = c_int
        
        rc = lib.dsmBeginTxn(handle.value)
        
        if rc != DSM_RC_SUCCESSFUL:
            print(f"✗ dsmBeginTxn failed: {rc}")
            sys.exit(1)
        
        print("✓ Transaction started")
    except Exception as e:
        print(f"✗ Begin transaction error: {e}")
        sys.exit(1)

    # Send object with MC override
    print("\n[STEP 11] Sending object (dsmSendObj)...")
    print(f"  Using ObjAttr version {ObjAttrVersion}, size {ctypes.sizeof(ObjAttr())} bytes")
    
    try:
        obj_attr = ObjAttr()
        # Zero out entire structure
        ctypes.memset(ctypes.addressof(obj_attr), 0, ctypes.sizeof(obj_attr))
        
        # Set version
        obj_attr.stVersion = ObjAttrVersion
        
        # Set owner - properly copy into char array using buffer copy
        owner_str = b"root"
        # Get the address of the owner field within the structure
        owner_offset = ObjAttr.owner.offset
        owner_addr = ctypes.addressof(obj_attr) + owner_offset
        # Copy the string into the owner array
        ctypes.memmove(owner_addr, owner_str, len(owner_str))
        # Ensure null termination (already zeroed by memset above)
        
        # Set size estimate
        obj_attr.sizeEstimate.hi = 0
        obj_attr.sizeEstimate.lo = file_size
        
        # Set basic attributes
        obj_attr.objCompressed = 0
        obj_attr.objInfoLength = 0
        obj_attr.objInfo = None
        obj_attr.disableDeduplication = 0
        
        # OVERRIDE management class to STANDARD
        mc_name_bytes = b"STANDARD"
        obj_attr.mcNameP = mc_name_bytes
        
        # ========== DEBUG PRINTS ==========
        print("\n  DEBUG: obj_name (dsmObjName) Contents:")
        print(f"    fs: '{obj_name.fs.decode('utf-8', errors='ignore').rstrip(chr(0))}'")
        print(f"    hl: '{obj_name.hl.decode('utf-8', errors='ignore').rstrip(chr(0))}'")
        print(f"    ll: '{obj_name.ll.decode('utf-8', errors='ignore').rstrip(chr(0))}'")
        print(f"    objType: {obj_name.objType}")
        
        print("\n  DEBUG: obj_attr (ObjAttr) Contents:")
        print(f"    stVersion: {obj_attr.stVersion}")
        
        # Decode owner
        owner_bytes = bytes(obj_attr.owner)
        null_pos = owner_bytes.find(b'\x00')
        if null_pos > 0:
            owner_decoded = owner_bytes[:null_pos].decode('utf-8', errors='ignore')
        else:
            owner_decoded = owner_bytes.decode('utf-8', errors='ignore').rstrip('\x00')
        print(f"    owner: '{owner_decoded}' (bytes: {owner_bytes[:8]})")
        
        print(f"    sizeEstimate.hi: {obj_attr.sizeEstimate.hi}")
        print(f"    sizeEstimate.lo: {obj_attr.sizeEstimate.lo}")
        print(f"    objCompressed: {obj_attr.objCompressed}")
        print(f"    objInfoLength: {obj_attr.objInfoLength}")
        print(f"    objInfo: {obj_attr.objInfo}")
        
        if obj_attr.mcNameP:
            try:
                mc_decoded = ctypes.string_at(obj_attr.mcNameP).decode('utf-8', errors='ignore')
                print(f"    mcNameP: '{mc_decoded}'")
            except:
                print(f"    mcNameP: <pointer set>")
        else:
            print(f"    mcNameP: NULL")
        
        print(f"    disableDeduplication: {obj_attr.disableDeduplication}")
        
        # Full hex dump of obj_attr
        print("\n  DEBUG: Full ObjAttr Hex Dump:")
        buf = (c_ubyte * ctypes.sizeof(obj_attr)).from_address(ctypes.addressof(obj_attr))
        for i in range(0, min(len(buf), 112), 16):
            chunk = buf[i:i+16]
            hex_str = ' '.join(f'{b:02x}' for b in chunk)
            ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
            print(f"    [{i:3d}]: {hex_str:<48} | {ascii_str}")
        # ==================================
        
        lib.dsmSendObj.argtypes = [
            c_uint32,
            c_ubyte,
            c_void_p,
            POINTER(dsmObjName),
            POINTER(ObjAttr),
            c_void_p
        ]
        lib.dsmSendObj.restype = c_int16
        
        rc = lib.dsmSendObj(
            handle.value,
            c_ubyte(stBackup),
            None,
            ctypes.byref(obj_name),
            ctypes.byref(obj_attr),
            None
        )
        
        if rc != DSM_RC_SUCCESSFUL:
            print(f"\n✗ dsmSendObj failed: {rc}")
            try:
                lib.dsmRCMsg.argtypes = [c_uint32, c_int, c_char_p]
                lib.dsmRCMsg.restype = None
                error_msg = create_string_buffer(1024)
                lib.dsmRCMsg(handle.value, rc, error_msg)
                msg = error_msg.value.decode('utf-8', errors='ignore')
                if msg:
                    print(f"  Error: {msg}")
            except:
                pass
            
            if rc == 2065:
                print("\n  RC2065: Structure version mismatch")
                print(f"  Current: ObjAttr v{ObjAttrVersion}, size {ctypes.sizeof(obj_attr)}")
                print("  Solutions:")
                print("  1. Try changing line 25: ObjAttrVersion = 4")
                print("  2. Try changing line 25: ObjAttrVersion = 3")
                print("  3. Run tsm_structure_tester.py to find correct version")
            
            sys.exit(1)
        
        print("✓ Object metadata sent")
        
    except Exception as e:
        print(f"✗ dsmSendObj error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Send data
    print("\n[STEP 12] Sending data...")
    
    try:
        lib.dsmSendData.argtypes = [c_uint32, POINTER(DataBlk)]
        lib.dsmSendData.restype = c_int
        
        bytes_sent = 0
        buffer = create_string_buffer(READSIZE)
        
        with open(file_to_backup, 'rb') as ifd:
            while True:
                num_bytes = ifd.readinto(buffer)
                if num_bytes == 0:
                    break
                
                data_blk = DataBlk()
                data_blk.stVersion = 1
                data_blk.bufferLen = num_bytes
                data_blk.numBytes = 0
                data_blk.bufferPtr = ctypes.cast(buffer, c_void_p)
                
                rc = lib.dsmSendData(handle.value, ctypes.byref(data_blk))
                
                if rc != DSM_RC_SUCCESSFUL:
                    print(f"\n✗ dsmSendData failed: {rc}")
                    sys.exit(1)
                
                bytes_sent += num_bytes
                progress = int(100 * bytes_sent / file_size) if file_size > 0 else 100
                print(f"  Progress: {bytes_sent}/{file_size} ({progress}%)", end='\r')
        
        print(f"\n✓ Data sent: {bytes_sent} bytes")
        
    except Exception as e:
        print(f"\n✗ dsmSendData error: {e}")
        sys.exit(1)

    # End send object
    print("\n[STEP 13] Closing object...")
    
    try:
        lib.dsmEndSendObjEx.argtypes = [POINTER(dsmEndSendObjExIn_t), POINTER(dsmEndSendObjExOut_t)]
        lib.dsmEndSendObjEx.restype = c_int
        
        end_in = dsmEndSendObjExIn_t()
        end_in.stVersion = 1
        end_in.dsmHandle = handle.value
        
        end_out = dsmEndSendObjExOut_t()
        ctypes.memset(ctypes.addressof(end_out), 0, ctypes.sizeof(end_out))
        end_out.stVersion = 1
        
        rc = lib.dsmEndSendObjEx(ctypes.byref(end_in), ctypes.byref(end_out))
        
        if rc != DSM_RC_SUCCESSFUL:
            print(f"✗ dsmEndSendObjEx failed: {rc}")
            sys.exit(1)
        
        print("✓ Object closed")
        print(f"  Bytes sent: {end_out.totalBytesSent}")
            
    except Exception as e:
        print(f"✗ dsmEndSendObjEx error: {e}")
        sys.exit(1)

    # Commit transaction
    print("\n[STEP 14] Committing...")
    
    try:
        lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
        lib.dsmEndTxn.restype = c_int
        
        vote = c_ubyte(DSM_VOTE_COMMIT)
        reason = c_uint16(0)
        
        rc = lib.dsmEndTxn(handle.value, vote, ctypes.byref(reason))
        
        if rc != DSM_RC_SUCCESSFUL:
            print(f"✗ dsmEndTxn failed: {rc}")
            sys.exit(1)
        
        print("✓ Transaction committed!")
        
    except Exception as e:
        print(f"✗ dsmEndTxn error: {e}")
        sys.exit(1)

    # Cleanup
    print("\n[STEP 15] Cleanup...")
    
    try:
        lib.dsmTerminate.argtypes = [c_uint32]
        lib.dsmTerminate.restype = c_int
        lib.dsmTerminate(handle.value)
        print("✓ Session terminated")
    except:
        pass

    # Summary
    print("\n" + "=" * 70)
    print("✓✓✓ BACKUP COMPLETED! ✓✓✓")
    print("=" * 70)
    print(f"File: {file_to_backup} ({file_size} bytes)")
    print(f"\nVerify:")
    print(f"  dsmc query backup '{filespace_name}/*'")
    print("=" * 70)

if __name__ == "__main__":
    main()