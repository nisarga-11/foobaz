#!/usr/bin/env python3
"""
IBM Storage Protect API - TSM 8.2 Server Complete Backup Script
Corrected based on dapibkup.c reference implementation

Key fixes:
1. dsmSendObj signature: uses c_void_p for sendData (not c_int16)
2. ObjAttr owner field: must use ctypes.memmove for proper assignment
3. Proper NULL handling for optional parameters
4. Correct sendType parameter handling
5. Fixed mcBindKey structure and backup_cg_exists check
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
ObjAttrVersion = 4
DataBlkVersion = 1
mcBindKeyVersion = 1

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

# Backup types (from dsmapitd.h)
stBackup = 0x01
stArchive = 0x02

# Transaction vote
DSM_VOTE_COMMIT = 1
DSM_VOTE_ABORT = 2

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
        ("value", c_uint64),
    ]

class ObjAttr(Structure):
    """
    CRITICAL: This structure MUST match dsmapitd.h exactly
    Based on C reference code (dapibkup.c line ~200)
    """
    _alignment_ = 8
    _fields_ = [
        ("stVersion", c_uint16),
        ("_rsv1", c_uint16),
        ("_rsv2", c_uint32),

        ("owner", c_char * (DSM_MAX_OWNER_LENGTH + 1)),
        ("_pad1", c_char * 7),

        ("sizeEstimate", dsStruct64),

        ("objCompressed", c_ubyte),
        ("_pad2", c_ubyte),
        ("objInfoLength", c_uint16),
        ("_pad3", c_uint32),

        ("objInfo", c_void_p),
        ("mcNameP", c_void_p),

        ("disableDeduplication", c_ubyte),
        ("useExtObjInfo", c_ubyte),
        ("_pad4", c_uint16),
        ("_pad5", c_uint32),
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
        ("totalBytesSent", dsStruct64),
        ("objCompressed", c_ubyte),
        ("totalCompressSize", dsStruct64),
        ("totalLFBytesSent", dsStruct64),
        ("encryptionType", c_uint8),
    ]

class mcBindKey(Structure):
    """Management class bind key - must match dsmapitd.h exactly"""
    _fields_ = [
        ("stVersion", c_uint16),
        ("mcName", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
        ("backup_cg_exists", c_uint8),   # Use c_uint8 not c_ubyte for clarity
        ("archive_cg_exists", c_uint8),
        ("backup_copy_dest", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
        ("archive_copy_dest", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
    ]

# ==================== Main Script ====================

def main():
    print("=" * 70)
    print("IBM Storage Protect - TSM 8.2 Backup Script (CORRECTED)")
    print("Based on: dapibkup.c reference implementation")
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

    # Begin transaction (BEFORE dsmBindMC - per C reference)
    print("\n[STEP 9] Starting transaction...")
    
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

    # Bind management class (AFTER BeginTxn - per C reference)
    print("\n[STEP 10] Binding management class...")
    
    try:
        lib.dsmBindMC.argtypes = [c_uint32, POINTER(dsmObjName), c_ubyte, POINTER(mcBindKey)]
        lib.dsmBindMC.restype = c_int
        
        mc_bind_key = mcBindKey()
        ctypes.memset(ctypes.addressof(mc_bind_key), 0, ctypes.sizeof(mc_bind_key))
        mc_bind_key.stVersion = mcBindKeyVersion
        
        rc = lib.dsmBindMC(
            handle.value,
            ctypes.byref(obj_name),
            c_ubyte(stBackup),
            ctypes.byref(mc_bind_key)
        )
        
        if rc == DSM_RC_SUCCESSFUL:
            mc_name = mc_bind_key.mcName.decode('utf-8', errors='ignore').rstrip('\x00')
            backup_dest = mc_bind_key.backup_copy_dest.decode('utf-8', errors='ignore').rstrip('\x00')
            
            print(f"  ✓ Bound to MC: {mc_name}")
            print(f"    Backup copy dest: {backup_dest}")
            print(f"    backup_cg_exists value: {mc_bind_key.backup_cg_exists}")
            
            # Check if backup copy group exists (0 = does not exist, non-zero = exists)
            # Per C code: if (!MCBindKey.backup_cg_exists) then no copy group
            if mc_bind_key.backup_cg_exists == 0:
                print(f"  ⚠ '{mc_name}' has no backup copy group defined")
                # Abort transaction (DSM_VOTE_ABORT = 2)
                try:
                    lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
                    lib.dsmEndTxn.restype = c_int
                    reason = c_uint16(0)
                    lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
                except:
                    pass
                print(f"  → Transaction aborted - cannot backup to this management class")
                print(f"\n  SOLUTION: Either:")
                print(f"    1. Define a backup copy group for '{mc_name}' management class")
                print(f"    2. Use include-exclude rules to bind to a different management class")
                print(f"    3. Check your dsm.opt or dsm.sys configuration")
                print(f"\n  TSM Server Commands:")
                print(f"    dsmadmc \"query mgmtclass {mc_name} format=detailed\"")
                print(f"    dsmadmc \"query copygroup * {mc_name}\"")
                print(f"    dsmadmc \"define copygroup STANDARD {mc_name} type=backup dest=<POOLNAME>\"")
                sys.exit(1)
            else:
                print(f"  ✓ Backup copy group exists - ready to send data")
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
            # Abort transaction
            try:
                lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
                lib.dsmEndTxn.restype = c_int
                reason = c_uint16(0)
                lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
            except:
                pass
            sys.exit(1)
            
    except Exception as e:
        print(f"✗ dsmBindMC error: {e}")
        import traceback
        traceback.print_exc()
        # Abort transaction
        try:
            lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
            lib.dsmEndTxn.restype = c_int
            reason = c_uint16(0)
            lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
        except:
            pass
        sys.exit(1)

    # Send object
    print("\n[STEP 11] Sending object (dsmSendObj)...")
    print(f"  Following C reference: dapibkup.c lines 580-615")
    
    try:
        # Initialize ObjAttr - CRITICAL: Follow C code exactly (line ~560)
        obj_attr = ObjAttr()
        
        # Zero entire structure first (C: memset(&objAttr,0x00,sizeof(objAttr)))
        ctypes.memset(ctypes.addressof(obj_attr), 0, ctypes.sizeof(obj_attr))
        
        # Set version (C: objAttr.stVersion = ObjAttrVersion;)
        obj_attr.stVersion = ObjAttrVersion  # = 4
        
        # Set owner field using memmove (C reference line ~240-242)
        # The C code uses strcpy(objAttr.owner,dlg->item_buff)
        # For single-user systems (Intel), owner should be empty string
        owner_str = ""  # Empty string for single-user systems
        owner_bytes = owner_str.encode('utf-8')
        # Use memmove to copy into fixed char array
        ctypes.memmove(
            ctypes.addressof(obj_attr) + ObjAttr.owner.offset,
            owner_bytes,
            min(len(owner_bytes), DSM_MAX_OWNER_LENGTH)
        )
        
        # Set size estimate (C: objAttr.sizeEstimate.hi/lo = filesize64.hi/lo)
        obj_attr.sizeEstimate.value = file_size
        
        # Set compression (C: objAttr.objCompressed = bFalse)
        obj_attr.objCompressed = 0  # bFalse
        
        # No objInfo for simple backup (C: sets this later if needed)
        obj_attr.objInfoLength = 0
        obj_attr.objInfo = None
        
        # Management class override NULL = use include-exclude rules
        # (C: objAttr.mcNameP = mcOverride or NULL)
        obj_attr.mcNameP = None
        
        # Dedup and ext info (C code defaults)
        obj_attr.disableDeduplication = 0
        obj_attr.useExtObjInfo = 0
        
        print(f"  ✓ ObjAttr initialized:")
        print(f"    stVersion: {obj_attr.stVersion}")
        print(f"    sizeEstimate: {obj_attr.sizeEstimate.value}")
        print(f"    objCompressed: {obj_attr.objCompressed}")
        print(f"    objInfoLength: {obj_attr.objInfoLength}")
        print(f"    Structure size: {ctypes.sizeof(obj_attr)} bytes")

        # CRITICAL FIX: dsmSendObj signature from C reference (line ~580)
        # The C signature is:
        # dsInt16_t dsmSendObj(dsUint32_t handle, 
        #                      dsUint16_t sendType,
        #                      void      *sendDataP,     <- This is the key!
        #                      dsmObjName *objNameP,
        #                      ObjAttr    *objAttrP,
        #                      DataBlk    *dataBlkP)
        
        lib.dsmSendObj.argtypes = [
            c_uint32,                # handle
            c_uint16,                # sendType (NOT c_int16!)
            c_void_p,                # sendDataP (NULL for backup, sndArchiveData* for archive)
            POINTER(dsmObjName),     # objNameP
            POINTER(ObjAttr),        # objAttrP  
            c_void_p                 # dataBlkP (NULL if not sending data inline)
        ]
        lib.dsmSendObj.restype = c_int16

        # Call dsmSendObj (C reference line ~608)
        rc = lib.dsmSendObj(
            handle.value,
            c_uint16(stBackup),      # sendType
            None,                    # sendDataP (NULL for backup)
            ctypes.byref(obj_name),
            ctypes.byref(obj_attr),
            None                     # dataBlkP (NULL = will use dsmSendData)
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
            # Abort transaction
            try:
                lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
                lib.dsmEndTxn.restype = c_int
                reason = c_uint16(0)
                lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
            except:
                pass
            sys.exit(1)
        
        print("✓ Object metadata sent")
        
    except Exception as e:
        print(f"✗ dsmSendObj error: {e}")
        import traceback
        traceback.print_exc()
        # Abort transaction
        try:
            lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
            lib.dsmEndTxn.restype = c_int
            reason = c_uint16(0)
            lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
        except:
            pass
        sys.exit(1)

    # Send data (C reference lines ~625-665)
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
                
                # C reference: dataBlk.stVersion = DataBlkVersion;
                data_blk = DataBlk()
                data_blk.stVersion = DataBlkVersion
                data_blk.bufferLen = num_bytes
                data_blk.numBytes = 0
                data_blk.bufferPtr = ctypes.cast(buffer, c_void_p)
                
                rc = lib.dsmSendData(handle.value, ctypes.byref(data_blk))
                
                if rc != DSM_RC_SUCCESSFUL:
                    print(f"\n✗ dsmSendData failed: {rc}")
                    # Abort transaction
                    try:
                        lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
                        lib.dsmEndTxn.restype = c_int
                        reason = c_uint16(0)
                        lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
                    except:
                        pass
                    sys.exit(1)
                
                bytes_sent += data_blk.numBytes
                progress = int(100 * bytes_sent / file_size) if file_size > 0 else 100
                print(f"  Progress: {bytes_sent}/{file_size} ({progress}%)", end='\r')
        
        print(f"\n✓ Data sent: {bytes_sent} bytes")
        
    except Exception as e:
        print(f"\n✗ dsmSendData error: {e}")
        # Abort transaction
        try:
            lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
            lib.dsmEndTxn.restype = c_int
            reason = c_uint16(0)
            lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
        except:
            pass
        sys.exit(1)

    # End send object (C reference lines ~670-680)
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
            # Abort transaction
            try:
                lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
                lib.dsmEndTxn.restype = c_int
                reason = c_uint16(0)
                lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
            except:
                pass
            sys.exit(1)
        
        print("✓ Object closed")
        
        # Display results (C reference lines ~683-700)
        total_bytes = end_out.totalBytesSent.value
        total_compress = end_out.totalCompressSize.value
        total_lf = end_out.totalLFBytesSent.value
        
        print(f"  Total bytes sent: {total_bytes}")
        print(f"  Compressed: {'Yes' if end_out.objCompressed else 'No'}")
        if end_out.objCompressed:
            print(f"  Compressed size: {total_compress}")
        
        encrypt_types = {
            0: "Not encrypted",
            1: "CLIENTENCRKEY",
            2: "USER"
        }
        encrypt_type = encrypt_types.get(end_out.encryptionType, f"Unknown ({end_out.encryptionType})")
        print(f"  Encryption: {encrypt_type}")
        
        if total_lf > 0:
            print(f"  LAN-free bytes: {total_lf}")
            
    except Exception as e:
        print(f"✗ dsmEndSendObjEx error: {e}")
        # Abort transaction
        try:
            lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
            lib.dsmEndTxn.restype = c_int
            reason = c_uint16(0)
            lib.dsmEndTxn(handle.value, c_ubyte(DSM_VOTE_ABORT), ctypes.byref(reason))
        except:
            pass
        sys.exit(1)

    # Commit transaction (C reference lines ~710-725)
    print("\n[STEP 14] Committing transaction...")
    
    try:
        lib.dsmEndTxn.argtypes = [c_uint32, c_ubyte, POINTER(c_uint16)]
        lib.dsmEndTxn.restype = c_int
        
        vote = c_ubyte(DSM_VOTE_COMMIT)
        reason = c_uint16(0)
        
        rc = lib.dsmEndTxn(handle.value, vote, ctypes.byref(reason))
        
        if rc != DSM_RC_SUCCESSFUL or reason.value != 0:
            print(f"✗ dsmEndTxn failed: rc={rc}, reason={reason.value}")
            try:
                lib.dsmRCMsg.argtypes = [c_uint32, c_int, c_char_p]
                lib.dsmRCMsg.restype = None
                error_msg = create_string_buffer(1024)
                lib.dsmRCMsg(handle.value, rc, error_msg)
                msg = error_msg.value.decode('utf-8', errors='ignore')
                if msg:
                    print(f"  RC Error: {msg}")
                if reason.value != 0:
                    lib.dsmRCMsg(handle.value, reason.value, error_msg)
                    msg = error_msg.value.decode('utf-8', errors='ignore')
                    if msg:
                        print(f"  Reason: {msg}")
            except:
                pass
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
    print("✓✓✓ BACKUP COMPLETED SUCCESSFULLY! ✓✓✓")
    print("=" * 70)
    print(f"File: {file_to_backup}")
    print(f"Size: {file_size} bytes")
    print(f"Sent: {bytes_sent} bytes")
    print(f"\nVerify with:")
    print(f"  dsmc query backup '{filespace_name}/*'")
    print(f"  dsmc query backup '{filespace_name}{hl_path}{ll_name}'")
    print("=" * 70)

if __name__ == "__main__":
    main()