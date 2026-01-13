import ctypes
import os

LIB_PATH = "/opt/tivoli/tsm/client/api/bin64/libApiTSM64.so"

class dsmApiVersion(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("version", ctypes.c_int32),
        ("release", ctypes.c_int32),
        ("level", ctypes.c_int32)
    ]

class dsmInitExIn_t(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("apiVersionP", ctypes.POINTER(dsmApiVersion)),
        ("clientNodeNameP", ctypes.c_char_p),
        ("clientPasswordP", ctypes.c_char_p),
        ("applicationTypeP", ctypes.c_char_p),
        ("configfile", ctypes.c_char_p),
        ("options", ctypes.c_char_p),
        ("userNameP", ctypes.c_char_p),
        ("userPasswordP", ctypes.c_char_p)
    ]

class dsmInitExOut_t(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("apiReleaseP", ctypes.POINTER(dsmApiVersion)),
        ("arch", ctypes.c_char * 32),
        ("adsmLevel", ctypes.c_int32)
    ]

def main():
    # 1. Force the API environment variables
    os.environ['DSMI_DIR'] = "/opt/tivoli/tsm/client/api/bin64"
    os.environ['DSMI_CONFIG'] = "/opt/tivoli/tsm/client/api/bin64/dsm.opt"
    
    # Verify dsm.opt exists to prevent RC 102
    if not os.path.exists(os.environ['DSMI_CONFIG']):
        print(f"ERROR: Configuration file {os.environ['DSMI_CONFIG']} not found!")
        return

    tsm = ctypes.CDLL(LIB_PATH)

    # 2. Set function signature for memory safety
    tsm.dsmInitEx.argtypes = [
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(dsmInitExIn_t),
        ctypes.POINTER(dsmInitExOut_t),
        ctypes.c_void_p
    ]
    tsm.dsmInitEx.restype = ctypes.c_int32

    # Step 3: Call Setup
    tsm.dsmSetUp(ctypes.c_int32(0), None, None)

    # Step 4: Populate data
    ver = dsmApiVersion(8, 1, 27)
    init_in = dsmInitExIn_t()
    init_in.apiVersionP = ctypes.pointer(ver)
    init_in.clientNodeNameP = b"FOOBAZ"
    init_in.clientPasswordP = b"foobaz123"
    init_in.applicationTypeP = b"TSMAPI"
    
    # Initialize all other pointers to NULL explicitly
    init_in.configfile = None
    init_in.options = None
    init_in.userNameP = None
    init_in.userPasswordP = None
    
    init_out = dsmInitExOut_t()
    dsmHandle = ctypes.c_uint32(0)

    print(f"Connecting to BUGSBUNNY for node {init_in.clientNodeNameP.decode()}...")
    
    try:
        rc = tsm.dsmInitEx(
            ctypes.byref(dsmHandle), 
            ctypes.byref(init_in), 
            ctypes.byref(init_out), 
            None
        )
        
        if rc == 0:
            print("="*40)
            print(f"✓ SUCCESS! Session Handle: {dsmHandle.value}")
            print(f"✓ Server Arch: {init_out.arch.decode().strip()}")
            print("="*40)
            tsm.dsmTerminate(dsmHandle)
        else:
            print(f"✗ Failed with RC: {rc}")
            if rc == 102:
                print("Hint: The API library cannot find dsm.opt or dsm.sys.")
    except Exception as e:
        print(f"Critical Failure: {e}")

if __name__ == "__main__":
    main()