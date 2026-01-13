import ctypes
import os

LIB_PATH = "/opt/tivoli/tsm/client/api/bin64/libApiTSM64.so"

# Minimal version structure
class dsmApiVersion(ctypes.Structure):
    _fields_ = [
        ("version", ctypes.c_int32),
        ("release", ctypes.c_int32),
        ("level", ctypes.c_int32)
    ]

def main():
    # Ensure environment is set
    os.environ['LD_LIBRARY_PATH'] = "/opt/tivoli/tsm/client/api/bin64"
    
    try:
        tsm = ctypes.CDLL(LIB_PATH)
        print("✓ Library Loaded")
        
        # 1. Setup the structure
        ver = dsmApiVersion()
        
        # 2. Call the simplest possible function
        # This function fills the 'ver' structure with the library's version
        tsm.dsmQueryApiVersion(ctypes.byref(ver))
        
        print(f"✓ API Call Success!")
        print(f"✓ Library Version: {ver.version}.{ver.release}.{ver.level}")
        
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == "__main__":
    main()