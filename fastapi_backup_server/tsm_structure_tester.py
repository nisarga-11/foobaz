#!/usr/bin/env python3
"""
TSM ObjAttr Structure Tester
Tests different structure versions and configurations to find the correct one
"""
import ctypes
from ctypes import *
import os
import sys

# Constants
DSM_MAX_OWNER_LENGTH = 64

class dsStruct64(Structure):
    _fields_ = [
        ("hi", c_uint32),
        ("lo", c_uint32),
    ]

print("=" * 70)
print("TSM ObjAttr Structure Tester")
print("=" * 70)

# Test different structure versions
print("\nTesting different ObjAttr structure definitions:\n")

# Version 4 - Basic (from header)
class ObjAttr_v4_basic(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("owner", c_char * (DSM_MAX_OWNER_LENGTH + 1)),
        ("sizeEstimate", dsStruct64),
        ("objCompressed", c_ubyte),
        ("objInfoLength", c_uint16),
        ("objInfo", c_char_p),
        ("mcNameP", c_char_p),
        ("disableDeduplication", c_ubyte),
        ("useExtObjInfo", c_ubyte),
    ]

print(f"Version 4 (basic):")
print(f"  Size: {ctypes.sizeof(ObjAttr_v4_basic)} bytes")
print(f"  Fields: {len(ObjAttr_v4_basic._fields_)}")
for name, _ in ObjAttr_v4_basic._fields_:
    offset = getattr(ObjAttr_v4_basic, name).offset
    print(f"    {name:25s} offset: {offset:3d}")

# Version 4 - With pragma pack(1)
class ObjAttr_v4_packed(Structure):
    _pack_ = 1
    _fields_ = [
        ("stVersion", c_uint16),
        ("owner", c_char * (DSM_MAX_OWNER_LENGTH + 1)),
        ("sizeEstimate", dsStruct64),
        ("objCompressed", c_ubyte),
        ("objInfoLength", c_uint16),
        ("objInfo", c_char_p),
        ("mcNameP", c_char_p),
        ("disableDeduplication", c_ubyte),
        ("useExtObjInfo", c_ubyte),
    ]

print(f"\nVersion 4 (packed):")
print(f"  Size: {ctypes.sizeof(ObjAttr_v4_packed)} bytes")
print(f"  Fields: {len(ObjAttr_v4_packed._fields_)}")
for name, _ in ObjAttr_v4_packed._fields_:
    offset = getattr(ObjAttr_v4_packed, name).offset
    print(f"    {name:25s} offset: {offset:3d}")

# Version 3 - Without useExtObjInfo
class ObjAttr_v3(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("owner", c_char * (DSM_MAX_OWNER_LENGTH + 1)),
        ("sizeEstimate", dsStruct64),
        ("objCompressed", c_ubyte),
        ("objInfoLength", c_uint16),
        ("objInfo", c_char_p),
        ("mcNameP", c_char_p),
        ("disableDeduplication", c_ubyte),
    ]

print(f"\nVersion 3 (no useExtObjInfo):")
print(f"  Size: {ctypes.sizeof(ObjAttr_v3)} bytes")
print(f"  Fields: {len(ObjAttr_v3._fields_)}")
for name, _ in ObjAttr_v3._fields_:
    offset = getattr(ObjAttr_v3, name).offset
    print(f"    {name:25s} offset: {offset:3d}")

# Version 5 - Potential future version
class ObjAttr_v5(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("owner", c_char * (DSM_MAX_OWNER_LENGTH + 1)),
        ("sizeEstimate", dsStruct64),
        ("objCompressed", c_ubyte),
        ("objInfoLength", c_uint16),
        ("objInfo", c_char_p),
        ("mcNameP", c_char_p),
        ("disableDeduplication", c_ubyte),
        ("useExtObjInfo", c_ubyte),
        ("reserved1", c_uint16),  # Potential reserved field
        ("reserved2", c_uint32),  # Potential reserved field
    ]

print(f"\nVersion 5 (with reserved fields):")
print(f"  Size: {ctypes.sizeof(ObjAttr_v5)} bytes")
print(f"  Fields: {len(ObjAttr_v5._fields_)}")
for name, _ in ObjAttr_v5._fields_:
    offset = getattr(ObjAttr_v5, name).offset
    print(f"    {name:25s} offset: {offset:3d}")

print("\n" + "=" * 70)
print("INSTRUCTIONS:")
print("=" * 70)
print("\n1. Check your header file for the exact ObjAttr definition:")
print("   grep -B 5 -A 30 'typedef struct' /opt/tivoli/tsm/client/api/bin64/sample/dsmapitd.h | grep -B 5 -A 30 ObjAttr")
print("\n2. Look for the #define ObjAttrVersion line in the header")
print("\n3. Compare structure sizes:")
print("   - If header shows pragma pack or __attribute__((packed)), use packed version")
print("   - Otherwise use basic (natural alignment)")
print("\n4. Common sizes:")
print("   - Version 3: ~96 bytes")
print("   - Version 4: ~104 bytes") 
print("   - Version 5: ~112 bytes")
print("\n5. Match the structure fields EXACTLY as shown in header file")
print("\n6. Count all fields including:")
print("   - Boolean flags (dsmBool_t)")
print("   - Reserved fields")
print("   - Padding fields (if pragma pack is NOT used)")

print("\n" + "=" * 70)
print("NEXT STEPS:")
print("=" * 70)
print("\n1. Run this to see your actual header definition:")
print("   grep -A 15 '^typedef struct' /opt/tivoli/tsm/client/api/bin64/sample/dsmapitd.h | grep -A 15 ObjAttr | head -20")
print("\n2. Look for lines between 'typedef struct' and '}ObjAttr;'")
print("\n3. Share the output and I'll create the exact structure")
print("\n4. Also check if there's a #pragma pack before the structure:")
print("   grep -B 20 'typedef struct' /opt/tivoli/tsm/client/api/bin64/sample/dsmapitd.h | grep -A 20 ObjAttr | grep -i pragma")

print("\n" + "=" * 70)