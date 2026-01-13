#!/usr/bin/env python3
"""
Client script to interact with S3 Log Parser FastAPI server
Usage: python client_parser.py <start_time> <end_time> [operation]
Example: python client_parser.py 06/Nov/2025:04:00:00 06/Nov/2025:05:00:00 PUT
"""

import sys
import requests
import json
from datetime import datetime

# API Configuration
API_BASE_URL = "http://localhost:8000"

def print_banner():
    """Print banner"""
    print("\n" + "🪟" * 30)
    print("  S3 LOG PARSER API CLIENT")
    print("🪟" * 30 + "\n")

def validate_time_format(time_str):
    """Validate time format DD/MMM/YYYY:HH:MM:SS"""
    time_fmt = "%d/%b/%Y:%H:%M:%S"
    try:
        datetime.strptime(time_str, time_fmt)
        return True
    except ValueError:
        return False

def parse_logs(start_time, end_time, operation_filter="ALL"):
    """
    Call the /parse endpoint to parse logs and mirror operations
    """
    print(f"🔍 Parsing logs from {start_time} to {end_time}")
    print(f"⚙️  Operation filter: {operation_filter}")
    print(f"📡 Sending request to: {API_BASE_URL}/parse\n")
    
    payload = {
        "start_time": start_time,
        "end_time": end_time,
        "operation_filter": operation_filter
    }
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/parse",
            json=payload,
            timeout=300  # 5 minutes timeout
        )
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            print("✅ SUCCESS!")
            print("\n" + "="*70)
            print("  RESPONSE SUMMARY")
            print("="*70)
            print(f"\nStatus: {data.get('status')}")
            print(f"Message: {data.get('message')}")
            
            if 'details' in data:
                details = data['details']
                print("\n📊 Details:")
                print(f"   • Time Range: {details.get('time_range')}")
                print(f"   • Operation Filter: {details.get('operation_filter')}")
                print(f"   • Logs Downloaded: {details.get('logs_downloaded')}")
                print(f"   • Total Operations: {details.get('total_operations')}")
                print(f"   • PUT Operations: {details.get('put_operations')}")
                print(f"   • GET Operations: {details.get('get_operations')}")
                print(f"   • DELETE Operations: {details.get('delete_operations')}")
                print(f"   • Files Remaining: {details.get('files_remaining')}")
                
                if details.get('remaining_files'):
                    print(f"\n📁 Remaining Files:")
                    for file in details['remaining_files']:
                        print(f"     - {file}")
                
                print(f"\n📄 Log File: {details.get('log_file')}")
                print(f"📂 Download Directory: {details.get('download_dir')}")
            
            return data
        
        else:
            print(f"❌ ERROR: Request failed with status code {response.status_code}")
            try:
                error_data = response.json()
                print(f"\nError details: {error_data.get('detail', 'No details available')}")
            except:
                print(f"\nResponse: {response.text}")
            return None
    
    except requests.exceptions.ConnectionError:
        print(f"❌ ERROR: Could not connect to API server at {API_BASE_URL}")
        print("   Make sure the FastAPI server is running:")
        print("   uvicorn main:app --host 0.0.0.0 --port 8000")
        return None
    
    except requests.exceptions.Timeout:
        print("❌ ERROR: Request timed out")
        return None
    
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return None

def get_status():
    """Get current status of downloads directory"""
    print("\n📊 Getting status...\n")
    
    try:
        response = requests.get(f"{API_BASE_URL}/status", timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Status retrieved successfully!")
            print(f"\nMessage: {data.get('message')}")
            
            if 'details' in data:
                details = data['details']
                print(f"\n📂 Download Directory: {details.get('download_dir')}")
                print(f"📁 File Count: {details.get('file_count')}")
                
                if details.get('files'):
                    print(f"\n📄 Files:")
                    for file in details['files']:
                        print(f"   • {file['name']}")
                        print(f"     Size: {file['size']} bytes")
                        print(f"     Modified: {file['modified']}")
            
            return data
        else:
            print(f"❌ ERROR: Status code {response.status_code}")
            return None
    
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return None

def list_files():
    """List all files in downloads directory"""
    print("\n📁 Listing files...\n")
    
    try:
        response = requests.get(f"{API_BASE_URL}/files", timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Found {data.get('count')} file(s)")
            print(f"📂 Directory: {data.get('download_dir')}\n")
            
            if data.get('files'):
                print("Files:")
                for i, file in enumerate(data['files'], 1):
                    print(f"   {i}. {file}")
            else:
                print("   (No files)")
            
            return data
        else:
            print(f"❌ ERROR: Status code {response.status_code}")
            return None
    
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return None

def download_file(filename):
    """Download a specific file"""
    print(f"\n⬇️  Downloading file: {filename}\n")
    
    try:
        response = requests.get(f"{API_BASE_URL}/download/{filename}", timeout=60)
        
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"✅ File downloaded successfully: {filename}")
            return True
        elif response.status_code == 404:
            print(f"❌ ERROR: File '{filename}' not found")
            return False
        else:
            print(f"❌ ERROR: Status code {response.status_code}")
            return False
    
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False

def clear_downloads():
    """Clear all files from downloads directory"""
    print("\n🧹 Clearing all downloads...\n")
    
    try:
        response = requests.delete(f"{API_BASE_URL}/files", timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ {data.get('message')}")
            if 'details' in data:
                print(f"   Deleted: {data['details'].get('deleted_count')} file(s)")
            return data
        else:
            print(f"❌ ERROR: Status code {response.status_code}")
            return None
    
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return None

def main():
    """Main function"""
    print_banner()
    
    # Check arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Parse logs:")
        print("    python client_parser.py <start_time> <end_time> [operation]")
        print("    Example: python client_parser.py 06/Nov/2025:04:00:00 06/Nov/2025:05:00:00 PUT")
        print()
        print("  Get status:")
        print("    python client_parser.py --status")
        print()
        print("  List files:")
        print("    python client_parser.py --list")
        print()
        print("  Download file:")
        print("    python client_parser.py --download <filename>")
        print()
        print("  Clear downloads:")
        print("    python client_parser.py --clear")
        print()
        print("Supported operations: PUT, GET, DELETE, ALL (default)")
        sys.exit(1)
    
    # Handle commands
    command = sys.argv[1]
    
    if command == "--status":
        get_status()
    
    elif command == "--list":
        list_files()
    
    elif command == "--download":
        if len(sys.argv) < 3:
            print("❌ ERROR: Please specify a filename")
            print("Usage: python client_parser.py --download <filename>")
            sys.exit(1)
        download_file(sys.argv[2])
    
    elif command == "--clear":
        confirm = input("⚠️  Are you sure you want to clear all downloads? (yes/no): ")
        if confirm.lower() == 'yes':
            clear_downloads()
        else:
            print("❌ Cancelled")
    
    else:
        # Parse logs
        if len(sys.argv) < 3:
            print("❌ ERROR: Please provide start_time and end_time")
            print("Usage: python client_parser.py <start_time> <end_time> [operation]")
            sys.exit(1)
        
        start_time = sys.argv[1]
        end_time = sys.argv[2]
        operation_filter = sys.argv[3].upper() if len(sys.argv) > 3 else "ALL"
        
        # Validate time format
        if not validate_time_format(start_time):
            print(f"❌ ERROR: Invalid start_time format: {start_time}")
            print("   Use format: DD/MMM/YYYY:HH:MM:SS")
            print("   Example: 06/Nov/2025:04:00:00")
            sys.exit(1)
        
        if not validate_time_format(end_time):
            print(f"❌ ERROR: Invalid end_time format: {end_time}")
            print("   Use format: DD/MMM/YYYY:HH:MM:SS")
            print("   Example: 06/Nov/2025:05:00:00")
            sys.exit(1)
        
        # Validate operation
        valid_operations = ["ALL", "PUT", "GET", "DELETE"]
        if operation_filter not in valid_operations:
            print(f"❌ ERROR: Invalid operation: {operation_filter}")
            print(f"   Supported operations: {', '.join(valid_operations)}")
            sys.exit(1)
        
        # Parse logs
        result = parse_logs(start_time, end_time, operation_filter)
        
        if result:
            print("\n✅ Operation completed successfully!")
        else:
            print("\n❌ Operation failed!")
            sys.exit(1)

if __name__ == "__main__":
    main()