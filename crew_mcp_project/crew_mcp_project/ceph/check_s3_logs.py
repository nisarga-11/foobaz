import boto3

AWS_ACCESS_KEY = "abc"
AWS_SECRET_KEY = "abc"
REGION = "us-east-1"
ENDPOINT_URL = "http://fenrir-vm158.storage.tucson.ibm.com:8080"
LOG_BUCKET = "dest-slog-bkt1"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION,
    endpoint_url=ENDPOINT_URL
)

log_files = [
    "src-slog-bkt1-2025-11-10-05-14-10-5L55A68B1E4SBGMK",
    "src-slog-bkt1-2025-11-10-06-14-31-9YFQEAPWQCMMGWUB"
]

for log_file in log_files:
    print("=" * 80)
    print(f"📄 LOG FILE: {log_file}")
    print("=" * 80)
    
    try:
        response = s3.get_object(Bucket=LOG_BUCKET, Key=log_file)
        content = response['Body'].read().decode('utf-8')
        
        print("\n📋 RAW CONTENT:")
        print(content)
        
        print("\n🔍 OPERATIONS FOUND:")
        lines = content.strip().split('\n')
        for i, line in enumerate(lines, 1):
            if 'PUT' in line:
                print(f"   Line {i}: ✏️  PUT operation")
            elif 'GET' in line:
                print(f"   Line {i}: 📥 GET operation")
            elif 'DELETE' in line:
                print(f"   Line {i}: 🗑️  DELETE operation")
            print(f"   {line[:100]}...")  # Show first 100 chars
            print()
        
    except Exception as e:
        print(f"❌ Error reading {log_file}: {e}")
    
    print("\n")