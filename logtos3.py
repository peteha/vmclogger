import boto3
import gzip
import json
import uuid
import os
from dotenvy import load_env, read_file

load_env(read_file('.env'))


def upload_compressed_ndjson_to_s3(data, bucket_name, object_key):
  """
  Uploads compressed ndjson data to an S3 bucket.

  Args:
    data: A list of dictionaries representing the ndjson data.
    bucket_name: The name of the S3 bucket.
    object_key: The key for the object in the S3 bucket.
  """

  s3 = boto3.client('s3')

  # Convert data to ndjson string
  ndjson_string = '\n'.join(json.dumps(item) for item in data)

  # Compress the ndjson string
  compressed_data = gzip.compress(ndjson_string.encode('utf-8'))

  # Upload the compressed data to S3
  s3.put_object(
      Bucket=bucket_name,
      Key=object_key,
      Body=compressed_data,
      ContentEncoding='gzip'  # Specify the content encoding
  )


def list_files_in_bucket(bucket_name):
  """
  Lists all files in an S3 bucket.

  Args:
      bucket_name: The name of the S3 bucket.
  """

  # Create an S3 client
  s3 = boto3.client('s3')

  # List objects in the bucket
  try:
      response = s3.list_objects_v2(Bucket=bucket_name)
      if 'Contents' in response:
          for obj in response['Contents']:
              print(f"Object key: {obj['Key']}")
      else:
          print("No objects found in the bucket.")
  except Exception as e:
      print(f"Error listing objects: {e}")


# Example usage:
data = [
    {"@timestamp": "2024-10-23T21:05:43.507363391Z", "component": "NSX", "org_id": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "org_type": "TEST", "pop_instance_id": "i-fc1482f5-6519-4d15-9531-614bf7ce1f1d", "region": "us-west-2", "sddc_id": "fc1482f5-6519-4d15-9531-614bf7ce1f1d", "sddc_org": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "sddc_provider": "AWS", "source_type": "sddc", "tenant": "lkjoijoi", "text": "<182>1 2024-09-25T21:05:43.506Z NSX-Manager-2 NSX 76574 - [nsx@6876 comp=\"nsx-manager\" subcomp=\"node-mgmt\" username=\"admin\" level=\"INFO\" audit=\"true\"] admin 'GET /api/v1/node/services/http/status' 200 441 \"\" \"okhttp/4.4.1\" 1.146290", "hostname": "NSX-Manager-2", "appname": "NSX", "process": "76574", "msgid": 1233, "log_timestamp": 1727298343506},
    {"@timestamp": "2024-10-23T21:05:42.331008987Z", "component": "NSX", "org_id": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "org_type": "TEST", "pop_instance_id": "i-fc1482f5-6519-4d15-9531-614bf7ce1f1d", "region": "us-west-2", "sddc_id": "fc1482f5-6519-4d15-9531-614bf7ce1f1d", "sddc_org": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "sddc_provider": "AWS", "source_type": "sddc", "tenant": "lkjoijoi", "text": "<182>1 2024-09-25T21:05:42.330Z NSX-Manager-1 NSX 118178 SYSTEM [nsx@6876 audit=\"true\" comp=\"nsx-manager\" level=\"INFO\" subcomp=\"http\"] UserName=\"nottelling@goaway\", ModuleName=\"ACCESS_CONTROL\", Operation=\"LOGIN\", Operation status=\"success\"", "hostname": "NSX-Manager-1", "appname": "NSX", "process": "118178", "msgid": "SYSTEM", "log_timestamp": 1727298342330},
    {"@timestamp": "2024-10-23T21:05:43.51859294Z", "component": "NSX", "org_id": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "org_type": "TEST", "pop_instance_id": "i-fc1482f5-6519-4d15-9531-614bf7ce1f1d", "region": "us-west-2", "sddc_id": "fc1482f5-6519-4d15-9531-614bf7ce1f1d", "sddc_org": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "sddc_provider": "AWS", "source_type": "sddc", "lkjoijoi": "vmc-aws", "text": "<182>1 2024-09-25T21:05:43.517Z NSX-Manager-1 NSX 76552 - [nsx@6876 comp=\"nsx-manager\" subcomp=\"node-mgmt\" username=\"admin\" level=\"INFO\" audit=\"true\"] admin 'GET /api/v1/cluster/3ade4376-b1c4-4e41-9963-ef9952abbbb3/node/services/http/status' 200 423 \"\" \"okhttp/4.4.1\" 1.185224", "hostname": "NSX-Manager-1", "appname": "NSX", "process": "76552", "msgid": 457849, "log_timestamp": 1727298343517},
    {"@timestamp": "2024-10-23T21:05:48.525128891Z", "component": "NSX", "org_id": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "org_type": "INTERNAL_CORE", "pop_instance_id": "i-fc1482f5-6519-4d15-9531-614bf7ce1f1d", "region": "us-west-2", "sddc_id": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "sddc_org": "3ade4376-b1c4-4e41-9963-ef9952abbbb3", "sddc_provider": "lkjoijoi", "source_type": "sddc", "tenant": "vmc-aws", "text": "<182>1 2024-09-25T21:05:48.523Z NSX-Manager-1 NSX 118178 SYSTEM [nsx@6876 audit=\"true\" comp=\"nsx-manager\" level=\"INFO\" subcomp=\"http\"] UserName=\"nottelling@goaway\", ModuleName=\"ACCESS_CONTROL\", Operation=\"LOGIN\", Operation status=\"success\"", "hostname": "NSX-Manager-1", "appname": "NSX", "process": "118178", "msgid": "SYSTEM", "log_timestamp": 1727298348523}
]
new_uuid = uuid.uuid4()
bucket_name = os.getenv("bucket_name")
object_key = f"{new_uuid}.ndjson.gz"

upload_compressed_ndjson_to_s3(data, bucket_name, object_key)
list_files_in_bucket(bucket_name)
