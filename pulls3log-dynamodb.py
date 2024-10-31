import json
import os
import requests
import boto3
import gzip
from decimal import Decimal
from dotenvy import load_env, read_file
import ndjson

# Load environment variables
load_env(read_file('.env'))


def s3getfile(filename):
    """Fetches and decompresses a file from S3"""
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=filename)
        retndjson = ndjson.loads(gzip.decompress(response['Body'].read()))
        return retndjson
    except Exception as e:
        print(f"Error retrieving file {filename} from S3: {e}")
        return []


def send_json_to_logger(url, data, headers=None):
    """Sends JSON data to an HTTP endpoint"""
    if headers is None:
        headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        print(f"Data sent successfully to {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending data: {e}")


def list_new_files_and_store_in_dynamodb(bucket_name, table_name, url):
    """
    Lists new files from an S3 bucket based on last modified timestamp,
    stores their keys in DynamoDB, writes ONLY the NEW file names to a file,
    and cleans up the DynamoDB table by removing entries for files that no longer exist in S3.
    """
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={'id': 'last_processed_timestamp'})
        last_processed_timestamp = Decimal(response['Item']['value']) if 'Item' in response else Decimal(0)
    except Exception as e:
        print(f"Error getting last processed timestamp from DynamoDB: {e}")
        last_processed_timestamp = Decimal(0)

    try:
        new_files = []
        s3_keys = set()
        response = s3.list_objects_v2(Bucket=bucket_name)

        while True:
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3_keys.add(obj['Key'])
                    if Decimal(obj['LastModified'].timestamp()) > last_processed_timestamp:
                        new_files.append(obj['Key'])
            if 'NextContinuationToken' in response:
                response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
            else:
                break

        if new_files:
            with table.batch_writer() as batch:
                for key in new_files:
                    batch.put_item(Item={'id': key, 'value': 'processed'})
                    indata = s3getfile(key)
                    for item in indata:
                        data = {"events": [{"text": json.dumps(item)}]}
                        send_json_to_logger(url, data)

            last_modified_timestamp = max(Decimal(obj['LastModified'].timestamp()) for obj in response['Contents'])
            table.put_item(Item={'id': 'last_processed_timestamp', 'value': str(last_modified_timestamp)})
            print(f"Processed {len(new_files)} new files.")
        else:
            print("No new files found.")

        response = table.scan()
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        with table.batch_writer() as batch:
            for item in items:
                if item['id'] != 'last_processed_timestamp' and item['id'] not in s3_keys:
                    batch.delete_item(Key={'id': item['id']})
                    print(f"Removed {item['id']} from DynamoDB as it no longer exists in S3.")
    except Exception as e:
        print(f"Error listing objects, storing keys, or cleaning up DynamoDB: {e}")


# Example usage
bucket_name = os.getenv("bucket_name")
table_name = os.getenv("table_name")
url = os.getenv("url")
list_new_files_and_store_in_dynamodb(bucket_name, table_name, url)
