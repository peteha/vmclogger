import os
import boto3
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv(override=True)

def list_new_files_and_store_in_dynamodb(bucket_name, table_name, output_file):
    """
    Lists new files from an S3 bucket based on last modified timestamp,
    stores their keys in DynamoDB (using Decimal for timestamps),
    writes ONLY the NEW file names to a file, and cleans up the DynamoDB
    table by removing entries for files that no longer exist in S3.

    Args:
        bucket_name: The name of the S3 bucket.
        table_name: The name of the DynamoDB table.
        output_file: The path to the output file.
    """

    # Create S3 client and DynamoDB resource
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)  # Get the table object

    # Get the last processed timestamp from DynamoDB (if available)
    try:
        response = table.get_item(Key={'id': 'last_processed_timestamp'})
        if 'Item' in response:
            last_processed_timestamp = Decimal(response['Item']['value'])
        else:
            last_processed_timestamp = Decimal(0)  # Start from timestamp 0
    except Exception as e:
        print(f"Error getting last processed timestamp from DynamoDB: {e}")
        last_processed_timestamp = Decimal(0)

    # List objects in the bucket
    try:
        new_files = []
        s3_keys = set()  # Keep track of keys in S3
        response = s3.list_objects_v2(Bucket=bucket_name)
        while True:
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3_keys.add(obj['Key'])  # Add the key to the set
                    if Decimal(obj['LastModified'].timestamp()) > last_processed_timestamp:
                        new_files.append(obj['Key'])

            if 'NextContinuationToken' in response:
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    ContinuationToken=response['NextContinuationToken']
                )
            else:
                break

        # Store new file keys in DynamoDB and write to output file
        if new_files:
            with table.batch_writer() as batch:
                for key in new_files:
                    batch.put_item(Item={'id': key, 'value': 'processed'})

            with open(output_file, 'w') as f:
                for key in new_files:
                    f.write(f"{key}\n")

            print(f"Processed {len(new_files)} new files.")
            print(f"New file names written to {output_file}")

            # Update the last processed timestamp (get the latest modified timestamp)
            last_modified_timestamp = max(
                Decimal(obj['LastModified'].timestamp()) for obj in response['Contents']
            )
            table.put_item(Item={'id': 'last_processed_timestamp', 'value': str(last_modified_timestamp)})

        else:
            print("No new files found.")

        # Clean up the DynamoDB table
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

# Example usage:
bucket_name = os.getenv("bucket_name")
table_name = os.getenv("table_name")
output_file = os.getenv("output_file")
list_new_files_and_store_in_dynamodb(bucket_name, table_name, output_file)