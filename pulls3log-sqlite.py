import json
import os
import requests
import boto3
import gzip
import sqlite3
from decimal import Decimal

# Ensure to install it using `pip install python-dotenv`
from dotenvy import load_env, read_file
import ndjson

try:
    load_env(read_file('.env'))
except Exception as e:
    raise EnvironmentError(f"Failed to load .env file: {e}")

bucket_name = os.getenv("bucket_name")
sqlitedb = os.getenv("sqlitedb")
url = os.getenv("url")
if not bucket_name or not url:
    raise EnvironmentError("bucket_name and/or url environment variables are not set")


def s3getfile(bucket_name, filename):
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
        payload = data
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Data sent successfully to {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending data: {e}")


def init_db():
    """Initializes the SQLite database and creates tables if they don't exist"""
    try:
        with sqlite3.connect(sqlitedb) as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS processed_files (
                         id TEXT PRIMARY KEY,
                         value TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS metadata (
                         key TEXT PRIMARY KEY,
                         value TEXT)''')
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error initializing database: {e}")


def get_last_processed_timestamp():
    """Retrieves the last processed timestamp from the SQLite database"""
    try:
        with sqlite3.connect(sqlitedb) as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM metadata WHERE key = 'last_processed_timestamp'")
            result = c.fetchone()
            if result and result[0]:
                return Decimal(result[0])
            return Decimal(0)
    except sqlite3.Error as e:
        print(f"Error retrieving last processed timestamp: {e}")
        return Decimal(0)


def set_last_processed_timestamp(timestamp):
    """Stores the last processed timestamp in the SQLite database"""
    try:
        with sqlite3.connect(sqlitedb) as conn:
            c = conn.cursor()
            c.execute("REPLACE INTO metadata (key, value) VALUES ('last_processed_timestamp', ?)", (str(timestamp),))
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error setting last processed timestamp: {e}")


def list_new_files_and_store_in_sqlite(s3bucket, s3url):
    s3 = boto3.client('s3')
    last_processed_timestamp = get_last_processed_timestamp()
    try:
        new_files = []
        s3_keys = set()
        response = s3.list_objects_v2(Bucket=s3bucket)
        while True:
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3_keys.add(obj['Key'])
                    if Decimal(obj['LastModified'].timestamp()) > last_processed_timestamp:
                        new_files.append(obj['Key'])
            if 'NextContinuationToken' in response:
                response = s3.list_objects_v2(Bucket=s3bucket, ContinuationToken=response['NextContinuationToken'])
            else:
                break
        if new_files:
            with sqlite3.connect(sqlitedb) as conn:
                c = conn.cursor()
                for key in new_files:
                    c.execute("REPLACE INTO processed_files (id, value) VALUES (?, 'processed')", (key,))
                    conn.commit()
                    indata = s3getfile(s3bucket, key)
                    for item in indata:
                        data = {"events": [{"text": json.dumps(item)}]}
                        send_json_to_logger(s3url, data)
                last_modified_timestamp = max(Decimal(obj['LastModified'].timestamp()) for obj in response['Contents'])
                set_last_processed_timestamp(last_modified_timestamp)
            print(f"Processed {len(new_files)} new files.")
        else:
            print("No new files found.")
        with sqlite3.connect(sqlitedb) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM processed_files")
            items = c.fetchall()
            for item in items:
                if item[0] not in s3_keys:
                    c.execute("DELETE FROM processed_files WHERE id = ?", (item[0],))
                    print(f"Removed {item[0]} from SQLite as it no longer exists in S3.")
            conn.commit()
    except Exception as error:
        print(f"Error listing objects, storing keys, or cleaning up SQLite DB: {error}")


# Initialize the SQLite database
init_db()
# Example usage
list_new_files_and_store_in_sqlite(bucket_name, url)
