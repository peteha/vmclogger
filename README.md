Scripts for Dealing with VMC Logging to S3 Bucket

This has been created as a proof of concept to pull S3 logs from a bucket are writen.

Based on https://blogs.vmware.com/cloud-foundation/2024/09/18/vmware-cloud-on-aws-sddc-logs-update/

'logtos3.py' - writes a NDJSON file in GZIP to the S3 bucket 
- Includes example code from VMC log.

'pulls3log-dynamodb.py' - pulls the s3 NDJSON log and writes to a JSON http endpoint
- Uses DynamoDB for key file to index S3 bucket files
- Sends only new files to HTTP endpoint when seen in S3 bucket
- Could be used a time based polling script to pull data from S3 periodically

'pulls3log-sqlite.py' - pulls the s3 NDJSON log and writes to a JSON http endpoint
- Uses sqlite for key file to index S3 bucket files
- Sends only new files to HTTP endpoint when seen in S3 bucket
- Could be used a time based polling script to pull data from S3 periodically

Use:
- Create a .env file in the directory:

```
bucket_name=<s3bucketname>
table_name=<dynamodbname>
url=<HTTP Endpoint>
```

- Setup Amazon credentials for boto - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
- Setup dynamo DB table for use in AWS
- Setup s3 bucket for testing
