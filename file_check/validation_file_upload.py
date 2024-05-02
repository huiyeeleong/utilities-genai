import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Extract the bucket name and object key from the Step Functions input
    bucket_name = event['bucket']
    object_key = event['key']
    
    # Initialize the existence flag
    exists = False

    try:
        # Attempt to get the object metadata
        s3.head_object(Bucket=bucket_name, Key=object_key)
        # If the above call succeeds, the file exists
        exists = True
    except ClientError as e:
        # If the error was a 404 (Not Found), the file does not exist
        if e.response['Error']['Code'] == '404':
            exists = False
        else:
            # For other errors, log the error message and continue
            print(f"Error checking file existence: {str(e)}")

    # Return the existence result
    return {
        'bucket': bucket_name,
        'key': object_key
    }
