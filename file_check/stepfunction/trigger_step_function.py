import json
import boto3

def lambda_handler(event, context):
    # Extract bucket name and file key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Initialize the Step Functions client
    sfn = boto3.client('stepfunctions')

    # Specify your state machine ARN here
    state_machine_arn = 'arn:aws:states:us-east-1:467519156370:stateMachine:hui-file-validation'

    # Start Step Functions execution
    input = {"bucket": bucket, "key": key}
    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(input)
    )

    return response
