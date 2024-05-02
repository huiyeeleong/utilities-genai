import json
import boto3
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Extract bucket name and file key from the S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Initialize the Step Functions client
        sfn = boto3.client('stepfunctions')
        
        # Specify your state machine ARN here
        state_machine_arn = 'arn:aws:states:us-east-1:467519156370:stateMachine:hui-file-validation-usecase3'
        
        # Prepare input for the state machine execution
        input = {"bucket": bucket, "key": key}
        
        # Start Step Functions execution
        response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(input)
        )
        
        # Log the response from starting the state machine execution
        logger.info("Started state machine execution: %s", response)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully triggered Step Functions execution.",
                "stateMachineArn": state_machine_arn,
                "executionArn": response['executionArn']
            })
        }
    except Exception as e:
        logger.error("Error triggering Step Functions execution: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
