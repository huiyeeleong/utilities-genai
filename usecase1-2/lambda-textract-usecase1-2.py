import boto3
import time
import json
import os

def lambda_handler(event, context):
    # Initialize a boto3 Textract client
    textract = boto3.client('textract')
    s3_client = boto3.client('s3')
    
    # Specify the path to the PDF file in the S3 bucket
    object_key = event['key']  
    pdf_file_path = object_key 
    json_file_name = os.path.splitext(os.path.basename(pdf_file_path))[0] + '.json'
    
    # Start the asynchronous analysis of the document
    start_response = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': 'bill-compare-hui', 'Name': pdf_file_path}},
        FeatureTypes=['FORMS']
    )
    
    # Get the JobId from the response
    job_id = start_response['JobId']
    
    # Wait for the job to complete
    print(f'Started job with id: {job_id}')
    print('Waiting for job to complete...')
    while True:
        status_response = textract.get_document_analysis(JobId=job_id)
        status = status_response['JobStatus']
        if status in ['SUCCEEDED', 'FAILED']:
            print(f'Job status: {status}')
            break
        time.sleep(5)
    
    # Proceed if the job succeeded
    if status == 'SUCCEEDED':
        # Get the results
        pages = []
        next_token = None
        while True:
            response_options = {
                'JobId': job_id,
                'MaxResults': 1000
            }
            if next_token:
                response_options['NextToken'] = next_token
            response = textract.get_document_analysis(**response_options)
            
            pages.extend(response['Blocks'])
            
            next_token = response.get('NextToken', None)
            if not next_token:
                break
    
        # Collect key-value pairs in a list and print them
        extracted_data = []
        for block in pages:
            if block['BlockType'] == 'KEY_VALUE_SET':
                if 'KEY' in block['EntityTypes']:
                    key_text = ""
                    for relationship in block.get('Relationships', []):
                        if relationship['Type'] == 'CHILD':
                            for child_id in relationship['Ids']:
                                child = [c for c in pages if c['Id'] == child_id][0]
                                if child['BlockType'] == 'WORD':
                                    key_text += child['Text'] + " "
                    key_text = key_text.strip()
                elif 'VALUE' in block['EntityTypes'] and 'Relationships' in block:
                    value_text = ""
                    for relationship in block['Relationships']:
                        if relationship['Type'] == 'CHILD':
                            for child_id in relationship['Ids']:
                                child = [c for c in pages if c['Id'] == child_id][0]
                                if child['BlockType'] == 'WORD':
                                    value_text += child['Text'] + " "
                    value_text = value_text.strip()
                    pair = {"Key": key_text, "Value": value_text}
                    extracted_data.append(pair)
                    print(f'{key_text}: {value_text}')  # Print each key-value pair
    
        # Convert extracted data to JSON
        json_data = json.dumps(extracted_data, indent=4)
    
        # # Save JSON file locally (optional)
        # local_json_filename = json_file_name
        # with open(local_json_filename, 'w') as json_file:
        #     json_file.write(json_data)
    
        # Upload JSON file to S3
        s3_bucket = 'bill-compare-hui'  # Or your target S3 bucket
        s3_object_name = f'json-output/{json_file_name}'
        s3_client.put_object(Bucket=s3_bucket, Key=s3_object_name, Body=json_data)
    
        print(f"JSON file uploaded to s3://{s3_bucket}/{s3_object_name}")
        return {
            "status": "SUCCEEDED",
            "s3Bucket": s3_bucket,
            "s3Key": s3_object_name,
            "message": f"Analysis completed successfully. Results uploaded to {s3_object_name}."
        }

    
    else:
        print(f'Document analysis failed with status: {status}')
        return {
            "status": "FAILED",
            "message": "Document analysis failed."
        }

