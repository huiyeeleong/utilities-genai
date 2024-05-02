import boto3
import time
import json
import os

def lambda_handler(event, context):
    # Initialize a boto3 Textract client
    textract = boto3.client('textract')
    s3_client = boto3.client('s3')

    # You might want to get the PDF file path from the event object if it's dynamically provided
    #pdf_file_path = event.get('pdf_file_path', 'invoice-data/CI_Invoice_10992378_aaah.pdf')'
    pdf_file_path = event.get('key')
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

    if status == 'SUCCEEDED':
        pages = []
        next_token = None
        while True:
            response_options = {'JobId': job_id, 'MaxResults': 1000}
            if next_token:
                response_options['NextToken'] = next_token
            response = textract.get_document_analysis(**response_options)
            
            pages.extend(response['Blocks'])
            
            next_token = response.get('NextToken', None)
            if not next_token:
                break

        # Collect all text in a list
        extracted_text = []
        for block in pages:
            if block['BlockType'] == 'LINE':
                extracted_text.append(block['Text'])

        # Convert extracted text to JSON
        json_data = json.dumps(extracted_text, indent=4)

        # Save JSON file locally - This part might need to be changed since Lambda has limited local storage
        # local_json_filename = '/tmp/' + json_file_name
        # with open(local_json_filename, 'w') as json_file:
        #     json_file.write(json_data)

        # Optionally upload JSON file to S3
        s3_bucket = 'bill-compare-hui'
        s3_object_name = f'energy-output/{json_file_name}'
        s3_client.put_object(Bucket=s3_bucket, Key=s3_object_name, Body=json_data)

        print(f"JSON file uploaded to s3://{s3_bucket}/{s3_object_name}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"JSON file uploaded to s3://{s3_bucket}/{s3_object_name}",
                "s3Path": f"{s3_object_name}"
            })
        }
    else:
        print(f'Document analysis failed with status: {status}')
        return {
            'statusCode': 500,
            'body': json.dumps('Document analysis failed.')
        }

