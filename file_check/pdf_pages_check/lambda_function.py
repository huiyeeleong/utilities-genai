import json
import boto3
import fitz  # PyMuPDF

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    
    bucket_name = event['bucket']
    object_key = event['key']
    
    # Download the PDF file to the /tmp directory (note: Lambda has limited space in /tmp)
    local_tmp_path = f"/tmp/{object_key.split('/')[-1]}"  # Assuming object_key has no '/'
    s3_client.download_file(bucket_name, object_key, local_tmp_path)
    
    # Open the PDF and count the pages
    try:
        doc = fitz.open(local_tmp_path)
        num_pages = len(doc)
        doc.close()
    except Exception as e:
        print(f"Error processing PDF file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Failed to process PDF file'})
        }
    
    # Return the number of pages along with the original input
    event['numPages'] = num_pages
    return event
