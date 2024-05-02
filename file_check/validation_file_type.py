import json

def lambda_handler(event, context):
    # Extract the object key from the event
    object_key = event['key']
    
    # Check if the file is a PDF by looking at the extension
    is_pdf = object_key.lower().endswith('.pdf')

    # Return the result along with the original input for continuity
    event['isPdf'] = is_pdf
    return event
