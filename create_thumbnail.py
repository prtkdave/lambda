import os
import json
import boto3
import io
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
from PIL import Image

# Function to create thumbnail
def create_thumbnail(bucket_name, key, thumbnail_dir):
    s3 = boto3.client('s3')
    thumbnail_key = os.path.join(thumbnail_dir, os.path.splitext(key)[0] + '_thumbnail.jpg')
    
    try:
        # Get the image object
        response = s3.get_object(Bucket=bucket_name, Key=key)
        image_body = response['Body'].read()
        
        # Create thumbnail
        image = Image.open(io.BytesIO(image_body))
        image.thumbnail((100, 100))
        
        # Convert image to RGB mode if it has transparency
        if image.mode == 'RGBA':
            image = image.convert('RGB')
        
        # Save thumbnail to S3
        thumbnail_obj = io.BytesIO()
        image.save(thumbnail_obj, format='JPEG')
        thumbnail_obj.seek(0)
        s3.put_object(Body=thumbnail_obj, Bucket=bucket_name, Key=thumbnail_key)
        print(f"Thumbnail created and saved to '{bucket_name}/{thumbnail_key}'")
        
        return thumbnail_key
    except ClientError as e:
        error_message = f"Client error occurred: {e.response['Error']['Message']}"
        print(error_message)
        print(f"Error accessing object in bucket '{bucket_name}' with key '{key}'")
        return None
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(f"Error processing object in bucket '{bucket_name}' with key '{key}'")
        return None

# Function to save data to DynamoDB
def save_to_dynamodb(table_name, data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    try:
        for key, details in data.items():
            details['upload_date'] = datetime.now(timezone.utc).isoformat()
            table.put_item(Item={
                'key': details['key'],
                'uri': details['uri'],
                'object_size': details['object_size'],
                'object_type': details['object_type'],
                'thumbnail_url': details.get('thumbnail_url', 'N/A'),
                'upload_date': details['upload_date']
            })
        print("Data saved to DynamoDB successfully")
    except Exception as e:
        print("Error saving data to DynamoDB:", e)



# Lambda function for processing uploaded object and saving to DynamoDB
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    today = datetime.now().strftime('%Y-%m-%d')

    subject = f"S3 Uploads Report - {today}"
    body = ""
    thumbnail_dir = 'thumbnail_dir'  # Name of the directory to store thumbnails
    
    upload_details = {}
    
    for record in event['Records']:
        # Get the details of the uploaded object
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        size = record['s3']['object'].get('size', 'N/A')
        obj_type = record['s3']['object']['key'].split('.')[-1]
        uri = f"s3://{bucket}/{key}"
        
        # Capture the details
        upload_details[key] = {
            'uri': uri,
            'key': key,
            'object_size': f"{size} bytes",
            'object_type': obj_type,
            'date': today  # Add the current date to the data
        }
        
        # If the uploaded object is an image, create a thumbnail
        if obj_type.lower() in ['jpg', 'jpeg', 'png']:
            thumbnail_key = create_thumbnail(bucket, key, thumbnail_dir)
            if thumbnail_key:
                upload_details[key]['thumbnail_url'] = f"s3://{bucket}/{thumbnail_dir}/{thumbnail_key}"
    
    # Save upload details to DynamoDB
    save_to_dynamodb('bitscloud-db', upload_details)

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed successfully!')
    }
