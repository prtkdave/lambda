import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

def send_email(sender, recipients, subject, body_text, body_html=None):
    ses_client = boto3.client('ses')
    charset = 'UTF-8'
    if body_html is None:
        body_html = body_text
    
    try:
        response = ses_client.send_email(
            Destination={'ToAddresses': recipients},
            Message={
                'Body': {
                    'Text': {'Charset': charset, 'Data': body_text},
                    'Html': {'Charset': charset, 'Data': body_html}
                },
                'Subject': {'Charset': charset, 'Data': subject},
            },
            Source=sender
        )
        print("Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("Failed to send email:", e.response['Error']['Message'])

def load_from_dynamodb(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    try:
        twenty_four_hours_ago = datetime.now(timezone.utc) - timedelta(hours=24)
        response = table.scan(
            FilterExpression='upload_date >= :upload_date',
            ExpressionAttributeValues={':upload_date': twenty_four_hours_ago.isoformat()}
        )
        return response.get('Items', [])
    except Exception as e:
        print("Error loading data from DynamoDB:", e)
        return []

def lambda_handler(event, context):
    sender = '2023ht66521@wilp.bits-pilani.ac.in'
    recipients = ['2023ht66521@wilp.bits-pilani.ac.in']
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"S3 Uploads Report - {today}"
    
    upload_details = load_from_dynamodb('bitscloud-db')
    
    body_html = "<html><head></head><body>"
    body_html += "<p>Dear Receiver,</p>"
    body_html += "<p>Please find S3 upload report for the last 24 hours as below:</p>"
    body_html += "<table border='1'><tr><th>S3 Uri</th><th>Object Name</th><th>Object Size</th><th>Object Type</th><th>Thumbnail URL</th></tr>"
    for details in upload_details:
        body_html += f"<tr><td>{details['uri']}</td><td>{details['key']}</td><td>{details['object_size']}</td><td>{details['object_type']}</td><td>{details.get('thumbnail_url', '')}</td></tr>"
    body_html += "</table>"
    
    send_email(sender, recipients, subject, body_html, body_html)
    
    return {'statusCode': 200, 'body': json.dumps('Email sent successfully!')}
