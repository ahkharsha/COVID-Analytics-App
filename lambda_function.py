import json
import base64
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'covid-pipeline-data-week12'

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        data['processed_at_utc'] = datetime.utcnow().isoformat()
        file_name = f"live_data/processed_covid_{uuid.uuid4().hex}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data)
        )

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed Kinesis stream and saved to S3.')
    }
