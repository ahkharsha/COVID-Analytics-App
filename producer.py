import boto3
import csv
import json
import time
import sys

# Get the hospital name from the terminal command, default to 'Hospital-A' if left blank
hospital_name = sys.argv[1] if len(sys.argv) > 1 else "Hospital-A"

# Connect to Kinesis
kinesis = boto3.client('kinesis', region_name='ap-south-1')
STREAM_NAME = 'covid-live-stream'

print(f"Connecting to Kinesis stream: {STREAM_NAME} as {hospital_name}...")

with open('data/day_wise.csv', 'r') as file:
    reader = csv.DictReader(file)
    
    print(f"Beginning live transmission for {hospital_name}. Press Ctrl+C to stop.")
    for row in reader:
        
        # INJECT THE HOSPITAL IDENTITY INTO THE DATA
        row['Source_Hospital'] = hospital_name
        
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(row),
            PartitionKey=row['Date'] # Partition key ensures data is ordered properly
        )
        
        print(f"🚀 {hospital_name} transmitted data for {row['Date']}")
        
        # Sleep for 1 second to simulate live streaming
        time.sleep(1)
