import os
import boto3
import time
import subprocess, json, sys
from pathlib import Path

# Replace with your ASU ID
ASU_ID = "1229175872"
REQUEST_QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/551765811849/1229175872-req-queue"
RESPONSE_QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/551765811849/1229175872-resp-queue"
INPUT_BUCKET_NAME = f"{ASU_ID}-in-bucket"
OUTPUT_BUCKET_NAME = f"{ASU_ID}-out-bucket"


sqs = boto3.client('sqs', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')


def process_messages():
    while True:
        try:
            # Retrieve message from SQS request queue
            response = sqs.receive_message(
                QueueUrl=REQUEST_QUEUE_URL,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            
            if 'Messages' not in response:
                print("No messages in queue, waiting...")
                time.sleep(5)
                continue

            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            print("Received message:", message)
            filename = message['Body']
            corr_id = "default_corr_id"
            if 'MessageAttributes' in message and 'correlation_id' in message['MessageAttributes']:

                corr_id = message['MessageAttributes']['correlation_id']['StringValue']
            
            # Download the image from S3 input bucket
            s3.download_file(INPUT_BUCKET_NAME, filename, '/tmp/' + filename)

            
            try:
                current_dir = os.path.dirname(__file__)
                script_path = os.path.join(current_dir, "../model/face_recognition.py")
                data_path   = os.path.join(current_dir, "../model/data.pt")

                model_dir = Path(__file__).resolve().parent.parent / "model"
                image_path = Path('/tmp') / filename

                # Use the same interpreter you're running now
                result = subprocess.run(
                    [sys.executable, 'face_recognition.py', str(image_path)],
                    cwd=str(model_dir),
                    capture_output=True,
                    text=True,
                    check=True,
                )
                
                name = result.stdout.strip()   # what the script printed
                print("Predicted name:", name)

            except subprocess.CalledProcessError as e:
                print("Script failed:", e.stderr or e.stdout)
            except FileNotFoundError:
                print("Could not find face_recognition.py")


            # Store the recognition result in the S3 output bucket
            output_key = os.path.splitext(filename)[0]
            s3.put_object(Bucket=OUTPUT_BUCKET_NAME, Key=output_key, Body=name)

            # Push the recognition result to the response queue
            sqs.send_message(
                QueueUrl=RESPONSE_QUEUE_URL,
                MessageBody=f"{output_key}:{name}",
                MessageAttributes={
                    'correlation_id': {
                        'StringValue': corr_id,
                        'DataType': 'String'
                    }
                }
            )
            print(f"Sent message to response queue with correlation_id: {corr_id}")

            # Delete the message from the request queue
            sqs.delete_message(
                QueueUrl=REQUEST_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )

            # Delete the temporary file
            # os.remove('/tmp/' + filename)
            # print(f"Deleted temporary file /tmp/{filename}")    
            print(f"Successfully processed {filename}, result: {name}")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    process_messages()
