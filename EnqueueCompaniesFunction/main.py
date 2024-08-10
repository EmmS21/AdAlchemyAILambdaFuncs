import os
import sys
import json
from pymongo import MongoClient
import boto3

def lambda_handler(event, context):
    try:
        env_vars = {
            'MONGO_URI': os.environ.get('MONGO_URI'),
            'MONGO_DB_NAME': os.environ.get('MONGO_DB_NAME'),
            'MONGO_COLLECTION_NAME': os.environ.get('MONGO_COLLECTION_NAME'),
            'SQS_QUEUE_URL': os.environ.get('SQS_QUEUE_URL')
        }      
        missing_vars = [var for var, value in env_vars.items() if not value]
        if missing_vars:
            print(f"Error: Missing required environment variables: {', '.join(missing_vars)}", file=sys.stderr)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': f"Missing required environment variables: {', '.join(missing_vars)}"})
            }
        with MongoClient(env_vars['MONGO_URI']) as mongo_client:
            db = mongo_client[env_vars['MONGO_DB_NAME']]
            collection = db[env_vars['MONGO_COLLECTION_NAME']]         
            if collection.count_documents({}) == 0:
                raise ValueError("No companies found in MongoDB")
            companies = collection.find({}, {'_id': 0, 'business_name': 1, 'personas': 1, 'email': 1})
            sqs_client = boto3.client('sqs')
            for company in companies:
                company_data = {
                    'business_name': company.get('business_name', ''),
                    'personas': json.dumps(company.get('personas', [])),
                    'email': company.get('email', '')
                }
                response = sqs_client.send_message(
                    QueueUrl=env_vars['SQS_QUEUE_URL'],
                    MessageBody=json.dumps(company_data)
                )           
                print(f"Enqueued company: {company_data['business_name']} with message ID: {response['MessageId']}")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'All companies enqueued successfully'})
        }   
    except Exception as e:
        print(f"An error occurred: {str(e)}", file=sys.stderr)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
