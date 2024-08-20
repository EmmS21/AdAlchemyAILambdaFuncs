import os
import sys
import json
import asyncio
from pymongo import MongoClient
import requests
from datetime import datetime
from pymongo.errors import AutoReconnect, OperationFailure
import boto3
from bson import ObjectId

def serialize_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def send_discord_message(webhook_url, content):
    MAX_LENGTH = 1900  # Leave some room for potential overhead

    if len(content) <= MAX_LENGTH:
        discord_message = {'content': content}
        response = requests.post(webhook_url, json=discord_message)
        response.raise_for_status()
        print(f"Sent message. Response: {response.status_code}")
    else:
        parts = [content[i:i+MAX_LENGTH] for i in range(0, len(content), MAX_LENGTH)]
        for i, part in enumerate(parts, 1):
            discord_message = {'content': f"(Part {i}/{len(parts)}) {part}"}
            response = requests.post(webhook_url, json=discord_message)
            response.raise_for_status()
            print(f"Sent part {i}/{len(parts)}. Response: {response.status_code}")

# Function to run the marketing agent and get results
async def run_marketing_agent(business_name: str, website_url: str, existing_data: dict = None):
    """
    Calls the marketing AI agent hosted on a specific URL to generate marketing materials.

    Args:
    - business_name: The name of the business being processed.
    - website_url: The URL where the AI agent is hosted.

    Returns:
    - The JSON response from the AI agent.

    This function is needed to generate the marketing materials for each company.
    It communicates with an external AI service (e.g., a Modal AI agent) to get the necessary marketing data.
    """
    try:
        print("Calling the Modal AI Agent")
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "business_name": business_name,
            "existing_data": existing_data
        }

        response = requests.post(website_url, headers=headers, json=data)
        if response.status_code == 200:
            return response.json()
        else:
            raise RuntimeError(f"Failed to call the Modal AI Agent: {response.status_code} - {response.text}")

    except requests.RequestException as e:
        print(f"Error while calling the Modal AI agent: {str(e)}", file=sys.stderr)
        raise

# Function to write the AI-generated data to MongoDB
async def write_to_mongodb(data: dict, mongo_uri: str, database: str, collection_name: str, existing_data: dict = None):
    """
    Writes the AI agent's output to MongoDB.

    Args:
    - data: The AI agent's output to be stored.
    - mongo_uri: The URI for connecting to MongoDB.
    - database: The name of the database.
    - collection_name: The name of the collection where the data will be stored.

    Returns:
    - A success message if the write operation completes successfully.

    This function is needed to persist the generated marketing data in MongoDB for future reference.
    It organizes the data and adds a timestamp before saving it to the database.
    """
    try:
        with MongoClient(mongo_uri) as client:
            db = client[database]
            collection = db[collection_name]
            
            current_date = datetime.now()
            date_written = {
                'year': current_date.year,
                'month': current_date.month,
                'day': current_date.day
            }
            
            if existing_data and '_id' in existing_data:
                # Updating existing document
                update_fields = {
                    'list_of_keywords': data['list_of_keywords'],
                    'list_of_ad_text': data['list_of_ad_text'],
                    'date_written': date_written
                }
                result = collection.update_one(
                    {'_id': existing_data['_id']},
                    {'$set': update_fields}
                )
                if result.modified_count == 0:
                    print(f"No document was updated for {collection_name}")
                else:
                    print(f"Document updated successfully for {collection_name}")
            else:
                # Creating new document
                structured_data = {
                    'list_of_keywords': data['list_of_keywords'],
                    'list_of_ad_text': data['list_of_ad_text'],
                    'list_of_paths_taken': data.get('list_of_paths_taken'),
                    'business': data.get('business'),
                    'user_personas': data.get('user_personas'),
                    'date_written': date_written
                }
                collection.insert_one(structured_data)
                print(f"New document created for {collection_name}")

            # Discord notification logic (unchanged)
            mappings_db = client['mappings']
            company_collection = mappings_db['companies']
            company_doc = company_collection.find_one({'business_name': collection_name})
            if company_doc and 'webhook_url' in company_doc:
                webhook_url = company_doc['webhook_url']
                content = ('Hi, I am the Company Researcher tasked with understanding your business, '
                           'defining user personas and generating paths to research keywords to use '
                           'to market your business. I just finished a task to research your business. '
                           'Please use the /business slash command to see how I defined your business, '
                           '/paths to see the research paths I generated and /personas to see the '
                           'personas I defined')
                try:
                    print("Sending message to Discord...")
                    send_discord_message(webhook_url, content)
                    print("Discord message sent successfully")
                    return "Success: Data written to MongoDB and Discord message sent"
                except requests.exceptions.RequestException as e:
                    print(f"Error sending message to Discord: {str(e)}", file=sys.stderr)
                    return f"Success: Data written to MongoDB, but failed to send Discord message: {str(e)}"
            else:
                return "Success: Data written to MongoDB, but no webhook_url found for the business"

    except (AutoReconnect, OperationFailure) as e:
        print(f"Failed to write to MongoDB: {str(e)}", file=sys.stderr)
        raise
     
# The main processing function that handles each company
async def process_company(event, context):
    """
    Processes each company retrieved from the SQS queue.

    Args:
    - event: The event data passed by the Lambda service, containing SQS messages.
    - context: The runtime information provided by Lambda.

    This function retrieves company data from the SQS message, runs the marketing agent,
    writes the results to MongoDB, and sends a notification email. It includes a retry
    mechanism for the AI agent call. If all retries fail, the message is moved to a Dead-Letter Queue (DLQ).
    """
    env_vars = {
        'MONGO_URI': os.environ.get('MONGO_URI'),
        'WEBSITE_URL': os.environ.get('WEBSITE_URL'),
        'DLQ_URL': os.environ.get('DLQ_URL'),
        'MONGO_COLLECTION_NAME': os.environ.get('MONGO_COLLECTION_NAME'),
        'STEP_FUNCTION_ARN': os.environ.get('STEP_FUNCTION_ARN')
    }

    sqs_client = boto3.client('sqs')
    sfn_client = boto3.client('stepfunctions')

    for record in event['Records']:
        company_data = json.loads(record['body'])
        business_name = company_data['business_name']

        try:
            with MongoClient(env_vars['MONGO_URI']) as client:
                db = client[env_vars['MONGO_COLLECTION_NAME']]
                collection = db[business_name]
                existing_doc = collection.find_one(sort=[('_id', -1)])

            existing_data = None
            if existing_doc and all(field in existing_doc for field in ['user_personas', 'business', 'list_of_paths_taken']):
                existing_data = {
                    '_id': existing_doc['_id'],
                    'business_name': business_name,
                    'user_personas': existing_doc['user_personas'],
                    'business': existing_doc['business'],
                    'list_of_paths_taken': existing_doc['list_of_paths_taken']
                }
            max_retries = 1
            for attempt in range(max_retries + 1):
                try:
                    output = await run_marketing_agent(
                        business_name, 
                        env_vars['WEBSITE_URL'], 
                        json.loads(json.dumps(existing_data, default=serialize_objectid))
                    )
                    break 
                except Exception as e:
                    if attempt < max_retries:
                        print(f"Attempt {attempt + 1} failed for {business_name}. Retrying...")
                    else:
                        raise 

            # Write researcher output to MongoDB
            print('output*******', output)
            try:
                await write_to_mongodb(output, env_vars['MONGO_URI'], env_vars['MONGO_COLLECTION_NAME'], business_name, existing_data)
            except Exception as mongo_error:
                print(f"Failed to write to MongoDB for {business_name}: {str(mongo_error)}", file=sys.stderr)
                raise

            # Start Step Functions execution
            sfn_input = json.dumps({
                'business_name': business_name,
            })
            sfn_response = sfn_client.start_execution(
                stateMachineArn=env_vars['STEP_FUNCTION_ARN'],
                input=sfn_input
            )
            print(f"Started Step Functions execution for {business_name}: {sfn_response['executionArn']}")

        except Exception as e:
            print(f"Failed to process company {business_name}: {str(e)}", file=sys.stderr)
            # If all attempts fail, move the message to the Dead-Letter Queue (DLQ)
            try:
                sqs_client.send_message(
                    QueueUrl=env_vars['DLQ_URL'],
                    MessageBody=json.dumps(company_data, default=serialize_objectid)
                )
                print(f"Moved company {business_name} to DLQ")
            except Exception as dlq_error:
                print(f"Failed to move company {business_name} to DLQ: {str(dlq_error)}", file=sys.stderr)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'All companies processed successfully'}, default=serialize_objectid)
    }

def lambda_handler(event, context):
    """
    The entry point for the Lambda function, which is triggered by SQS messages.

    This function creates a new asyncio event loop and processes each company message received from SQS.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(process_company(event, context))
