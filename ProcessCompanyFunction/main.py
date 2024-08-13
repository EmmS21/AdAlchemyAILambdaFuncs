import os
import sys
import json
import asyncio
from pymongo import MongoClient
import requests
from datetime import datetime
from pymongo.errors import AutoReconnect, OperationFailure
import boto3

# Function to send notifications via email using AWS SES
def send_notifications(email_address, message_subject, message_body):
    """
    Sends an email notification using AWS SES.

    Args:
    - email_address: The recipient's email address.
    - message_subject: The subject of the email.
    - message_body: The body of the email.

    This function is needed to notify the stakeholders (e.g., company owners) once the AI agent
    has completed its work and the results are ready for review.
    """
    ses_client = boto3.client('ses', region_name='us-east-2')
    source_email = os.environ.get('EMAIL')

    ses_client.send_email(
        Source=source_email,  
        Destination={
            'ToAddresses': [email_address],
        },
        Message={
            'Subject': {'Data': message_subject},
            'Body': {'Text': {'Data': message_body}},
        }
    )

# Function to run the marketing agent and get results
async def run_marketing_agent(business_name: str, personas: list, website_url: str):
    """
    Calls the marketing AI agent hosted on a specific URL to generate marketing materials.

    Args:
    - business_name: The name of the business being processed.
    - personas: The personas data associated with the business.
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
        personas_str = ", ".join(personas)
        data = {
            "persona": personas_str,
            "business_name": business_name
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
async def write_to_mongodb(data: dict, mongo_uri: str, database: str, collection_name: str):
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
    structured_data = {
        'list_of_keywords': data['list_of_keywords'],
        'list_of_ad_text': data['list_of_ad_text'],
        'list_of_paths_taken': data['list_of_paths_taken'],
        'business': data['business'],
        'user_personas': data['user_personas']
    }
    try:
        with MongoClient(mongo_uri) as client:
            db = client[database]
            collection = db[collection_name]
            
            current_date = datetime.now()
            structured_data['date_written'] = {
                'year': current_date.year,
                'month': current_date.month,
                'day': current_date.day
            }
            
            collection.insert_one(structured_data)
            return "Success"
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
        personas = company_data['personas']
        email_address = company_data['email']

        try:
            max_retries = 1
            for attempt in range(max_retries + 1):
                try:
                    output = await run_marketing_agent(business_name, personas, env_vars['WEBSITE_URL'])
                    break  
                except Exception as e:
                    if attempt < max_retries:
                        print(f"Attempt {attempt + 1} failed for {business_name}. Retrying...")
                    else:
                        raise 

            # Write researcher output to MongoDB
            try:
                result = await write_to_mongodb(output, env_vars['MONGO_URI'], env_vars['MONGO_COLLECTION_NAME'], business_name)
            except Exception as mongo_error:
                print(f"Failed to write to MongoDB for {business_name}: {str(mongo_error)}", file=sys.stderr)
                raise

            # Send notifications
            try:
                message_subject = "AdAlchemyAI: Initial Research Completed"
                message_body = f"The initial research carried out by the Initial Market Researcher AI Agent for {business_name} is complete. Please review the research using the /paths and /business slash commands in your Discord bot."
                send_notifications(email_address, message_subject, message_body)
            except Exception as notify_error:
                print(f"Failed to send notification for {business_name}: {str(notify_error)}", file=sys.stderr)
                raise
            print(f"Processing complete for company: {business_name}")

            # Start Step Functions execution
            sfn_input = json.dumps({
                'business_name': business_name,
                'email': email_address
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
                    MessageBody=json.dumps(company_data)
                )
                print(f"Moved company {business_name} to DLQ")
            except Exception as dlq_error:
                print(f"Failed to move company {business_name} to DLQ: {str(dlq_error)}", file=sys.stderr)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'All companies processed successfully'})
    }

def lambda_handler(event, context):
    """
    The entry point for the Lambda function, which is triggered by SQS messages.

    This function creates a new asyncio event loop and processes each company message received from SQS.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(process_company(event, context))
