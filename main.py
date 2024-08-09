import os
import sys
import json
import asyncio
from pymongo import MongoClient
import dagger

async def run_dagger(business_name: str, personas: str, mongo_uri: str, website_url: str):
    config = dagger.Config(log_output=sys.stderr)
    print("Setting configs")
    
    try:
        async with dagger.Connection(config) as client:
            print("Starting Dagger module")
            mongo_uri_secret = client.set_secret("mongo_uri", mongo_uri)
            website_url_secret = client.set_secret("website_url", website_url)
            researcher = client.container().from_("python:3.9")
            researcher = researcher.with_exec(["pip", "install", "dagger-io"])
            print("Setting secrets")
            
            researcher = (
                researcher
                .with_exec(["dagger", "mod", "install", "github.com/EmmS21/daggerverse/ResearcherContainer@bd0e80d862e048ff0033b305e33c935b8ae40da3"])
                .with_secret_variable("connection", mongo_uri_secret)
                .with_secret_variable("modal_entry_point", website_url_secret)
                .with_env_variable("BUSINESS_NAME", business_name)
                .with_env_variable("PERSONAS", personas)
                .with_exec(["dagger", "call", "--module", "ResearcherContainer", "run"])
            )
            print("Getting stdout from researcher")

            result = await researcher.stdout()
            return result
    except dagger.DaggerError as e:
            print(f"Dagger error: {str(e)}", file=sys.stderr)
            raise
    except Exception as e:
            print(f"Unexpected error in run_dagger: {str(e)}", file=sys.stderr)
            raise

async def process_event(event, context):
    print("Starting lambda_handler function")

    # Get environment variables
    env_vars = {
        'MONGO_URI': os.environ.get('MONGO_URI'),
        'MONGO_DB_NAME': os.environ.get('MONGO_DB_NAME'),
        'MONGO_COLLECTION_NAME': os.environ.get('MONGO_COLLECTION_NAME'),
        'WEBSITE_URL': os.environ.get('WEBSITE_URL')
    }
    
    # Check for missing environment variables
    missing_vars = [var for var, value in env_vars.items() if not value]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}", file=sys.stderr)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Missing required environment variables: {', '.join(missing_vars)}"})
        }
    
    try:
        with MongoClient(env_vars['MONGO_URI']) as mongo_client:
            db = mongo_client[env_vars['MONGO_DB_NAME']]
            collection = db[env_vars['MONGO_COLLECTION_NAME']]            
            document = collection.find_one({}, {'_id': 0, 'business_name': 1, 'personas': 1})
            if not document:
                raise ValueError("No data found in MongoDB")
            
            business_name = document.get('business_name', '')
            personas = json.dumps(document.get('personas', []))
            
            print(f"Retrieved data for business: {business_name}")
        
        print("Running Dagger operations...")
        output = await run_dagger(business_name, personas, env_vars['MONGO_URI'], env_vars['WEBSITE_URL'])
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data processed successfully', 'result': output})
        }
    except Exception as e:
        print(f"An error occurred: {str(e)}", file=sys.stderr)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def lambda_handler(event, context):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(process_event(event, context))