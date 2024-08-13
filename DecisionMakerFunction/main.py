import json
import re
import os
import requests
from pymongo import MongoClient
from datetime import datetime

def lambda_handler(event, context):
    business_name = event.get('business_name')
    if not business_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing business_name parameter')
        }

    connection_string = os.environ.get('MONGO_URI')
    client = MongoClient(connection_string)
    db = client["judge_data"]

    # Create the collection if it doesn't exist
    if business_name not in db.list_collection_names():
        db.create_collection(business_name)
    
    # Get the collection
    collection = db[business_name]

    def call_modal_agent(business_name):
        website_url =  os.environ.get('WEBSITE_URL')
        try:
            print("Calling the Modal AI Agent")
            headers = {
                "Content-Type": "application/json"
            }
            payload = {
                "business_name": business_name
            }
            response = requests.post(website_url, 
                                    headers=headers,
                                    data=json.dumps(payload))
            
            if response.status_code == 200:
                return response.json()  
            else:
                raise RuntimeError(f"Failed to call the Modal AI Agent: {response.status_code} - {response.text}")
        
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None

    def clean_and_parse_json(raw_json):
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', raw_json)
        if json_match:
            json_content = json_match.group(1)
        else:
            json_content = raw_output
        json_content = json_content.strip()
        try:
            parsed_json = json.loads(json_content)
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {str(e)}")
            print("Problematic JSON content:")
            print(json_content)
            return None

    raw_output = call_modal_agent(business_name)

    if raw_output:
        try:
            output = clean_and_parse_json(raw_output)
            print("Parsed output type:", type(output))
            print("Parsed output:", output)

            # Prepare the document to be inserted
            document = {
                "keywords": output["keywords"],
                "ad_variations": output["ad_text_variations"],
                "last_update": datetime.now().isoformat()
            }

            # # Insert the document into the collection
            result = collection.insert_one(document)
            print(f"Document inserted with id: {result.inserted_id}")

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {str(e)}")
            print("Failed to parse the following raw output:")
            print(raw_output)
    else:
        print("Failed to get output from Modal AI Agent")

    # Close the MongoDB connection
    client.close()
