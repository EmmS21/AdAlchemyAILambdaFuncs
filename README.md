# AdAlchemyAILambdaFuncs

AdAlchemyAILambdaFuncs is a collection of AWS Lambda functions designed to automate the process of generating marketing research for small businesses. These functions interact with an AI agent to produce tailored marketing materials, which are then stored in a MongoDB database and notified to the business owners via email. The system is designed to be scalable and efficient, handling multiple companies in a single run.

## Project Structure

### Lambda Functions

1. **EnqueueCompaniesFunction**
   - **File:** `main.py`
   - **Description:** 
     - This function reads company data from a MongoDB collection and enqueues each company into an AWS SQS queue. The company data includes business names, personas, and email addresses. The SQS messages are then processed by the `ProcessCompanyFunction`.
   - **Environment Variables:**
     - `MONGO_URI`: MongoDB connection URI.
     - `MONGO_DB_NAME`: Name of the MongoDB database.
     - `MONGO_COLLECTION_NAME`: Name of the MongoDB collection containing company data.
     - `SQS_QUEUE_URL`: The URL of the SQS queue to enqueue company data.

2. **ProcessCompanyFunction**
   - **File:** `main.py`
   - **Description:**
     - This function processes each company from the SQS queue. It interacts with an AI agent to generate marketing materials based on the company's personas and stores the results in MongoDB. Once the process is complete, an email notification is sent to the business owner.
   - **Environment Variables:**
     - `MONGO_URI`: MongoDB connection URI.
     - `WEBSITE_URL`: The URL of the AI agent that generates the marketing materials.
     - `DLQ_URL`: The URL of the Dead-Letter Queue (DLQ) to handle failed processing.
     - `MONGO_COLLECTION_NAME`: Name of the MongoDB collection to store the results.

## Setup

### Prerequisites

- AWS Account
- MongoDB instance
- AWS SQS Queue
- AWS SES configured for sending emails

### Deployment

1. **Set Up Environment Variables:**
   - Ensure the following environment variables are set in your AWS Lambda functions:
     - `MONGO_URI`
     - `MONGO_DB_NAME`
     - `MONGO_COLLECTION_NAME`
     - `SQS_QUEUE_URL`
     - `WEBSITE_URL`
     - `DLQ_URL`

2. **Deploy the Lambda Functions:**
   - Use the AWS Management Console, AWS CLI, or an Infrastructure-as-Code (IaC) tool like Terraform to deploy the functions to your AWS account.

3. **Configure Triggers:**
   - Set up the `EnqueueCompaniesFunction` to be triggered by an appropriate AWS service (e.g., AWS EventBridge) to enqueue companies at regular intervals.
   - The `ProcessCompanyFunction` should be triggered by messages in the SQS queue.

## Usage

### Enqueueing Companies

When the `EnqueueCompaniesFunction` is triggered, it reads the company data from MongoDB and enqueues each company into the SQS queue. The function handles missing environment variables and ensures that companies are only enqueued if they exist in the MongoDB collection.

### Processing Companies

The `ProcessCompanyFunction` processes each SQS message by:
1. Interacting with the AI agent to generate marketing materials.
2. Storing the results in MongoDB.
3. Sending an email notification to the business owner.
   
If the processing fails after all retries, the company data is moved to a Dead-Letter Queue (DLQ) for further investigation.

## Error Handling

- **Missing Environment Variables:** Both functions will log an error and return a 500 status code if required environment variables are missing.
- **MongoDB Errors:** The functions handle MongoDB connection errors and retries if necessary.
- **AI Agent Errors:** If the AI agent call fails, the function retries before eventually moving the message to the DLQ.

## License

This project is licensed under the MIT License.

## Contact

For questions or support, please reach out to [emmanuel@adalchemy.com](mailto:emmanuel@adalchemy.com).
