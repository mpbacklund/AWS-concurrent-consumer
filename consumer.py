import boto3
import time
import json
from logger_config import logger

class consumer():
    def __init__(self, source, storageType, destination):
        self.source = source
        self.storageType = storageType
        self.destination = destination
        self.sourceClient = boto3.client('s3')
        self.destClient = boto3.client(storageType, region_name="us-east-1")
        logger.info(f"Initialized consumer with source={source}, storageType={storageType}, destination={destination}")
            
    def listen(self):
        logger.info("Starting to listen for requests...")
        timeEnd = time.time() + 30

        while time.time() < timeEnd:
            try:
                response = self.sourceClient.list_objects_v2(Bucket=self.source)
                if 'Contents' in response:
                    # get the smallest key
                    smallestKey = self.getSmallestKey(response)
                    logger.info(f"Found smallest key: {smallestKey}")
                    
                    # get the request
                    request = self.getRequest(smallestKey)

                    # extract data from request
                    request_data = self.getRequestData(request)
                    logger.debug(f"Request data: {request_data}")

                    # get the request type
                    requestType = self.getRequestType(request_data)
                    logger.info(f"Processing request of type: {requestType}")

                    # send the request data to the correct place (delete, create, update)
                    if requestType == "create":
                        self.create(request_data)
                        logger.info(f"Sent request to {self.destination}")
                    elif requestType == "update":
                        self.update(request_data)
                    elif requestType == "delete":
                        self.delete(request_data)
                    
                    # delete request from source after it has been processed
                    self.sourceClient.delete_object(Bucket=self.source, Key=smallestKey)
                    logger.info(f"Deleted processed request with key: {smallestKey}")
                else:
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error during processing: {e}")

    # get the smallest key from the list of objects
    def getSmallestKey(self, response):
        smallestKey = sorted(response['Contents'], key=lambda x: x["Key"])[0]['Key']
        return smallestKey
    
    # given a key, get a request from the source bucket
    def getRequest(self, key):
        request = self.sourceClient.get_object(Bucket=self.source, Key=key)
        return request
    
    # decodes the request into a json object
    def getRequestData(self, request):
        request_data = json.loads(request['Body'].read().decode('utf-8'))
        return request_data

    # routes the request to a processor based on the type of request it is
    def getRequestType(self, request):
        requestType = request['type']
        # see if we need to send this to 
        if requestType == "create":
            # self.create(request)
            return "create"
        if requestType == "delete":
            return "delete"
        if requestType == "update":
            return "update"
        
        return "none"

    def create(self, widget):
        if self.storageType == 's3':
            owner = widget['owner'].replace(" ", "-").lower()
            widget_key = f"widgets/{owner}/{widget['widgetId']}"
            self.destClient.put_object(Bucket=self.destination, Key=widget_key, Body=json.dumps(widget))
        elif self.storageType == 'dynamodb':
            item = {
                'id': {'S': widget['widgetId']},
                'owner': {'S': widget['owner']},
                'label': {'S': widget['label']},
                'description': {'S': widget['description']}
            }

            # Handle otherAttributes
            if 'otherAttributes' in widget:
                for attribute in widget['otherAttributes']:
                    item[attribute['name']] = {'S': attribute['value']}

            print(item)

            self.destClient.put_item(TableName=self.destination, Item=item)
        elif self.storageType == 'sqs':
            message_body = json.dumps(widget)  # Serialize the widget as JSON
            response = self.destClient.send_message(
                QueueUrl=self.destination,  # The SQS queue URL
                MessageBody=message_body    # The message content
            )
            print(f"Message sent to SQS. MessageId: {response['MessageId']}")

    def delete(self, widget):
        if self.storageType == 's3':
            owner = widget['owner'].replace(" ", "-").lower()
            widget_key = f"widgets/{owner}/{widget['widgetId']}"

            response = self.destClient.delete_object(Bucket=self.destination, Key=widget_key)
            logger.info(f"Deleted widget with key {widget_key} from S3. Response: {response}")

        elif self.storageType == 'dynamodb':
            key = {
                'id': {'S': widget['widgetId']}
            }
            
            # Delete the item from the DynamoDB table
            response = self.destClient.delete_item(TableName=self.destination, Key=key)
            logger.info(f"Deleted widget with ID {widget['widgetId']} from DynamoDB. Response: {response}")
        elif self.storageType == 'sqs':
            pass

    def update(self, widget):
        if self.storageType == 's3':
            # Construct the S3 key for the widget
            owner = widget['owner'].replace(" ", "-").lower()
            widget_key = f"widgets/{owner}/{widget['widgetId']}"
            
            # Overwrite the existing object with the updated widget data
            response = self.destClient.put_object(
                Bucket=self.destination,
                Key=widget_key,
                Body=json.dumps(widget)
            )
            logger.info(f"Updated widget in S3 with key {widget_key}. Response: {response}")
        
        elif self.storageType == 'dynamodb':
            # Define the primary key for the widget
            key = {'id': {'S': widget['widgetId']}}
            
            # Construct the UpdateExpression and ExpressionAttributeValues
            update_expression = "SET owner = :owner, label = :label, description = :description"
            expression_attribute_values = {
                ':owner': {'S': widget['owner']},
                ':label': {'S': widget['label']},
                ':description': {'S': widget['description']}
            }
            
            # Include additional attributes, if present
            if 'otherAttributes' in widget:
                for index, attribute in enumerate(widget['otherAttributes']):
                    attribute_placeholder = f":attr{index}"
                    attribute_name = attribute['name']
                    update_expression += f", {attribute_name} = {attribute_placeholder}"
                    expression_attribute_values[attribute_placeholder] = {'S': attribute['value']}
            
            # Perform the update
            response = self.destClient.update_item(
                TableName=self.destination,
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )
            logger.info(f"Updated widget in DynamoDB with ID {widget['widgetId']}. Response: {response}")
        
        elif self.storageType == 'sqs':
            # SQS does not support direct updates to messages. Typically, you'd:
            # 1. Delete the old message.
            # 2. Send a new message with the updated information.
            pass
