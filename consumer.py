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
                    smallestKey = self.getSmallestKey(response)
                    logger.info(f"Found smallest key: {smallestKey}")
                    
                    request = self.getRequest(smallestKey)
                    request_data = self.getRequestData(request)
                    logger.debug(f"Request data: {request_data}")

                    requestType = self.getRequestType(request_data)
                    logger.info(f"Processing request of type: {requestType}")

                    if requestType == "create":
                        self.create(request_data)

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
        else:
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