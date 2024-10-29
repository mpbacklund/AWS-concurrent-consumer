import unittest
from unittest.mock import patch, MagicMock
from moto import mock_s3, mock_dynamodb2
import boto3
import json
import time
from consumer import consumer  # assuming the consumer class is in a file called consumer_module.py

class TestConsumer(unittest.TestCase):

    @mock_s3
    @mock_dynamodb2
    def setUp(self):
        # Set up the mock S3 bucket
        self.source_bucket = 'test-source-bucket'
        self.destination_table = 'test-destination-table'
        self.client_s3 = boto3.client('s3', region_name="us-east-1")
        self.client_s3.create_bucket(Bucket=self.source_bucket)

        # Set up the mock DynamoDB table
        self.client_dynamodb = boto3.client('dynamodb', region_name="us-east-1")
        self.client_dynamodb.create_table(
            TableName=self.destination_table,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        self.consumer_instance = consumer(
            source=self.source_bucket,
            storageType='dynamodb',
            destination=self.destination_table
        )

    @mock_s3
    def test_listen_with_request(self):
        # Mock data for S3
        request_data = {'type': 'create', 'widgetId': '123', 'owner': 'Owner1', 'label': 'Test Label', 'description': 'Test Description'}
        self.client_s3.put_object(
            Bucket=self.source_bucket,
            Key='request1.json',
            Body=json.dumps(request_data)
        )

        with patch.object(self.consumer_instance, 'processRequest') as mock_process:
            mock_process.return_value = None
            self.consumer_instance.listen()
            mock_process.assert_called_once_with(request_data)

    @mock_s3
    def test_listen_without_requests(self):
        # Testing behavior when no objects are in S3
        with patch('time.sleep', return_value=None) as mock_sleep:
            start_time = time.time()
            self.consumer_instance.listen()
            end_time = time.time()
            # Assert that it listened for close to 30 seconds
            self.assertTrue(end_time - start_time >= 30)
            mock_sleep.assert_called()

    @mock_s3
    def test_process_request_create(self):
        # Test the 'create' route in processRequest
        request_data = {'type': 'create', 'widgetId': '123', 'owner': 'Owner1', 'label': 'Test Label', 'description': 'Test Description'}
        with patch.object(self.consumer_instance, 'create') as mock_create:
            self.consumer_instance.processRequest(request_data)
            mock_create.assert_called_once_with(request_data)

    @mock_dynamodb2
    def test_create_dynamodb(self):
        # Test the creation of a widget in DynamoDB
        widget_data = {
            'widgetId': '123',
            'owner': 'Owner1',
            'label': 'Test Label',
            'description': 'Test Description',
            'otherAttributes': [
                {'name': 'color', 'value': 'blue'},
                {'name': 'size', 'value': 'medium'}
            ]
        }
        self.consumer_instance.create(widget_data)

        # Check DynamoDB for the item
        response = self.client_dynamodb.get_item(
            TableName=self.destination_table,
            Key={'id': {'S': '123'}}
        )
        item = response.get('Item')
        self.assertIsNotNone(item)
        self.assertEqual(item['owner']['S'], 'Owner1')
        self.assertEqual(item['label']['S'], 'Test Label')
        self.assertEqual(item['color']['S'], 'blue')
        self.assertEqual(item['size']['S'], 'medium')

    @mock_s3
    def test_create_s3(self):
        # Recreate consumer with S3 as storage type
        consumer_s3 = consumer(source=self.source_bucket, storageType='s3', destination=self.source_bucket)
        widget_data = {'widgetId': '123', 'owner': 'Owner1', 'label': 'Test Label', 'description': 'Test Description'}

        consumer_s3.create(widget_data)

        # Verify item in S3
        response = self.client_s3.get_object(Bucket=self.source_bucket, Key="widgets/owner1/123")
        content = json.loads(response['Body'].read().decode('utf-8'))
        self.assertEqual(content, widget_data)

if __name__ == '__main__':
    unittest.main()
