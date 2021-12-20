import os
import pandas as pd
import boto3
import pprint
from datetime import datetime
from io import StringIO
from datahub_logger import getLogger
LOGGER = getLogger()


def lambda_handler(event, context):
    LOGGER.lambda_init(event, context)
    dynamodb_attributes = SubtenantDynamodbTable()
    dynamodb_attributes.execute()


class SubtenantDynamodbTable:
    def __init__(self):
        self._account_name = os.environ.get('ACCOUNT_NAME')
        self._tenant_id = os.environ.get('TENANT_ID')
        self._tenant_env = os.environ.get('TENANT_ENV')
        self._internal_bucket_name = f"{self._account_name}-" \
                                     f"data-central-logs" \
                                     f"-internal"
        self._current_time = f"ds={datetime.utcnow().strftime('%Y%m%d')}"
        self._prefix = "services/access_report/dynamodb"
        self._filename = "dynamodb_table_attributes.csv"
        self._key = f"{self._prefix}{self._current_time}/{self._filename}"
        self._server_side_encryption = "AES256"
        self._dynamodb_table = "acs_permissions"

    def execute(self):
        data = self._get_dynamodb_table_attributes()
        pprint.pprint(data)
        self._write_csv(data)

    def _get_dynamodb_table_attributes(self):

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('acs_permissions')
        response = table.scan(
                    ProjectionExpression="redshift_cluster,\
                                          tenant_id,\
                                          environment,\
                                          identity_id,\
                                          access")
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                            ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        '''
        LOGGER.info("Obtaining attributes from "
                    f"Dynamodb table: {self._dynamodb_table}")
        data = DynamoDb().with_table(
                   self._dynamodb_table).get_item_by_scan()
        '''
        return data

    def _write_csv(self, data):
        df = pd.DataFrame(data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, sep=",", index=False)
        LOGGER.info(f"writing CSV file to {self._internal_bucket_name} with "
                    f"key {self._key}")
        s3 = boto3.client('s3')
        s3.put_object(
                    Bucket=self._internal_bucket_name,
                    Key=self._key,
                    Body=csv_buffer.getvalue(),
                    SSECustomerAlgorithm=self._server_side_encryption)
        LOGGER.info("uploaded file")
