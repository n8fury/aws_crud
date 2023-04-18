import boto3
import json
import logging
from decimal import Decimal


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = 'demo_crud'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

getMethod = 'GET'
postMethod = 'POST'
deleteMethod = 'DELETE'
patchMethod = 'PATCH'
healthPath = '/health'
productPath = '/product'
productsPath = '/products'


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)

        return json.JSONEncoder.default(self, obj)


def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow_Origin': '*'
        }
    }

    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response


def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == productPath:
        response = getProduct(event['queryStringParameters']['productId'])
    elif httpMethod == getMethod and path == productsPath:
        response = getProducts()
    elif httpMethod == postMethod and path == productPath:
        response = saveProduct(json.loads(event['body']))
    elif httpMethod == patchMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = modifyProduct(
            requestBody['productId'], requestBody['updateKey'], requestBody['updateValue'])
    elif httpMethod == deleteMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = deleteProduct(requestBody['productId'])
    else:
        response = buildResponse(404, 'Not Found')

    return response


def getProduct(productID):
    try:
        response = table.get_item(
            Key={
                'productId': productID
            }
        )
        if 'item' in response:
            return buildResponse(200, response['item'])
        else:
            return buildResponse(404, {'Message': 'ProductID %s not found' % productID})

    except Exception as e:
        logger.exception(e)
        



def getProducts():
    try:
        response = table.scan()
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])

        body = {
            'products': response
        }
        return buildResponse(200, body)
    except Exception as e:
        logger.exception(e)



def saveProduct(requestBody):
    try:
        table.put_item(
            Item=requestBody
        )
        body = {
            'Operation': 'Save',
            'Message': 'Success',
            'Product': requestBody
        }

        return buildResponse(200, body)

    except Exception as e:
        logger.exception(e)


def modifyProduct(productID, updateKey, updateValue):
    try:
        response = table.update_item(
            Key={
                'productId': productID
            },
            UpdateExpression='set {} = :value'.format(updateKey),
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'Success',
            'UpdatedAttributes': response['Attributes']
        }
        return buildResponse(200, body)
    except Exception as e:
        logger.exception(e)                