import boto3
from boto3.dynamodb.conditions import Key

def create_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.create_table(
        TableName="courses",
        KeySchema=[
            {"AttributeName": "course_id", "KeyType": "HASH"}  # Partition key
        ],
        AttributeDefinitions=[{"AttributeName": "course_id", "AttributeType": "N"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    
    return table


def list_tables(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    return dynamodb.tables.all()


def put_item(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")
    response = table.put_item(Item={"course_id": 706, "name": "Data Engineering"})

    return response


def get_item(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")
    response = table.get_item(Key={"course_id": 706})

    return response


def update_item(dynamodb=None):
    """Updates the name for course_id = 706 to DE"""

    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")

    response = table.update_item(
        Key={"course_id": 706},
        UpdateExpression="SET #name = :new_name",
        ExpressionAttributeNames={"#name": "name"},
        ExpressionAttributeValues={":new_name": "DE"},
        ReturnValues="UPDATED_NEW",
    )


def query_items(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")

    response = table.query(KeyConditionExpression=Key("course_id").eq(706))

    return response


def scan_items(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")
    response = table.scan()

    return response


def delete_item(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")
    response = table.delete_item(Key={"course_id": 706})

    return response


def delete_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("courses")
    table.delete()

    return table
