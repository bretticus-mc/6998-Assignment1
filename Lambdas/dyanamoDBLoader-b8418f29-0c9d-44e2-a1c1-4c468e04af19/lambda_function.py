import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime

def lambda_handler(event, context):
    # uni is the primary/paritition key
    # note they all have unique attributes
    df = pd.read_csv('hw1_restaurants_list_dynamo_db.csv', index_col=0)
    duplicateRows = df[df.duplicated()]
    print(duplicateRows)
    print(len(duplicateRows))
    #df = pd.read_csv('sample.csv', index_col=0)
    df = df.rename(columns={'Id': 'BusinessID'})
    df['Address'] = df['Address'].apply(lambda x: x.strip('[]').split(', ')[0].strip('\'') if x else None)
    # ,Name,Id,Address,Coordinates,Reviews,Rating,Zip Code
    
    df = df.astype(str)
    restaurants = df.to_dict('records')

    """
    restaurants = [{'uni': 'ab001',
                'email': 'aa001@columbia.edu',
                'name': 'Akash',
                'from': 'India',
                'like_criket': 'yes'
                },
               {'uni': 'xx777',
                'email': 'xi@columbia.edu',
                'name': 'Xi',
                'from': 'China',
                'like_soccer': 'yes'
                },
               {'uni': 'aa000',
                'email': 'aa000@columbia.edu',
                'name': 'John',
                'from': 'U.S.',
                'like_football': 'yes'
                }
               ]
    """
    # 1
    insert_data(restaurants)
    # 2
    # lookup_data({'uni': 'xx777'})
    # 3
    # update_item({'uni': 'xx777'}, 'Canada')
    # 4
    # delete_item({'uni': 'xx777'})
    return

def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for idx,data in enumerate(data_list[5000:]):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        data['insertedAtTimestamp'] = timestamp
        response = table.put_item(Item=data)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print("Insertion Failure")
            print(response['ResponseMetadata']['HTTPStatusCode'])
            print(data)
        if not idx % 200:
            print("Inserted ", str(idx))
    print('@insert_data: response', response)
    return response
    
def lookup_data(key, db=None, table='6998Demo'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']
        
def update_item(key, feature, db=None, table='6998Demo'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # change student location
    response = table.update_item(
        Key=key,
        UpdateExpression="set #feature=:f",
        ExpressionAttributeValues={
            ':f': feature
        },
        ExpressionAttributeNames={
            "#feature": "from"
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response)
    return response
    
def delete_item(key, db=None, table='6998Demo'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response)
        return response