import json
import boto3
import time
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

HOST = "search-hw1-domain-izn7u7qf2iylewxt2nd3ykvqzq.us-east-1.es.amazonaws.com"
INDEX = "restaurants"
REGION = "us-east-1"
TABLE = "yelp-restaurants"
HISTORY_TABLE = "user-history"

sqs = boto3.client('sqs')
db = boto3.resource('dynamodb')
dynamodb_table = db.Table(TABLE)
history_db = db.Table(HISTORY_TABLE)
ses = boto3.client('ses', region_name=REGION)

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)

open_search = OpenSearch(
    hosts=[{
        'host': HOST,
        'port': 443
    }],
    http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection
    )


def get_slot(message, slotName):
    slots = message["MessageAttributes"]
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['StringValue']
    else:
        return None

def extract_slots_from_message(message):
    cuisine = get_slot(message, "cuisine")
    location = get_slot(message, "location")
    numPeople = get_slot(message, "numPeople")
    email = get_slot(message, "email")
    time = get_slot(message, "time")
    date = get_slot(message, "date")
    message_slots = {
        "cuisine": cuisine,
        "location": location,
        "numPeople": numPeople,
        "email": email,
        "time": time,
        "date": date
    }
    return message_slots

def insert_into_history_table(email, location, cuisine):
    data = {'Email': email, 'location': location, 'cuisine': cuisine}
    response = history_db.put_item(Item=data)
    print(response)

def query_open_search(customer_query):
    restaurant_ids = query(customer_query)
    dynamo_responses = []
    for rid in restaurant_ids:
        dynamo_responses.append(lookup_dynamo_data(rid))
    
    return dynamo_responses
    """
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': restaurant_ids})
    }
    """
    
def query(term):
    seed = str(round(time.time()))
    q = {
        'query': {
            'multi_match': {
                'query': term
            }, 
        },
        "sort" : {
        "_script" : { 
            "script" : "Math.random()",
            "type" : "number",
            "order" : "asc"
        }
      },
      "size": 3
    }
    
    res = open_search.search(index=INDEX, body=q)
    print("###OPEN SEARCH RESULT###")
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source']['RestaurantID'])
    return results
    
"""
{'took': 541, 'timed_out': False, '_shards': {'total': 5, 'successful': 5, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 980, 'relation': 'eq'}, 'max_score': 1.726913, 'hits': [{'_index': 'restaurants', '_id': '800', '_score': 1.726913, '_source': {'RestaurantID': 'ttn8OS7X986fYIuEEXG09g', 'Cuisine': 'Greek'}}]}}

"""

def lookup_dynamo_data(key):
    try:
        response = dynamodb_table.get_item(Key={"BusinessID": key})
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print("##DYANMO RESPONSE##")
        print(response['Item'])
        return response['Item']

def get_response(dynamo_responses, slots):
    cuisine = slots["cuisine"]
    num_people = slots["numPeople"]
    time = slots["time"]
    date = slots["date"]
    if date != "None" and time != "None" and num_people != "None":
        print(dynamo_responses[0]["Name"])
        response = f"Hello! Here are my {cuisine} restaurant suggestions for"\
                    f" {num_people} people on {date} at {time}: 1. {dynamo_responses[0]['Name']}"\
                    f", located at {dynamo_responses[0]['Address']}, "\
                    f"2. {dynamo_responses[1]['Name']}, located at {dynamo_responses[1]['Address']}, "\
                    f"and 3. {dynamo_responses[2]['Name']}, located at {dynamo_responses[2]['Address']}. Enjoy your meal!"
    else:
        response = f"Hello! Based on your past searches, here are my {cuisine} restaurant suggestions:"\
                    f"1. {dynamo_responses[0]['Name']}"\
                    f", located at {dynamo_responses[0]['Address']}, "\
                    f"2. {dynamo_responses[1]['Name']}, located at {dynamo_responses[1]['Address']}, "\
                    f"and 3. {dynamo_responses[2]['Name']}, located at {dynamo_responses[2]['Address']}. Enjoy your meal!"
    
    return response
    
    # “Hello! Here are my Japanese restaurant suggestions for 2 people, for today at 7 pm: 1. Sushi Nakazawa, located at 23 Commerce St, 2. Jin Ramen, located at 3183 Broadway, 3. Nikko, located at 1280 Amsterdam Ave. Enjoy your meal!”

# https://github.com/RekhuGopal/PythonHacks/blob/main/AWSBoto3Hacks/AWSBoto3-SES-Lambda.py
def send_email(text, rec_email):
    VERIFIED_SENDER = "mj2944@columbia.edu"
    html_text = f"""<html>
    <head></head>
    <body>
    <p>{text}</p>
    </body>
    </html>
    """
    print("send email to ", rec_email)
    try:
        response = ses.send_email(
            Destination={
                'ToAddresses': [
                    rec_email,
                ],
            },
            Message={
                'Body': {
                    'Html': {
        
                        'Data': html_text
                    },
                    'Text': {
        
                        'Data': text
                    },
                },
                'Subject': {

                    'Data': "Restaurant Recommendations"
                },
            },
            Source=VERIFIED_SENDER
        )
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print("Email sent")
    

def lambda_handler(event, context):
    print("LF2 invoked")
    
    queue_url = 'https://sqs.us-east-1.amazonaws.com/436451714672/Q1'
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    
    if 'Messages' not in response:
        return {
            'statusCode': 200,
            'body': json.dumps('No messages on queue')
        }
    
    messages = response['Messages']
    receipt_handle = messages[0]['ReceiptHandle']
    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    print(json.dumps(messages[0]))
    slots = extract_slots_from_message(messages[0])
    customer_query = slots['cuisine']
    print("Query is: ", customer_query)
    insert_into_history_table(slots["email"], slots["location"], customer_query)
    dynamo_responses = query_open_search(customer_query)
    ses_response = get_response(dynamo_responses, slots)
    send_email(ses_response, slots['email'])
    
    return ses_response
