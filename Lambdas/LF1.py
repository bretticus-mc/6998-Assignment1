import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, date as dt, timedelta, timezone
import dateutil

TABLE = "user-history"

db = boto3.resource('dynamodb')
dynamodb_table = db.Table(TABLE)

def lookup_email_in_dynamo(email):
    try:
        response = dynamodb_table.get_item(Key={"Email": email})
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        if 'Item' in response:
            return response['Item']
        return None

def continue_to_dining_suggestions_intent(session_attributes, email):
    slots = {
        "location": None,
        "cuisine": None,
        "date": None,
        "time": None,
        "numPeople": None
    }
        
    session_attributes["continue_conversation"] = "yes"
    session_attributes["email"] = email
    message_content = "What city or city area are you looking to dine in?"
    validation_result = build_validation_result(True, "location", message_content)
    response = elicit_slot(session_attributes, slots, "DiningSuggestionsIntent", validation_result)
    return response

def user_id_validation(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = get_slots(intent_request)
    slot_values = get_slot_values(intent_request)
    email = slot_values['email']
    if not email:
        return delegate(intent_request, session_attributes, slots, "UserIdIntent")
    continue_conversation = slot_values['continue_conversation']
    if continue_conversation:
        if continue_conversation in {"yes", "Yes", "y", "Y"}:
            return continue_to_dining_suggestions_intent(session_attributes, email)
        else:
            message = {
                'contentType': 'PlainText',
                'content': "Have a great day!"
            }
            fulfillment_state = "Fulfilled"
            return close(intent_request, session_attributes, fulfillment_state, message)
    dynamo_response = lookup_email_in_dynamo(email)
    print("##DYANMO RESPONSE##")
    print(dynamo_response)
    if dynamo_response:
        # email exists
        location = dynamo_response["location"]
        cuisine = dynamo_response["cuisine"]
        sqs_message = create_sqs_message(location, cuisine, "None", "None", email, "None")
        status = push_message_to_sqs(sqs_message)
        
        message_content = f"You should receive a recommendation soon based on your previous search for {cuisine} restaurants in {location}. Would you like to continue this conversation and receive new recommendations based on new parameters?"
        validation_result = build_validation_result(True, "continue_conversation", message_content)
        response = elicit_slot(session_attributes, slots, "UserIdIntent", validation_result)
        return response
    else:
        return continue_to_dining_suggestions_intent(session_attributes, email)
        
    
    

def is_valid_location(location):
    valid_locs = {'new york', 'new york city', 'ny', 'nyc', 'manhattan'}
    return location.lower() in valid_locs

def is_valid_cuisine(cuisine):
    valid_cuisines = {'french', 'greek', 'indian', 'chinese', 'italian', 'japanese'}
    return cuisine.lower() in valid_cuisines

def is_valid_date(date):
    datetime_object = datetime.strptime(date, '%Y-%m-%d').date()
    return datetime_object >= dt.today()

def is_valid_time(date, time):
    et_offset = timedelta(hours=-5)
    et_tz = timezone(et_offset)
    datetime_str = f"{date} {time}"
    datetime_object = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
    datetime_object = datetime_object.replace(tzinfo=et_tz)
    return datetime_object > datetime.now(et_tz)

def is_valid_num_people(num_people):
    num_people = int(num_people)
    return num_people > 0 and num_people <= 30

def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': message_content
    }

def valid_slots_message():
    return {
        'isValid': True
    }

def validate_slots(location, cuisine, time, numPeople, date):
    print(date, time)
    if location and not is_valid_location(location):
        return build_validation_result(False, 'location', "Invalid location. Only New York City is supported, please enter NYC")
    if cuisine and not is_valid_cuisine(cuisine):
        return build_validation_result(False, 'cuisine', "Only French, Greek, Indian, Chinese, Italian, and Japanese are supported. Please enter a cuisine from this list.")
    if date and not is_valid_date(date):
        return build_validation_result(False, 'date', "Entered date is in the past. Please enter a valid date")
    if date and time and not is_valid_time(date, time):
        return build_validation_result(False, 'time', "Entered date/time is in the past. Please enter a valid time")
    if numPeople and not is_valid_num_people(numPeople):
        return build_validation_result(False, 'numPeople', "Please enter a valid number of people")
    print("returning valid slot message")
    return valid_slots_message()
    
   
def validate_dialog(intent_request):
    slot_values = get_slot_values(intent_request)
    slots = get_slots(intent_request)
    session_attributes = get_session_attributes(intent_request)
    print("###session attributes ###")
    print(session_attributes)
    intent = intent_request['sessionState']['intent']['name']
    validation_result = validate_slots(**slot_values)
    if not validation_result['isValid']:
        print("###Invalid Slot###")
        invalid_slot = validation_result['violatedSlot']
        print(invalid_slot)
        slots[invalid_slot] = None
        response = elicit_slot(session_attributes, slots, intent, validation_result)
        print(response)
        return response
        
    print("###Delegating###")
    response = delegate(intent_request, session_attributes, slots, intent)
    print(response)
    return response

def elicit_slot(session_attributes, slots, intent_name, validation_result):
    invalid_slot = validation_result['violatedSlot']
    message = validation_result['message']
    print(session_attributes)
    return {
        "sessionState": {
            "sessionAttributes": session_attributes,
            "dialogAction": {
                "type": "ElicitSlot",
                'slotToElicit': invalid_slot,
            },
            "intent": {
                "name": intent_name,
                "slots": slots
            }
        },
        "messages": [
                {
                    'contentType': 'PlainText',
                    'content': message 
                }
            ]
    }

def delegate(intent_request, session_attributes, slots, intent_name):
    intent_request['sessionState']['intent']['slots'] = slots
    return {
        "sessionState": {
            #"activeContexts": intent_request['sessionState']['activeContexts'],
            "dialogAction": {
                "type": "Delegate"
            },
            "intent": intent_request['sessionState']['intent'],
            "sessionAttributes": session_attributes
        }
    }


def create_sqs_message(location, cuisine, time, numPeople, email, date):
    message = {
        'location': {
            'StringValue': location, 
            'DataType': 'String'
            
        }, 
        'cuisine': {
            'StringValue': cuisine,
            'DataType': 'String'
        },
        'time': {
            'StringValue': time,
            'DataType': 'String'
        },
        'numPeople': {
            'StringValue': numPeople,
            'DataType': 'String'
        },
        'email': {
            'StringValue': email,
            'DataType': 'String'
        },
        'date': {
            'StringValue': date,
            'DataType': 'String'
        }
        
    }
    return message
    
def push_message_to_sqs(message):
    sqs = boto3.resource('sqs')
    queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/436451714672/Q1')
    #sqs = boto3.client('sqs', aws_access_key_id=None, aws_secret_access_key=None, endpoint_url='http://localhost:9324')
    #sqs = boto3.resource('sqs')
    #queue = sqs.get_queue_by_name(QueueName='Q1.fifo')
    response = queue.send_message(MessageBody='dining_suggestion_slots', MessageAttributes=message)
    status = response['ResponseMetadata']['HTTPStatusCode']
    return status
    
    
def get_slot_values(intent_request):
    slot_values = {}
    slots = intent_request['sessionState']['intent']['slots']
    
    for slot in slots:
        if slots[slot] and 'interpretedValue' in slots[slot]['value']:
            slot_values[slot] = slots[slot]['value']['interpretedValue']
        elif slots[slot] and 'resolvedValues' in slots[slot]['value']:
            slot_values[slot] = slots[slot]['value']['resolvedValues'][0]
        else:
            slot_values[slot] = None
    return slot_values
        

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']
    
def get_slot(intent_request, slotName):
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['interpretedValue']
    else:
        return None

def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']
    return {}

def elicit_intent(intent_request, session_attributes, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'sessionAttributes': session_attributes
        },
        'messages': [ message ] if message != None else None,
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

def close(intent_request, session_attributes, fulfillment_state, message):
    print("### Close is being called###")
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    response = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }
    print("###RESPONSE###")
    print(response)
    return response

def dining_suggestion_intent(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = get_slots(intent_request)
    
    location = get_slot(intent_request, 'location')
    cuisine = get_slot(intent_request, 'cuisine')
    time = get_slot(intent_request, 'time')
    numPeople = get_slot(intent_request, 'numPeople')
    email = session_attributes["email"]
    print("##EMAIL FROM SESSION ATTRIBUTES##")
    print(email)
    date = get_slot(intent_request, 'date')
    
    sqs_message = create_sqs_message(location, cuisine, time, numPeople, email, date)
    status = push_message_to_sqs(sqs_message)
    
    print("status type", type(status))
    if status == 200:
        text = "You’re all set. Expect my suggestions shortly! Have a good day."
    else:
        text = "SQS send failed - Please try again"
    message = {
        'contentType': 'PlainText',
        'content': text
    }
    fulfillment_state = "Fulfilled"
    return close(intent_request, session_attributes, fulfillment_state, message)
    
    
def dispatch(intent_request):
    invocation_source = intent_request['invocationSource']
    intent_name = intent_request['sessionState']['intent']['name']
    # Dispatch to your bot's intent handlers
    print("###dispatching###")
    if invocation_source == 'DialogCodeHook':
        if intent_name == 'UserIdIntent':
            return user_id_validation(intent_request)
        print("###validating###")
        return validate_dialog(intent_request)
    elif invocation_source == 'FulfillmentCodeHook':
        return dining_suggestion_intent(intent_request)
    
    text = "Sorry, I am unable to understand your request. Please try again."
    message = {
        'contentType': 'PlainText',
        'content': text
    }
    fulfillment_state = "Fulfilled"
    return close(intent_request, session_attributes, fulfillment_state, message)
    raise Exception('Invalid Message')
    
    
def lambda_handler(event, context):
    print("###Event###")
    print(json.dumps(event))
    print("###Context###")
    print(context)
    response = dispatch(event)
    return response