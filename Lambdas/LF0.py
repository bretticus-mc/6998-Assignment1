import boto3
import json

client = boto3.client('lexv2-runtime')

def send_request_to_lex(text):
    response = client.recognize_text(
        botId = "CG3DLOCPTD",
        botAliasId = "TSTALIASID",
        localeId = "en_US",
        sessionId = "test_session",
        text = text
    )
    return response

def extract_message_from_lex(lex_response):
    return lex_response['messages'][0]['content']

def extract_request_text(event):
    body = json.loads(event['body'])
    request_text = body['messages'][0]['unstructured']['text']
    return request_text
    
def create_response(body_text):
    response = {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "http://homework1-bucket-askdj.s3-website-us-east-1.amazonaws.com",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': json.dumps({
            'messages': [
                {
                    'type': 'unstructured',
                    'unstructured': {
                        'text': body_text
                    }
                }
            ]
        })
    }
    
    #"body": "{\"messages\":[{\"type\":\"unstructured\",\"unstructured\":{\"text\":\"hello\"}}]}"
    print("FINAL RESPONSE")
    print(json.dumps(response))
    return response

    
def lambda_handler(event, context):
    #When the API receives a request, you should 
    #1. extract the text message from the API request, 
    #2. send it to your Lex chatbot,
    #3. wait for the response, 
    #4. send back the response from Lex as the API response.
    print("###REQUEST###")
    print(json.dumps(event))
    request_text = extract_request_text(event)
    lex_response = send_request_to_lex(request_text)
    lex_response_message = extract_message_from_lex(lex_response)
    print("###LEX RESPONSE###")
    print(json.dumps(lex_response))

    response = create_response(lex_response_message)
    return response