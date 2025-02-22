import json
from confluent_kafka import Consumer
import boto3



# DynamoDB configuration
DYNAMODB_TABLE = 'FantasyPlayerScores'

def lambda_handler(event, context):
    # Kafka configuration (replace with your Confluent Kafka details)
    api_key = ''

    api_secret=''

    Bootstrap_server=''
    kafka_topic = "topic_m"


    kafka_server_config = {
        'bootstrap.servers': Bootstrap_server,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'session.timeout.ms': '45000'
    }
    Consumer.subscribe([kafka_topic])

    try:
        # Poll for messages (timeout in seconds, adjust based on Lambda timeout)
        messages = Consumer.poll(timeout=10.0)
        if not messages:
            return {'statusCode': 200, 'body': 'No new messages'}

        # Process each message
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(DYNAMODB_TABLE)

        for msg in messages:
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            data = json.loads(msg.value().decode('utf-8'))
            process_ball_event(table, data)

    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': f"Error: {str(e)}"}
    finally:
        Consumer.close()

    return {'statusCode': 200, 'body': 'Processed messages successfully'}

def process_ball_event(table, data):
    """
    Process a single ball event and update player scores in DynamoDB.
    
    Args:
        table: DynamoDB table object
        data: Dictionary containing ball-by-ball data
    """
    # Extract relevant fields from the message
    match_id = data['match_id']
    batter = data['batter']
    bowler = data['bowler']
    total_runs = int(data['total_runs'])
    extra_runs = int(data['extra_runs'])
    is_wicket = int(data['is_wicket'])
    dismissal_kind = data['dismissal_kind']

    # Calculate batsman runs (runs scored off the bat, excluding extras)
    batsman_runs = total_runs if extra_runs == 0 else 0

    # Fantasy scoring rules (customize as needed)
    batting_points = batsman_runs * 1  # 1 point per run
    wicket_points = 5  # 5 points per wicket

    # Update batter's stats
    table.update_item(
        Key={'match_id': match_id, 'player': batter},
        UpdateExpression="ADD runs :r, points :p",
        ExpressionAttributeValues={
            ':r': batsman_runs,
            ':p': batting_points
        }
    )

    # Update bowler's stats if a wicket is taken (exclude run outs)
    bowler_wicket_types = ['caught', 'bowled', 'lbw', 'stumped', 'hit wicket', 'caught and bowled']
    if is_wicket == 1 and dismissal_kind in bowler_wicket_types:
        table.update_item(
            Key={'match_id': match_id, 'player': bowler},
            UpdateExpression="ADD wickets :w, points :p",
            ExpressionAttributeValues={
                ':w': 1,
                ':p': wicket_points
            }
        )

# Example usage (for testing locally, remove in Lambda)
if __name__ == "__main__":
    sample_event = {
        "match_id": "335982",
        "inning": "1",
        "batting_team": "Kolkata Knight Riders",
        "bowling_team": "Royal Challengers Bangalore",
        "over": "0",
        "ball": "1",
        "batter": "SC Ganguly",
        "bowler": "P Kumar",
        "total_runs": "1",
        "extra_runs": "1",
        "extras_type": "1 legbyes",
        "is_wicket": "0",
        "dismissal_kind": "NA"
    }
    process_ball_event(boto3.resource('dynamodb').Table(DYNAMODB_TABLE), sample_event)