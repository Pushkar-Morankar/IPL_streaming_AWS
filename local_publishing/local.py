import csv
import json
import time
from confluent_kafka import Producer

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'client.id': 'csv-producer'
}

# Kafka topic name
topic = 'your_topic_name'  # Replace with your topic name

def delivery_report(err, msg):
    """Callback function to handle delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [partition {msg.partition()}]')

def publish_csv_to_kafka(csv_file_path):
    # Create Producer instance
    producer = Producer(kafka_config)
    
    try:
        with open(csv_file_path, 'r') as file:
            # Create CSV reader object
            csv_reader = csv.DictReader(file)
            
            # Process each row
            for row in csv_reader:
                # Convert row to JSON string
                message = json.dumps(row)
                
                # Publish message to Kafka
                producer.produce(
                    topic,
                    value=message.encode('utf-8'),
                    callback=delivery_report
                )
                
                # Flush to ensure message is sent
                producer.flush()
                
                print(f"Published: {message}")
                
                # Wait for 15 seconds before next message
                time.sleep(15)
                
    except FileNotFoundError:
        print(f"Error: File {csv_file_path} not found")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Clean up
        producer.flush()

if __name__ == "__main__":
    # Specify your CSV file path
    csv_file_path = 'path/to/your/file.csv'  # Replace with your CSV file path
    
    # Start publishing
    publish_csv_to_kafka(csv_file_path)