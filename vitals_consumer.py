import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import time
import random

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
RETRY_MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

class VitalsValidator:
    def __init__(self, min_heart_rate, max_heart_rate, min_breaths, max_breaths):
        self.min_heart_rate = min_heart_rate
        self.max_heart_rate = max_heart_rate
        self.min_breaths = min_breaths
        self.max_breaths = max_breaths

    def validate(self, record):
        """
        Validates vital signs data against pre-defined ranges.
        Returns True if valid, False otherwise.
        """
        try:
            heart_rate = record.get("heart_rate")
            breaths_per_minute = record.get("breaths_per_minute")

            if not isinstance(heart_rate, (int, float)) or not isinstance(breaths_per_minute, (int, float)):
                logging.warning(f"Invalid data types for heart_rate or breaths_per_minute. Record: {record}")
                return False

            if not (self.min_heart_rate <= heart_rate <= self.max_heart_rate):
                logging.warning(f"Heart rate out of range: {heart_rate}. Record: {record}")
                return False

            if not (self.min_breaths <= breaths_per_minute <= self.max_breaths):
                logging.warning(f"Breaths per minute out of range: {breaths_per_minute}. Record: {record}")
                return False

            return True
        except (TypeError, KeyError) as e:
            logging.error(f"Error validating record: {record}. Error: {e}")
            return False

def create_kafka_consumer():
    """Creates a Kafka consumer instance."""
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    kafka_sasl_username = os.environ.get("KAFKA_SASL_USERNAME")
    kafka_sasl_password = os.environ.get("KAFKA_SASL_PASSWORD")
    input_topic = os.environ.get("INPUT_TOPIC")

    if not all([kafka_bootstrap_servers, kafka_sasl_username, kafka_sasl_password, input_topic]):
        raise ValueError("Missing required environment variables for Kafka consumer.")

    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=kafka_bootstrap_servers.split(","),
        sasl_mechanism='SCRAM-SHA-512',
        security_protocol='SASL_PLAINTEXT',
        sasl_plain_username=kafka_sasl_username,
        sasl_plain_password=kafka_sasl_password,
        auto_offset_reset='earliest',  # or 'latest'
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000  # Check for shutdown every 1 second
    )
    return consumer

def create_kafka_producer():
    """Creates a Kafka producer instance."""
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    kafka_sasl_username = os.environ.get("KAFKA_SASL_USERNAME")
    kafka_sasl_password = os.environ.get("KAFKA_SASL_PASSWORD")

    if not all([kafka_bootstrap_servers, kafka_sasl_username, kafka_sasl_password]):
        raise ValueError("Missing required environment variables for Kafka producer.")

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers.split(","),
        sasl_mechanism='SCRAM-SHA-512',
        security_protocol='SASL_PLAINTEXT',
        sasl_plain_username=kafka_sasl_username,
        sasl_plain_password=kafka_sasl_password,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer

def publish_message(producer, topic, message, retries=RETRY_MAX_ATTEMPTS):
    """Publishes a message to a Kafka topic with retries."""
    for attempt in range(retries):
        try:
            producer.send(topic, value=message).get(timeout=10)  # Adjust timeout as needed
            logging.info(f"Published message to topic {topic}: {message}")
            return True
        except KafkaError as e:
            logging.warning(f"Attempt {attempt + 1} failed to publish to topic {topic}: {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))  # Exponential backoff
            else:
                logging.error(f"Failed to publish to topic {topic} after {retries} attempts.  Error: {e}")
                return False
    return False

def main():
    """Main function to consume, validate, and produce messages."""
    input_topic = os.environ.get("INPUT_TOPIC")
    output_topic = os.environ.get("OUTPUT_TOPIC")
    error_topic = os.environ.get("ERROR_TOPIC")

    if not all([input_topic, output_topic, error_topic]):
        raise ValueError("Missing required environment variables for topics.")

    #Vital Signs Validation Ranges
    min_heart_rate = int(os.environ.get("MIN_HEART_RATE", 40))
    max_heart_rate = int(os.environ.get("MAX_HEART_RATE", 220))
    min_breaths = int(os.environ.get("MIN_BREATHS", 5))
    max_breaths = int(os.environ.get("MAX_BREATHS", 60))

    validator = VitalsValidator(min_heart_rate, max_heart_rate, min_breaths, max_breaths)

    try:
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()

        logging.info("Kafka consumer and producer initialized successfully.")

        while True:
            for message in consumer:
                record = message.value
                logging.debug(f"Received message: {record}")

                if validator.validate(record):
                    if not publish_message(producer, output_topic, record):
                        logging.error(f"Failed to publish valid record to {output_topic}: {record}")
                else:
                    logging.warning(f"Invalid record: {record}")
                    if not publish_message(producer, error_topic, record):
                        logging.error(f"Failed to publish invalid record to {error_topic}: {record}")

            # Check for termination signal or other exit conditions here if needed.
            # For example, you could check for a file to exist and break the loop.

    except ValueError as e:
        logging.error(f"Configuration error: {e}")
    except KafkaError as e:
        logging.error(f"Kafka error: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
    finally:
        try:
            consumer.close()
        except:
            pass #Best effort close
        try:
            producer.close()
        except:
            pass #Best effort close

if __name__ == "__main__":
    main()
