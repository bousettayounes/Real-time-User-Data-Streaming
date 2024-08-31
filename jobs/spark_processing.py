import logging
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from typing import Dict, List
import threading
from airflow.models import Variable  # Import Airflow Variable

logging.basicConfig(level=logging.INFO)

# Get variables from Airflow
BROKER = Variable.get("Broker")
DATABASE_SERVER = Variable.get("Database_Server")
DATABASE_PORT = Variable.get("DataBase_Port")
KAFKA_PORT = Variable.get("Kafka_Port")
SCHEMA_URL = Variable.get("Schema_URL")
TOPIC_NAME = Variable.get("topic_name")
GROUP_ID = Variable.get("group_id")  

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS streaming_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS streaming_data.users (
        id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    logging.info("Table created successfully!")

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('Spark_Data_Streaming') \
            .config("spark.cassandra.connection.host", DATABASE_SERVER) \
            .config("spark.cassandra.connection.port", DATABASE_PORT) \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
    return s_conn

def create_cassandra_connection():
    try:
        cluster = Cluster(
            contact_points=[DATABASE_SERVER],
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=5
        )
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

def get_avro_deserializer():
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_URL})
    try:
        schema = schema_registry_client.get_latest_version(f"{TOPIC_NAME}-value").schema
        return AvroDeserializer(schema_registry_client, schema.schema_str)
    except Exception as e:
        logging.error(f"Error retrieving Avro schema: {e}")
        return None

def kafka_consumer(topic: str, group_id: str, bootstrap_servers: str, avro_deserializer):
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('Reached end of partition')
            else:
                logging.error(f'Error while consuming message: {msg.error()}')
        else:
            try:
                deserialized_value = avro_deserializer(msg.value(), None)
                yield deserialized_value
            except Exception as e:
                logging.error(f"Error deserializing message: {e}")

def kafka_to_spark_stream(spark, kafka_data: List[Dict]):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])
    spark_df = spark.createDataFrame(kafka_data, schema)
    return spark_df

def process_data(spark, kafka_data):
    df = kafka_to_spark_stream(spark, kafka_data)
    
    # Write to Cassandra
    (df.write
     .format("org.apache.spark.sql.cassandra")
     .mode("append")
     .option("keyspace", "streaming_data")
     .option("table", "users")
     .save())

def run_kafka_consumer(avro_deserializer, kafka_data):
    for message in kafka_consumer(TOPIC_NAME, GROUP_ID, f"{BROKER}:{KAFKA_PORT}", avro_deserializer):
        kafka_data.append(message)

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)
            
            avro_deserializer = get_avro_deserializer()
            if avro_deserializer is not None:
                kafka_data = []
                
                # Start Kafka consumer in a separate thread
                consumer_thread = threading.Thread(target=run_kafka_consumer, args=(avro_deserializer, kafka_data))
                consumer_thread.start()
                
                logging.info("Kafka consumer started. Beginning to process data...")
                
                try:
                    while True:
                        if kafka_data:
                            process_data(spark_conn, kafka_data)
                            kafka_data.clear()
                except KeyboardInterrupt:
                    logging.info("Stopping the application...")
                finally:
                    consumer_thread.join(timeout=1)
                    spark_conn.stop()
            else:
                logging.error("Failed to create Avro deserializer")
        else:
            logging.error("Failed to create Cassandra connection")
    else:
        logging.error("Failed to create Spark connection")
