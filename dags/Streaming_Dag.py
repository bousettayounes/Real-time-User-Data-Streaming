import requests, uuid, json, time, logging
from airflow.decorators import dag, task
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
import airflow
from airflow.models import Variable 

# get variables from Airflow
BROKER = Variable.get("Broker")
KAFKA_PORT = Variable.get("Kafka_Port")
SCHEMA_URL = Variable.get("Schema_URL")
TOPIC_NAME = Variable.get("topic_name")

# DAG default arguments
def_args = {
    "owner": "YNS_Bousetta",
    "start_date": airflow.utils.dates.days_ago(0)
}

# Function to get schema from the Schema Registry
def get_schema(schema_registry_url, schema_subject):
    schema_url = f"{schema_registry_url}/subjects/{schema_subject}/versions/latest"
    response = requests.get(schema_url)
    response.raise_for_status()
    schema_data = response.json()
    schema_str = schema_data['schema']
    return avro.loads(schema_str)

# Define the DAG using the @dag decorator
@dag(
    dag_id="Streaming_Data_From_API",
    default_args=def_args,
    schedule_interval="@daily",
    catchup=False,
    tags=['streaming', 'API', 'Kafka']
)
def streaming_data_dag():

    # Task to get data from the API
    @task
    def get_data():
        res = requests.get("https://randomuser.me/api/")
        res = res.json()
        return res['results'][0]

    # Task to format the data
    @task
    def format_data(res):
        data = {}
        location = res['location']
        data['id'] = str(uuid.uuid4())
        data['first_name'] = res['name']['first']
        data['last_name'] = res['name']['last']
        data['gender'] = res['gender']
        data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                          f"{location['city']}, {location['state']}, {location['country']}"
        data['post_code'] = str(location['postcode'])
        data['email'] = res['email']
        data['username'] = res['login']['username']
        data['dob'] = res['dob']['date']
        data['registered_date'] = res['registered']['date']
        data['phone'] = res['phone']
        data['picture'] = res['picture']['medium']
        return data

    # Task to stream data to Kafka using Avro serialization
    @task
    def stream_data(data):
        schema_registry_url = SCHEMA_URL  # URL of your Schema Registry
        schema_subject = f"{TOPIC_NAME}-value"  # Schema subject in the Schema Registry
        
        # Get schema from Schema Registry
        value_schema = get_schema(schema_registry_url, schema_subject)
        
        producer_config = {
            'bootstrap.servers': f"{BROKER}:{KAFKA_PORT}",
            'schema.registry.url': schema_registry_url
        }
        
        producer = AvroProducer(producer_config, default_value_schema=value_schema)
        
        current_time = time.time()
        while time.time() <= current_time + 50:
            producer.produce(topic=TOPIC_NAME, value=data)
            producer.flush()
            logging.info("Data sent successfully with Avro serialization")
            time.sleep(1)
    
    # Task dependencies
    raw_data = get_data()
    formatted_data = format_data(raw_data)
    stream_data(formatted_data)  

# Instantiation of the DAG
streaming_data_dag()
