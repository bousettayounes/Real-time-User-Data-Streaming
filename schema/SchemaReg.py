import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# Schema Registry configuration
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define your Avro schema
avro_schema = {
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "first_name", "type": "string"},
    {"name": "last_name", "type": "string"},
    {"name": "gender", "type": "string"},
    {"name": "address", "type": "string"},
    {"name": "post_code", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "dob", "type": "string"},  
    {"name": "registered_date", "type": "string"},
    {"name": "phone", "type": "string"},
    {"name": "picture", "type": "string"}
  ]
}

# Convert the Avro schema to a JSON string
schema_str = json.dumps(avro_schema)

# Create a Schema object
schema = Schema(schema_str, schema_type="AVRO")

# Register the schema
try:
    schema_id = schema_registry_client.register_schema(subject_name="API_Data-value", schema=schema)
    print(f"Schema registered successfully. Schema ID: {schema_id}")
except Exception as e:
    print(f"Error registering schema: {e}")

# Optionally, verify the registered schema
try:
    registered_schema = schema_registry_client.get_latest_version("API_Data-value")
    print(f"Retrieved schema: {registered_schema.schema.schema_str}")
except Exception as e:
    print(f"Error retrieving schema: {e}")