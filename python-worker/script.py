import json
import os
from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch, NotFoundError

# Kafka settings
KAFKA_BROKER = "kafka.modapto.atc.gr:9092"
TOPICS = [
    "modapto-module-creation",
    "modapto-module-deletion",
    "modapto-module-update",
    "smart-service-assigned",
    "smart-service-unassigned"
]
TARGET_TOPIC = "aegis-test"

# Elasticsearch settings
ES_HOST = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ES_USERNAME = os.getenv("ELASTIC_USERNAME", "elastic")
ES_PASSWORD = os.getenv("ELASTIC_PASSWORD", "changeme")
ES_INDEX = "modapto-modules"

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD),
    headers={
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
    }
)

# Kafka consumer & producer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    enable_auto_commit=True,
    group_id="modapto-handler"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

print("Kafka Consumer running...")

def create_index_if_missing(index_name):
    if not es.indices.exists(index=index_name):
        print(f"Index '{index_name}' is not found. Creating index.. ")
        es.indices.create(index=index_name)
        print(f"Index '{index_name}' is created successfully.")


def update_smart_services(module_id, service_data, assign=True):
    try:
        doc = es.get(index=ES_INDEX, id=module_id)["_source"]
        services = doc.get("smartServices") or []

        if assign:
            if not any(s.get("serviceId") == service_data["serviceId"] for s in services):
                services.append(service_data)
        else:
            services = [s for s in services if s.get("serviceId") != service_data["serviceId"]]

        es.update(index=ES_INDEX, id=module_id, body={
            "doc": {
                "smartServices": services
            }
        })
        print(f"{' Assigned' if assign else ' Unassigned'} smart service in module '{module_id}'")
    except NotFoundError:
        print(f" Module ID '{module_id}' not found in index '{ES_INDEX}'")



for msg in consumer:
    topic = msg.topic
    event = msg.value
    print(f" Received message from topic: {topic}")
    print(f" Raw event: {json.dumps(event, indent=2)}")

    try:
        results = event.get("results", {})
        module_id = results.get("id")

        if topic == "modapto-module-creation":
            print(" Handling module creation...")
            create_index_if_missing(ES_INDEX)
            doc = {
                "name": results.get("name"),
                "id": module_id,
                "endpoint": results.get("endpoint"),
                "timestamp_elastic": msg.timestamp,
                "timestamp_dt": event.get("timestamp"),
                "smartServices": []
            }
            es.index(index=ES_INDEX, id=module_id, document=doc)
            print(f" Inserted document into '{ES_INDEX}': {module_id}")
            producer.send(TARGET_TOPIC, value=doc)

        elif topic == "modapto-module-update":
            print(" Handling module update...")
            if module_id:
                update_doc = {
                    "doc": {
                        "name": results.get("name"),
                        "endpoint": results.get("endpoint"),
                        "timestamp_dt": event.get("timestamp"),
                        "timestamp_elastic": msg.timestamp
                    },
                    "doc_as_upsert": True
                }
                es.update(index=ES_INDEX, id=module_id, body=update_doc)
                print(f" Updated document in '{ES_INDEX}': {module_id}")
            else:
                print(" No module_id found in update event.")

        elif topic == "modapto-module-deletion":
            print(" Handling module deletion...")
            if module_id:
                try:
                    es.delete(index=ES_INDEX, id=module_id)
                    print(f" Deleted document from '{ES_INDEX}': {module_id}")
                except NotFoundError:
                    print(f" Document with ID '{module_id}' not found in '{ES_INDEX}'")
            else:
                print(" No module_id found in deletion event.")
      
        elif topic in ["smart-service-assigned", "smart-service-unassigned"]:
                    module_id = event.get("module")
                    assign = topic == "smart-service-assigned"
                    service_data = {
                        "name": results.get("name"),
                        "catalogueId": results.get("serviceCatalogId"),
                        "serviceId": results.get("id"),
                        "url": results.get("endpoint")
                    }
                    update_smart_services(module_id, service_data, assign=assign)

    except Exception as e:
        print(f" Error handling message: {e}")
