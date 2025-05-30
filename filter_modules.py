import json
from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch

# Kafka settings
KAFKA_BROKER = 'kafka.modapto.atc.gr:9092'
TOPICS = [
    'modapto-module-creation',
    'modapto-module-deletion',
    'modapto-module-update'
]
TARGET_TOPIC = 'aegis-test'

# Elasticsearch settings
ES_HOST = "${ELASTICSEARCH_URL}"
ES_INDEX = 'modapto-modules'

# Connect to Elasticsearch without authentication
es = Elasticsearch(
    ES_HOST,
    basic_auth=("elastic", "-AwgIfDWbt_Mb+Z=_+Ck"),
    headers={
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
    }
)

# Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=True,
    group_id='modapto-handler'
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

print("🚀 Kafka Consumer running...")

for msg in consumer:
    topic = msg.topic
    event = msg.value
    print(f"📩 Received message from topic: {topic}")
    print(f"🔎 Raw event: {json.dumps(event, indent=2)}")

    try:
        results = event.get("results", {})
        module_id = results.get("id")
        print(f"🧩 Parsed ID: {module_id}")

        if topic == "modapto-module-creation":
            print("📦 Handling module creation...")
            new_doc = {
                "name": results.get("name"),
                "id": module_id,
                "endpoint": results.get("endpoint"),
                "timestamp_elastic": msg.timestamp,
                "sourceComponent": event.get("sourceComponent"),
                "timestamp_dt": event.get("timestamp"),
                "priority": event.get("priority"),
                "event": {
                    "original": json.dumps(event)
                }
            }
            print(f"📤 Sending new_doc to Kafka topic {TARGET_TOPIC}: {json.dumps(new_doc, indent=2)}")
            producer.send(TARGET_TOPIC, value=new_doc)
            print(f"✅ Sent to {TARGET_TOPIC}: {module_id}")

        elif topic == "modapto-module-deletion":
            print("❌ Handling module deletion...")
            if module_id:
                print(f"🗑️ Deleting document with ID: {module_id} from index {ES_INDEX}")
                es.delete(index=ES_INDEX, id=module_id, ignore=[404])
                print(f"🗑️ Deleted from Elasticsearch: {module_id}")
            else:
                print("⚠️ No module_id found in deletion event.")

        elif topic == "modapto-module-update":
            print("🔁 Handling module update...")
            if module_id:
                update_doc = {
                    "doc": {
                        "name": results.get("name"),
                        "endpoint": results.get("endpoint"),
                        "timestamp_dt": event.get("timestamp"),
                        "priority": event.get("priority"),
                        "sourceComponent": event.get("sourceComponent"),
                        "event": {
                            "original": json.dumps(event)
                        }
                    },
                    "upsert": {
                        "name": results.get("name"),
                        "id": module_id,
                        "endpoint": results.get("endpoint"),
                        "timestamp_elastic": msg.timestamp,
                        "sourceComponent": event.get("sourceComponent"),
                        "timestamp_dt": event.get("timestamp"),
                        "priority": event.get("priority"),
                        "event": {
                            "original": json.dumps(event)
                        }
                    }
                }
                print(f"🧾 Elasticsearch update body: {json.dumps(update_doc, indent=2)}")
                es.update(index=ES_INDEX, id=module_id, body=update_doc)
                print(f"🔄 Updated in Elasticsearch: {module_id}")
            else:
                print("⚠️ No module_id found in update event.")

    except Exception as e:
        print(f"❌ Error handling message: {e}")
