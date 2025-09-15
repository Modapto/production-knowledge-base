import json
import os
import sys
import logging
import base64
import threading
import signal
import time
from typing import Literal, Optional
from datetime import datetime

try:
    from kafka import KafkaConsumer, KafkaProducer
except Exception:
    KafkaConsumer = None
    KafkaProducer = None

# --- Elasticsearch ---
from elasticsearch import Elasticsearch, NotFoundError

# --- HTTP API ---
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, conint, confloat

# ---------------------------------
# Logging
# ---------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("modapto-worker")

# ---------------------------------
# Config / Env
# ---------------------------------
RUN_MODE = os.getenv("RUN_MODE", "both").lower()  # 'api' | 'kafka' | 'both'
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")

GROUPING_TOPIC = "grouping-predictive-maintenance"
THRESHOLD_TOPIC = "threshold-predictive-maintenance"

GROUPING_INDEX = "sew-grouping-predictive-maintenance-results"
THRESHOLD_INDEX = "sew-threshold-predictive-maintenance-results"

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_URL", "kafka:9092")
TOPICS = [
    "modapto-module-creation",
    "modapto-module-deletion",
    "modapto-module-update",
    "smart-service-assigned",
    "smart-service-unassigned",
    "base64-input-events",
    GROUPING_TOPIC,
    THRESHOLD_TOPIC,
]
TARGET_TOPIC = "aegis-test"

# MAPPINGS

TIMEWINDOW_MAPPING = {
    "properties": {
        "begin": {"type": "date"},  
        "end":   {"type": "date"},
    }
}

GROUPING_RESULT_MAPPINGS = {
    "properties": {
        "id":             {"type": "keyword"},
        "moduleId":       {"type": "keyword"},
        "smartServiceId": {"type": "keyword"},
        "costSavings":    {"type": "double"},
        "timeWindow":     TIMEWINDOW_MAPPING,
        "groupingMaintenance":   {"type": "object", "dynamic": True},
        "individualMaintenance": {"type": "object", "dynamic": True},
        "timestamp":      {"type": "date"},
    }
}

THRESHOLD_RESULT_MAPPINGS = {
    "properties": {
        "id":             {"type": "keyword"},
        "moduleId":       {"type": "keyword"},
        "smartServiceId": {"type": "keyword"},
        "recommendation": {"type": "text", "analyzer": "standard"},
        "details":        {"type": "text", "analyzer": "standard"},
        "timestamp":      {"type": "date"},
    }
}



# Elasticsearch
ES_HOST = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ES_USERNAME = os.getenv("ELASTIC_USERNAME", "elastic")
ES_PASSWORD = os.getenv("ELASTIC_PASSWORD", "changeme")

# Indexes
ES_INDEX = "modapto-modules"
DECODED_INDEX = "decoded-events"
PROCESS_DRIFT_INDEX = "process_drift"  

logger.info(f"Elasticsearch: {ES_HOST}")
logger.info(f"Kafka broker: {KAFKA_BROKER}")
logger.info(f"Kafka topics: {TOPICS}")
logger.info(f"RUN_MODE={RUN_MODE}")

# ---------------------------------
# Elasticsearch client
# ---------------------------------
try:
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USERNAME, ES_PASSWORD),
        headers={
            "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
            "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    )
    es.info()
    logger.info("Elasticsearch connection successful")
except Exception as e:
    logger.error(f"Elasticsearch connection failed: {e}")
    sys.exit(1)

# ---------------------------------
# Kafka init 
# ---------------------------------
consumer = None
producer = None
kafka_ready = False
stop_event = threading.Event()

def init_kafka():
    global consumer, producer, kafka_ready
    if KafkaConsumer is None or KafkaProducer is None:
        logger.error("kafka-python is not installed. Install it to use Kafka mode.")
        raise RuntimeError("Kafka not available")

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
            group_id="modapto-handler",
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )
        kafka_ready = True
        logger.info("Kafka connection successful")
    except Exception as e:
        kafka_ready = False
        logger.error(f"Kafka connection failed: {e}")
        raise

# ---------------------------------
# ES helpers
# ---------------------------------
def create_index_if_missing(index_name, mappings=None, settings=None):
    try:
        if not es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' not found. Creating...")
            body = {}
            if settings:
                body["settings"] = settings
            if mappings:
                body["mappings"] = {"properties": mappings["properties"]} if "properties" in mappings else mappings
            es.indices.create(index=index_name, body=body or None)
            logger.info(f"Index '{index_name}' created.")
    except Exception as e:
        logger.error(f"Failed to create index '{index_name}': {e}")
        raise

def find_document_by_module_id(module_id, index_name=ES_INDEX):
    try:
        search_query = {"query": {"term": {"moduleId": module_id}}}
        search_response = es.search(index=index_name, body=search_query)
        hits = search_response.get("hits", {}).get("hits", [])
        if not hits:
            logger.warning(f"No doc with moduleId '{module_id}' in '{index_name}'")
            return None, None
        if len(hits) > 1:
            logger.warning(f"Multiple docs for moduleId '{module_id}'. Using first.")
        hit = hits[0]
        return hit["_id"], hit["_source"]
    except Exception as e:
        logger.error(f"Error searching moduleId '{module_id}': {e}")
        return None, None

# ---------------------------------
# Mappings 
# ---------------------------------
PROCESS_DRIFT_MAPPINGS = {
    "properties": {
        "moduleId": {"type": "keyword"},  
        "module": {"type": "keyword"},    
        "component": {"type": "keyword"},

        # UI form 
        "stage": {"type": "keyword"},
        "cell": {"type": "keyword"},
        "failureType": {"type": "keyword"},
        "failureDescription": {"type": "text"},
        "maintenanceAction": {"type": "text"},
        "componentReplacement": {"type": "keyword"},
        "workerName": {"type": "keyword"},
        "driftDone": {"type": "boolean"},

        # Timestamps
        "timestamp": {"type": "date"},               # event timestamp UI
        "timestamp_api_received": {"type": "date"},  # server-side
        "starttime": {"type": "date"},               # moment START declared
        "endtime": {"type": "date"},                 # moment DONE toggled

        # Meta
        "source": {"type": "keyword"}  # 'eds'
    }
}



def update_smart_services(module_id, service_data, assign=True, event=None, msg=None):
    try:
        doc_id, doc_source = find_document_by_module_id(module_id)
        if not doc_id or not doc_source:
            logger.error(f"Cannot update smart services: module '{module_id}' not found")
            return

        services = doc_source.get("smartServices") or []
        if assign:
            if not any(s.get("serviceId") == service_data["serviceId"] for s in services):
                services.append(service_data)
                logger.info(f"Assigned service '{service_data['serviceId']}' to module '{module_id}'")
            else:
                logger.warning(f"Service '{service_data['serviceId']}' already assigned")
        else:
            before = len(services)
            services = [s for s in services if s.get("serviceId") != service_data["serviceId"]]
            if len(services) < before:
                logger.info(f"Unassigned service '{service_data['serviceId']}' from '{module_id}'")
            else:
                logger.warning(f"Service '{service_data['serviceId']}' was not assigned to begin with")

        es.update(
            index=ES_INDEX,
            id=doc_id,
            body={
                "doc": {
                    "timestamp_elastic": getattr(msg, "timestamp", None),
                    "timestamp_dt": event.get("timestamp") if event else None,
                    "smartServices": services,
                }
            },
        )
    except Exception as e:
        logger.error(f"Failed to update smart services for '{module_id}': {e}")

def decode_base64_event(event):
    try:
        event_id = event.get("id")
        timestamp = event.get("timestamp")
        encoded_data = event.get("encoded")
        if not encoded_data or not event_id:
            logger.warning("Missing 'encoded' or 'id' in base64 event")
            return

        create_index_if_missing(DECODED_INDEX)
        decoded = base64.b64decode(encoded_data).decode("utf-8")
        doc = {
            "id": event_id,
            "timestamp": timestamp,
            "decoded": decoded,
            "original_encoded": encoded_data[:100] + "..." if len(encoded_data) > 100 else encoded_data,
        }
        es.index(index=DECODED_INDEX, id=event_id, document=doc)
        logger.info(f"Decoded and stored base64 event '{event_id}'")
    except Exception as e:
        logger.error(f"Failed to decode base64 event: {e}")
        logger.debug(f"Event data: {json.dumps(event, indent=2)}")

def handle_module_creation(event, msg):
    logger.info("Handling module creation...")
    results = event.get("results", {})
    module_id = results.get("id")
    if not module_id:
        logger.error("No module ID in creation event")
        return

    create_index_if_missing(ES_INDEX)
    doc = {
        "name": results.get("name"),
        "moduleId": module_id,
        "endpoint": results.get("endpoint"),
        "timestamp_elastic": getattr(msg, "timestamp", None),
        "timestamp_dt": event.get("timestamp"),
        "smartServices": [],
    }
    es.index(index=ES_INDEX, document=doc)
    logger.info(f"Inserted module document (moduleId='{module_id}') into '{ES_INDEX}'")

    if producer:
        producer.send(TARGET_TOPIC, value=doc)
        logger.debug(f"Sent module creation to topic '{TARGET_TOPIC}'")

def handle_module_update(event, msg):
    logger.info("Handling module update...")
    results = event.get("results", {})
    module_id = event.get("module")
    if not module_id:
        logger.error("No module ID in update event")
        return

    try:
        doc_id, _ = find_document_by_module_id(module_id)
        if not doc_id:
            logger.warning(f"No doc for moduleId '{module_id}'. Creating new.")
            doc = {
                "name": results.get("name"),
                "moduleId": module_id,
                "endpoint": results.get("endpoint"),
                "timestamp_dt": event.get("timestamp"),
                "timestamp_elastic": getattr(msg, "timestamp", None),
                "smartServices": [],
            }
            es.index(index=ES_INDEX, document=doc)
            logger.info(f"Created new module document for '{module_id}'")
            return

        update_doc = {
            "doc": {
                "name": results.get("name"),
                "endpoint": results.get("endpoint"),
                "timestamp_dt": event.get("timestamp"),
                "timestamp_elastic": getattr(msg, "timestamp", None),
            }
        }
        es.update(index=ES_INDEX, id=doc_id, body=update_doc)
        logger.info(f"Updated module document for '{module_id}'")
    except Exception as e:
        logger.error(f"Failed to update module '{module_id}': {e}")

def handle_module_deletion(event):
    logger.info("Handling module deletion...")
    module_id = event.get("module")
    if not module_id:
        logger.error("No module ID in deletion event")
        return

    try:
        search_query = {"query": {"term": {"moduleId": module_id}}}
        resp = es.search(index=ES_INDEX, body=search_query)
        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            logger.warning(f"No documents with moduleId '{module_id}' for deletion")
            return
        deleted = 0
        for hit in hits:
            es.delete(index=ES_INDEX, id=hit["_id"])
            deleted += 1
        logger.info(f"Deleted {deleted} document(s) for moduleId '{module_id}'")
    except Exception as e:
        logger.error(f"Failed to delete module '{module_id}': {e}")

def handle_smart_service_event(event, topic, msg):
    results = event.get("results", {})
    module_id = event.get("module")
    assign = topic == "smart-service-assigned"
    if not module_id:
        logger.error(f"No module ID in {topic} event")
        return

    service_data = {
        "name": results.get("name"),
        "catalogueId": results.get("serviceCatalogId"),
        "serviceId": results.get("id"),
        "endpoint": results.get("endpoint"),
    }
    logger.info(f"Handling smart service {'assignment' if assign else 'unassignment'} for '{module_id}'")
    update_smart_services(module_id, service_data, assign=assign, event=event, msg=msg)


def handle_grouping_predictive_maintenance(event):
    """
    payload SewGroupingPredictiveMaintenanceResul
    """
    try:
        create_index_if_missing(GROUPING_INDEX, mappings=GROUPING_RESULT_MAPPINGS)

        doc = {
            "id":               event.get("id"),
            "moduleId":         event.get("moduleId"),
            "smartServiceId":   event.get("smartServiceId"),
            "costSavings":      event.get("costSavings"),
            "timeWindow":       event.get("timeWindow"),
            "groupingMaintenance":   event.get("groupingMaintenance"),
            "individualMaintenance": event.get("individualMaintenance"),
            "timestamp":        event.get("timestamp"),
        }


        es.index(index=GROUPING_INDEX, id=doc.get("id"), document=doc)
        logger.info(f"[ES] Indexed grouping PM result (moduleId={doc.get('moduleId')}) into {GROUPING_INDEX}")
    except Exception as e:
        logger.error(f"Failed to index grouping PM result: {e}")
        logger.debug(f"Payload: {json.dumps(event, indent=2)}")


def handle_threshold_predictive_maintenance(event):
    """
    Payload SewThresholdBasedPredictiveMaintenanceResult.
    """
    try:
        create_index_if_missing(THRESHOLD_INDEX, mappings=THRESHOLD_RESULT_MAPPINGS)

        doc = {
            "id":               event.get("id"),
            "moduleId":         event.get("moduleId"),
            "smartServiceId":   event.get("smartServiceId"),
            "recommendation":   event.get("recommendation"),
            "details":          event.get("details"),
            "timestamp":        event.get("timestamp"),
        }

        es.index(index=THRESHOLD_INDEX, id=doc.get("id"), document=doc)
        logger.info(f"[ES] Indexed threshold PM result (moduleId={doc.get('moduleId')}) into {THRESHOLD_INDEX}")
    except Exception as e:
        logger.error(f"Failed to index threshold PM result: {e}")
        logger.debug(f"Payload: {json.dumps(event, indent=2)}")



# ---------------------------------
# FastAPI models & app
# ---------------------------------


class DriftPayload(BaseModel):
    # IDs / required
    moduleId: str = Field(..., min_length=1)
    component: str = Field(..., min_length=1)

    # UI form fields (all required όπως το Angular validation σου τώρα)
    stage: str = Field(..., min_length=1)
    cell: str = Field(..., min_length=1)
    failureType: str = Field(..., min_length=1)
    failureDescription: str = Field(..., min_length=1)
    maintenanceAction: str = Field(..., min_length=1)
    componentReplacement: str = Field(..., min_length=1)
    workerName: str = Field(..., min_length=1)
    driftDone: bool = Field(...)

    # generic timestamp from UI and lifecycle all ISO
    timestamp: str  
    starttime: str 
    endtime: str    

    # optional module name
    module: Optional[str] = None

app = FastAPI(title="Process Drift API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN] if FRONTEND_ORIGIN != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/healthz")
def healthz():
    ok_es = False
    try:
        es.info()
        ok_es = True
    except Exception:
        ok_es = False
    return {
        "ok": True,
        "elastic_ok": ok_es,
        "kafka_enabled": RUN_MODE in ("kafka", "both"),
        "kafka_ready": kafka_ready,
        "mode": RUN_MODE,
        "time": datetime.utcnow().isoformat() + "Z",
    }

@app.post("/process-drift")
def process_drift(payload: DriftPayload):
    """Upsert by moduleId στο index 'process_drift'."""
    try:
        create_index_if_missing(PROCESS_DRIFT_INDEX, mappings=PROCESS_DRIFT_MAPPINGS)

        endtime_val = payload.endtime if payload.endtime else None
        doc = {
            "moduleId": payload.moduleId,
            "module": payload.module,
            "component": payload.component,
            "stage": payload.stage,
            "cell": payload.cell,
            "failureType": payload.failureType,
            "failureDescription": payload.failureDescription,
            "maintenanceAction": payload.maintenanceAction,
            "componentReplacement": payload.componentReplacement,
            "workerName": payload.workerName,
            "driftDone": payload.driftDone,
            "timestamp": payload.timestamp,
            "timestamp_api_received": datetime.utcnow().isoformat() + "Z",
            "starttime": payload.starttime,
            "endtime": endtime_val,
            "source": "eds",
        }

        res = es.update(
            index=PROCESS_DRIFT_INDEX,
            id=payload.moduleId,
            body={"doc": doc, "doc_as_upsert": True},
            refresh="wait_for",
        )
        return {"ok": True, "result": res.get("result")}
    except Exception as e:
        logger.exception("Failed to upsert process-drift")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/process-drift/{module_id}")
def get_process_drift_by_module(module_id: str):
    """id=module_id. fallback to search."""
    try:
        create_index_if_missing(PROCESS_DRIFT_INDEX, mappings=PROCESS_DRIFT_MAPPINGS)

        try:
            hit = es.get(index=PROCESS_DRIFT_INDEX, id=module_id)
            return {"ok": True, "id": hit["_id"], "source": hit["_source"]}
        except NotFoundError:
            pass  # fallback

        es_query = {
            "bool": {
                "should": [
                    {"term": {"moduleId": module_id}},
                    {"term": {"moduleId.keyword": module_id}},
                    {"match_phrase": {"moduleId": module_id}},
                ],
                "minimum_should_match": 1,
            }
        }
        resp = es.search(
            index=PROCESS_DRIFT_INDEX,
            query=es_query,
            size=1,
            sort=[
                {"timestamp_api_received": {"order": "desc", "unmapped_type": "date"}},
                {"timestamp": {"order": "desc", "unmapped_type": "date"}},
            ],
        )
        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            raise HTTPException(status_code=404, detail="404: No process-drift found for moduleId")

        hit = hits[0]
        return {"ok": True, "id": hit["_id"], "source": hit["_source"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to read process-drift")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/process-drift/{module_id}")
def delete_process_drift(module_id: str):
    """delete declared drift (id=moduleId). Fallback delete_by_query"""
    try:
        create_index_if_missing(PROCESS_DRIFT_INDEX, mappings=PROCESS_DRIFT_MAPPINGS)

        try:
            res = es.delete(index=PROCESS_DRIFT_INDEX, id=module_id, refresh="wait_for")
            return {"ok": True, "result": "deleted", "by": "id"}
        except NotFoundError:
            pass

        es_query = {
            "bool": {
                "should": [
                    {"term": {"moduleId": module_id}},
                    {"term": {"moduleId.keyword": module_id}},
                    {"match_phrase": {"moduleId": module_id}},
                ],
                "minimum_should_match": 1,
            }
        }
        res = es.delete_by_query(index=PROCESS_DRIFT_INDEX, body={"query": es_query}, refresh=True)
        deleted = res.get("deleted", 0)
        if deleted == 0:
            raise HTTPException(status_code=404, detail="404: No process-drift found to delete for moduleId")
        return {"ok": True, "result": "deleted", "by": "query", "deleted_count": deleted}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to delete process-drift")
        raise HTTPException(status_code=500, detail=str(e))

def _search_es(index: str, module_id: Optional[str], smart_service_id: Optional[str], from_: int, size: int):
    must = []
    if module_id:
        must.append({"term": {"moduleId": module_id}})
    if smart_service_id:
        must.append({"term": {"smartServiceId": smart_service_id}})

    query = {"bool": {"must": must}} if must else {"match_all": {}}

    resp = es.search(
        index=index,
        body={
            "query": query,
            "from": from_,
            "size": size,
            "sort": [{"timestamp": {"order": "desc", "unmapped_type": "date"}}],
        },
    )
    hits = resp.get("hits", {}).get("hits", [])
    return [{"id": h.get("_id"), **h.get("_source", {})} for h in hits]


@app.get("/predictive-maintenance/grouping")
def get_grouping_results(
    moduleId: Optional[str] = Query(default=None),
    smartServiceId: Optional[str] = Query(default=None),
    from_: int = Query(default=0, ge=0, alias="from"),
    size: int = Query(default=50, ge=1, le=500),
):
    """
    returns grouping PM results from index sew-grouping-predictive-maintenance-results

    """
    try:
        create_index_if_missing(GROUPING_INDEX, mappings=GROUPING_RESULT_MAPPINGS)
        data = _search_es(GROUPING_INDEX, moduleId, smartServiceId, from_, size)
        return {"ok": True, "count": len(data), "items": data}
    except Exception as e:
        logger.exception("Failed to fetch grouping results")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predictive-maintenance/threshold")
def get_threshold_results(
    moduleId: Optional[str] = Query(default=None),
    smartServiceId: Optional[str] = Query(default=None),
    from_: int = Query(default=0, ge=0, alias="from"),
    size: int = Query(default=50, ge=1, le=500),
):
    """
    returns threshold-based PM results from index sew-threshold-predictive-maintenance-results.
    """
    try:
        create_index_if_missing(THRESHOLD_INDEX, mappings=THRESHOLD_RESULT_MAPPINGS)
        data = _search_es(THRESHOLD_INDEX, moduleId, smartServiceId, from_, size)
        return {"ok": True, "count": len(data), "items": data}
    except Exception as e:
        logger.exception("Failed to fetch threshold results")
        raise HTTPException(status_code=500, detail=str(e))



# ---------------------------------
# Kafka consumer loop (thread)
# ---------------------------------
def kafka_loop():
    logger.info("Kafka consumer thread starting...")
    try:
        init_kafka()
    except Exception:
        logger.error("Kafka init failed; consumer thread not started.")
        return

    message_count = 0
    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout_ms=500)
            if not msg:
                continue
            for tp, records in msg.items():
                for record in records:
                    message_count += 1
                    topic = record.topic
                    event = record.value
                    logger.info(f"[Kafka] #{message_count} from '{topic}'")
                    try:
                        if topic == "modapto-module-creation":
                            handle_module_creation(event, record)
                        elif topic == "modapto-module-update":
                            handle_module_update(event, record)
                        elif topic == "modapto-module-deletion":
                            handle_module_deletion(event)
                        elif topic in ["smart-service-assigned", "smart-service-unassigned"]:
                            handle_smart_service_event(event, topic, record)
                        elif topic == GROUPING_TOPIC:
                             handle_grouping_predictive_maintenance(event)
                        elif topic == THRESHOLD_TOPIC:
                            handle_threshold_predictive_maintenance(event)
                        elif topic == "base64-input-events":
                            logger.info("Handling base64 encoded input...")
                            decode_base64_event(event)
                        else:
                            logger.warning(f"Unknown topic received: '{topic}'")
                        logger.info(f"[Kafka] processed from '{topic}'")
                    except Exception as e:
                        logger.error(f"[Kafka] Error processing message from '{topic}': {e}")
                        logger.debug(f"Failed event data: {json.dumps(event, indent=2)}")
                        continue
    except Exception as e:
        logger.error(f"[Kafka] Fatal loop error: {e}")
    finally:
        logger.info("[Kafka] Closing consumer/producer...")
        try:
            if consumer:
                consumer.close()
            if producer:
                producer.close()
        except Exception as e:
            logger.error(f"Kafka] Error on close: {e}")
        logger.info("[Kafka] Thread exit.")

# ---------------------------------
# Entrypoint helpers
# ---------------------------------
def run_api_blocking():
    import uvicorn
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "5000"))
    logger.info(f"Starting FastAPI on {host}:{port} (CORS origin: {FRONTEND_ORIGIN})")
    uvicorn.run(app, host=host, port=port, log_level="info")

def main_both():
    t = threading.Thread(target=kafka_loop, name="kafka-consumer", daemon=True)
    t.start()
    run_api_blocking()
    stop_event.set()
    t.join(timeout=5)

def main_kafka_only():
    kafka_loop()

def main_api_only():
    run_api_blocking()

def signal_handler(sig, frame):
    logger.info(f"Signal {sig} received; shutting down...")
    stop_event.set()
    time.sleep(0.5)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ---------------------------------
# Main
# ---------------------------------
if __name__ == "__main__":
    if RUN_MODE == "both":
        main_both()
    elif RUN_MODE == "kafka":
        main_kafka_only()
    elif RUN_MODE == "api":
        main_api_only()
    else:
        logger.warning(f"Unknown RUN_MODE '{RUN_MODE}', defaulting to 'both'")
        main_both()
