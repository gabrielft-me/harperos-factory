import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import json
import os

app = FastAPI(title="HarperOps Ingestion API")

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_INBOUND_EMAIL = "inbound.email.raw"

conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

class EmailPayload(BaseModel):
    sender: str
    subject: str
    body: str
    timestamp: str

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.post("/webhook/email")
async def ingest_email(email: EmailPayload):
    """
    Receives an email webhook (e.g. from SendGrid/Mailgun or simulated)
    and publishes it to Kafka for asynchronous processing.
    """
    try:
        # Serialize payload
        value = json.dumps(email.model_dump()).encode('utf-8')
        
        # Produce to Kafka
        producer.produce(
            TOPIC_INBOUND_EMAIL, 
            value=value, 
            callback=delivery_report
        )
        producer.poll(0) # Trigger callback
        
        return {"status": "queued", "topic": TOPIC_INBOUND_EMAIL}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok", "kafka": KAFKA_BROKER}


