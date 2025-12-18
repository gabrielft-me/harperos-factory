import asyncio
import json
import os
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
from confluent_kafka import Consumer, KafkaError

# Configuration
TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_INBOUND_EMAIL = "inbound.email.raw"
TASK_QUEUE = "harperops-tq"

# --- Activities ---
@activity.defn
async def classify_email(email_data: dict) -> str:
    """
    Simulates using an LLM to classify the email intent.
    """
    subject = email_data.get("subject", "").lower()
    if "urgent" in subject or "claim" in subject:
        return "claim"
    elif "quote" in subject or "insurance" in subject:
        return "new_business"
    else:
        return "general_inquiry"

# --- Workflow ---
@workflow.defn
class IntakeWorkflow:
    @workflow.run
    async def run(self, email_data: dict) -> dict:
        workflow.logger.info(f"Processing email from {email_data.get('sender')}")
        
        # Step 1: Classify
        intent = await workflow.execute_activity(
            classify_email,
            email_data,
            start_to_close_timeout=timedelta(seconds=5)
        )
        
        # Step 2: Decide next step (Logic Placeholder)
        result = {
            "status": "processed",
            "intent": intent,
            "original_subject": email_data.get("subject")
        }
        
        workflow.logger.info(f"Workflow complete. Intent: {intent}")
        return result

# --- Kafka Consumer Logic ---
async def consume_kafka_and_trigger_workflow(client: Client):
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'temporal-ingest-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_INBOUND_EMAIL])

    print(f"Listening to Kafka topic: {TOPIC_INBOUND_EMAIL}...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            # Message received
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received event: {data['subject']}")
            
            # Start Temporal Workflow
            await client.start_workflow(
                IntakeWorkflow.run,
                data,
                id=f"email-{data.get('timestamp')}", # Dedup ID
                task_queue=TASK_QUEUE,
            )
    finally:
        consumer.close()

# --- Main Worker Entrypoint ---
async def main():
    # Connect to Temporal
    client = await Client.connect(TEMPORAL_HOST)
    print("Connected to Temporal!")

    # Run Worker and Kafka Consumer concurrently
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[IntakeWorkflow],
        activities=[classify_email],
    )

    print("Worker started. Press Ctrl+C to exit.")
    
    await asyncio.gather(
        worker.run(),
        consume_kafka_and_trigger_workflow(client)
    )

if __name__ == "__main__":
    asyncio.run(main())
