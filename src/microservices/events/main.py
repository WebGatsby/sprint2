from fastapi import FastAPI, Request, status
from schemas import MovieEvent, UserEvent, PaymentEvent
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import os

app = FastAPI()
print("✅ Event service started from:", __file__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

loop = asyncio.get_event_loop()
producer: AIOKafkaProducer = None
consumer_task = None

@app.on_event("startup")
async def startup():
    global producer, consumer_task

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    print("✅ Kafka producer started")

    consumer_task = asyncio.create_task(start_consumer())

@app.on_event("shutdown")
async def shutdown():
    global consumer_task
    await producer.stop()
    print("❎ Kafka producer stopped")

    if consumer_task:
        consumer_task.cancel()
        print("❎ Kafka consumer cancelled")

# ✅ Надёжный фоновый consumer
async def start_consumer():
    consumer = AIOKafkaConsumer(
        "movie-events", "user-events", "payment-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="event-logger-group",
    )
    await consumer.start()
    print("👂 Kafka consumer started")
    try:
        while True:
            result = await consumer.getmany(timeout_ms=500)
            for tp, messages in result.items():
                for msg in messages:
                    print(f"📨 Kafka received from {msg.topic}: {msg.value.decode()}")
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        print("🛑 Consumer cancelled")
    finally:
        await consumer.stop()

# In-memory хранилище
movie_events = []
user_events = []
payment_events = []

# Отправка в Kafka
async def send_to_kafka(topic: str, data: dict):
    message = json.dumps(data, default=str).encode("utf-8")
    await producer.send_and_wait(topic, message)
    print(f"📤 Sent to Kafka topic '{topic}': {message.decode()}")

@app.get("/api/events/health")
def health_check():
    return {"status": True}

@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def create_movie_event(request: Request):
    body = await request.json()
    payload = body.get("data", body)
    event = MovieEvent(**payload)
    movie_events.append(event.dict())
    await send_to_kafka("movie-events", event.dict())
    return {"status": "success"}

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def create_user_event(request: Request):
    body = await request.json()
    payload = body.get("data", body)
    event = UserEvent(**payload)
    user_events.append(event.dict())
    await send_to_kafka("user-events", event.dict())
    return {"status": "success"}

@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def create_payment_event(request: Request):
    body = await request.json()
    payload = body.get("data", body)
    event = PaymentEvent(**payload)
    payment_events.append(event.dict())
    await send_to_kafka("payment-events", event.dict())
    return {"status": "success"}