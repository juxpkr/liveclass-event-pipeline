import json
import logging
import os
import uuid
import random
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "liveclass-events")

# 메타데이터
COURSES = [
    ("course_001", 49000, 1800),
    ("course_002", 39000, 1500),
    ("course_003", 59000, 2400),
    ("course_004", 69000, 2100),
]
SEGMENTS = ["guest", "new_user", "returning_user", "power_user"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["KR", "US", "JP", "VN"]

# 퍼널 전환율
PURCHASE_RATE = 0.35
VIDEO_PLAY_RATE = 0.75
VIDEO_COMPLETE_RATE = 0.55

# 이상치 데이터
LATE_EVENT_RATE = 0.05
INVALID_EVENT_RATE = 0.03
DUPLICATE_EVENT_RATE = 0.02

LATE_EVENT_MIN_SECONDS = 180
LATE_EVENT_MAX_SECONDS = 600

def now():
    return datetime.now(timezone.utc)

def to_str(dt):
    return dt.isoformat().replace("+00:00", "Z")

def create_event(event_type, t, user_id, session_id, segment, course, device, country):
    course_id, price, duration = course
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": to_str(t),
        "user_id": user_id,
        "session_id": session_id,
        "user_segment": segment,
        "course_id": course_id,
        "price": price if event_type == "purchase" else None,
        "video_position_sec": 0 if event_type == "video_play" else duration if event_type == "video_complete" else None,
        "video_duration_sec": duration if event_type in ["video_play", "video_complete"] else None,
        "device_type": device,
        "country": country,
        "producer_time": None,
    }

def generate_session_events():
    user_id = f"user_{random.randint(1, 1000):04d}"
    session_id = str(uuid.uuid4())
    segment = random.choice(SEGMENTS)
    course = random.choice(COURSES)
    device = random.choice(DEVICES)
    country = random.choice(COUNTRIES)

    t = now()
    events = [create_event("class_view", t, user_id, session_id, segment, course, device, country)]

    # purchase: class_view 이후 일정 확률로 발생
    if random.random() < PURCHASE_RATE:
        t += timedelta(seconds=random.randint(10, 180))
        events.append(create_event("purchase", t, user_id, session_id, segment, course, device, country))

        # video_play: purchase한 세션에서만 발생
        if random.random() < VIDEO_PLAY_RATE:
            t += timedelta(seconds=random.randint(5, 60))
            events.append(create_event("video_play", t, user_id, session_id, segment, course, device, country))

            # video_complete: video_play 이후 일정 확률로 발생
            if random.random() < VIDEO_COMPLETE_RATE:
                t += timedelta(seconds=random.randint(60, course[2]))
                events.append(create_event("video_complete", t, user_id, session_id, segment, course, device, country))

    return events

def apply_event_anomaly(event):
    anomaly_roll = random.random()

    if anomaly_roll < INVALID_EVENT_RATE:
        invalid_case = random.choice([
            "missing_event_id",
            "missing_course_id",
            "unknown_event_type",
            "purchase_without_price",
        ])

        if invalid_case == "missing_event_id":
            event["event_id"] = None
        elif invalid_case == "missing_course_id":
            event["course_id"] = None
        elif invalid_case == "unknown_event_type":
            event["event_type"] = "unknown_event"
        elif invalid_case == "purchase_without_price":
            event["event_type"] = "purchase"
            event["price"] = None

        return event

    if anomaly_roll < INVALID_EVENT_RATE + LATE_EVENT_RATE:
        delayed_seconds = random.randint(LATE_EVENT_MIN_SECONDS, LATE_EVENT_MAX_SECONDS)
        event["event_time"] = to_str(now() - timedelta(seconds=delayed_seconds))
        return event

    return event

def main():
    logger.info("이벤트 생성기를 시작합니다...")

    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka에 성공적으로 연결되었습니다.")
        except Exception as e:
            logger.warning(f"Kafka가 준비될 때까지 대기 중... ({e})")
            time.sleep(5)

    topic_name = KAFKA_TOPIC
    pending_queue = []

    try:
        while True:
            current_time = now()

            # 세션 이벤트들은 각각 다른 event_time을 가짐 (class_view → purchase → video_play 순서)
            # 큐에 쌓아두고 event_time 기준으로 순서대로 발송해 실제 유저 행동 흐름을 시뮬레이션
            if random.random() < 0.7:
                for event in generate_session_events():
                    event_time = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
                    pending_queue.append((event_time, event))

            events_to_send = []
            future_events = []
            for event_time, event in pending_queue:
                if event_time <= current_time:
                    events_to_send.append(event)
                else:
                    future_events.append((event_time, event))
            pending_queue = future_events
            for event in events_to_send:
                event = apply_event_anomaly(event)
                event["producer_time"] = to_str(now())

                producer.send(topic_name, value=event)
                logger.info(
                    f"[전송] "
                    f"Type: {str(event['event_type']):<15} | "
                    f"User: {event.get('user_id')} | "
                    f"Pending: {len(pending_queue)}"
                )

                if event.get("event_id") is not None and random.random() < DUPLICATE_EVENT_RATE:
                    duplicate_event = event.copy()
                    duplicate_event["producer_time"] = to_str(now())
                    producer.send(topic_name, value=duplicate_event)

            if events_to_send:
                producer.flush()

            time.sleep(1.0)

    except KeyboardInterrupt:
        logger.info("이벤트 생성을 종료합니다.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()