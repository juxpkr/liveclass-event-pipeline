import json
import uuid
import random
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# 메타데이터 (producer.py 기준)
COURSES = [
    ("c_001", "programming", "i_101", 50000, 1800),
    ("c_002", "design", "i_102", 30000, 2400),
    ("c_003", "business", "i_103", 45000, 2100),
]
SEGMENTS = ["guest", "new_user", "returning_user", "power_user"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["KR", "US", "JP", "VN"]

def now():
    return datetime.now(timezone.utc)

def to_str(dt):
    return dt.isoformat().replace("+00:00", "Z")

def create_event(event_type, t, user_id, session_id, segment, course, device, country):
    course_id, category, instructor_id, price, duration = course
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": to_str(t),
        "user_id": user_id,
        "session_id": session_id,
        "user_segment": segment,
        "course_id": course_id,
        "category": category,
        "instructor_id": instructor_id,
        "price": price if event_type == "purchase" else None,
        "video_position_sec": 0 if event_type == "video_play" else duration if event_type == "video_complete" else None,
        "video_duration_sec": duration if event_type in ["video_play", "video_complete"] else None,
        "device_type": device,
        "country": country,
        "producer_time": None # 전송 직전에 현재 시간으로 덮어씀
    }

def generate_session_events():
    user_id = f"user_{random.randint(1, 1000):04d}"
    session_id = str(uuid.uuid4())
    segment = random.choice(SEGMENTS)
    course = random.choice(COURSES)
    device = random.choice(DEVICES)
    country = random.choice(COUNTRIES)

    # 이벤트 발생 시간 (현실감을 위해 현재보다 조금 과거에서 시작할 수도 있음)
    t = now() 
    events = [create_event("class_view", t, user_id, session_id, segment, course, device, country)]

    # 시간차(timedelta) 로직 적용
    if random.random() < 0.7:
        t += timedelta(seconds=random.randint(5, 60))
        events.append(create_event("video_play", t, user_id, session_id, segment, course, device, country))

        if random.random() < 0.4:
            t += timedelta(seconds=random.randint(60, course[4]))
            events.append(create_event("video_complete", t, user_id, session_id, segment, course, device, country))

    if random.random() < 0.1:
        t += timedelta(seconds=random.randint(10, 180))
        events.append(create_event("purchase", t, user_id, session_id, segment, course, device, country))

    return events

def main():
    print(f"[{to_str(now())}] 이벤트 생성기를 시작합니다...")

    # Kafka 연결 (Docker 내부망이므로 'kafka:9092' 사용)
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[SUCCESS] Kafka에 성공적으로 연결되었습니다!")
        except Exception as e:
            print(f"[ERROR] Kafka가 준비될 때까지 대기 중... ({e})")
            time.sleep(5)

    topic_name = "liveclass-events"

    try:
        # 실시간 스트리밍을 위한 무한 루프
        while True:
            session_events = generate_session_events()
            for event in session_events:
                # 시스템에 들어오는 시간(Kafka 전송 시간) 기록
                event["producer_time"] = to_str(now())
                
                # Kafka로 쏘기
                producer.send(topic_name, value=event)
                print(f"[전송 완료] Type: {event['event_type']:<15} | User: {event['user_id']}")

            producer.flush()
            
            # 0.5초 ~ 2초 쉬었다가 다음 세션 발생 (실시간 트래픽 흉내)
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\n이벤트 생성을 종료합니다.")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()