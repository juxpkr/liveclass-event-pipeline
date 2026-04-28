FROM python:3.10-slim-bookworm

WORKDIR /app

# requirements.txt 파일을 복사하고 라이브러리 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 파이썬 스크립트를 복사
COPY producer.py .

# 컨테이너가 실행될 때 파이썬 스크립트를 실행
CMD ["python", "-u", "producer.py"]