FROM python:3.10-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Streamlit 기본 포트
EXPOSE 8501

CMD ["streamlit", "run", "Overview.py", "--server.port=8501", "--server.address=0.0.0.0"]