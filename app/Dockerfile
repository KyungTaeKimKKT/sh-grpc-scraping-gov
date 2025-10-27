# ===========================
# Dockerfile: GRPC Scraping Gov Service (Slim)
# docker build -t shinwoohipo/grpc-scraping-gov:1.0.0 .
# ===========================
FROM python:3.11-slim

# 필수 패키지 설치 (ping 명령어 포함)
RUN apt-get update && apt-get install -y --no-install-recommends \
    iputils-ping netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python 의존성
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# gRPC 코드 및 서비스 복사
COPY . .

# 기본 포트
ENV GRPC_PORT=50160
EXPOSE ${GRPC_PORT}

# 실행 명령 (비동기 gRPC 서버 기준)
CMD ["python", "server.py"]

