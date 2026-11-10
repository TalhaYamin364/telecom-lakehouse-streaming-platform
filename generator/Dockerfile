# Python 3.12 on Debian, pinned by digest; confluent-kafka ships a prebuilt arm64 wheel, so no compiler is needed.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdr_events.py producer.py ./

USER nobody
ENTRYPOINT ["python", "producer.py"]
