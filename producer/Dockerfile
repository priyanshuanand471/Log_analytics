FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python==2.2.15
WORKDIR /app
COPY producer/generate.py /app/generate.py
CMD ["python", "-u", "generate.py", "--mode", "kafka"]
