FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logging_pb2.py logging_pb2_grpc.py counter_pb2.py counter_pb2_grpc.py app.py .

CMD ["python", "app.py"]