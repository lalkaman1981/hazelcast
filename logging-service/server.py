import logging
import hazelcast
from concurrent import futures
import grpc
import sys
import os

# Add proto to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'proto'))

from logging_pb2 import LogRequest, LogResponse, GetRequest, GetResponse
from logging_pb2_grpc import LoggingServiceServicer, add_LoggingServiceServicer_to_server

class LoggingService(LoggingServiceServicer):
    def __init__(self):
        self.client = None
        self.map = None

    def get_client(self):
        if self.client is None:
            self.client = hazelcast.HazelcastClient(
                cluster_members=["hazelcast1:5701", "hazelcast2:5702", "hazelcast3:5703"]
            )
            self.map = self.client.get_map("messages").blocking()
        return self.client

    def LogMessage(self, request, context):
        self.get_client()
        message = request.message
        # Use a key, perhaps incrementing id
        key = str(self.map.size() + 1)
        self.map.put(key, message)
        print(f"Logged message: {message}")
        return LogResponse(success=True)

    def GetMessages(self, request, context):
        self.get_client()
        messages = []
        for key in self.map.key_set():
            messages.append(self.map.get(key))
        return GetResponse(messages=messages)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port(f'[::]:{port}')
    print(f"Logging service starting on port {port}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else '50051'
    serve(port)