from flask import Flask, request, jsonify
import grpc
import random
import sys
import os

# Add proto to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'proto'))

from logging_pb2 import LogRequest, GetRequest
from logging_pb2_grpc import LoggingServiceStub
from counter_pb2 import UpdateRequest, GetBalanceRequest
from counter_pb2_grpc import CounterServiceStub

app = Flask(__name__)

# List of logging service instances
logging_instances = [
    'logging1:50051',
    'logging2:50052',
    'logging3:50053'
]

counter_instance = 'counter:50052'

def get_logging_stub():
    address = random.choice(logging_instances)
    channel = grpc.insecure_channel(address)
    return LoggingServiceStub(channel)

def get_counter_stub():
    channel = grpc.insecure_channel(counter_instance)
    return CounterServiceStub(channel)

@app.route('/log', methods=['POST'])
def log_message():
    data = request.get_json()
    message = data.get('message')
    if not message:
        return jsonify({'error': 'message required'}), 400

    stub = get_logging_stub()
    try:
        response = stub.LogMessage(LogRequest(message=message))
        return jsonify({'success': response.success})
    except grpc.RpcError as e:
        return jsonify({'error': str(e)}), 500

@app.route('/messages', methods=['GET'])
def get_messages():
    stub = get_logging_stub()
    try:
        response = stub.GetMessages(GetRequest())
        return jsonify({'messages': list(response.messages)})
    except grpc.RpcError as e:
        return jsonify({'error': str(e)}), 500

@app.route('/balance/<account>', methods=['GET'])
def get_balance(account):
    stub = get_counter_stub()
    try:
        response = stub.GetBalance(GetBalanceRequest(account=account))
        return jsonify({'balance': response.balance})
    except grpc.RpcError as e:
        return jsonify({'error': str(e)}), 500

@app.route('/update_balance', methods=['POST'])
def update_balance():
    data = request.get_json()
    account = data.get('account')
    amount = data.get('amount')
    if not account or amount is None:
        return jsonify({'error': 'account and amount required'}), 400

    stub = get_counter_stub()
    try:
        response = stub.UpdateBalance(UpdateRequest(account=account, amount=amount))
        return jsonify({'success': response.success, 'new_balance': response.new_balance})
    except grpc.RpcError as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)