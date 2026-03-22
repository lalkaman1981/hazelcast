import logging
from concurrent import futures
import grpc
import sys
import os
import psycopg2

# Add proto to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'proto'))

from counter_pb2 import UpdateRequest, UpdateResponse, GetBalanceRequest, GetBalanceResponse
from counter_pb2_grpc import CounterServiceServicer, add_CounterServiceServicer_to_server

class CounterService(CounterServiceServicer):
    def __init__(self):
        self.conn = psycopg2.connect(
            host="postgres",
            database="counterdb",
            user="user",
            password="password"
        )
        self.create_table()

    def create_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                    account TEXT PRIMARY KEY,
                    balance INTEGER DEFAULT 0
                )
            """)
            self.conn.commit()

    def UpdateBalance(self, request, context):
        account = request.account
        amount = request.amount
        with self.conn.cursor() as cur:
            cur.execute("SELECT balance FROM balances WHERE account = %s", (account,))
            row = cur.fetchone()
            if row:
                new_balance = row[0] + amount
                cur.execute("UPDATE balances SET balance = %s WHERE account = %s", (new_balance, account))
            else:
                new_balance = amount
                cur.execute("INSERT INTO balances (account, balance) VALUES (%s, %s)", (account, new_balance))
            self.conn.commit()
        print(f"Updated {account} by {amount}, new balance: {new_balance}")
        return UpdateResponse(success=True, new_balance=new_balance)

    def GetBalance(self, request, context):
        account = request.account
        with self.conn.cursor() as cur:
            cur.execute("SELECT balance FROM balances WHERE account = %s", (account,))
            row = cur.fetchone()
            balance = row[0] if row else 0
        return GetBalanceResponse(balance=balance)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_CounterServiceServicer_to_server(CounterService(), server)
    server.add_insecure_port(f'[::]:{port}')
    print(f"Counter service starting on port {port}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else '50052'
    serve(port)