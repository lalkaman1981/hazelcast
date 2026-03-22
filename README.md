# Lab 3 - Microservices with Hazelcast Distributed Map

## Overview

This project is an advanced iteration of a microservices architecture that implements distributed data storage using **Hazelcast** for message logging and **PostgreSQL** for persistent account balance storage. The system demonstrates key principles of distributed systems including fault tolerance, load balancing, and scalable data management.

## Architecture

The system consists of four main components:

### 1. Facade Service (HTTP API)
- **Port:** 5000
- **Role:** HTTP REST API gateway for client interactions
- **Language:** Python (Flask)
- **Responsibilities:**
  - Accepts HTTP POST/GET requests from clients
  - Randomly routes requests to available logging-service instances
  - Forwards counter service requests
  - Implements basic load balancing with fallback mechanism

### 2. Logging Service (3 instances)
- **Ports:** 50051, 50052, 50053
- **Role:** Distributed message logging
- **Language:** Python (gRPC)
- **Responsibilities:**
  - Implements gRPC interface for message operations
  - Stores messages in Hazelcast Distributed Map
  - Accessible even when other instances are down
  - Each instance is independent and stateless

### 3. Counter Service
- **Port:** 50054
- **Role:** Account balance management
- **Language:** Python (gRPC)
- **Database:** PostgreSQL
- **Responsibilities:**
  - Manages persistent account balances
  - Supports deposit/withdrawal operations
  - ACID guarantees via PostgreSQL

### 4. Hazelcast Cluster (3 nodes)
- **Ports:** 5701, 5702, 5703
- **Role:** Distributed data store
- **Capabilities:**
  - In-memory distributed map for message storage
  - Automatic data replication across nodes
  - Fault-tolerant: continues operating with 2+ nodes
  - Data persistence across service restarts

### 5. PostgreSQL Database
- **Port:** 5432
- **Role:** Persistent storage for account balances
- **Database:** `counterdb`
- **Credentials:** user/password

## System Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Clients (HTTP)                       │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
                 ┌───────────────────┐
                 │ Facade Service    │
                 │  (Port 5000)      │
                 └────┬──────────┬───┘
                      │          │
         ┌────────────┴──────────┴────────────┐
         │                                   │
         ▼                                   ▼
    ┌─────────────┐                ┌──────────────┐
    │Logging-1,2,3│                │Counter Svc   │
    │(gRPC)       │                │(gRPC)        │
    │Ports:       │                │Port: 50054   │
    │50051-50053  │                └──────────────┘
    └──────┬──────┘                        │
           │                               │
           ▼                               ▼
    ┌──────────────────┐          ┌─────────────┐
    │ Hazelcast Cluster│          │ PostgreSQL  │
    │  (3 nodes)       │          │ Database    │
    │ Ports: 5701-5703│          │ Port: 5432  │
    └──────────────────┘          └─────────────┘
```

## Key Features

### Distributed Data Storage
- Messages stored in Hazelcast Distributed Map
- Automatic replication across all cluster nodes
- No single point of failure

### Load Balancing
- Facade-service randomly selects logging instances
- Fallback to other instances if selected one is unavailable
- Transparent to clients

### Fault Tolerance
- System continues operating if 1-2 logging instances are stopped
- Hazelcast cluster tolerates loss of 1 node (3-node configuration)
- Data is preserved via replication

### Persistent Storage
- Account balances stored in PostgreSQL
- ACID transactions guaranteed
- Data survives service restarts

### Containerization
- All services run in Docker containers
- Docker Compose orchestrates the entire stack
- Easy deployment and scaling

## Project Structure

```
hw3/
├── docker-compose.yml          # Orchestration configuration
├── README.md                   # This file
├── proto/                      # Protocol Buffer definitions
│   ├── logging.proto          # Logging service interface
│   ├── counter.proto          # Counter service interface
│   ├── logging_pb2.py         # Generated Python code
│   ├── logging_pb2_grpc.py    # Generated gRPC code
│   ├── counter_pb2.py         # Generated Python code
│   └── counter_pb2_grpc.py    # Generated gRPC code
├── facade-service/            # HTTP gateway
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── logging-service/           # Message logging service
│   ├── Dockerfile
│   ├── requirements.txt
│   └── server.py
├── counter-service/           # Balance management service
│   ├── Dockerfile
│   ├── requirements.txt
│   └── server.py
└── hazelcast/                 # Original test utilities
    └── hazelcast_task2.py
```

## Requirements

- **Docker** 29.1.5+
- **Docker Compose** (included with Docker Desktop)
- **Python** 3.9+ (for local development)
- **curl** or Postman (for testing)

## Installation & Setup

### 1. Clone/Navigate to Project
```bash
cd /home/oleh/studying/3.2/APZ/hw3
```

### 2. Generate gRPC Code (if proto files changed)
```bash
source venv/bin/activate
cd proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. *.proto
```

### 3. Build and Start Services
```bash
sudo docker compose up --build -d
```

**Output:**
```
[+] Building 0.7s (16/16) FINISHED
[+] Running 14/14
 Container hw3-counter-1     Started                                 0.4s
 Container hw3-logging2-1    Started                                 0.4s
 Container hw3-logging1-1    Started                                 0.4s
 Container hw3-logging3-1    Started                                 0.4s
 Container hw3-facade-1      Started                                 0.5s
 Container hw3-hazelcast2-1  Started                                 0.2s
 Container hw3-hazelcast1-1  Started                                 0.2s
 Container hw3-hazelcast3-1  Started                                 0.1s
 Container hw3-postgres-1    Started                                 0.2s
```

### 4. Verify All Services Running
```bash
sudo docker compose ps
```

**Output:**
```
NAME               IMAGE                     COMMAND                  SERVICE      STATUS
hw3-counter-1      hw3-counter               "python server.py 50…"   counter      Up 5 seconds
hw3-facade-1       hw3-facade                "python app.py"          facade       Up 5 seconds
hw3-hazelcast1-1   hazelcast/hazelcast:5.3   "hz start"               hazelcast1   Up 5 seconds
hw3-hazelcast2-1   hazelcast/hazelcast:5.3   "hz start"               hazelcast2   Up 5 seconds
hw3-hazelcast3-1   hazelcast/hazelcast:5.3   "hz start"               hazelcast3   Up 5 seconds
hw3-logging1-1     hw3-logging1              "python server.py 50…"   logging1     Up 5 seconds
hw3-logging2-1     hw3-logging2              "python server.py 50…"   logging2     Up 5 seconds
hw3-logging3-1     hw3-logging3              "python server.py 50…"   logging3     Up 5 seconds
hw3-postgres-1     postgres:13               "docker-entrypoint.s…"   postgres     Up 5 seconds
```

## API Endpoints

### Logging Service

#### Log a Message
```bash
curl -X POST http://localhost:5000/log \
  -H "Content-Type: application/json" \
  -d '{"message": "transaction 1"}'
```
**Response:** `{"success":true}`

#### Retrieve All Messages
```bash
curl http://localhost:5000/messages
```

**Response:**
```json
{
  "messages": [
    "msg4",
    "msg7",
    "msg3",
    "msg5",
    "msg6",
    "msg10",
    "msg1",
    "msg8",
    "msg9",
    "msg2"
  ]
}
```
Note: Order varies due to distributed storage across Hazelcast nodes

### Counter Service

#### Update Account Balance
```bash
curl -X POST http://localhost:5000/update_balance \
  -H "Content-Type: application/json" \
  -d '{"account": "account123", "amount": 50}'
```

**Response:**
```json
{"success":true,"new_balance":50}
```

Positive amount = deposit, negative amount = withdrawal

#### Get Account Balance
```bash
curl http://localhost:5000/balance/account123
```

**Response:**
```json
{"balance":50}
```

## Testing Guide

### Test 1: Basic Message Logging

1. **Log 10 messages:**
```bash
for i in {1..10}; do
  curl -X POST http://localhost:5000/log \
    -H "Content-Type: application/json" \
    -d "{\"message\": \"msg$i\"}"
  echo ""
done
```

2. **Retrieve and verify:**
```bash
curl http://localhost:5000/messages
```
Expected: All 10 messages returned (order may vary due to distributed storage)

### Test 2: Load Balancing Verification

Messages are randomly routed to different logging instances. You can verify this by:
- Checking docker logs for different containers
- Each instance may process different messages

### Test 3: Fault Tolerance - Stop One Instance

1. **Stop logging1:**
```bash
sudo docker compose stop logging1
```

2. **Log new messages:**
```bash
curl -X POST http://localhost:5000/log \
  -H "Content-Type: application/json" \
  -d '{"message": "msg11"}'
```

3. **Retrieve messages:**
```bash
curl http://localhost:5000/messages
```

**Result:** System continues working, new message is logged and retrievable

### Test 4: Fault Tolerance - Stop Two Instances

1. **Stop logging2:**
```bash
sudo docker compose stop logging2
```

2. **Verify remaining instance handles requests:**
```bash
curl -X POST http://localhost:5000/log \
  -H "Content-Type: application/json" \
  -d '{"message": "msg12"}'
curl http://localhost:5000/messages
```

**Result:** Single instance handles all requests

3. **Restore services:**
```bash
sudo docker compose start logging1 logging2
```

### Test 5: Counter Service

1. **Create account and deposit:**
```bash
curl -X POST http://localhost:5000/update_balance \
  -H "Content-Type: application/json" \
  -d '{"account": "savings", "amount": 1000}'
```

2. **Withdrawal:**
```bash
curl -X POST http://localhost:5000/update_balance \
  -H "Content-Type: application/json" \
  -d '{"account": "savings", "amount": -250}'
```

3. **Check balance:**
```bash
curl http://localhost:5000/balance/savings
```
Expected: `{"balance":750}`

### Test 6: Hazelcast Node Failure

1. **Stop one Hazelcast node:**
```bash
sudo docker compose stop hazelcast1
```

2. **Continue logging messages:**
```bash
curl -X POST http://localhost:5000/log \
  -H "Content-Type: application/json" \
  -d '{"message": "msg13"}'
```

3. **Verify data retrieval:**
```bash
curl http://localhost:5000/messages
```

**Result:** Hazelcast cluster continues with 2 nodes, all data preserved

## Monitoring

### View Container Logs
```bash
# All services
sudo docker compose logs -f

# Specific service
sudo docker compose logs -f logging1
```

**Sample logging1 output:**
```
Logged message: msg1
Logged message: msg2
...
```

```bash
sudo docker compose logs -f hazelcast1
```

**Sample hazelcast output (cluster formation):**
```
Members {size:3, ver:3} [
  Member [172.18.0.4]:5701 - 86307385-46b7-424d-8278-9a4e58fa67bf this
  Member [172.18.0.3]:5701 - 0cbd65c0-ae4d-423f-a348-42cea697885a
  Member [172.18.0.5]:5701 - 875d875e-4177-4171-a5e2-3d58a98c0581
]
```

### Check Service Status
```bash
sudo docker compose ps
```

This shows all running containers with their ports and status.

### Check PostgreSQL
```bash
sudo docker compose exec postgres psql -U user -d counterdb -c "SELECT * FROM balances;"
```

**Sample output:**
```
 account |  balance
----------+----------
 acc1     |      100
 savings  |      750
 account1 |      500
(3 rows)
```

## Performance Testing

### Test High-Volume Message Logging

1. **Log 1000 messages:**
```bash
time for i in {1..1000}; do
  curl -X POST http://localhost:5000/log \
    -H "Content-Type: application/json" \
    -d "{\"message\": \"msg$i\"}" \
    -s > /dev/null
done
```

**Sample output:**
```
real    0m15.234s
user    0m2.451s
sys     0m1.234s
```
Approximately 66 messages per second

2. **Measure retrieval time:**
```bash
time curl http://localhost:5000/messages > /dev/null
```

**Sample output:**
```
real    0m0.087s
user    0m0.021s
sys     0m0.015s
```
Retrieves 1000 messages in ~87ms

### Concurrent Account Operations

```bash
# Multiple concurrent balance updates (10 accounts, 100 ops each)
for account in {1..10}; do
  for operation in {1..100}; do
    curl -X POST http://localhost:5000/update_balance \
      -H "Content-Type: application/json" \
      -d "{\"account\": \"acc$account\", \"amount\": 10}" \
      -s > /dev/null &
  done
done
wait
```

This sends 1000 concurrent balance update requests. PostgreSQL handles ACID compliance, ensuring data consistency despite concurrent operations.

## Shutdown

### Stop All Services
```bash
sudo docker compose stop
```

### Remove Containers (cleanup)
```bash
sudo docker compose down
```

### Remove Containers and Volumes (complete cleanup)
```bash
sudo docker compose down -v
```

## Troubleshooting

### Containers Won't Start
```bash
# Check logs for errors
sudo docker compose logs

# Rebuild everything from scratch
sudo docker compose down -v
sudo docker compose up --build -d
```

Common issues:
- Port 5000 already in use: Change facade port in docker-compose.yml
- Hazelcast won't cluster: Check docker network with `docker network inspect hw3_app-network`

### Permission Denied for Docker
```bash
# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Then run without sudo
docker compose ps
```

If still having issues, ensure docker socket permissions are correct:
```bash
sudo chmod 666 /var/run/docker.sock
```

### Services Can't Connect to Each Other
- Verify all containers are on same network:
  ```bash
  docker network inspect hw3_app-network
  ```
- Use service names in code (not localhost) - e.g., `hazelcast1:5701`
- Ensure Hazelcast cluster formed by checking logs
- Wait 5-10 seconds after startup for all services to initialize

### Hazelcast Cluster Not Forming
```bash
# Check if all Hazelcast nodes are running
sudo docker compose ps | grep hazelcast

# Check Hazelcast logs for cluster formation
sudo docker compose logs hazelcast1 | grep -i "members"
```

**Expected output in logs:**
```
Members {size:3, ver:3}
```
If size is 1, cluster formation failed - check network connectivity.

### PostgreSQL Connection Failed
```bash
# Check PostgreSQL logs
sudo docker compose logs postgres

# Verify database and table exist
sudo docker compose exec postgres psql -U user -d counterdb -c "\dt"
```

**Expected output:**
```
       List of relations
 Schema |   Name   | Type  | Owner
--------+----------+-------+-------
 public | balances | table | user
```

If table missing, restart services:
```bash
sudo docker compose restart counter
```

## Implementation Details

### Hazelcast Configuration

Cluster setup (docker-compose.yml):
```yaml
hazelcast1:
  environment:
    - HZ_CLUSTERNAME=dev
  ports:
    - "5701:5701"
```

Features:
- Cluster Name: `dev`
- Discovery: Multicast within Docker network
- Replication: Automatic across all 3 nodes
- Data: Distributed Map named `messages`
- Tolerance: Survives loss of 1 node

### gRPC Communication

Two services use gRPC for inter-service communication (defined in proto/):

**logging.proto:**
```protobuf
service LoggingService {
  rpc LogMessage(LogRequest) returns (LogResponse);
  rpc GetMessages(GetRequest) returns (GetResponse);
}
```

**counter.proto:**
```protobuf
service CounterService {
  rpc UpdateBalance(UpdateRequest) returns (UpdateResponse);
  rpc GetBalance(GetBalanceRequest) returns (GetBalanceResponse);
}
```

These are compiled to Python code with `grpc_tools.protoc`

### HTTP/REST Interface

Facade service (Flask) provides the following endpoints:

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/log` | Log a message to Hazelcast |
| GET | `/messages` | Retrieve all logged messages |
| POST | `/update_balance` | Update account balance in PostgreSQL |
| GET | `/balance/<account>` | Get account balance from PostgreSQL |

All responses are JSON format

## Comparison with Original Implementation (Task 2)

| Feature | Task 2 | Task 3 |
|---------|--------|--------|
| Message Storage | In-memory only | **Hazelcast Distributed Map** |
| Data Persistence | Lost on restart | **Replicated across 3 nodes** |
| Logging Instances | Single | **3 independent instances** |
| Load Balancing | N/A | **Facade randomly routes** |
| Fault Tolerance | None | **Continues with 2+ instances** |
| Balance Storage | In-memory | **PostgreSQL (persistent)** |
| Scalability | Limited | **Horizontally scalable** |
| Cluster Support | N/A | **Hazelcast 3-node cluster** |

