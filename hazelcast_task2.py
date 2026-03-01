import threading
import time
import subprocess
import random

import hazelcast
from hazelcast import HazelcastClient


def stop_node_graceful(name="hz3"):
    try:
        res = subprocess.run(
            ["sudo", "docker", "stop", name],
            check=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        print("Stopped:", res.stdout.strip())
    except subprocess.CalledProcessError as e:
        print("Error (non-zero exit):", e.returncode, e.stderr)
    except subprocess.TimeoutExpired:
        print("Timeout while stopping")

def kill_node_force(name="hz3"):
    try:
        res = subprocess.run(
            ["sudo", "docker", "kill", "--signal=SIGKILL", name],
            check=True, capture_output=True, text=True, timeout=10
        )
        print("Killed:", res.stdout.strip())
    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)

def restore_node(name="hz3"):
    try:
        res = subprocess.run(
            ["sudo", "docker", "start", name],
            check=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        print("Restored:", res.stdout.strip())
    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)

def new_client():
    return hazelcast.HazelcastClient(
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
    )

def check_data_integrity():
    client = new_client()
    m = client.get_map("capitals").blocking()
    print(f"Current map size: {m.size()}")
    client.shutdown()

def task_populate_map(mode = None):


    stop_dct = {}
    stop_func = None
    stop_steps = None
    stop_range = range(50, 500 + 1)
    number_to_stop = 0
    choices = ["hz1", "hz2", "hz3"]    

    if mode == "graceful_one":
        number_to_stop = 1
        stop_func = stop_node_graceful
        choices = random.sample(choices, number_to_stop)
        stop_steps = random.sample(stop_range, number_to_stop)
        stop_dct[stop_steps[0]] = choices[0]

    elif mode == "graceful_two":
        number_to_stop = 2
        stop_func = stop_node_graceful
        choices = random.sample(choices, number_to_stop)
        stop_steps = random.sample(stop_range, number_to_stop)
        stop_dct[stop_steps[0]] = choices[0]
        stop_dct[stop_steps[1]] = choices[1]

    elif mode == "graceful_two_sequential":
        number_to_stop = 2
        stop_func = stop_node_graceful
        choices = random.sample(choices, number_to_stop)
        stop_steps = random.sample(stop_range, number_to_stop)
        stop_dct[stop_steps[0]] = choices[0]
        stop_dct[stop_steps[1]] = choices[1]

    elif mode == "force_two":
        number_to_stop = 2
        stop_func = kill_node_force
        choices = random.sample(choices, number_to_stop)
        stop_steps = random.sample(stop_range, number_to_stop)
        stop_dct[stop_steps[0]] = choices[0]
        stop_dct[stop_steps[1]] = choices[1]

    client = new_client()
    m = client.get_map("capitals").blocking()

    print("map size before:", m.size())

    for i in range(1000):

        if mode and i in stop_dct:
            if mode == "graceful_two" or mode == "force_two":
                for _, node_name in stop_dct.items():
                    print(f"Stopping {node_name} at step {i}...")
                    stop_func(node_name)
                stop_dct.clear()

            else:
                print(f"Stopping {stop_dct[i]} at step {i}...")
                stop_func(stop_dct[i])

        m.put(str(i), f"value-{i}")
    print("map size after put:", m.size())
    client.shutdown()
    
    check_data_integrity()
    
    if mode:
        for node in choices:
            restore_node(node)


def print_cluster_info(client: HazelcastClient):

    members = client.cluster_service.get_members()
    print("cluster members:")
    for m in members:
        print("  ", m.address)

    print("partition count:", client.partition_service.get_partition_count())


def simulate_without_locks(name, iterations=10000):
    client = new_client()
    m = client.get_map("counter").blocking()

    m.put_if_absent("key", 0)
    for k in range(iterations):
        v = m.get("key")
        v += 1
        m.put("key", v)
    print(f"{name} done")
    client.shutdown()


def simulate_with_pessimistic(name, iterations=10000):
    client = new_client()
    m = client.get_map("counter").blocking()
    m.put_if_absent("key", 0)
    
    for k in range(iterations):
        m.lock("key") 
        try:
            v = m.get("key")
            v += 1
            m.put("key", v)
        finally:
            m.unlock("key") 
            
    print(f"{name} (pessimistic) done")
    client.shutdown()


def simulate_with_optimistic(name, iterations=10000):
    client = new_client()
    m = client.get_map("counter").blocking()
    m.put_if_absent("key", 0)
    for k in range(iterations):
        while True:
            v = m.get("key")
            if m.replace_if_same("key", v, v + 1):
                break
    print(f"{name} (optimistic) done")
    client.shutdown()


def measure_concurrency():
    for description, func in [
        ("no locks", simulate_without_locks),
        ("pessimistic", simulate_with_pessimistic),
        ("optimistic", simulate_with_optimistic),
    ]:

        client = new_client()
        m = client.get_map("counter").blocking()
        m.destroy()
        client.shutdown()

        start = time.perf_counter()
        threads = []
        for i in range(3):
            t = threading.Thread(target=func, args=(f"client-{i}",))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        end = time.perf_counter()

        client = new_client()
        m = client.get_map("counter").blocking()
        final = m.get("key")
        client.shutdown()

        print(f"{description}: elapsed {end - start:.3f}s, final counter={final}")

def queue_demo():
    producer = new_client()
    q = producer.get_queue("bounded").blocking()
    q.clear()

    def consumer(name):
        c = new_client()
        q2 = c.get_queue("bounded").blocking()
        while True:
            item = q2.take()
            print(f"{name} got {item}")
            if item == "STOP":
                break
        c.shutdown()

    c1 = threading.Thread(target=consumer, args=("consumer1",))
    c2 = threading.Thread(target=consumer, args=("consumer2",))
    c1.start()
    c2.start()

    for i in range(1, 101):
        print("producing", i)
        q.put(i)
    q.put("STOP")
    q.put("STOP")

    c1.join()
    c2.join()
    producer.shutdown()


if __name__ == "__main__":
    # print("=== populate map ===")
    # task_populate_map(mode="none")

    # client = new_client()
    # print_cluster_info(client)
    # client.shutdown()

    # print("\n=== measure concurrency ===")
    # measure_concurrency()

    print("\n=== queue demo ===")
    queue_demo()
