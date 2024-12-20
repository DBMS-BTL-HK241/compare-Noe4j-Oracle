from neo4j import GraphDatabase
import concurrent.futures
import time
import psutil  

# config database
URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "root25122003"
driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

SUCCESS_COUNT = 0
FAILURE_COUNT = 0

def update_node_version(tx, node_id):
    result = tx.run("""
        MATCH (n:Test {id: $node_id})
        RETURN n.version AS version
    """, node_id=node_id)

    record = result.single()
    if not record:
        raise Exception(f"Node {node_id} not found")

    current_version = record["version"]
    new_version = current_version + 1

    tx.run("""
        MATCH (n:Test {id: $node_id})
        WHERE n.version = $current_version
        SET n.version = $new_version
    """, node_id=node_id, current_version=current_version, new_version=new_version)

def worker(node_id):
    global SUCCESS_COUNT, FAILURE_COUNT
    with driver.session() as session:
        try:
            session.execute_write(update_node_version, node_id)
            SUCCESS_COUNT += 1
        except Exception:
            FAILURE_COUNT += 1

if __name__ == "__main__":
    with driver.session() as session:
        session.run("""
            MERGE (n:Test {id: 1})
            ON CREATE SET n.version = 0
        """)

    NUM_TRANSACTIONS = 1000000
    NUM_WORKERS = 100

  
    cpu_before = psutil.cpu_percent(interval=1)
    memory_before = psutil.virtual_memory().used / (1024 ** 2)  # MB

    start_time = time.time()

    # Thực thi đồng thời
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(worker, 1) for _ in range(NUM_TRANSACTIONS)]
        concurrent.futures.wait(futures)

    end_time = time.time()

    # Theo dõi tài nguyên sau khi thực thi
    cpu_after = psutil.cpu_percent(interval=1)
    memory_after = psutil.virtual_memory().used / (1024 ** 2)  # MB

    # Kết quả
    elapsed_time = end_time - start_time
    throughput = SUCCESS_COUNT / elapsed_time
    failure_rate = FAILURE_COUNT / NUM_TRANSACTIONS * 100

    print(f"Completed {NUM_TRANSACTIONS} transactions in {elapsed_time:.2f} seconds")
    print(f"Success count: {SUCCESS_COUNT}")
    print(f"Failure count: {FAILURE_COUNT}")
    print(f"Throughput: {throughput:.2f} transactions/second")
    print(f"Failure rate: {failure_rate:.2f}%")
    print(f"CPU Usage: {cpu_after - cpu_before:.2f}%")
    print(f"Memory Usage: {memory_after - memory_before:.2f} MB")

    # Kiểm tra giá trị version cuối cùng
    with driver.session() as session:
        result = session.run("MATCH (n:Test {id: 1}) RETURN n.version AS version")
        print(f"Final version: {result.single()['version']}")

    driver.close()
