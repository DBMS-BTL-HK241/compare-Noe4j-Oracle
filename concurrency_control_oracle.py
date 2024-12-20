import cx_Oracle
import concurrent.futures
import time
import psutil  

# Cấu hình kết nối đến Oracle
dsn = cx_Oracle.makedsn("localhost", 1521, "XE")  # Dùng thông tin kết nối của bạn
connection = cx_Oracle.connect("username", "password", dsn)

SUCCESS_COUNT = 0
FAILURE_COUNT = 0

def update_node_version(cursor, node_id):
    cursor.execute("""
        SELECT version FROM Test WHERE id = :node_id
    """, node_id=node_id)
    record = cursor.fetchone()
    if not record:
        raise Exception(f"Node {node_id} not found")

    current_version = record[0]
    new_version = current_version + 1

    cursor.execute("""
        UPDATE Test 
        SET version = :new_version 
        WHERE id = :node_id AND version = :current_version
    """, node_id=node_id, current_version=current_version, new_version=new_version)
    connection.commit()

def worker(node_id):
    global SUCCESS_COUNT, FAILURE_COUNT
    try:
        with connection.cursor() as cursor:
            update_node_version(cursor, node_id)
            SUCCESS_COUNT += 1
    except Exception:
        FAILURE_COUNT += 1

if __name__ == "__main__":
    # Tạo bản ghi ban đầu nếu chưa có
    with connection.cursor() as cursor:
        cursor.execute("""
            MERGE INTO Test t
            USING dual
            ON (t.id = 1)
            WHEN NOT MATCHED THEN
                INSERT (t.id, t.version) VALUES (1, 0)
        """)
        connection.commit()

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
    with connection.cursor() as cursor:
        cursor.execute("SELECT version FROM Test WHERE id = 1")
        print(f"Final version: {cursor.fetchone()[0]}")

    connection.close()
