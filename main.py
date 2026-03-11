import mysql.connector
import threading
import time
import random
import logging
import argparse
import sys
import signal
import os
from collections import Counter

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Configuration ---
# Use command-line arguments for flexibility

def str_to_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return v.lower() in ("yes", "true", "t", "1")

parser = argparse.ArgumentParser(description='MySQL HA Scaling Test Client.')
parser.add_argument('--host', default=os.getenv('DB_HOST'), help='MySQL server hostname or IP address (or Load Balancer IP/DNS)')
parser.add_argument('--port', type=int, default=int(os.getenv('DB_PORT', 3306)), help='MySQL server port')
parser.add_argument('--user', default=os.getenv('DB_USER'), help='MySQL username')
parser.add_argument('--password', default=os.getenv('DB_PASSWORD'), help='MySQL password')
parser.add_argument('--database', default=os.getenv('DB_NAME'), help='MySQL database name')
parser.add_argument('--create-db', action='store_true', default=str_to_bool(os.getenv('CREATE_DB')), help='Create database if it does not exist')
parser.add_argument('--workers', type=int, default=int(os.getenv('WORKERS', 5)), help='Number of concurrent worker threads')
parser.add_argument('--short-query-interval', type=float, default=float(os.getenv('SHORT_QUERY_INTERVAL', 0.5)), help='Seconds between short queries (approx)')
parser.add_argument('--long-query-chance', type=float, default=float(os.getenv('LONG_QUERY_CHANCE', 0.1)), help='Probability (0.0 to 1.0) of running a long query instead of a short one')
parser.add_argument('--long-query-duration', type=int, default=int(os.getenv('LONG_QUERY_DURATION', 10)), help='Duration (seconds) for the simulated long query (SELECT SLEEP)')
parser.add_argument('--connect-timeout', type=int, default=int(os.getenv('CONNECT_TIMEOUT', 10)), help='Connection timeout in seconds')
parser.add_argument('--write-ratio', type=float, default=float(os.getenv('WRITE_RATIO', 0.0)), help='Probability (0.0 to 1.0) of running a write query (INSERT/UPDATE/DELETE) instead of a SELECT 1')
parser.add_argument('--report-interval', type=int, default=int(os.getenv('REPORT_INTERVAL', 30)), help='Seconds between summary reports (0 to disable)')

args = parser.parse_args()

# Validate required arguments (now that they can be from env)
required_args = ['host', 'user', 'password', 'database']
missing_args = [arg for arg in required_args if getattr(args, arg) is None]
if missing_args:
    parser.print_help()
    print(f"\nError: The following arguments are required: --{', --'.join(missing_args)} (can also be set via environment variables)")
    sys.exit(1)

# If create_db is specified, try to create the database
if args.create_db:
    logging.info(f"Attempting to create database {args.database} if it doesn't exist...")
    temp_connection = None
    try:
        # Connect without database selected
        temp_connection = mysql.connector.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            connection_timeout=args.connect_timeout
        )
        cursor = temp_connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
        cursor.close()
        logging.info(f"Database {args.database} created successfully or already exists")
    except mysql.connector.Error as err:
        logging.error(f"Failed to create database: {err}")
        sys.exit(1)
    finally:
        if temp_connection:
            temp_connection.close()


# --- Global Flag for stopping threads ---
stop_event = threading.Event()

# --- Shared Stats ---
stats_lock = threading.Lock()
stats = {
    'total_queries': 0,
    'successful_queries': 0,
    'failed_queries': 0,
    'successful_writes': 0,
    'failed_writes': 0,
    'total_latency': 0.0,
    'reconnections': 0,
    'connection_failures': 0,
    'errors': Counter()
}

def update_stats(query_success=False, is_write=False, latency=0.0, reconn=False, conn_fail=False, error_code=None):
    with stats_lock:
        if reconn:
            stats['reconnections'] += 1
        elif conn_fail:
            stats['connection_failures'] += 1
        else:
            stats['total_queries'] += 1
            if query_success:
                stats['successful_queries'] += 1
                if is_write:
                    stats['successful_writes'] += 1
                stats['total_latency'] += latency
            else:
                stats['failed_queries'] += 1
                if is_write:
                    stats['failed_writes'] += 1
        if error_code:
            stats['errors'][error_code] += 1

def log_stats_summary():
    with stats_lock:
        total = stats['total_queries']
        success = stats['successful_queries']
        failed = stats['failed_queries']
        avg_latency = stats['total_latency'] / success if success > 0 else 0
        
        logging.info("--- HA Probe Statistics Summary ---")
        logging.info(f"Queries: Total: {total} | Success: {success} | Failed: {failed}")
        if stats['successful_writes'] > 0 or stats['failed_writes'] > 0:
            logging.info(f"Writes: Success: {stats['successful_writes']} | Failed: {stats['failed_writes']}")
        logging.info(f"Avg Success Latency: {avg_latency:.4f}s")
        logging.info(f"Connections: Reconnected: {stats['reconnections']} | Failures: {stats['connection_failures']}")
        if stats['errors']:
            logging.info(f"Top Errors: {stats['errors'].most_common(3)}")
        logging.info("-----------------------------------")

def ensure_test_table(connection):
    """Creates a simple table for write tests if it doesn't exist."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ha_test_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                worker_id INT,
                val VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        connection.commit()
        cursor.close()
    except mysql.connector.Error as err:
        logging.error(f"Failed to ensure test table: {err}")

# --- Signal Handler for graceful shutdown ---
def signal_handler(sig, frame):
    logging.info('Stop signal received, shutting down workers...')
    stop_event.set()

signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, signal_handler) # Handle termination signals

# --- Worker Thread Function ---
def db_worker(worker_id):
    """
    Represents a single application "user" connecting to the database.
    Establishes a connection, performs queries, and handles errors/reconnects.
    """
    thread_name = threading.current_thread().name
    logging.info(f"Starting worker {worker_id}")
    connection = None
    retry_delay = 1 # Initial reconnect delay
    last_error_time = None # Track the timestamp of the last connection error

    while not stop_event.is_set():
        try:
            # --- Establish Connection (if needed) ---
            if connection is None or not connection.is_connected():
                logging.info("Attempting to connect...")
                try:
                    connection = mysql.connector.connect(
                        host=args.host,
                        port=args.port,
                        user=args.user,
                        password=args.password,
                        database=args.database,
                        connection_timeout=args.connect_timeout,
                        autocommit=True
                    )
                    current_time = time.time()
                    if last_error_time is not None:
                        downtime = current_time - last_error_time
                        logging.info(f"Connection recovered after {downtime:.2f} seconds of downtime. Connection ID: {connection.connection_id}")
                        last_error_time = None  # Reset the error timestamp
                    else:
                        logging.info(f"Connection established successfully. Connection ID: {connection.connection_id}")
                    
                    if args.write_ratio > 0:
                        ensure_test_table(connection)

                    retry_delay = 1 # Reset retry delay on successful connection
                    update_stats(reconn=True)
                except mysql.connector.Error as err:
                    logging.error(f"Connection failed: {err}. Retrying in {retry_delay}s...")
                    update_stats(conn_fail=True, error_code=err.errno)
                    if last_error_time is None:
                        last_error_time = time.time()  # Record the time of the first error
                    connection = None # Ensure connection is None if connect fails
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30) # Exponential backoff up to 30s
                    continue # Skip to next loop iteration to retry connection

            # --- Perform Queries ---
            cursor = None # Define cursor outside try block for finally clause
            is_write = False
            try:
                cursor = connection.cursor()

                # Decide query type
                rand = random.random()
                if rand < args.long_query_chance:
                    # Long Query Simulation (using SLEEP)
                    query = f"SELECT SLEEP({args.long_query_duration})"
                    start_time = time.time()
                    logging.info(f"Executing long query: {query}")
                    cursor.execute(query)
                    cursor.fetchone()
                    duration = time.time() - start_time
                    logging.info(f"Long query completed in {duration:.2f}s")
                    update_stats(query_success=True, latency=duration)
                elif rand < (args.long_query_chance + args.write_ratio):
                    # Write Query Simulation
                    is_write = True
                    start_time = time.time()
                    write_type = random.choice(['INSERT', 'UPDATE', 'DELETE'])
                    if write_type == 'INSERT':
                        query = "INSERT INTO ha_test_data (worker_id, val) VALUES (%s, %s)"
                        cursor.execute(query, (worker_id, f"worker-{worker_id}-at-{time.time()}"))
                    elif write_type == 'UPDATE':
                        query = "UPDATE ha_test_data SET val = %s WHERE worker_id = %s LIMIT 1"
                        cursor.execute(query, (f"updated-by-{worker_id}-at-{time.time()}", worker_id))
                    else: # DELETE
                        query = "DELETE FROM ha_test_data WHERE worker_id = %s LIMIT 1"
                        cursor.execute(query, (worker_id,))
                    
                    duration = time.time() - start_time
                    logging.debug(f"Write query ({write_type}) completed in {duration:.4f}s")
                    update_stats(query_success=True, is_write=True, latency=duration)
                else:
                    # Short Query Simulation
                    query = "SELECT 1" # A very lightweight query
                    start_time = time.time()
                    cursor.execute(query)
                    cursor.fetchone()
                    duration = time.time() - start_time
                    update_stats(query_success=True, latency=duration)

                cursor.close() # Close cursor promptly
                cursor = None # Mark as closed

                # Wait before next query
                time.sleep(args.short_query_interval * (0.8 + random.random() * 0.4)) # Add some jitter

            except mysql.connector.Error as err:
                logging.error(f"Query failed: {err}")
                update_stats(query_success=False, is_write=is_write, error_code=err.errno)
                # Check for specific errors that indicate connection loss
                if err.errno in (mysql.connector.errorcode.CR_SERVER_GONE_ERROR,
                                 mysql.connector.errorcode.CR_SERVER_LOST,
                                 mysql.connector.errorcode.CR_CONNECTION_ERROR,
                                 mysql.connector.errorcode.ER_CON_COUNT_ERROR,
                                 mysql.connector.errorcode.ER_IPSOCK_ERROR,
                                 mysql.connector.errorcode.ER_LOCK_WAIT_TIMEOUT, # Might indicate node issues
                                 mysql.connector.errorcode.ER_QUERY_INTERRUPTED): # Can happen during failover
                    logging.warning("Connection likely lost, attempting reconnect on next cycle.")
                    if last_error_time is None:
                        last_error_time = time.time()  # Record the time of the first connection loss
                    if connection and connection.is_connected():
                        try:
                            connection.close() # Attempt to close the faulty connection
                        except Exception as close_err:
                            logging.error(f"Error closing faulty connection: {close_err}")
                    connection = None # Force reconnect attempt in the next loop
                    time.sleep(1) # Brief pause before trying to reconnect
                else:
                     # Other errors might be query syntax, permissions etc. - log and continue
                     time.sleep(2) # Pause slightly after other errors

            finally:
                # Ensure cursor is closed if it was opened and an error occurred before explicit close
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception as cur_close_err:
                         logging.error(f"Error closing cursor in finally block: {cur_close_err}")


        except Exception as e:
            # Catch unexpected errors in the main loop
            logging.critical(f"Unexpected critical error in worker loop: {e}", exc_info=True)
            update_stats(query_success=False, is_write=is_write, error_code="CRITICAL")
            if last_error_time is None:
                last_error_time = time.time()  # Record the time of the first critical error
            if connection and connection.is_connected():
                try:
                    connection.close()
                except Exception as close_err:
                     logging.error(f"Error closing connection after critical error: {close_err}")
            connection = None # Force reconnect
            time.sleep(5) # Wait longer after critical errors

    # --- Cleanup on exit ---
    if connection and connection.is_connected():
        logging.info("Closing connection...")
        try:
            connection.close()
        except Exception as close_err:
            logging.error(f"Error closing connection during shutdown: {close_err}")
    logging.info(f"Worker {worker_id} stopped.")


# --- Main Execution ---
if __name__ == "__main__":
    logging.info(f"Starting MySQL HA Test Client with {args.workers} workers.")
    logging.info(f"Target: mysql://{args.user}:***@{args.host}:{args.port}/{args.database}")
    logging.info(f"Short query interval: {args.short_query_interval}s")
    logging.info(f"Long query chance: {args.long_query_chance * 100}%")
    logging.info(f"Long query duration: {args.long_query_duration}s")
    logging.info(f"Write ratio: {args.write_ratio * 100}%")
    logging.info(f"Report interval: {args.report_interval}s")

    threads = []
    for i in range(args.workers):
        thread = threading.Thread(target=db_worker, args=(i+1,), name=f"Worker-{i+1}", daemon=True)
        threads.append(thread)
        thread.start()
        time.sleep(0.1) # Stagger thread starts slightly

    logging.info(f"All {args.workers} workers started. Running until Ctrl+C is pressed.")

    # Keep the main thread alive while workers run, waiting for the stop event
    try:
        last_report_time = time.time()
        while not stop_event.is_set():
            # Check worker health
            for i, t in enumerate(threads):
                if not t.is_alive() and not stop_event.is_set():
                    logging.warning(f"Worker-{i+1} seems to have died unexpectedly. Restarting...")
                    new_thread = threading.Thread(target=db_worker, args=(i+1,), name=f"Worker-{i+1}", daemon=True)
                    threads[i] = new_thread
                    new_thread.start()

            # Periodic Stats Reporting
            if args.report_interval > 0 and time.time() - last_report_time > args.report_interval:
                log_stats_summary()
                last_report_time = time.time()

            time.sleep(1) # Main thread sleep interval

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received in main thread, signaling workers to stop...")
        stop_event.set()

    # Final report
    logging.info("Exiting... Final summary:")
    log_stats_summary()

    # Wait for all threads to finish
    logging.info("Waiting for worker threads to complete...")
    for thread in threads:
        thread.join(timeout=args.long_query_duration + 5) # Wait a bit longer than longest query + buffer
        if thread.is_alive():
             logging.warning(f"Thread {thread.name} did not exit cleanly after timeout.")

    logging.info("MySQL HA Test Client finished.")
    sys.exit(0)
