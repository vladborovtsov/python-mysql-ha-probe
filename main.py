import mysql.connector
import threading
import time
import random
import logging
import argparse
import sys
import signal

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Configuration ---
# Use command-line arguments for flexibility

parser = argparse.ArgumentParser(description='MySQL HA Scaling Test Client.')
parser.add_argument('--host', required=True, help='MySQL server hostname or IP address (or Load Balancer IP/DNS)')
parser.add_argument('--port', type=int, default=3306, help='MySQL server port')
parser.add_argument('--user', required=True, help='MySQL username')
parser.add_argument('--password', required=True, help='MySQL password')
parser.add_argument('--database', required=True, help='MySQL database name')
parser.add_argument('--create-db', action='store_true', help='Create database if it does not exist')
parser.add_argument('--workers', type=int, default=5, help='Number of concurrent worker threads')
parser.add_argument('--short-query-interval', type=float, default=0.5, help='Seconds between short queries (approx)')
parser.add_argument('--long-query-chance', type=float, default=0.1, help='Probability (0.0 to 1.0) of running a long query instead of a short one')
parser.add_argument('--long-query-duration', type=int, default=10, help='Duration (seconds) for the simulated long query (SELECT SLEEP)')
parser.add_argument('--connect-timeout', type=int, default=10, help='Connection timeout in seconds')

args = parser.parse_args()

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
                        # Consider adding pool_name and pool_size if using connection pooling
                        # autocommit=True # Optional: depending on your transaction needs
                    )
                    logging.info(f"Connection established successfully. Connection ID: {connection.connection_id}")
                    retry_delay = 1 # Reset retry delay on successful connection
                except mysql.connector.Error as err:
                    logging.error(f"Connection failed: {err}. Retrying in {retry_delay}s...")
                    connection = None # Ensure connection is None if connect fails
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30) # Exponential backoff up to 30s
                    continue # Skip to next loop iteration to retry connection

            # --- Perform Queries ---
            cursor = None # Define cursor outside try block for finally clause
            try:
                cursor = connection.cursor()

                # Decide whether to run a long or short query
                if random.random() < args.long_query_chance:
                    # Long Query Simulation (using SLEEP)
                    query = f"SELECT SLEEP({args.long_query_duration})"
                    start_time = time.time()
                    logging.info(f"Executing long query: {query}")
                    cursor.execute(query)
                    # Fetch result even though SLEEP returns simple value, ensures command completes
                    cursor.fetchone()
                    duration = time.time() - start_time
                    logging.info(f"Long query completed in {duration:.2f}s")
                else:
                    # Short Query Simulation
                    query = "SELECT 1" # A very lightweight query
                    start_time = time.time()
                    # logging.debug(f"Executing short query: {query}") # Use debug for frequent logs
                    cursor.execute(query)
                    result = cursor.fetchone()
                    duration = time.time() - start_time
                    # logging.debug(f"Short query completed in {duration:.4f}s, result: {result}")

                cursor.close() # Close cursor promptly
                cursor = None # Mark as closed

                # Wait before next query
                time.sleep(args.short_query_interval * (0.8 + random.random() * 0.4)) # Add some jitter

            except mysql.connector.Error as err:
                logging.error(f"Query failed: {err}")
                # Check for specific errors that indicate connection loss
                if err.errno in (mysql.connector.errorcode.CR_SERVER_GONE_ERROR,
                                 mysql.connector.errorcode.CR_SERVER_LOST,
                                 mysql.connector.errorcode.CR_CONNECTION_ERROR,
                                 mysql.connector.errorcode.ER_CON_COUNT_ERROR,
                                 mysql.connector.errorcode.ER_IPSOCK_ERROR,
                                 mysql.connector.errorcode.ER_LOCK_WAIT_TIMEOUT, # Might indicate node issues
                                 mysql.connector.errorcode.ER_QUERY_INTERRUPTED): # Can happen during failover
                    logging.warning("Connection likely lost, attempting reconnect on next cycle.")
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

    threads = []
    for i in range(args.workers):
        thread = threading.Thread(target=db_worker, args=(i+1,), name=f"Worker-{i+1}", daemon=True)
        threads.append(thread)
        thread.start()
        time.sleep(0.1) # Stagger thread starts slightly

    logging.info(f"All {args.workers} workers started. Running until Ctrl+C is pressed.")

    # Keep the main thread alive while workers run, waiting for the stop event
    try:
        while not stop_event.is_set():
            # Check worker health (optional, can add complexity)
            # for i, t in enumerate(threads):
            #     if not t.is_alive() and not stop_event.is_set():
            #         logging.warning(f"Worker-{i+1} seems to have died unexpectedly. Restarting...")
            #         # Basic restart logic (consider max restarts, backoff)
            #         new_thread = threading.Thread(target=db_worker, args=(i+1,), name=f"Worker-{i+1}", daemon=True)
            #         threads[i] = new_thread
            #         new_thread.start()

            time.sleep(1) # Main thread sleep interval

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received in main thread, signaling workers to stop...")
        stop_event.set()

    # Wait for all threads to finish
    logging.info("Waiting for worker threads to complete...")
    for thread in threads:
        thread.join(timeout=args.long_query_duration + 5) # Wait a bit longer than longest query + buffer
        if thread.is_alive():
             logging.warning(f"Thread {thread.name} did not exit cleanly after timeout.")

    logging.info("MySQL HA Test Client finished.")
    sys.exit(0)