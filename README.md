# MySQL High Availability Testing Client

A robust Python-based testing client designed to evaluate MySQL high availability setups by simulating real-world database workloads. This tool helps in testing database failover scenarios, connection handling, and overall system stability under various query patterns.

## Features

- Multiple concurrent worker threads simulating database users
- Configurable mix of short and long-running queries
- Robust error handling and automatic reconnection
- Graceful shutdown handling
- Detailed logging with thread-specific information
- Command-line configuration for all important parameters

## Prerequisites

- Python 3.x
- MySQL Server or compatible database
- Required Python packages:
  ```bash
  mysql-connector-python
  ```

## Installation

1. Clone this repository or download the script
2. Install the required dependency:
   ```bash
   pip install mysql-connector-python
   ```

## Usage

Basic usage example:

```
bash python mysql_ha_test.py --host localhost --user myuser --password mypassword --database testdb
```

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--host` | MySQL server hostname or IP address | Required |
| `--port` | MySQL server port | 3306 |
| `--user` | MySQL username | Required |
| `--password` | MySQL password | Required |
| `--database` | MySQL database name | Required |
| `--create-db` | Create database if it doesn't exist | False |
| `--workers` | Number of concurrent worker threads | 5 |
| `--short-query-interval` | Seconds between short queries | 0.5 |
| `--long-query-chance` | Probability of running a long query | 0.1 |
| `--long-query-duration` | Duration for simulated long queries (seconds) | 10 |
| `--connect-timeout` | Connection timeout in seconds | 10 |

### Advanced Usage Example

```bash
bash python mysql_ha_test.py
--host db.example.com
--port 3306
--user testuser
--password secretpass
--database hatest
--create-db
--workers 10
--short-query-interval 1.0
--long-query-chance 0.2
--long-query-duration 15
--connect-timeout 5

```


## License
MIT

## Contributing
Contributions are welcome!