# IceRunner

IceRunner is a PyArrow Flight server implementation for Apache Iceberg tables. It provides a seamless way to read and write data to Iceberg tables using PyArrow Flight protocol.

## Features

- **Flight Server**: Exposes Iceberg tables through a PyArrow Flight interface
- **Concurrent Access**: Supports multiple readers and writers simultaneously
- **DuckDB Integration**: Uses DuckDB for efficient SQL queries on Iceberg tables
- **Strong Typing**: Full type support between PyArrow and Iceberg schemas
- **Simple CLI**: Easy-to-use command-line interface for server, reader, and writer operations

## Installation

### Prerequisites

- Python 3.8+
- PyArrow 10.0.0+
- PyIceberg
- DuckDB

### Install Dependencies

```bash
pip install pyarrow pyiceberg duckdb
```

## Usage

IceRunner can be run in three modes:

### 1. Server Mode

Starts a Flight server that exposes Iceberg tables:

```bash
python -m icerunner serve -w /path/to/warehouse -p 8816 -n my_table
```

### 2. Reader Mode

Starts a client that continuously reads data from the server:

```bash
python -m icerunner read -p 8816 -n my_table -i 2
```

### 3. Writer Mode

Starts a client that continuously writes data to the server:

```bash
python -m icerunner write -p 8816 -n my_table -i 3
```

### Command Line Options

- `-w, --warehouse-path`: Path to the warehouse directory (default: "warehouse")
- `-p, --port`: Port for the Flight server (default: 8816)
- `-n, --table-name`: Name of the table to read/write (default: "concurrent_test")
- `-i, --interval`: Interval in seconds between operations (default: 1)

## Architecture

IceRunner consists of three main components:

1. **IceRunnerConnector**: Manages the connection to Iceberg tables, handling schema conversion, table creation, and data operations.

2. **IceRunnerFlightServer**: Implements the PyArrow Flight interface, exposing endpoints for reading and writing data.

3. **Client Utilities**: Provides utilities for reading from and writing to the Flight server.

### Data Flow

```text
Writer → PyArrow Flight Client → Flight Server → IceRunnerConnector → Iceberg Table
Reader ← PyArrow Flight Client ← Flight Server ← IceRunnerConnector ← Iceberg Table
```

## Type Conversion

IceRunner automatically converts between PyArrow and Iceberg types:

| PyArrow Type | Iceberg Type |
|--------------|--------------|
| Int64        | LongType     |
| String       | StringType   |
| Timestamp    | TimestampType|
| Boolean      | BooleanType  |
| Float64      | DoubleType   |
| Float32      | FloatType    |

## Example

```python
# Start the server in one terminal
python -m icerunner serve

# In another terminal, start a writer
python -m icerunner write

# In a third terminal, start a reader
python -m icerunner read
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
