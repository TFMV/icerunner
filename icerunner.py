import os
import random
import time
import socket
import threading
import logging
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.flight as flight
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    TimestampType,
    BooleanType,
    DoubleType,
    FloatType,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("icerunner")

# Global icerunner constants
ICERUNNER_WAREHOUSE_PATH = "warehouse"
ICERUNNER_PORT = 8816
ICERUNNER_TABLE_NAME = "icerunner_test"
ICERUNNER_NAMESPACE = "default"


class IceRunnerConnector:
    """
    Connector for Iceberg tables using PyArrow and DuckDB.
    Manages table creation, querying, and data insertion.
    """

    def __init__(self, warehouse_path: str):
        self.warehouse_path = Path(warehouse_path).absolute()
        os.makedirs(self.warehouse_path, exist_ok=True)

        self.catalog_params = {
            "type": "sql",
            "uri": f"sqlite:///{self.warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{self.warehouse_path}",
        }

        self.catalog = load_catalog("default", **self.catalog_params)
        namespaces = [ns[0] for ns in self.catalog.list_namespaces()]

        if ICERUNNER_NAMESPACE not in namespaces:
            self.catalog.create_namespace(ICERUNNER_NAMESPACE)

        self.con = duckdb.connect()
        self._init_duckdb()
        self._reflect_views()

    def _init_duckdb(self) -> None:
        """Initialize DuckDB with Iceberg extension and settings."""
        self.con.execute("INSTALL iceberg;")
        self.con.execute("LOAD iceberg;")
        self.con.execute("SET unsafe_enable_version_guessing=true;")

    def _reflect_views(self) -> None:
        """Create views for all tables in the catalog."""
        table_identifiers = self.catalog.list_tables("default")
        table_names = [tbl[1] for tbl in table_identifiers]

        # Setup the iceberg extension once before reflecting views
        self._init_duckdb()

        for table in table_names:
            full_table_name = f"{ICERUNNER_NAMESPACE}.{table}"
            table_path = f"{self.warehouse_path}/{ICERUNNER_NAMESPACE}.db/{table}"
            self.con.execute(
                f"""
                CREATE OR REPLACE VIEW {table} AS
                SELECT * FROM iceberg_scan(
                    '{table_path}', 
                    version='?',
                    allow_moved_paths=true
                )
                """
            )
            logger.info(f"Created view: {table} for table: {full_table_name}")

    @property
    def tables(self) -> List[str]:
        """Get list of all tables in the namespace."""
        return [
            tbl_id[1] for tbl_id in self.catalog.list_tables((ICERUNNER_NAMESPACE,))
        ]

    def _convert_pyarrow_to_iceberg_type(
        self, pa_type
    ) -> Union[LongType, StringType, TimestampType, BooleanType, DoubleType, FloatType]:
        """Convert PyArrow type to Iceberg type."""
        if pa.types.is_int64(pa_type):
            return LongType()
        elif pa.types.is_string(pa_type):
            return StringType()
        elif pa.types.is_timestamp(pa_type):
            return TimestampType()
        elif pa.types.is_boolean(pa_type):
            return BooleanType()
        elif pa.types.is_float64(pa_type):
            return DoubleType()
        elif pa.types.is_float32(pa_type):
            return FloatType()
        else:
            # Default fallback
            logger.warning(f"Unsupported type: {pa_type}, defaulting to StringType")
            return StringType()

    def create_table(self, table_name: str, data: pa.Table) -> bool:
        """Create a new Iceberg table from PyArrow table."""
        full_table_name = f"{ICERUNNER_NAMESPACE}.{table_name}"

        if self.catalog.table_exists(full_table_name):
            logger.info(f"Table {full_table_name} already exists")
            return True

        fields = []
        for i, field in enumerate(data.schema, 1):
            iceberg_type = self._convert_pyarrow_to_iceberg_type(field.type)
            fields.append(
                NestedField(
                    i,
                    field.name,
                    iceberg_type,
                    required=False,
                )
            )

        schema = Schema(*fields)
        iceberg_table = self.catalog.create_table(
            identifier=full_table_name,
            schema=schema,
        )

        iceberg_table.append(data)
        logger.info(f"Created table: {full_table_name}")
        return True

    def insert(self, table_name: str, data: pa.Table) -> bool:
        """Insert data into an existing Iceberg table."""
        full_table_name = f"{ICERUNNER_NAMESPACE}.{table_name}"

        try:
            iceberg_table = self.catalog.load_table(full_table_name)
            iceberg_table.refresh()

            with iceberg_table.transaction() as transaction:
                transaction.append(data)

            logger.info(f"Inserted data into {full_table_name}: {len(data)} rows")
            return True
        except Exception as e:
            logger.error(f"Error inserting data into {full_table_name}: {e}")
            raise

    def query(self, table_name: str) -> pa.Table:
        """Query all data from a table."""
        self._reflect_views()
        try:
            result = self.con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
            return result
        except Exception as e:
            logger.error(f"Error querying table {table_name}: {e}")
            raise

    def count(self, table_name: str) -> int:
        """Get count of rows in a table."""
        self._reflect_views()
        try:
            count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Error counting rows in table {table_name}: {e}")
            raise

    def sql(self, sql_command: str) -> pa.Table:
        """Execute a SQL command and return results as PyArrow table."""
        self._reflect_views()
        try:
            return self.con.execute(sql_command).fetch_arrow_table()
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            raise


class IceRunnerFlightServer(flight.FlightServerBase):
    """
    PyArrow Flight server for IceRunner.
    Provides endpoints for reading and writing data.
    """

    def __init__(self, location: flight.Location, warehouse_path: str):
        super().__init__(location)
        self.connector = IceRunnerConnector(warehouse_path)

    def do_get(self, context, ticket):
        table_name = ticket.ticket.decode("utf-8")
        if table_name not in self.connector.tables:
            raise flight.FlightUnavailableError(f"Table {table_name} not found")

        try:
            table = self.connector.query(table_name)
            return flight.RecordBatchStream(table.to_batches())
        except Exception as e:
            logger.error(f"Error in do_get for table {table_name}: {e}")
            raise flight.FlightInternalError(f"Internal error: {str(e)}")

    def do_put(self, context, descriptor, reader, writer):
        table_name = descriptor.path[0].decode("utf-8")
        try:
            batches = list(reader)
            if not batches:
                return

            combined_table = pa.Table.from_batches(batches)
            self.connector.insert(table_name, combined_table)
        except Exception as e:
            logger.error(f"Error in do_put for table {table_name}: {e}")
            raise flight.FlightInternalError(f"Internal error: {str(e)}")

    def get_flight_info(self, context, descriptor):
        table_name = descriptor.path[0].decode("utf-8")
        if table_name not in self.connector.tables:
            raise flight.FlightUnavailableError(f"Table {table_name} not found")

        try:
            location = flight.Location.for_grpc_tcp("localhost", ICERUNNER_PORT)
            endpoints = [flight.FlightEndpoint(table_name.encode("utf-8"), [location])]

            schema = self.connector.query(table_name).schema
            return flight.FlightInfo(schema, descriptor, endpoints, -1, -1)
        except Exception as e:
            logger.error(f"Error in get_flight_info for table {table_name}: {e}")
            raise flight.FlightInternalError(f"Internal error: {str(e)}")


def create_sample_table(schema=None):
    """Create a sample table with default or custom schema."""
    if schema is None:
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        )

    return pa.Table.from_pylist(
        [
            {"id": 1, "value": "sample_value_1"},
            {"id": 2, "value": "sample_value_2"},
        ],
        schema=schema,
    )


def run_server(warehouse_path: str, table_name: str, port: int):
    """Run the IceRunner Flight server."""
    try:
        location = flight.Location.for_grpc_tcp("localhost", port)
        server = IceRunnerFlightServer(location, warehouse_path)

        # Create initial table if it doesn't exist
        sample_table = create_sample_table()

        if table_name not in server.connector.tables:
            server.connector.create_table(table_name, sample_table)

        # Start the server in a new thread
        server_thread = threading.Thread(target=server.serve)
        server_thread.daemon = True
        server_thread.start()

        logger.info(f"Flight server started at grpc://localhost:{port}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down server...")
            server.shutdown()
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        raise


def run_reader(table_name: str, port: int, interval: int = 1):
    """Run a client that reads data from the IceRunner server."""
    client = flight.connect(f"grpc://localhost:{port}")
    logger.info(
        f"Reader connected to grpc://localhost:{port}, reading from {table_name}"
    )

    while True:
        try:
            flight_info = client.get_flight_info(
                flight.FlightDescriptor.for_path(table_name.encode())
            )
            endpoint = flight_info.endpoints[0]
            reader = client.do_get(endpoint.ticket)
            table = reader.read_all()
            count = len(table)
            logger.info(f"Current count: {count}")
        except Exception as e:
            logger.error(f"Error reading data: {e}")

        time.sleep(interval)


def run_writer(table_name: str, port: int, interval: int = 1):
    """Run a client that writes data to the IceRunner server."""
    client = flight.connect(f"grpc://localhost:{port}")
    logger.info(f"Writer connected to grpc://localhost:{port}, writing to {table_name}")

    while True:
        try:
            data = pa.Table.from_pylist(
                [{"id": int(time.time()), "value": f"val-{random.randint(100, 999)}"}],
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("value", pa.string(), nullable=False),
                    ]
                ),
            )

            writer, _ = client.do_put(
                flight.FlightDescriptor.for_path(table_name.encode()),
                data.schema,
            )
            writer.write_table(data)
            writer.close()

            logger.info(f"Uploaded data: {data.to_pydict()}")
        except Exception as e:
            logger.error(f"Error writing data: {e}")

        time.sleep(interval)


def main():
    """Main entry point for the IceRunner application."""
    import argparse

    parser = argparse.ArgumentParser(
        description="IceRunner - Iceberg PyArrow Flight Server"
    )
    parser.add_argument(
        "command",
        choices=("serve", "read", "write"),
        help="Command to run (serve, read, or write)",
    )
    parser.add_argument(
        "-w",
        "--warehouse-path",
        default=ICERUNNER_WAREHOUSE_PATH,
        help=f"Path to the warehouse (default: {ICERUNNER_WAREHOUSE_PATH})",
    )
    parser.add_argument(
        "-p",
        "--port",
        default=ICERUNNER_PORT,
        type=int,
        help=f"Port to use for the Flight server (default: {ICERUNNER_PORT})",
    )
    parser.add_argument(
        "-n",
        "--table-name",
        default=ICERUNNER_TABLE_NAME,
        help=f"Table name to use (default: {ICERUNNER_TABLE_NAME})",
    )
    parser.add_argument(
        "-i",
        "--interval",
        default=1,
        type=int,
        help="Interval in seconds between operations (default: 1)",
    )

    args = parser.parse_args()

    try:
        if args.command == "serve":
            run_server(args.warehouse_path, args.table_name, args.port)
        elif args.command == "read":
            run_reader(args.table_name, args.port, args.interval)
        elif args.command == "write":
            run_writer(args.table_name, args.port, args.interval)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
