import os
import random
import time
import socket
import threading
import logging
import argparse
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from urllib.parse import urlparse
import json
import hashlib
import uuid
import ipaddress
import faker

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
ICERUNNER_DEFAULT_BATCH_SIZE = (
    1000  # Number of rows to process at once during replication
)


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

    def get_current_snapshot_id(self, table_name: str) -> Optional[int]:
        """Get the current snapshot ID for a table if it exists."""
        full_table_name = f"{ICERUNNER_NAMESPACE}.{table_name}"

        try:
            if self.catalog.table_exists(full_table_name):
                iceberg_table = self.catalog.load_table(full_table_name)
                current_snapshot = iceberg_table.current_snapshot()
                if current_snapshot:
                    return current_snapshot.snapshot_id
            return None
        except Exception as e:
            logger.error(f"Error getting snapshot ID for {table_name}: {e}")
            return None

    def get_changes_since_snapshot(
        self, table_name: str, snapshot_id: int
    ) -> Optional[pa.Table]:
        """Get changes since the specified snapshot ID."""
        full_table_name = f"{ICERUNNER_NAMESPACE}.{table_name}"

        try:
            if not self.catalog.table_exists(full_table_name):
                return None

            iceberg_table = self.catalog.load_table(full_table_name)
            current_snapshot = iceberg_table.current_snapshot()

            if not current_snapshot or current_snapshot.snapshot_id == snapshot_id:
                # No changes since the specified snapshot
                return None

            # Get the changes between snapshots using DuckDB's iceberg_snapshots function
            self._reflect_views()
            changes = self.con.execute(
                f"""
                WITH snapshot_data AS (
                    SELECT * FROM iceberg_snapshots('{self.warehouse_path}/{ICERUNNER_NAMESPACE}.db/{table_name}')
                )
                SELECT s.* 
                FROM {table_name} s
                JOIN snapshot_data sd ON sd.snapshot_id > {snapshot_id} AND sd.snapshot_id <= {current_snapshot.snapshot_id}
            """
            ).fetch_arrow_table()

            return changes
        except Exception as e:
            logger.error(
                f"Error getting changes for {table_name} since snapshot {snapshot_id}: {e}"
            )
            return None


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


def create_sample_table(schema=None, num_rows=100, data_profile="analytics"):
    """
    Create a sample table with realistic data.

    Args:
        schema: Optional custom schema. If None, will create schema based on data_profile.
        num_rows: Number of rows to generate.
        data_profile: Type of data to generate ("analytics", "events", "sales", "iot").

    Returns:
        PyArrow Table with sample data
    """
    # Initialize faker for generating realistic data
    fake = faker.Faker()

    if schema is None:
        if data_profile == "analytics":
            # Web analytics data profile
            schema = pa.schema(
                [
                    pa.field("visitor_id", pa.string(), nullable=False),
                    pa.field("session_id", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("page_url", pa.string(), nullable=False),
                    pa.field("referrer", pa.string()),
                    pa.field("user_agent", pa.string()),
                    pa.field("device_type", pa.string()),
                    pa.field("country", pa.string()),
                    pa.field("city", pa.string()),
                    pa.field("browser", pa.string()),
                    pa.field("os", pa.string()),
                    pa.field("duration_seconds", pa.int32()),
                    pa.field("page_views", pa.int16()),
                    pa.field("conversion", pa.bool_()),
                ]
            )

            # Generate data
            data = []
            for _ in range(num_rows):
                visitor_id = str(uuid.uuid4())
                timestamp = fake.date_time_between(start_date="-30d", end_date="now")

                data.append(
                    {
                        "visitor_id": visitor_id,
                        "session_id": f"{visitor_id}_{int(timestamp.timestamp())}",
                        "timestamp": timestamp,
                        "page_url": fake.uri_path(),
                        "referrer": fake.uri() if random.random() > 0.3 else None,
                        "user_agent": fake.user_agent(),
                        "device_type": random.choice(["desktop", "mobile", "tablet"]),
                        "country": fake.country(),
                        "city": fake.city(),
                        "browser": random.choice(
                            ["Chrome", "Firefox", "Safari", "Edge"]
                        ),
                        "os": random.choice(
                            ["Windows", "MacOS", "Linux", "iOS", "Android"]
                        ),
                        "duration_seconds": random.randint(5, 1800),
                        "page_views": random.randint(1, 20),
                        "conversion": random.random() > 0.9,
                    }
                )

        elif data_profile == "events":
            # Event log data profile
            schema = pa.schema(
                [
                    pa.field("event_id", pa.string(), nullable=False),
                    pa.field("event_type", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("user_id", pa.string()),
                    pa.field("device_id", pa.string()),
                    pa.field("ip_address", pa.string()),
                    pa.field("severity", pa.string()),
                    pa.field("component", pa.string()),
                    pa.field("message", pa.string()),
                    pa.field("properties", pa.string()),
                    pa.field("duration_ms", pa.int64()),
                    pa.field("status_code", pa.int16()),
                ]
            )

            # Generate data
            data = []
            event_types = [
                "page_view",
                "click",
                "form_submit",
                "api_call",
                "error",
                "login",
                "logout",
            ]
            components = [
                "frontend",
                "backend",
                "database",
                "auth",
                "api",
                "cache",
                "scheduler",
            ]
            severities = ["debug", "info", "warning", "error", "critical"]

            for _ in range(num_rows):
                event_type = random.choice(event_types)
                timestamp = fake.date_time_between(start_date="-7d", end_date="now")
                duration = (
                    random.randint(1, 5000)
                    if event_type in ["api_call", "form_submit"]
                    else None
                )
                status = (
                    random.choice([200, 201, 400, 404, 500])
                    if event_type == "api_call"
                    else None
                )

                data.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_type": event_type,
                        "timestamp": timestamp,
                        "user_id": str(uuid.uuid4()) if random.random() > 0.2 else None,
                        "device_id": (
                            fake.mac_address() if random.random() > 0.3 else None
                        ),
                        "ip_address": str(
                            ipaddress.IPv4Address(random.randint(0, 2**32 - 1))
                        ),
                        "severity": random.choice(severities),
                        "component": random.choice(components),
                        "message": fake.sentence(),
                        "properties": (
                            json.dumps({"key1": fake.word(), "key2": fake.word()})
                            if random.random() > 0.5
                            else None
                        ),
                        "duration_ms": duration,
                        "status_code": status,
                    }
                )

        elif data_profile == "sales":
            # Sales data profile
            schema = pa.schema(
                [
                    pa.field("order_id", pa.string(), nullable=False),
                    pa.field("customer_id", pa.string(), nullable=False),
                    pa.field("transaction_date", pa.timestamp("ms"), nullable=False),
                    pa.field("product_id", pa.string(), nullable=False),
                    pa.field("product_name", pa.string(), nullable=False),
                    pa.field("category", pa.string()),
                    pa.field("quantity", pa.int16(), nullable=False),
                    pa.field("unit_price", pa.float64(), nullable=False),
                    pa.field("total_amount", pa.float64(), nullable=False),
                    pa.field("payment_method", pa.string()),
                    pa.field("store_id", pa.string()),
                    pa.field("salesperson", pa.string()),
                    pa.field("promotion_code", pa.string()),
                    pa.field("is_returned", pa.bool_()),
                ]
            )

            # Generate data
            data = []
            products = [
                {
                    "id": "P001",
                    "name": "Laptop",
                    "category": "Electronics",
                    "price": 1299.99,
                },
                {
                    "id": "P002",
                    "name": "Smartphone",
                    "category": "Electronics",
                    "price": 899.99,
                },
                {
                    "id": "P003",
                    "name": "Coffee Maker",
                    "category": "Appliances",
                    "price": 79.99,
                },
                {
                    "id": "P004",
                    "name": "Running Shoes",
                    "category": "Footwear",
                    "price": 129.99,
                },
                {
                    "id": "P005",
                    "name": "Office Chair",
                    "category": "Furniture",
                    "price": 249.99,
                },
                {
                    "id": "P006",
                    "name": "Headphones",
                    "category": "Electronics",
                    "price": 199.99,
                },
                {
                    "id": "P007",
                    "name": "Backpack",
                    "category": "Accessories",
                    "price": 59.99,
                },
                {
                    "id": "P008",
                    "name": "Desk Lamp",
                    "category": "Lighting",
                    "price": 39.99,
                },
            ]

            payment_methods = [
                "Credit Card",
                "Debit Card",
                "PayPal",
                "Cash",
                "Bank Transfer",
            ]
            store_ids = ["S001", "S002", "S003", "S004", "S005"]

            # Generate 20 customers
            customers = [str(uuid.uuid4()) for _ in range(20)]

            for _ in range(num_rows):
                product = random.choice(products)
                quantity = random.randint(1, 5)
                total = round(product["price"] * quantity, 2)
                customer = random.choice(customers)
                transaction_date = fake.date_time_between(
                    start_date="-90d", end_date="now"
                )

                data.append(
                    {
                        "order_id": str(uuid.uuid4()),
                        "customer_id": customer,
                        "transaction_date": transaction_date,
                        "product_id": product["id"],
                        "product_name": product["name"],
                        "category": product["category"],
                        "quantity": quantity,
                        "unit_price": product["price"],
                        "total_amount": total,
                        "payment_method": random.choice(payment_methods),
                        "store_id": random.choice(store_ids),
                        "salesperson": fake.name(),
                        "promotion_code": (
                            f"PROMO{random.randint(10, 99)}"
                            if random.random() > 0.7
                            else None
                        ),
                        "is_returned": random.random() < 0.05,
                    }
                )

        elif data_profile == "iot":
            # IoT sensor data profile
            schema = pa.schema(
                [
                    pa.field("reading_id", pa.string(), nullable=False),
                    pa.field("device_id", pa.string(), nullable=False),
                    pa.field("sensor_type", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("value", pa.float64(), nullable=False),
                    pa.field("unit", pa.string()),
                    pa.field("latitude", pa.float64()),
                    pa.field("longitude", pa.float64()),
                    pa.field("battery_level", pa.float32()),
                    pa.field("signal_strength", pa.int8()),
                    pa.field("alert_triggered", pa.bool_()),
                    pa.field("firmware_version", pa.string()),
                ]
            )

            # Generate data
            data = []

            sensor_types = [
                {"type": "temperature", "unit": "celsius", "min": -10, "max": 50},
                {"type": "humidity", "unit": "percent", "min": 0, "max": 100},
                {"type": "pressure", "unit": "hPa", "min": 900, "max": 1100},
                {"type": "air_quality", "unit": "ppm", "min": 0, "max": 500},
                {"type": "light", "unit": "lux", "min": 0, "max": 10000},
                {"type": "noise", "unit": "dB", "min": 20, "max": 120},
            ]

            # Generate 30 devices
            devices = [
                f"IOT-{fake.lexify('???')}-{fake.numerify('####')}" for _ in range(30)
            ]
            firmware_versions = ["v1.0.0", "v1.1.2", "v1.2.0", "v2.0.1", "v2.1.0"]

            for _ in range(num_rows):
                device_id = random.choice(devices)
                sensor = random.choice(sensor_types)
                timestamp = fake.date_time_between(start_date="-3d", end_date="now")
                value = random.uniform(sensor["min"], sensor["max"])
                # Some sensors trigger alerts on certain thresholds
                alert = False
                if sensor["type"] == "temperature" and (value > 40 or value < 0):
                    alert = True
                elif sensor["type"] == "air_quality" and value > 300:
                    alert = True

                data.append(
                    {
                        "reading_id": str(uuid.uuid4()),
                        "device_id": device_id,
                        "sensor_type": sensor["type"],
                        "timestamp": timestamp,
                        "value": round(value, 2),
                        "unit": sensor["unit"],
                        "latitude": float(fake.latitude()),
                        "longitude": float(fake.longitude()),
                        "battery_level": round(random.uniform(0, 100), 1),
                        "signal_strength": random.randint(-120, -30),
                        "alert_triggered": alert,
                        "firmware_version": random.choice(firmware_versions),
                    }
                )
        else:
            # Default simple schema if no profile matches
            schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("value", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("is_active", pa.bool_()),
                ]
            )

            data = []
            for i in range(num_rows):
                data.append(
                    {
                        "id": i + 1,
                        "value": f"sample_value_{i+1}",
                        "timestamp": datetime.now()
                        - timedelta(hours=random.randint(0, 24 * 7)),
                        "is_active": random.choice([True, False]),
                    }
                )

    # Use the provided schema if one was passed in
    return pa.Table.from_pylist(data, schema=schema)


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


def parse_flight_url(url: str) -> Tuple[str, int]:
    """Parse a Flight URL into host and port components."""
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 8815  # Default Flight port if not specified
    return host, port


def get_remote_tables(client: flight.FlightClient) -> List[str]:
    """Get list of all tables available on a remote Flight server."""
    try:
        # Try to list tables using Flight GetFlightInfo
        flight_info = client.get_flight_info(
            flight.FlightDescriptor.for_command(b"LIST_TABLES")
        )
        reader = client.do_get(flight_info.endpoints[0].ticket)
        table = reader.read_all()
        return table["table_name"].to_pylist()
    except Exception as e:
        logger.error(f"Error getting tables from remote server: {e}")
        # If that fails, try to get a listing from a simple Flight path listing
        try:
            flight_info = client.list_flights()
            tables = []
            for info in flight_info:
                if hasattr(info.descriptor, "path") and info.descriptor.path:
                    tables.append(info.descriptor.path[0].decode("utf-8"))
            return tables
        except Exception as e:
            logger.error(f"Unable to determine tables from remote server: {e}")
            return []


class SyncState:
    """Class to track sync state between source and target tables."""

    def __init__(self, warehouse_path: str):
        self.warehouse_path = Path(warehouse_path).absolute()
        self.state_dir = self.warehouse_path / "sync_state"
        os.makedirs(self.state_dir, exist_ok=True)

    def get_state_path(self, source_url: str, target_table: str) -> Path:
        """Get the path to the state file for a specific sync pair."""
        # Create a deterministic filename from the source and target
        source_hash = hashlib.md5(source_url.encode()).hexdigest()[:8]
        return self.state_dir / f"{source_hash}_{target_table}.json"

    def get_last_sync_state(self, source_url: str, target_table: str) -> dict:
        """Get the last sync state for a source-target pair."""
        state_path = self.get_state_path(source_url, target_table)
        if state_path.exists():
            try:
                with open(state_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error reading sync state: {e}")

        # Return default state if no state file exists or on error
        return {
            "last_sync_time": None,
            "source_snapshot_id": None,
            "target_snapshot_id": None,
            "rows_synced": 0,
            "last_sync_status": "never_run",
        }

    def save_sync_state(self, source_url: str, target_table: str, state: dict) -> None:
        """Save the sync state for a source-target pair."""
        state_path = self.get_state_path(source_url, target_table)
        try:
            with open(state_path, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving sync state: {e}")


def run_mirror(
    source_url: str,
    target_table: str,
    warehouse_path: str,
    target_port: int = ICERUNNER_PORT,
    interval: int = 60,
    batch_size: int = ICERUNNER_DEFAULT_BATCH_SIZE,
    continuous: bool = True,
):
    """
    Mirror data from a remote Flight server to a local Iceberg table.

    Args:
        source_url: URL of the source Flight server (e.g., grpc://hostname:port)
        target_table: Name of the target table to write to
        warehouse_path: Path to the local warehouse directory
        target_port: Port for the local Flight server
        interval: Interval in seconds between sync operations (only used if continuous=True)
        batch_size: Number of rows to process in each batch
        continuous: Whether to run in continuous sync mode
    """
    # Connect to source Flight server
    logger.info(f"Connecting to source Flight server at {source_url}")
    source_client = flight.connect(source_url)

    # Parse the source URL to get hostname/port
    source_host, source_port = parse_flight_url(source_url)

    # Get the source table(s) if not specified in the URL
    source_path = urlparse(source_url).path
    if source_path and source_path != "/":
        # Extract table name from path if present
        source_table = source_path.strip("/")
    else:
        # Try to get available tables from the server
        available_tables = get_remote_tables(source_client)
        if not available_tables:
            logger.error("No source table specified and couldn't discover any tables")
            return
        source_table = available_tables[0]
        logger.info(
            f"No specific table requested, using discovered table: {source_table}"
        )

    # Set up local connector for target table
    connector = IceRunnerConnector(warehouse_path)

    # Set up sync state tracking
    sync_state = SyncState(warehouse_path)

    # Initialize counters for stats
    total_rows_synced = 0
    start_time = time.time()
    sync_count = 0

    def perform_sync():
        """Perform a single sync operation"""
        nonlocal total_rows_synced, sync_count

        sync_start = time.time()
        logger.info(
            f"Starting sync #{sync_count+1} from {source_url}/{source_table} to local table {target_table}"
        )

        # Get last sync state
        last_state = sync_state.get_last_sync_state(source_url, target_table)
        last_source_snapshot_id = last_state.get("source_snapshot_id")

        try:
            # Get data from source Flight server
            flight_info = source_client.get_flight_info(
                flight.FlightDescriptor.for_path(source_table.encode())
            )
            if not flight_info.endpoints:
                logger.error(f"No endpoints found for table {source_table}")
                return

            endpoint = flight_info.endpoints[0]

            # If we need to connect to a different endpoint server
            if endpoint.locations:
                location = endpoint.locations[0]
                if location.uri != source_url:
                    logger.info(f"Following endpoint location to {location.uri}")
                    endpoint_client = flight.connect(location.uri)
                else:
                    endpoint_client = source_client
            else:
                endpoint_client = source_client

            # Check if target table exists and create it if needed
            table_exists = target_table in connector.tables

            if not table_exists:
                # For new tables, we need to create it with the source schema
                # Get the first batch to determine schema
                try:
                    # Try to get schema information first, if the server supports it
                    cmd = {"command": "get_schema", "table": source_table}
                    schema_info = endpoint_client.get_flight_info(
                        flight.FlightDescriptor.for_command(json.dumps(cmd).encode())
                    )
                    schema_reader = endpoint_client.do_get(
                        schema_info.endpoints[0].ticket
                    )
                    schema_batch = schema_reader.read_next_batch()
                    sample_table = pa.Table.from_batches([schema_batch])
                except Exception as e:
                    logger.error(f"Error getting schema: {e}")
                    # Fallback: get actual data to determine schema
                    reader = endpoint_client.do_get(endpoint.ticket)
                    batch = reader.read_next_batch()
                    if batch is None:
                        logger.warning("Source table is empty, nothing to sync")
                        return
                    sample_table = pa.Table.from_batches([batch])

                # Create the target table with the source schema
                connector.create_table(target_table, sample_table)
                logger.info(
                    f"Created target table {target_table} with schema from source"
                )

                # Now do a full data sync since it's a new table
                reader = endpoint_client.do_get(endpoint.ticket)
                rows_synced = process_batches(
                    reader, connector, target_table, batch_size
                )
                total_rows_synced += rows_synced

                # Save the state after successful sync
                current_target_snapshot_id = connector.get_current_snapshot_id(
                    target_table
                )
                new_state = {
                    "last_sync_time": datetime.now().isoformat(),
                    "source_snapshot_id": "full_sync",  # We don't know the source snapshot ID yet
                    "target_snapshot_id": current_target_snapshot_id,
                    "rows_synced": rows_synced,
                    "last_sync_status": "success",
                }
                sync_state.save_sync_state(source_url, target_table, new_state)

                logger.info(
                    f"Initial sync completed: {rows_synced} rows synced to new table {target_table}"
                )
            else:
                # Table exists, we need to determine what records to sync
                if last_source_snapshot_id:
                    # Try to use snapshot-based incremental sync if possible
                    try:
                        # Query for changes since last sync using snapshot metadata
                        cmd = {
                            "command": "get_changes",
                            "table": source_table,
                            "snapshot_id": last_source_snapshot_id,
                        }
                        changes_info = endpoint_client.get_flight_info(
                            flight.FlightDescriptor.for_command(
                                json.dumps(cmd).encode()
                            )
                        )

                        if changes_info.endpoints:
                            # Server supports incremental sync
                            logger.info(
                                f"Performing incremental sync since snapshot {last_source_snapshot_id}"
                            )
                            changes_reader = endpoint_client.do_get(
                                changes_info.endpoints[0].ticket
                            )
                            rows_synced = process_batches(
                                changes_reader, connector, target_table, batch_size
                            )

                            # Get current source snapshot ID
                            source_metadata_cmd = {
                                "command": "get_metadata",
                                "table": source_table,
                            }
                            metadata_info = endpoint_client.get_flight_info(
                                flight.FlightDescriptor.for_command(
                                    json.dumps(source_metadata_cmd).encode()
                                )
                            )
                            metadata_reader = endpoint_client.do_get(
                                metadata_info.endpoints[0].ticket
                            )
                            metadata = metadata_reader.read_all().to_pydict()
                            current_source_snapshot_id = metadata.get(
                                "snapshot_id", "unknown"
                            )

                            logger.info(
                                f"Incremental sync completed: {rows_synced} rows synced"
                            )
                        else:
                            # Server doesn't support incremental sync, fall back to full sync
                            logger.info(
                                "Server doesn't support incremental sync, falling back to full sync"
                            )
                            reader = endpoint_client.do_get(endpoint.ticket)
                            rows_synced = process_batches(
                                reader, connector, target_table, batch_size
                            )
                            current_source_snapshot_id = "full_sync"

                            logger.info(
                                f"Full sync completed: {rows_synced} rows synced"
                            )
                    except Exception as e:
                        logger.error(
                            f"Error during incremental sync: {e}, falling back to full sync"
                        )
                        reader = endpoint_client.do_get(endpoint.ticket)
                        rows_synced = process_batches(
                            reader, connector, target_table, batch_size
                        )
                        current_source_snapshot_id = "full_sync"

                        logger.info(f"Full sync completed: {rows_synced} rows synced")
                else:
                    # No previous sync state, do a full sync
                    logger.info("No previous sync state found, performing full sync")
                    reader = endpoint_client.do_get(endpoint.ticket)
                    rows_synced = process_batches(
                        reader, connector, target_table, batch_size
                    )
                    current_source_snapshot_id = "full_sync"

                    logger.info(f"Full sync completed: {rows_synced} rows synced")

                # Update the total rows synced
                total_rows_synced += rows_synced

                # Save the state after successful sync
                current_target_snapshot_id = connector.get_current_snapshot_id(
                    target_table
                )
                new_state = {
                    "last_sync_time": datetime.now().isoformat(),
                    "source_snapshot_id": current_source_snapshot_id,
                    "target_snapshot_id": current_target_snapshot_id,
                    "rows_synced": rows_synced,
                    "last_sync_status": "success",
                }
                sync_state.save_sync_state(source_url, target_table, new_state)

            sync_count += 1
            sync_duration = time.time() - sync_start
            logger.info(f"Sync #{sync_count} completed in {sync_duration:.2f} seconds")

        except Exception as e:
            logger.error(f"Error during sync: {e}")

            # Save error state
            error_state = sync_state.get_last_sync_state(source_url, target_table)
            error_state["last_sync_time"] = datetime.now().isoformat()
            error_state["last_sync_status"] = f"error: {str(e)}"
            sync_state.save_sync_state(source_url, target_table, error_state)

    def process_batches(reader, connector, table_name, batch_size):
        """Process batches from a reader and insert them into a table."""
        batches = []
        rows_in_batch = 0
        batch_count = 0
        total_rows = 0

        for batch in reader:
            batches.append(batch)
            rows_in_batch += len(batch)

            if len(batches) >= batch_size:
                batch_table = pa.Table.from_batches(batches)
                connector.insert(table_name, batch_table)
                total_rows += rows_in_batch
                logger.info(f"Inserted batch {batch_count}: {rows_in_batch} rows")
                batches = []
                rows_in_batch = 0
                batch_count += 1

        # Insert any remaining batches
        if batches:
            batch_table = pa.Table.from_batches(batches)
            connector.insert(table_name, batch_table)
            total_rows += rows_in_batch
            logger.info(f"Inserted final batch {batch_count}: {rows_in_batch} rows")

        return total_rows

    # Perform initial sync
    perform_sync()

    # If continuous mode, keep syncing at the specified interval
    if continuous:
        logger.info(f"Continuous sync enabled, will sync every {interval} seconds")
        while True:
            time.sleep(interval)
            perform_sync()
            # Log statistics
            elapsed = time.time() - start_time
            logger.info(
                f"Summary: {total_rows_synced} rows synced in {sync_count} syncs over {elapsed:.2f} seconds"
            )
    else:
        logger.info("One-time sync completed")
        elapsed = time.time() - start_time
        logger.info(
            f"Summary: {total_rows_synced} rows synced in {elapsed:.2f} seconds"
        )


def main():
    """Main entry point for the IceRunner application."""
    parser = argparse.ArgumentParser(
        description="IceRunner - Iceberg PyArrow Flight Server"
    )
    parser.add_argument(
        "command",
        choices=("serve", "read", "write", "mirror"),
        help="Command to run (serve, read, write, or mirror)",
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
    parser.add_argument(
        "-s",
        "--source",
        help="Source Flight server URL for mirror mode (e.g., grpc://hostname:port/table)",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=ICERUNNER_DEFAULT_BATCH_SIZE,
        help=f"Number of rows to process in each batch (default: {ICERUNNER_DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--one-time",
        action="store_true",
        help="Perform a one-time sync rather than continuous (for mirror mode)",
    )

    args = parser.parse_args()

    try:
        if args.command == "serve":
            run_server(args.warehouse_path, args.table_name, args.port)
        elif args.command == "read":
            run_reader(args.table_name, args.port, args.interval)
        elif args.command == "write":
            run_writer(args.table_name, args.port, args.interval)
        elif args.command == "mirror":
            if not args.source:
                parser.error(
                    "Mirror mode requires --source parameter with Flight server URL"
                )
            run_mirror(
                source_url=args.source,
                target_table=args.table_name,
                warehouse_path=args.warehouse_path,
                target_port=args.port,
                interval=args.interval,
                batch_size=args.batch_size,
                continuous=not args.one_time,
            )
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
