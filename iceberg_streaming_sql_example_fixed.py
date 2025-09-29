# This file was generated from iceberg_streaming_sql_example.ipynb

# !pip install --quiet apache-beam[gcp] jupyter

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms.sql import SqlTransform
import subprocess
import argparse
import sys

class IcebergPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--iceberg_table", required=True, help="The Iceberg table to write to.")
        parser.add_argument("--iceberg_database", required=True, help="The Iceberg database to write to.")
        parser.add_argument("--catalog_name", default="taxi_rides", help="The Iceberg catalog name.")
        parser.add_argument("--catalog_uri", default="https://biglake.googleapis.com/iceberg/v1beta/restcatalog", help="The URI for the REST catalog.")
        parser.add_argument("--warehouse", required=True, help="GCS location for the Iceberg warehouse.")

def run_pipeline(args):
    options = PipelineOptions(args, streaming=True)
    iceberg_options = options.view_as(IcebergPipelineOptions)
    gcp_options = options.view_as(GoogleCloudOptions)

    # Get gcloud token for authentication
    print("Getting GCP access token...")
    token = subprocess.check_output(["gcloud", "auth", "application-default", "print-access-token"], text=True).strip()

    catalog_name = iceberg_options.catalog_name
    database_name = iceberg_options.iceberg_database
    table_name = iceberg_options.iceberg_table

    full_database_name = f"{catalog_name}.{database_name}"
    full_table_name = f"{full_database_name}.{table_name}"
    full_taxirides_table_name = f"{full_database_name}.taxirides"

    ddl_statements = [
        f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        TYPE iceberg
        PROPERTIES (
            'type'='rest',
            'uri'='{iceberg_options.catalog_uri}',
            'warehouse'='{iceberg_options.warehouse}',
            'header.x-goog-user-project'='{gcp_options.project}',
            'oauth2-server-uri'='https://oauth2.googleapis.com/token',
            'token'='{token}',
            'rest-metrics-reporting-enabled'='false'
        )
        """,
        f"CREATE DATABASE IF NOT EXISTS {full_database_name}",
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {full_taxirides_table_name} (
          ride_id VARCHAR,
          ride_status VARCHAR,
          `event_timestamp` TIMESTAMP,
          passenger_count BIGINT,
          meter_reading DOUBLE
        )
        TYPE 'pubsub'
        LOCATION 'projects/pubsub-public-data/topics/taxirides-realtime'
        """,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {full_table_name} (
          ride_minute TIMESTAMP,
          total_rides BIGINT,
          revenue DOUBLE,
          passenger_count BIGINT
        )
        TYPE 'iceberg'
        LOCATION '{full_database_name}.{table_name}'
        TBLPROPERTIES '{{ "triggering_frequency_seconds" : 60 }}'
        """
    ]

    query = f"""
        INSERT INTO {full_table_name}
        SELECT
          TUMBLE_START(`event_timestamp`, INTERVAL '60' SECOND) AS ride_minute,
          COUNT(*) AS total_rides,
          SUM(meter_reading) AS revenue,
          SUM(passenger_count) AS passenger_count
        FROM {full_taxirides_table_name}
        WHERE ride_status = 'dropoff'
        GROUP BY TUMBLE(`event_timestamp`, INTERVAL '60' SECOND)
    """

    print("Starting Beam pipeline...")
    with beam.Pipeline(options=options) as p:
        # Chain the DDL statements
        # Each DDL statement is executed in its own SqlTransform.
        # The first transform is applied to the pipeline `p`.
        # Subsequent transforms are applied to the output of the previous one,
        # effectively creating a chain.
        (p 
         | 'CreateCatalog' >> SqlTransform(query="", ddl=ddl_statements[0])
         | 'CreateDatabase' >> SqlTransform(query="", ddl=ddl_statements[1])
         | 'CreatePubSubTable' >> SqlTransform(query="", ddl=ddl_statements[2])
         | 'CreateIcebergTable' >> SqlTransform(query="", ddl=ddl_statements[3])
         | 'ExecuteQuery' >> SqlTransform(query=query)
        )
    print("Pipeline finished.")

if __name__ == '__main__':
    # --- REPLACE WITH YOUR VALUES ---
    project_id = "apache-beam-testing"
    gcs_warehouse = "gs://biglake_taxi_ride_metrics"
    database_name = "taxi_dataset"
    table_name = "ride_metrics_by_minute"
    beam_runner = "DirectRunner" # Or 'DataflowRunner'
    gcp_region = "us-central1" # Required for DataflowRunner
    temp_location = "gs://taxi_rides_test/"
    # --------------------------------

    pipeline_args = [
        f"--project={project_id}",
        f"--region={gcp_region}",
        f"--runner={beam_runner}",
        f"--job_name=iceberg-sql-streaming-script",
        f"--warehouse={gcs_warehouse}",
        f"--iceberg_database={database_name}",
        f"--iceberg_table={table_name}",
        f"--temp_location={temp_location}/temp",
        f"--staging_location={temp_location}/staging",
        "--jars=sdks/java/extensions/sql/iceberg/build/libs/beam-sdks-java-extensions-sql-iceberg-2.69.0-SNAPSHOT.jar,sdks/java/extensions/sql/build/libs/beam-sdks-java-extensions-sql-2.69.0-SNAPSHOT.jar,sdks/java/io/iceberg/build/libs/beam-sdks-java-io-iceberg-2.69.0-SNAPSHOT.jar,sdks/java/io/iceberg/bqms/build/libs/beam-sdks-java-io-iceberg-bqms-2.69.0-SNAPSHOT.jar,sdks/java/io/iceberg/hive/build/libs/beam-sdks-java-io-iceberg-hive-2.69.0-SNAPSHOT.jar"
    ] + sys.argv[1:]

    print(f"Running pipeline with args: {pipeline_args}")
    run_pipeline(pipeline_args)
