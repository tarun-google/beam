#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Automated Incident Analyst Demo.

This pipeline demonstrates a high-value use case:
1.  Ingest high-volume log stream.
2.  Aggregate errors by Service using Beam Windows.
3.  Trigger an "SRE Detective Agent" only when error threshold is breached.
4.  Agent performs RCA using multiple tools (Health, Deploys, Logs).
"""

import argparse
import logging
import random
import time
import typing

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.ml.agents.transforms import RunAgent

from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.genai import types

# --- Domain Objects ---
class LogEntry(typing.NamedTuple):
    timestamp: float
    service: str
    level: str
    message: str

class IncidentReport(typing.NamedTuple):
    service: str
    error_count: int
    window_start: float
    window_end: float

# --- Mock Data Source ---
class GenerateLogs(beam.DoFn):
    """Generates a stream of logs. 
    Simulates a 'PaymentService' outage starting at t=5."""
    
    def process(self, element):
        services = ["AuthService", "PaymentService", "Frontend"]
        start_time = time.time()
        
        # Emit 200 logs
        for i in range(200):
            timestamp = start_time + (i * 0.1)  # 10 logs/sec simulated
            service = random.choice(services)
            level = "INFO"
            msg = "Healthy"
            
            # Simulate Outage for PaymentService
            if service == "PaymentService" and i > 50:
                level = "ERROR"
                msg = "503 Service Unavailable: Connection Refused"

            # Simulate Multi-Stage Incident for ImageProcessingService (Memory Leak)
            # T=8s (i=80) -> Warning
            if 80 <= i < 100:
                service = "ImageProcessingService"
                level = "ERROR"
                msg = "WARNING: Memory usage at 95% (Risk of OOM)"
            # T=12s (i=120) -> Crash
            elif 120 <= i < 140:
                service = "ImageProcessingService"
                level = "ERROR"
                msg = "CRITICAL: Process crashed: OOMKilled"
            
            yield beam.window.TimestampedValue(
                LogEntry(timestamp, service, level, msg),
                timestamp
            )

# --- SRE Tools ---
def get_service_health(service_name: str) -> dict:
    """Checks CPU/Memory usage."""
    if service_name == "PaymentService":
        return {"cpu": "15%", "memory": "40%", "status": "nominal"}
    if service_name == "ImageProcessingService":
        return {"cpu": "80%", "memory": "99%", "status": "critical"}
    return {"cpu": "20%", "status": "nominal"}

def get_recent_deployments(service_name: str) -> dict:
    """Checks for recent deployments."""
    if service_name == "PaymentService":
        return {"last_deploy": "v2.4.0", "time": "5 minutes ago", "author": "tarun"}
    if service_name == "ImageProcessingService":
        return {"last_deploy": "v1.5.0", "time": "1 hour ago", "author": "devops"}
    return {"last_deploy": "v1.2.0", "time": "2 days ago"}

def query_error_logs(service_name: str, limit: int = 3) -> list:
    """Fetches recent error logs."""
    if service_name == "PaymentService":
        return [
            "ConnectionRefusedError: Failed to connect to DB-Shard-02",
            "Retry exhausted after 3 attempts",
            "Transaction rollback failed"
        ]
    if service_name == "ImageProcessingService":
       return [
            "java.lang.OutOfMemoryError: Java heap space",
            "Process OOMKilled by kernel",
            "CRASH REPORT: Dump saved to /tmp/core.1234"
       ]
    return []

# --- Pipeline ---
def run(argv=None):
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", help="GCP Project")
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    THRESHOLD = 5

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Ingest Logs
        logs = (
            p 
            | "Generate Stream" >> beam.Create([None]) 
            | beam.ParDo(GenerateLogs())
        )
        
        # 2. Aggregate Errors (Windowed)
        # We want to count ERRORs per Service in fixed windows
        incidents = (
            logs
            | "Filter Errors" >> beam.Filter(lambda x: x.level == "ERROR")
            | "Window" >> beam.WindowInto(window.FixedWindows(5)) # 5 sec windows
            | "Add Key" >> beam.WithKeys(lambda x: x.service)
            | "Count per Service" >> beam.CombinePerKey(beam.combiners.CountCombineFn())
            | "Check Threshold" >> beam.Filter(lambda x: x[1] > THRESHOLD)
            | "Create Incident" >> beam.ParDo(lambda x, w=beam.DoFn.WindowParam: [
                IncidentReport(
                    service=x[0], 
                    error_count=x[1],
                    window_start=float(w.start),
                    window_end=float(w.end)
                )
            ])
        )

        # 3. Investigate with Agent
        # Format the input for the agent
        def format_incident(incident: IncidentReport) -> str:
            return (f"ALERT: {incident.service} has {incident.error_count} errors "
                    f"between {incident.window_start} and {incident.window_end}. "
                    "Please investigate immediately.")

        # 3. Investigate with Agent
        # Format input as (SessionID, Query) where SessionID is Service Name
        inputs = (
            incidents
            | "Format Input" >> beam.Map(
                lambda x: (x.service, format_incident(x))
            )
        )

        agent_results = (
            inputs
            | "SRE Detective Agent" >> RunAgent(
                model_name="gemini-2.0-flash",
                project=known_args.src,
                location="us-central1",
                tools=[get_service_health, get_recent_deployments, query_error_logs],
                instruction="""
                You are a Senior SRE Detective.
                You have received an Incident Report.
                Your goal is to identify the Root Cause.
                
                Follow this investigation runbook:
                1. Check `get_service_health`. If CPU/Mem is high, it's a scaling issue.
                2. If Health is normal, check `get_recent_deployments`. If a deploy happened recently, it's likely a bad code change.
                3. Use `query_error_logs` to confirm specific errors.
                
                Output a concise Root Cause Analysis (RCA) and recommendation.
                """
            ).with_exception_handling()
        )
        
        # Handle Success
        (
            agent_results.output 
            | "Print RCA" >> beam.Map(lambda x: print(f"\n✅ RCA REPORT SENT (Session {x.session_id}):\n{x.final_text}\n"))
        )
        
        # Handle Failures
        (
            agent_results.failed_inputs
            | "Log Failures" >> beam.Map(lambda x: logging.error(f"Agent Failed: {x[1]}"))
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
