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

"""Transforms for Agents using Google ADK."""

import logging
import asyncio
import uuid
import os
import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.metrics import Metrics
from typing import Callable, Any, Optional, Tuple, NamedTuple, List, Union

from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

class AgentOutput(NamedTuple):
    """Output from an Agent."""
    final_text: str
    session_id: str

class RunAgentResult(NamedTuple):
    output: beam.PCollection
    failed_inputs: beam.PCollection

class RunAgent(beam.PTransform):
    """RunAgent transform to invoke a Google ADK Agent."""

    def __init__(
        self, 
        model_name: str,
        project: Optional[str] = None,
        location: Optional[str] = None,
        tools: Optional[List[Callable]] = None,
        instruction: Optional[str] = "",
    ):
        """
        Args:
            model_name: Name of the model to use (e.g. "gemini-2.0-flash").
            project: Google Cloud Project ID.
            location: Google Cloud Location (e.g. "us-central1").
            tools: List of python callables to use as tools.
            instruction: System instruction for the agent.
        """
        self._model_name = model_name
        self._project = project
        self._location = location
        self._tools = tools
        self._instruction = instruction
        self._enable_exception_handling = False
        self._dead_letter_tag = 'failed_inputs'
        
        # Validation
        if not self._model_name:
             raise ValueError("Must provide model_name.")
    
    def with_exception_handling(self, exc_class=Exception, use_subprocess=False, threshold=1):
        """Enables exception handling. Failed inputs will be output to a separate PCollection."""
        self._enable_exception_handling = True
        return self
    
    def expand(self, pcoll):
        output = pcoll | beam.ParDo(self._ADKRunnerFn(
            model_name=self._model_name,
            project=self._project,
            location=self._location,
            tools=self._tools,
            instruction=self._instruction,
        )).with_outputs(self._dead_letter_tag, main='output')
        
        if self._enable_exception_handling:
            return RunAgentResult(
                output=output.output,
                failed_inputs=output[self._dead_letter_tag]
            )
        return output.output

    class _ADKRunnerFn(beam.DoFn):
        def __init__(self, model_name, project, location, tools, instruction):
            self._model_name = model_name
            self._project = project
            self._location = location
            self._tools_config = tools
            self._instruction = instruction
            self._success_counter = Metrics.counter('RunAgent', 'successful_inferences')
            self._failure_counter = Metrics.counter('RunAgent', 'failed_inferences')
            self._dead_letter_tag = 'failed_inputs'

        def setup(self):
            # Initialize Tools
            adk_tools = []
            if self._tools_config:
                for tool_fn in self._tools_config:
                    adk_tools.append(FunctionTool(tool_fn))
            
            # Initialize Agent
            # Configure Vertex AI client parameters via environment variables
            if self._project:
                os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'true'
                os.environ['GOOGLE_CLOUD_PROJECT'] = self._project
                if self._location:
                    os.environ['GOOGLE_CLOUD_LOCATION'] = self._location

            self._agent_instance = Agent(
                name="beam_agent",
                model=self._model_name,
                tools=adk_tools,
                instruction=self._instruction or ""
            )
            

            # --- Initialize Shared Runner ---
            self._shared_handle = shared.Shared()
            
            def init_shared_runner():
                agent = self._agent_instance
                session_service = InMemorySessionService()
                app_name = "beam_agent_app"
                
                return Runner(
                    agent=agent, 
                    app_name=app_name, 
                    session_service=session_service
                )

            # Acquiring the shared runner
            self._runner = self._shared_handle.acquire(init_shared_runner)
            self._user_id = "beam_worker"

        def process(self, element):
            # Default Session ID
            session_id = str(uuid.uuid4())
            user_query = element
            
            # Keyed Input Detection (Tuple of 2) -> (Key, Query)
            # Use Key as Session ID
            if isinstance(element, (tuple, list)) and len(element) == 2:
                possible_key, possible_query = element
                if isinstance(possible_key, str):
                    session_id = possible_key
                    user_query = possible_query
            
            # Normalize Query
            if not isinstance(user_query, (str, getattr(types, 'Content', type(None)))):
                 if hasattr(user_query, 'contents'):
                     user_query = user_query.contents
            
            self._ensure_session(session_id)
            
            # Run ADK Logic
            try:
                final_text = asyncio.run(self._run_adk(user_query, session_id))
                self._success_counter.inc()
                yield AgentOutput(final_text=final_text, session_id=session_id)
            except Exception as e:
                logging.error(f"ADK Execution failed: {e}")
                self._failure_counter.inc()
                # Yield to DLQ -> (Element, ErrorMsg)
                yield beam.pvalue.TaggedOutput(self._dead_letter_tag, (element, str(e)))

        def _ensure_session(self, session_id):
            """Ensures session exists in the shared service for this ID."""
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            try:
                loop.run_until_complete(
                    self._runner.session_service.create_session(
                        app_name="beam_agent_app",
                        user_id=self._user_id,
                        session_id=session_id
                    )
                )
            except Exception:
                pass

        async def _run_adk(self, query, session_id):
            if isinstance(query, str):
                content = types.Content(role='user', parts=[types.Part(text=query)])
            else:
                content = query
            
            # Use the shared runner with specific Session ID
            events = self._runner.run(
                user_id=self._user_id, 
                session_id=session_id, 
                new_message=content
            )
            
            final_response_text = ""
            for event in events:
                if hasattr(event, 'is_final_response') and event.is_final_response():
                    if event.content and event.content.parts:
                        final_response_text = event.content.parts[0].text
            
            if not final_response_text:
                raise ValueError("Agent did not return a final response text.")
                
            return final_response_text
