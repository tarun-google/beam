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

import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import apache_beam as beam

# Mock ADK and GenAI libraries
class MockTypes:
    class Content:
        def __init__(self, role, parts):
            self.role = role
            self.parts = parts
    class Part:
        def __init__(self, text):
            self.text = text

# We need to patch the imports in transforms.py
import sys
import asyncio

# Create mock modules
mock_adk_agents = MagicMock()
mock_adk_tools = MagicMock()
mock_adk_runners = MagicMock()
mock_adk_sessions = MagicMock()
mock_genai = MagicMock()
mock_genai.types = MockTypes

sys.modules['google.adk.agents'] = mock_adk_agents
sys.modules['google.adk.tools'] = mock_adk_tools
sys.modules['google.adk.runners'] = mock_adk_runners
sys.modules['google.adk.sessions'] = mock_adk_sessions
sys.modules['google.genai'] = mock_genai

# Now import the transform to test
from apache_beam.ml.agents.transforms import RunAgent, RunAgentResult

class RunAgentTest(unittest.TestCase):
    def test_run_agent_lifecycle(self):
        # Verify setup initializes runner and process uses it
        
        # Mocks
        mock_agent_instance = MagicMock()
        mock_adk_agents.Agent.return_value = mock_agent_instance
        
        mock_session_service = MagicMock()
        mock_adk_sessions.InMemorySessionService.return_value = mock_session_service
        # create_session must be awaitable
        mock_session_service.create_session = AsyncMock()
        mock_session_service.get_session = AsyncMock()
        
        mock_runner = MagicMock()
        mock_adk_runners.Runner.return_value = mock_runner
        mock_runner.session_service = mock_session_service
        
        mock_event = MagicMock()
        mock_event.is_final_response.return_value = True
        mock_event.content.parts = [MockTypes.Part(text="Result")]
        # Runner.run returns iterator
        mock_runner.run.return_value = [mock_event]
        
        # Instantiate DoFn
        # Instantiate DoFn
        # Instantiate DoFn
        dofn = RunAgent._ADKRunnerFn(
            model_name="test-model",
            project="test-project",
            location="test-location",
            tools=[],
            instruction=""
        )
        
        # 1. SETUP
        # Mock os.environ to verify setting
        with patch.dict(os.environ, {}, clear=True):
            dofn.setup()
            
            # Verify Initialization
            mock_adk_agents.Agent.assert_called_once_with(
                name="beam_agent",
                model="test-model",
                tools=[],
                instruction=""
            )
            
            # Verify Environment Variables set
            self.assertEqual(os.environ['GOOGLE_GENAI_USE_VERTEXAI'], 'true')
            self.assertEqual(os.environ['GOOGLE_CLOUD_PROJECT'], 'test-project')
            self.assertEqual(os.environ['GOOGLE_CLOUD_LOCATION'], 'test-location')
        
        # 2. PROCESS (No Key = Random Session)
        mock_state = MagicMock()
        mock_state.read.return_value = None
        
        results = list(dofn.process("Query", session_state=mock_state))
        
        # Verify Execution
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].final_text, "Result")
        
        # Verify runner.run called
        mock_runner.run.assert_called_once()  
        
        # Verify state read and written
        mock_state.read.assert_called_once()
        mock_state.write.assert_called_once()
        
        # 3. PROCESS (Keyed Input = Session ID)
        mock_runner.run.reset_mock()
        mock_state.reset_mock()
        
        results_keyed = list(dofn.process(("user-session-123", "Query"), session_state=mock_state))
        
        self.assertEqual(len(results_keyed), 1)
        self.assertEqual(results_keyed[0].session_id, "user-session-123")
        
        # Verify runner.run called with correct session
        call_kwargs = mock_runner.run.call_args[1]
        self.assertEqual(call_kwargs['session_id'], "user-session-123")
        
        # Verify state interactions again
        mock_state.read.assert_called_once()
        mock_state.write.assert_called_once()

if __name__ == '__main__':
    unittest.main()
