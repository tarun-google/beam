import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

logging.basicConfig(level=logging.INFO)

class PrintFn(beam.DoFn):
  def process(self, element):
    logging.info(f"Processing element: {element}")
    yield element

pipeline_options = PipelineOptions()
pipeline = beam.Pipeline(options=pipeline_options)

# Create a PCollection from a list of elements for this batch job.
data = pipeline | 'Create' >> beam.Create([
  'Hello',
  'World',
  'This',
  'is',
  'a',
  'batch',
  'example',
])

# Apply the custom DoFn with resource hints.
data | 'PrintWithDoFn' >> beam.ParDo(PrintFn())

result = pipeline.run()
#logging.info(f"Submitted Dataflow job. Job ID: {result.job_id()}")
result.wait_until_finish()
