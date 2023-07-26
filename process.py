import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os



service_account_path = os.environ.get("SERVICE_ACCOUNT_PATH")
print("Service account file : ", service_account_path)
input_subscription = os.environ.get("INPUT_SUBSCRIPTION")
output_topic = os.environ.get("OUTPUT_TOPIC")

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)


output_file = 'outputs/part'

pubsub_data = (
                p
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
                | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)
              )

result = p.run()
result.wait_until_finish()
