from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import sys
print(sys.argv[0])

OUTPUT_PREFIX = "output_prefix"
STAGING_DIR = "staging"
TEMP_DIR = "temp"
JOB_NAME_PREFIX = "my-experiment-job"

def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')

  parser.add_argument('--project',
                      dest='project',
                      help='Google Cloud Project ID (also used as the bucket name)')

  parser.add_argument('--run_target',
                      dest='run_target',
                      default='local',
                      help='Where to run job (local,gcp)')

  known_args, pipeline_args = parser.parse_known_args(argv)

  extendList = list()
  if known_args.run_target == "gcp":
  	runner = "DataflowRunner"
  else:
  	runner = "DirectRunner"
  extendList.append("--runner=%s" % runner)

  if known_args.project:
  	# build the output location
  	known_args.output = "gs://" + known_args.project + "/" + OUTPUT_PREFIX
  	extendList.append("--project=%s" % known_args.project)
  	staging_location = "gs://" + known_args.project + "/" + STAGING_DIR
  	temp_location = "gs://" + known_args.project + "/" + TEMP_DIR
  	extendList.append("--staging_location=%s" % staging_location)
  	extendList.append("--temp_location=%s" % temp_location)
  elif known_args.run_target == "gcp":
  	print("Can't proceed with invalid Project ID")
  	exit(1)
  else:
  	known_args.output = "/tmp/output"

  from datetime import datetime
  from time import gmtime, strftime
  curr_time = str(datetime.now().strftime('%Y-%m-%dt%H-%M-%S'))
  #print(curr_time)
  job_name = JOB_NAME_PREFIX + "--" + curr_time #+ "-jobname"

  #extendList.append("--job_name=%s" % (JOB_NAME_PREFIX+"--"+curr_time))
  extendList.append("--job_name=%s" % (job_name))

  #print(extendList)

  pipeline_args.extend(extendList)

  #print(known_args, pipeline_args)
  #exit()

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | ReadFromText(known_args.input)

    # Count the occurrences of each word.
    counts = (
        lines
        | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    output = counts | 'Format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
