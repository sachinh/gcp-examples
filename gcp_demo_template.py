#from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.options.pipeline_options import StandardOptions
import apache_beam.transforms.window as window

import sys
import json
from pprint import pprint

#import io.BufferedIOBase

OUTPUT_PREFIX = "output_prefix"
STAGING_DIR = "staging"
TEMP_DIR = "temp"
JOB_NAME_PREFIX = "gcp-demo-template-job"

def buildSchemaString(header_list):
	schema_string = ""
	string_type = "STRING"
	sep = ""
	for header in header_list:
		if schema_string:
			sep = ", "
		schema_string += '%s%s:%s' % (sep,header.lower(),string_type)
	return "\'%s\'" % schema_string

def getDBSchema(source_table,src_headers):
	# start building the schema with the assumption that all fields are string
	if source_table == "impression":
		table_schema = buildSchemaString(src_headers)

	#table_schema += ", " + buildSchemaString(cntry_headers_list,prefix="country")
	#table_schema += ", " + buildSchemaString(advrtsr_headers_list,prefix="advertiser")

	#print(pcollData)
	#keys_only = pcollData | "keys" >> beam.Map(lambda (k,v): k)
	print(table_schema)
	return(table_schema)

class GenericFormatDoFn(beam.DoFn):
  def process(self, element,src_headers=None):
    #print(element)
    #print(headers_list)
    #print(len(element),len(headers_list))
    formatted_data = {}
    for index,elem in enumerate(element):
    	formatted_data[src_headers[index]] = str(elem)
    return [formatted_data]

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
		parser.add_argument(
		  '--input',
		  default='gs://dataflow-samples/shakespeare/kinglear.txt',
		  help='Path of the file to read from')
		parser.add_argument(
		  '--output',
		  required=True,
		  help='Output file to write results to.')
		'''		
		parser.add_argument('--input',
		                  dest='input',
		                  default='gs://big-data-pipe/country.csv',
		                  help='Country Data file to process.')
		'''
		'''
		parser.add_argument('--project',
		                  dest='project',
		                  default="big-data-pipe",
		                  help='Google Cloud Project ID (default: "big-data-pipe"')
		'''
		parser.add_argument('--bucket',
		                  dest='bucket',
		                  default="gs://ce_gcp_demo",
		                  help='Google Cloud Storage Bucket (default: "gs://ce_gcp_demo")')

		parser.add_argument('--run_target',
		                  dest='run_target',
		                  default='gcp',
		                  help='Where to run job (local,*gcp)')

		parser.add_value_provider_argument('--src_table',
		                  dest='src_table',
		                  default='impression',
		                  help='Choice of table to ingest (*impression,advertiser,country)')

		parser.add_argument('--dest_table',
		                  dest='dest_table',
		                  default='big-data-pipe:ce_gcp_demo.gcp_demo',
		                  help='Destination Table (default: "big-data-pipe:ce_gcp_demo.gcp_demo")')

		parser.add_value_provider_argument('--output_dest',
		                  dest='output_dest',
		                  default='bq',
		                  help='Output location (file,*bq)')

def execute_pipeline():
	# setting up some pipeline args
	pipeline_args = ['--output', 'gs://ce_gcp_demo/output']
	pipeline_args.extend(["--project","big-data-pipe"])
	pipeline_args.extend(["--runner","DataflowRunner"])
	pipeline_args.extend(["--staging_location","gs://ce_gcp_demo/staging"])
	pipeline_args.extend(["--temp_location","gs://ce_gcp_demo/temp"])
	pipeline_args.extend(["--save_main_session","True"])

	from datetime import datetime
	from time import gmtime, strftime
	
	curr_time = str(datetime.now().strftime('%Y-%m-%dt%H-%M-%S'))
	job_name = JOB_NAME_PREFIX + "--" + curr_time #+ "-jobname"

	pipeline_args.extend(["--job_name",job_name])

	#
	pipeline_options = PipelineOptions(pipeline_args)
	p = beam.Pipeline(options=pipeline_options)

	print("class maker")
	mypipeline_options = pipeline_options.view_as(MyPipelineOptions)

	print(pipeline_args)
	print(pipeline_options)
	print(mypipeline_options)
	print(mypipeline_options.get_all_options())
	print(mypipeline_options.display_data())

	# now do the rest
	# headers for impression data 
	imp_headers_list = ["Time", "UserId", "AdvertiserId", "OrderId", "LineItemId", "CreativeId", "CreativeVersion", "CreativeSize", "AdUnitId", "CustomTargeting", "Domain", "CountryId", "Country", "RegionId", "Region", "MetroId", "Metro", "CityId", "City", "PostalCodeId", "PostalCode", "BrowserId", "Browser", "OSId", "OS", "OSVersion", "BandwidthId", "BandWidth", "TimeUsec", "AudienceSegmentIds", "Product", "RequestedAdUnitSizes", "BandwidthGroupId", "MobileDevice", "MobileCapability", "MobileCarrier", "IsCompanion", "TargetedCustomCriteria", "DeviceCategory", "IsInterstitial", "EventTimeUsec2", "YieldGroupNames", "YieldGroupCompanyId", "MobileAppId", "RequestLanguage", "DealId", "DealType", "AdxAccountId", "SellerReservePrice", "Buyer", "Advertiser", "Anonymous", "ImpressionId"]
	imp_headers = ('Time:STRING, UserId:STRING, AdvertiserId:STRING, OrderId:STRING, LineItemId:STRING, CreativeId:STRING, CreativeVersion:STRING, CreativeSize:STRING, AdUnitId:STRING, CustomTargeting:STRING, Domain:STRING, CountryId:STRING, Country:STRING, RegionId:STRING, Region:STRING, MetroId:STRING, Metro:STRING, CityId:STRING, City:STRING, PostalCodeId:STRING, PostalCode:STRING, BrowserId:STRING, Browser:STRING, OSId:STRING, OS:STRING, OSVersion:STRING, BandwidthId:STRING, BandWidth:STRING, TimeUsec:STRING, AudienceSegmentIds:STRING, Product:STRING, RequestedAdUnitSizes:STRING, BandwidthGroupId:STRING, MobileDevice:STRING, MobileCapability:STRING, MobileCarrier:STRING, IsCompanion:STRING, TargetedCustomCriteria:STRING, DeviceCategory:STRING, IsInterstitial:STRING, EventTimeUsec2:STRING, YieldGroupNames:STRING, YieldGroupCompanyId:STRING, MobileAppId:STRING, RequestLanguage:STRING, DealId:STRING, DealType:STRING, AdxAccountId:STRING, SellerReservePrice:STRING, Buyer:STRING, Advertiser:STRING, Anonymous:STRING, ImpressionId:STRING')
	cntry_headers_list = ["CountryId", "CountryAbbrev", "CountryName", "CountryLocale", "CountryCurrency"]
	advrtsr_headers_list = ["AdvertiserId", "AdvertiserName", "AdvertiserType", "AdvertiserRegion"]

	src_file = "gs://ce_gcp_demo/input/impressions_data*.csv"
	src_headers = imp_headers_list

	# step 1. retrieve the data
	src_data = (p
				| "SourceRead" >> ReadFromText(src_file)
				| 'SourceSplit' >> beam.Map(lambda x: x.split(","))
				| 'Windowing' >> beam.WindowInto(window.FixedWindows(15, 0))
				| 'SourceFormat' >> beam.ParDo(GenericFormatDoFn(),src_headers=src_headers)
				#| 'SourceMapping' >> beam.Map(lambda imp: (imp["impression_countryid"], imp))
				)

	src_data | "SourceWrite" >> WriteToText("gs://ce_gcp_demo/output-")

	print("updating bigquery table")
	#getDBSchema()
	src_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
			"big-data-pipe:ce_gcp_demo.gcp_demo10",
			schema=src_headers,
			create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
			write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	p.run().wait_until_finish()

	return

def run(argv=None):
	"""Main entry point; defines and runs the wordcount pipeline."""

	'''
	parser = argparse.ArgumentParser()
	parser.add_argument('--input',
	                  dest='input',
	                  default='gs://big-data-pipe/country.csv',
	                  help='Country Data file to process.')

	parser.add_argument('--project',
	                  dest='project',
	                  default="big-data-pipe",
	                  help='Google Cloud Project ID (default: "big-data-pipe"')

	parser.add_argument('--bucket',
	                  dest='bucket',
	                  default="gs://ce_gcp_demo",
	                  help='Google Cloud Storage Bucket (default: "gs://ce_gcp_demo")')

	parser.add_argument('--run_target',
	                  dest='run_target',
	                  default='gcp',
	                  help='Where to run job (local,gcp)')

	parser.add_argument('--src_table',
	                  dest='src_table',
	                  default='impression',
	                  help='Choice of table to ingest (*impression,advertiser,country)')

	parser.add_argument('--dest_table',
	                  dest='dest_table',
	                  default='big-data-pipe:ce_gcp_demo.gcp_demo',
	                  help='Destination Table (default: "big-data-pipe:ce_gcp_demo.gcp_demo")')

	parser.add_argument('--output_dest',
	                  dest='output_dest',
	                  default='bq',
	                  help='Output location (file,*bq)')

	known_args, pipeline_args = parser.parse_known_args(argv)

	print(known_args)

	extendList = list()
	if known_args.run_target == "gcp":
		runner = "DataflowRunner"
	else:
		runner = "DirectRunner"
	extendList.append("--runner=%s" % runner)

	if known_args.bucket and known_args.project and known_args.bucket.startswith("gs:"):
		# build the output location
		known_args.output = known_args.bucket + "/" + OUTPUT_PREFIX
		extendList.append("--project=%s" % known_args.project)
		staging_location = known_args.bucket + "/" + STAGING_DIR
		temp_location = known_args.bucket + "/" + TEMP_DIR
		extendList.append("--staging_location=%s" % staging_location)
		extendList.append("--temp_location=%s" % temp_location)
	elif known_args.run_target == "gcp":
		print("Can't proceed with invalid Bucket and/or Project")
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
	#extendList.append("--num_workers=3")
	beam.utils.retry.with_exponential_backoff(num_retries=10)
	#print(extendList)

	pipeline_args.extend(extendList)

	# setting the schema from the env. input vars
	#tsu.setSchema(known_args.schema)
	#tsu.setFixedSchema()

	print(known_args, pipeline_args)
	#exit()

	# headers for impression data 
	imp_headers_list = ["Time", "UserId", "AdvertiserId", "OrderId", "LineItemId", "CreativeId", "CreativeVersion", "CreativeSize", "AdUnitId", "CustomTargeting", "Domain", "CountryId", "Country", "RegionId", "Region", "MetroId", "Metro", "CityId", "City", "PostalCodeId", "PostalCode", "BrowserId", "Browser", "OSId", "OS", "OSVersion", "BandwidthId", "BandWidth", "TimeUsec", "AudienceSegmentIds", "Product", "RequestedAdUnitSizes", "BandwidthGroupId", "MobileDevice", "MobileCapability", "MobileCarrier", "IsCompanion", "TargetedCustomCriteria", "DeviceCategory", "IsInterstitial", "EventTimeUsec2", "YieldGroupNames", "YieldGroupCompanyId", "MobileAppId", "RequestLanguage", "DealId", "DealType", "AdxAccountId", "SellerReservePrice", "Buyer", "Advertiser", "Anonymous", "ImpressionId"]
	imp_headers = ('Time:STRING, UserId:STRING, AdvertiserId:STRING, OrderId:STRING, LineItemId:STRING, CreativeId:STRING, CreativeVersion:STRING, CreativeSize:STRING, AdUnitId:STRING, CustomTargeting:STRING, Domain:STRING, CountryId:STRING, Country:STRING, RegionId:STRING, Region:STRING, MetroId:STRING, Metro:STRING, CityId:STRING, City:STRING, PostalCodeId:STRING, PostalCode:STRING, BrowserId:STRING, Browser:STRING, OSId:STRING, OS:STRING, OSVersion:STRING, BandwidthId:STRING, BandWidth:STRING, TimeUsec:STRING, AudienceSegmentIds:STRING, Product:STRING, RequestedAdUnitSizes:STRING, BandwidthGroupId:STRING, MobileDevice:STRING, MobileCapability:STRING, MobileCarrier:STRING, IsCompanion:STRING, TargetedCustomCriteria:STRING, DeviceCategory:STRING, IsInterstitial:STRING, EventTimeUsec2:STRING, YieldGroupNames:STRING, YieldGroupCompanyId:STRING, MobileAppId:STRING, RequestLanguage:STRING, DealId:STRING, DealType:STRING, AdxAccountId:STRING, SellerReservePrice:STRING, Buyer:STRING, Advertiser:STRING, Anonymous:STRING, ImpressionId:STRING')
	cntry_headers_list = ["CountryId", "CountryAbbrev", "CountryName", "CountryLocale", "CountryCurrency"]
	advrtsr_headers_list = ["AdvertiserId", "AdvertiserName", "AdvertiserType", "AdvertiserRegion"]

	#dept_headers = [("dept_id","dept_name","dept_start_year")]
	#dept_headers_list = ["dept_id","dept_name","dept_start_year"]
	#emp_headers_list = ["emp_id","emp_name","emp_dept","emp_country","emp_gender","emp_birth_year","emp_salary"]

	def buildSchemaString(header_list):
		schema_string = ""
		string_type = "STRING"
		sep = ""
		for header in header_list:
			if schema_string:
				sep = ", "
			schema_string += '%s%s:%s' % (sep,header.lower(),string_type)
		return "\'%s\'" % schema_string

	def getDBSchema(source_table,src_headers):
		# start building the schema with the assumption that all fields are string
		if source_table == "impression":
			table_schema = buildSchemaString(src_headers)

		#table_schema += ", " + buildSchemaString(cntry_headers_list,prefix="country")
		#table_schema += ", " + buildSchemaString(advrtsr_headers_list,prefix="advertiser")

		#print(pcollData)
		#keys_only = pcollData | "keys" >> beam.Map(lambda (k,v): k)
		print(table_schema)
		return(table_schema)

	class GenericFormatDoFn(beam.DoFn):
	  def process(self, element):
	    #print(element)
	    #print(headers_list)
	    #print(len(element),len(headers_list))
	    formatted_data = {}
	    for index,elem in enumerate(element):
	    	formatted_data[imp_headers_list[index]] = str(elem)
	    return [formatted_data]

	class WordcountOptions(PipelineOptions):
	    @classmethod
	    def _add_argparse_args(cls, parser):
	      parser.add_argument(
	          '--input',
	          default='gs://dataflow-samples/shakespeare/kinglear.txt',
	          help='Path of the file to read from')
	      parser.add_argument(
	          '--foobar',
	          required=True,
	          help='Output file to write results to.')

	#pipeline_options.view_as(StandardOptions).streaming = True

	if known_args.run_target == "local":
		if known_args.src_table == "impression":
			src_file = "/Users/sachinholla/Documents/GCP/impressions_data.csv"
			src_headers = imp_headers_list
		cntry_file = "/Users/sachinholla/Documents/GCP/country_data.csv"
		advrtsr_file = "/Users/sachinholla/Documents/GCP/advertiser_data.csv"
	else:
		if known_args.src_table == "impression":
			src_file = known_args.bucket + "/input/" + "impressions_data*.csv"
			src_headers = imp_headers_list
		cntry_file = known_args.bucket + "/input/" + "country_data.csv"
		advrtsr_file = known_args.bucket + "/input/" + "advertiser_data.csv"
	'''

	pipeline_options = PipelineOptions(pipeline_args)
	p = beam.Pipeline(options=pipeline_options)

	wordcount_options = pipeline_options.view_as(WordcountOptions)
	
	print(pipeline_args)
	print(pipeline_options)
	print(wordcount_options)
	sys.exit(0)

	#pipeline_options.view_as(SetupOptions).save_main_session = True

	#p = beam.Pipeline(options=pipeline_options)

	# step 1. retrieve the data
	src_data = (p
				| "SourceRead" >> ReadFromText(src_file)
				| 'SourceSplit' >> beam.Map(lambda x: x.split(","))
				| 'Windowing' >> beam.WindowInto(window.FixedWindows(15, 0))
				| 'SourceFormat' >> beam.ParDo(GenericFormatDoFn())
				#| 'SourceMapping' >> beam.Map(lambda imp: (imp["impression_countryid"], imp))
				)

	if known_args.run_target == "local":
		src_data | "SourceWrite" >> WriteToText(known_args.src_table)
	else:
		src_data | "SourceWrite" >> WriteToText(known_args.output + "-" + known_args.src_table)

	if known_args.output_dest == "bq":
		print("updating bigquery table")
		#getDBSchema()
		src_data | 'Write' >> beam.io.WriteToBigQuery(
	          known_args.dest_table,
	          schema=imp_headers,
	          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	p.run().wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  execute_pipeline()
  #run()
