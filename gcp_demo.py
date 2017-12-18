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
import json
from pprint import pprint

OUTPUT_PREFIX = "output_prefix"
STAGING_DIR = "staging"
TEMP_DIR = "temp"
JOB_NAME_PREFIX = "my-experiment-job"

def run(argv=None):
	"""Main entry point; defines and runs the wordcount pipeline."""

	parser = argparse.ArgumentParser()
	parser.add_argument('--input',
	                  dest='input',
	                  default='gs://big-data-pipe/country.csv',
	                  help='Country Data file to process.')

	parser.add_argument('--project',
	                  dest='project',
	                  help='Google Cloud Project ID')

	parser.add_argument('--bucket',
	                  dest='bucket',
	                  help='Google Cloud Storage Bucket (gs://...)')

	parser.add_argument('--run_target',
	                  dest='run_target',
	                  default='local',
	                  help='Where to run job (local,gcp)')

	parser.add_argument('--schema',
	                  dest='schema',
	                  default='/Users/sachinholla/Documents/GCP/advertiser.schema',
	                  help='Schema file (from bq)')

	parser.add_argument('--table',
	                  dest='table',
	                  default='big-data-pipe:dblclick_data.advertiser',
	                  help='Table')

	parser.add_argument('--output_dest',
	                  dest='output_dest',
	                  default='file',
	                  help='Output location (file,bq)')

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

	#print(extendList)

	pipeline_args.extend(extendList)

	# setting the schema from the env. input vars
	#tsu.setSchema(known_args.schema)
	#tsu.setFixedSchema()

	print(known_args, pipeline_args)
	#exit()

	# headers for impression data 
	imp_headers_list = ["Time", "UserId", "AdvertiserId", "OrderId", "LineItemId", "CreativeId", "CreativeVersion", "CreativeSize", "AdUnitId", "CustomTargeting", "Domain", "CountryId", "Country", "RegionId", "Region", "MetroId", "Metro", "CityId", "City", "PostalCodeId", "PostalCode", "BrowserId", "Browser", "OSId", "OS", "OSVersion", "BandwidthId", "BandWidth", "TimeUsec", "AudienceSegmentIds", "Product", "RequestedAdUnitSizes", "BandwidthGroupId", "MobileDevice", "MobileCapability", "MobileCarrier", "IsCompanion", "TargetedCustomCriteria", "DeviceCategory", "IsInterstitial", "EventTimeUsec2", "YieldGroupNames", "YieldGroupCompanyId", "MobileAppId", "RequestLanguage", "DealId", "DealType", "AdxAccountId", "SellerReservePrice", "Buyer", "Advertiser", "Anonymous", "ImpressionId"]
	cntry_headers_list = ["CountryId", "CountryAbbrev", "CountryName", "CountryLocale", "CountryCurrency"]
	advrtsr_headers_list = ["AdvertiserId", "AdvertiserName", "AdvertiserType", "AdvertiserRegion"]

	#dept_headers = [("dept_id","dept_name","dept_start_year")]
	#dept_headers_list = ["dept_id","dept_name","dept_start_year"]
	#emp_headers_list = ["emp_id","emp_name","emp_dept","emp_country","emp_gender","emp_birth_year","emp_salary"]

	def buildSchemaString(header_list,prefix="pfx"):
		schema_string = ""
		string_type = "STRING"
		sep = ""
		for header in header_list:
			if schema_string:
				sep = ", "
			schema_string += "%s%s_%s:%s" % (sep,prefix,header.lower(),string_type)
		return schema_string

	def getDBSchema():
		# start building the schema with the assumption that all fields are string
		# 1. start with the impressions
		table_schema = buildSchemaString(imp_headers_list,prefix="impressions")
		table_schema += ", " + buildSchemaString(cntry_headers_list,prefix="country")
		table_schema += ", " + buildSchemaString(advrtsr_headers_list,prefix="advertiser")

		#print(pcollData)
		#keys_only = pcollData | "keys" >> beam.Map(lambda (k,v): k)
		return(table_schema)

	class GenericFormatDoFn(beam.DoFn):
	  def process(self, element, headers_list=[], prefix="pfx"):
	    #print(element)
	    #print(headers_list)
	    if not headers_list:
	    	print("Exception!!! No headers list provided!")
	    	return

	    #print(len(element),len(headers_list))
	    formatted_data = {}
	    for index,elem in enumerate(element):
	    	#print(index,elem,headers_list[index])
	    	formatted_data["%s_%s" % (prefix,headers_list[index].lower())] = str(elem)
	    return [formatted_data]

	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = True

	if known_args.run_target == "local":
		imp_file = "/Users/sachinholla/Documents/GCP/impressions_data.csv"
		cntry_file = "/Users/sachinholla/Documents/GCP/country_data.csv"
		advrtsr_file = "/Users/sachinholla/Documents/GCP/advertiser_data.csv"
	else:
		imp_file = known_args.bucket + "/input/" + "impressions_data.csv"
		cntry_file = known_args.bucket + "/input/" + "country_data.csv"
		advrtsr_file = known_args.bucket + "/input/" + "advertiser_data.csv"

	with beam.Pipeline(options=pipeline_options) as p:

		# step 1. retrieve the impression data
		imp_data = (p
					| "ImpressionsRead" >> ReadFromText(imp_file)
					| 'ImpressionsSplit' >> beam.Map(lambda x: x.split(","))
					#| 'ImpressionsSplit' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\:\;\|\-\=\.\&\ \#\_\']?', x))
					| 'ImpressionsFormat' >> beam.ParDo(GenericFormatDoFn(),headers_list=imp_headers_list, prefix="impression")
					| 'ImpressionsMapping' >> beam.Map(lambda imp: (imp["impression_countryid"], imp))
					)

		if known_args.run_target == "local":
			imp_data | "ImpressionsWrite" >> WriteToText("impressions")
		else:
			imp_data | "ImpressionsWrite" >> WriteToText(known_args.output + "-impressions")

		# step 2. retrieve the Country data
		cntry_data = (p
					| "CountriesRead" >> ReadFromText(cntry_file)
					| 'CountriesSplit' >> beam.Map(lambda x: x.split(","))
					#| 'ImpressionsSplit' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\:\;\|\-\=\.\&\ \#\_\']?', x))
					| 'CountriesFormat' >> beam.ParDo(GenericFormatDoFn(),headers_list=cntry_headers_list, prefix="country")
					| 'CountriesMapping' >> beam.Map(lambda cntry: (cntry["country_countryid"], cntry))
					)

		if known_args.run_target == "local":
			cntry_data | "CountriesWrite" >> WriteToText("countries")
		else:
			cntry_data | "CountriesWrite" >> WriteToText(known_args.output + "-countries")

		# step 3. retrieve the Advertiser data
		advrtsr_data = (p
					| "AdvertisersRead" >> ReadFromText(advrtsr_file)
					| 'AdvertisersSplit' >> beam.Map(lambda x: x.split(","))
					#| 'ImpressionsSplit' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\:\;\|\-\=\.\&\ \#\_\']?', x))
					| 'AdvertisersFormat' >> beam.ParDo(GenericFormatDoFn(),headers_list=advrtsr_headers_list, prefix="advertiser")
					| 'AdvertisersMapping' >> beam.Map(lambda advrtsr: (advrtsr["advertiser_advertiserid"], advrtsr))
					)

		if known_args.run_target == "local":
			advrtsr_data | "AdvertisersWrite" >> WriteToText("advertisers")
		else:
			advrtsr_data | "AdvertisersWrite" >> WriteToText(known_args.output + "-advertisers")

		# step 4: now combine impressions and country data
		cntry_side_input = beam.pvalue.AsDict(cntry_data)

		def join_imp_cntry(impression, cntry_dict):
			imp=impression[1]

			if impression[0] in cntry_dict.keys():
				cntry = cntry_dict[impression[0]]
			else:
				print("No Country Found!")
				return(imp)

			imp.update(cntry)
			return (imp)

		joined_dicts = (imp_data 
						| 'JoiningCountry' >> beam.Map(join_imp_cntry, cntry_dict=cntry_side_input) 
						)
		if known_args.run_target == "local":
			joined_dicts | "CountryJoined" >> WriteToText("CountryJoined")
		else:
			joined_dicts | "CountryJoined" >> WriteToText(known_args.output + "-CountryJoined")

		# step 5: now combine impressions and advertiser data

		# need to first re-key the impressions data to be on AdvertiserId to enable the join
		rekeyed_imp_data = (joined_dicts
							| 'ImpressionsReMapping' >> beam.Map(lambda impression: (impression["impression_advertiserid"], impression))
							)

		advrtsr_side_input = beam.pvalue.AsDict(advrtsr_data)

		def join_imp_advrtsr(impression, advrtsr_dict):
			empty_advrtsr = {}
			empty_advrtsr['advertiser_advertiserid'] = ''		
			empty_advrtsr['advertiser_advertisername'] = ''
			empty_advrtsr['advertiser_advertiserregion'] = ''
			empty_advrtsr['advertiser_advertisertype'] = ''

			imp=impression[1]

			if impression[0] in advrtsr_dict.keys():
				advrtsr = advrtsr_dict[impression[0]]
			else:
				#print("No Advertiser Found!")
				advrtsr = empty_advrtsr

			imp.update(advrtsr)
			return (imp)

		joined_dicts = (rekeyed_imp_data 
						| 'JoiningAdvertiser' >> beam.Map(join_imp_advrtsr, advrtsr_dict=advrtsr_side_input) 
						)

		if known_args.run_target == "local":
			print("Writing out final dataset")
			joined_dicts | "AdvertiserJoined" >> WriteToText("AdvertiserJoined")
		else:
			joined_dicts | "AdvertiserJoined" >> WriteToText(known_args.output + "-AdvertiserJoined")

		if known_args.output_dest == "bq":
			print("updating bigquery table")
			#getDBSchema()
			joined_dicts | 'Write' >> beam.io.WriteToBigQuery(
		          "big-data-pipe:dblclick_data.gcp_demo",
		          schema=getDBSchema(),
		          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
		          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  #test_tsu = Table_Schema_Utils()
  #test_tsu.setSchema("gs://dblclick_data/schema/advertiser.schema")

  run()
