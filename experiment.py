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

pipeline_args = []
pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(SetupOptions).save_main_session = True

dept_headers = [("dept_id","dept_name","dept_start_year")]
dept_headers_list = ["dept_id","dept_name","dept_start_year"]
emp_headers_list = ["emp_id","emp_name","emp_dept","emp_country","emp_gender","emp_birth_year","emp_salary"]

class GenericFormatDoFn(beam.DoFn):
  def process(self, element, headers_list=[], prefix="pfx"):
    print(element)
    #return [{'dept_id':element[0],
    #         'dept_name':element[1],
    #         'dept_start_year':element[2]}]
    print(headers_list)
    if not dept_headers_list:
    	print("Exception!!! No headers list provided!")
    	return

    formatted_data = {}
    for index,elem in enumerate(element):
    	print(index,elem,headers_list[index])
    	formatted_data["%s.%s" % (prefix,headers_list[index])] = elem
    return [formatted_data]

class FormatEmpDoFn(beam.DoFn):
  def process(self, element):
    print(element)
    return [{'emp_id':element[0],
             'emp_name':element[1],
             'emp_dept':element[2],
             'emp_country':element[3],
             'emp_gender':element[4],
             'emp_birth_year':element[5],
             'emp_salary':element[6]
             }]

with beam.Pipeline(options=pipeline_options) as p:

	#new_depts = (p 
	#		| CsvFileSource("/Users/sachinholla/Documents/GCP/departments.csv").read_records("/Users/sachinholla/Documents/GCP/departments.csv") )

	print(dept_headers)
	dept_headers_pcoll = (p 
							| 'CreateHeaders' >> beam.Create(dept_headers) 
							| 'FlatMap1' >> beam.Map(lambda x: list(x))
						)
	dept_headers_pcoll | "write1" >> WriteToText("localfile10-")

	#sys.exit(1)

	new_depts = (p
					| "NewRead" >> ReadFromText("/Users/sachinholla/Documents/GCP/departments.csv")
					| 'SplitDeptsNew' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\ \#\_\']+', x))
				)
	#new_depts | "write2" >> WriteToText("localfile11-")
	depts_transformed = (new_depts 
							| 'Format' >> beam.ParDo(GenericFormatDoFn(),headers_list=dept_headers_list, prefix="dept")
							| 'keydept2' >> beam.Map(lambda dept: (dept["dept.dept_id"], dept))
						)
	depts_transformed | "write2" >> WriteToText("localfile11-")

	deps_si = beam.pvalue.AsDict(depts_transformed)

	employees = (p
					| "ReadingEmps2" >> ReadFromText("/Users/sachinholla/Documents/GCP/employees.csv")
					| 'SplitEmps2' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\ \#\_\']+', x))
					| 'Format2' >> beam.ParDo(GenericFormatDoFn(),headers_list=emp_headers_list, prefix="emp")
					| 'keyemp' >> beam.Map(lambda emp: (emp["emp.emp_dept"], emp))
				)
	employees |  "WriteEmps2" >> WriteToText("localfile2")

	def join_emp_dept2(employee, dept_dict):
		#print("employee")
		#print(employee)
		#print("dept_dict")
		#print(dept_dict)
		#print("types")
		#print(type(employee),type(dept_dict))
		#print("tuple")
		#print(employee[0])
		#print(employee[1])
		#print("correlating dicts")
		dept = dept_dict[employee[0]]
		#print(dept)
		emp=employee[1]
		#print(emp)
		emp.update(dept)
		#print(emp)

		return (emp)

	def join_emp_dept3(employee, dept_dict):
	  print(employee[0])
	  print(employee[1])
	  print(dept_dict[employee[0]])
	  new_emp = employee[1].update(dept_dict[employee[0]])
	  print(new_emp)

	  return new_emp
	  #return employee[1].update(dept_dict[employee[1]['emp_dept']])

	def multiply(values):
		#print(values)
		#return values
		return None

	def format_result(dict_values):
		print("formatting ...")
		print(dict_values)
		for k,v in dict_values.iteritems():
			print(k,v)
		#elem1 = dict_values[0]
		return dict_values

	joined_dicts = (employees 
					| 'Joining' >> beam.Map(join_emp_dept2, dept_dict=deps_si) 
					#| 'ToPairs' >> beam.Map(lambda v: (v, None))
					#| 'FlatMap2' >> beam.FlatMap(format_result)
					#| 'pcoll' >> beam.Create()
					#| 'convertingdict' >> beam.pvalue.AsDict()
					#| 'Group' >> beam.CombinePerKey(lambda vs: None)
					#| "RemoveDupes" >> beam.RemoveDuplicates() 
					)
	#joined_dicts = employees | 'Joining' >> beam.Map(join_emp_dept3, dept_dict=deps_si)

	print("type checling")
	print(type(joined_dicts))
	joined_dicts | "WriteJoin" >> WriteToText("localfile3")


	'''
	merged_pcolls = (
					(dept_headers_pcoll,new_depts)
					| "Flattening" >> beam.Flatten()
					| "FlatMap3" >> beam.Map(lambda x: x,)
					)
	merged_pcolls | "write3" >> WriteToText("localfile13-")

	print("dict?")
	print(beam.pvalue.AsDict(merged_pcolls))
	'''

	#sys.exit(0)

	'''
	departments = (p 
					| "ReadingDepts" >> ReadFromText("/Users/sachinholla/Documents/GCP/departments.csv")
					| 'SplitDepts' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\ \#\_\']+', x))
                 	| 'keydept' >> beam.Map(lambda dept: (dept[0], dept)))

	deps_si = beam.pvalue.AsDict(departments)
	#print(deps_si)

	departments | "WriteDepts" >> WriteToText("localfile")

	'''

	'''
	employees = (p
					| "ReadingEmps" >> ReadFromText("/Users/sachinholla/Documents/GCP/employees.csv")
					| 'SplitEmps' >> beam.Map(lambda x: re.findall(r'[0-9A-Za-z\ \#\_\']+', x))
					| 'keyemp' >> beam.Map(lambda emp: (emp[2], emp))
				)
	employees |  "WriteEmps" >> WriteToText("localfile2")
	'''

	def join_emp_dept(employee, dept_dict):
		print("employee")
		print(employee)
		print("dept_dict")
		print(dept_dict)
		print("types")
		print(type(employee),type(dept_dict))
		#return employee.update(dept_dict[employee[2]])
		print("tuple")
		print(employee[0])
		print(employee[1])
		#return (employee[0], employee[1].append(dept_dict[employee[0]]))
		print("correlating dicts")
		dept = dept_dict[employee[0]]
		#print(dept_dict[employee[0]])
		print(dept)
		emp=employee[1]
		print(emp)
		#print(employee[1].extend(dept_dict[employee[0]]))
		emp.extend(dept)
		print(emp)
		return (employee[0], employee[1].extend(dept_dict[employee[0]]))

	'''
	joined_dicts = employees | 'Joining' >> beam.Map(join_emp_dept, dept_dict=deps_si)

	joined_dicts | "WriteJoin" >> WriteToText("localfile3")
	'''

	#print(departments)

