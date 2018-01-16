import json
import csv

def tokenize(item_list=[]):
	token_list = ""
	for item in item_list:
		if token_list == "":
			token_list = item
		else:
			token_list += ",%s" % item

	return(token_list)

def stringize(values_list):
	string_value = ""
	for value in values_list:
		if string_value == "":
			string_value = value.encode('ascii','ignore')
		else:
			string_value += ",%s" % value.encode('ascii','ignore')

	return(string_value)

with open("countries.lines","rU") as fin:
	# now to write this out as a CSV
	with open("countries.lines.json", "wb+") as fout:

		for index,line in enumerate(fin):
			#print(line)
			json_line = json.loads(line)
			json_line["id"] = "%03d" % (index)
			#if "emoji" in json_line.keys():
			#	del json_line["emoji"]
			#if index == 0:
			#	#fout.write(stringize(json_line.keys()))
			#	#fout.write("\n")
			#	continue
			json_line["currencies"] = tokenize(json_line["currencies"])
			json_line["languages"] = tokenize(json_line["languages"])
			json_line["countryCallingCodes"] = tokenize(json_line["countryCallingCodes"])
			#fout.write(stringize(json_line.values()))
			fout.write(json.dumps(json_line))
			fout.write("\n")
			if index > 20:
				break

print("Finished writing")
