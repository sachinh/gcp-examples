import json

field_list = []
with open("/Users/sachinholla/Documents/GCP/advertiser.schema","rU") as fin:
	json_data = json.load(fin)
	for field in json_data["schema"]["fields"]:
		field_list.append(field["name"])

print(json.dumps(field_list))
print("done")