import os.path
import json

file_name = 'url_list.json'

def get_url(key):
	if os.path.isfile(file_name):
		with open(file_name, 'r') as fp:
			url_list = json.load(fp)
			
		return url_list[key]
	else:
		raise ValueError('Url File does not exist:', file_name)

# test
#print(get_url('proxy_list_url'))
#print(get_url('ticker_url'))
