import requests
from bs4 import BeautifulSoup
import os.path
import json
from get_url import get_url

file_name = 'proxy_server_list.json'

class ProxyServer:
	def __init__(self):
		self.proxy_list = []
		self.next_proxy_count = 0
		self.last_proxy_count = 0
		
		url = get_url('proxy_list_url')
	
		if not os.path.isfile(file_name):
			server_list = []

			res = requests.get(url, headers = {'User-Agent':'Mozilla/5.0'})
			soup = BeautifulSoup(res.text, 'lxml')
			for items in soup.select("tbody tr"):
				if items.select("td")[6].text == 'yes':
					if items.select("td")[4].text == 'anonymous' or items.select("td")[4].text == 'elite proxy':
						server_list.append({"https" : items.select("td")[0].text + ':' + items.select("td")[1].text})

			with open(file_name, 'w') as fp:
				fp.write(json.dumps(server_list))
					
		# read proxy_server_list
		with open(file_name, 'r') as fp:
			self.proxy_list = json.load(fp)
			
	def len(self):
		return len(self.proxy_list)

	def get_next_proxy(self):
		next_proxy = self.proxy_list[self.next_proxy_count]
		self.last_proxy_count = self.next_proxy_count
		self.next_proxy_count = (self.next_proxy_count + 1) % len(self.proxy_list)
		return next_proxy
		
	def remove_current_proxy(self):
		if self.last_proxy_count < len(self.proxy_list):
			del self.proxy_list[self.last_proxy_count]
			self.next_proxy_count = self.next_proxy_count % len(self.proxy_list)
			with open(file_name, 'w') as fp:
				fp.write(json.dumps(self.proxy_list))

# testing
#ps = ProxyServer()
#print(ps.get_next_proxy())
#print(ps.get_next_proxy())
#print(ps.get_next_proxy())
#print(ps.get_next_proxy())
#print(ps.get_next_proxy())
