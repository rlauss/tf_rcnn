import requests
from requests.exceptions import ProxyError
from bs4 import BeautifulSoup
from datetime import datetime
import re
import json
from tqdm import tqdm
import logging
import time
from get_url import get_url
from proxy_server import ProxyServer

# console utf-8: chcp 65001

# config logging
LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'
logging.basicConfig(filename = 'logging.txt', level = logging.DEBUG, 
                    format = LOG_FORMAT,
					filemode = 'w')
#requests_log = logging.getLogger("requests.packages.urllib3")
#requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True
logger = logging.getLogger()
ps = ProxyServer()

def get_dgap_adhoc_news_urls(url, url_prepend):
	url_list = []
	
	# get html page
	for _ in range(ps.len()):
		try:
			response = requests.get(url, proxies = ps.get_next_proxy())
		except ProxyError:
			ps.remove_current_proxy()
		else:
			break
	
	# beautifulsoup
	encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
	soup = BeautifulSoup(response.content, 'html.parser', from_encoding = encoding)
	
	# find links
	elems = soup.find(class_ = 'content').find_all(class_ = 'content_text')

	for i in range(0, len(elems)):
		url_list.append(url_prepend + elems[i].find('a')['href'])

	return url_list
	
def get_dgap_adhoc_news_urls_english(url_list, url_prepend):
	url_list_eng = []
	
	for url in url_list:
		# get html
		for _ in range(ps.len()):
			try:
				response = requests.get(url, proxies = ps.get_next_proxy())
			except ProxyError:
				ps.remove_current_proxy()
			else:
				break
	
		# beautifulsoup
		encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
		soup = BeautifulSoup(response.content, 'html.parser', from_encoding = encoding)
		
		# find links
		elems = soup.find('div', {'class': 'box blue news_interactive_box'}).find_all('li')
		
		# error checking list size
		if len(elems) != 6:
			#raise ValueError('There should be 6 elements in the list! Url:', url)
			continue
			
		# link to english page
		link = elems[5].find('a')['href']
		
		# error checking correct link
		if 'newsID=' not in link:
			raise ValueError('Seems to be an invalid news link! Url:', url)

		url_list_eng.append(url_prepend + link)

	return url_list_eng


def get_stock_data_from_url(url):
	stock_data = {}
	
	if 'newsID=' not in url:
		raise ValueError('Seems to be an invalid news link! Url:', url)
	
	# extract news id from url
	news_id = url.split('newsID=')[1]
	stock_data['news_id'] = news_id
	
	# get html
	for _ in range(ps.len()):
		try:
			response = requests.get(url, proxies = ps.get_next_proxy())
		except ProxyError:
			ps.remove_current_proxy()
		else:
			break
	
	# beautifulsoup
	encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
	soup = BeautifulSoup(response.content, 'html.parser', from_encoding = encoding)
	soup = soup.find(class_ = 'news_detail')

	# get wkn, isin and country
	stock_data['wkn'] = None
	stock_data['isin'] = None
	stock_data['country'] = None
	elems = soup.find('ul').find_all('li')
	for elem in elems:
		item = elem.get_text().strip().split(':')
		if len(item) == 2:
			if item[0].lower() == 'wkn':
				stock_data['wkn'] = item[1].strip()
			elif item[0].lower() == 'isin':
				stock_data['isin'] = item[1].strip()
			elif item[0].lower() == 'land':
				stock_data['country'] = item[1].strip()

	# get company name
	elems = soup.find_all('h2', {'class': 'darkblue'})
	stock_data['company'] = elems[0].get_text()

	# get date and time
	date = datetime.strptime(re.search(r'\d{2}.\d{2}.\d{4}', elems[1].get_text()).group(), '%d.%m.%Y').date()
	time = datetime.strptime(re.search(r'\d{2}:\d{2}', elems[1].get_text()).group(), '%H:%M').time()
	stock_data['date'] = str(date)
	stock_data['time'] = str(time)

	# get news text
	stock_data['text'] = soup.find('div', {'class': 'break-word news_main'}).get_text()

	return stock_data

stock_data = []
url_list_eng = []

# grab urls
print('Collecting news links')
num_pages = 100
base_url = get_url('news_url')
prepend_url = get_url('news_prepend_url')
pbar = tqdm(range(1, num_pages + 1))

for i in pbar:
	pbar.set_description('Processing page {} of {}, collected urls: {}'.format(i, num_pages, len(url_list_eng)))
	url_list = get_dgap_adhoc_news_urls(base_url.format(i), prepend_url)
	# get english links
	url_list_eng += get_dgap_adhoc_news_urls_english(url_list, prepend_url)

print('Collected urls:', len(url_list_eng), 'of:' num_pages * 20)

# scrape stock data
print('Scrape news data:')
pbar = tqdm(url_list_eng)
i = 0

for url in pbar:
	pbar.set_description('Processing link {} of {}'.format(i, len(url_list_eng)))
	logger.debug(url)
	stock_data.append(get_stock_data_from_url(url))
	i += 1

# write to json file
with open('stock_dgap_adhoc.json', 'w') as fp:
    json.dump(stock_data, fp, indent = 4, sort_keys = True)
