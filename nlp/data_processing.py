import pandas as pd
import numpy as np
import json
from datetime import datetime
import requests
from requests.exceptions import ProxyError
import pandas_datareader as pdr
import time
from proxy_server import ProxyServer
from get_url import get_url
from tqdm import tqdm

country_to_ticker_ending = {
	'Deutschland': '.DE',
	'Großherzogtum Luxemburg': '.F',
	'Österreich': '.VI',
	'Germany': '.DE',
	'Schweiz': '.SW',
	'Großbritannien': '.L',
	'Malta': '.DE',
	'Luxemburg': '.DE',
	'Finnland': '.DE',
	'Niederlande': '.DE',
	'The Netherlands': '.DE',
	'United Kingdom': '.L',
	'Sweden': '.DE',
	'China': '.DE',
	'Austria': '.VI',
	'Luxembourg': '.DE',
	'Switzerland': '.SW',
	}
	
def add_ticker_symbols(df):
	ps = ProxyServer()
	tickers = []
	url = get_url('ticker_url')
	headers = {'Content-Type':'text/json'}

	#for index, row in df.iterrows():
	for index, row in tqdm(df.iterrows(), total = df.shape[0]):
		# request ticker symbol for isin
		for i in range(ps.len()):
			try:
				payload = [{"idType": "ID_ISIN", "idValue": row['isin']}]
				response = requests.post(url, json = payload, headers = headers, proxies = ps.get_next_proxy())
			except ProxyError:
				ps.remove_current_proxy()
				tqdm.write('proxy error')
			else:
				break
				
		if row['country'] in country_to_ticker_ending:
			ending = country_to_ticker_ending[row['country']]
		else:
			ending = '.DE'
			tqdm.write('Unknown country:' + row['country'])
			
		try:
			jason_query = json.loads(response.text)
			tickers.append(jason_query[0]['data'][0]['ticker'] + ending)
		except:
			tickers.append(np.NaN)
		
	df['ticker'] = tickers
	return df
	
def add_stock_info(df):
	df_list = []
	dummy = {'High': [np.NaN], 'Low': [np.NaN], 'Open': [np.NaN], 'Close': [np.NaN], 'Volume': [np.NaN], 'perf': [np.NaN], 'label': [np.NaN]}

	#for index, row in df.iterrows():
	for index, row in tqdm(df.iterrows(), total = df.shape[0]):
		try:
			info = pdr.data.DataReader(row['ticker'], 'yahoo', start = row['date'], end = row['date'])
			info = info[:1]
			info = info.drop(columns = ['Adj Close'])
			perf = (1 - info['Open'].values[0] / info['Close'].values[0]) * 100
			info['perf'] = perf
			if perf >= 3.0:
				info['label'] = 1
			elif perf <= -3.0:
				info['label'] = 2
			else:
				info['label'] = 0
		except:
			info = pd.DataFrame(data = dummy)
		
		df_list.append(info)
		
	info = pd.concat(df_list, ignore_index = True, sort = True)
		
	return df.join(info)

# load data	
with open('stock_dgap_adhoc2.json') as fp:
	data = json.load(fp)
	
# convert to pandas dataframe
df = pd.DataFrame(data)

# get ticker symbols
df = add_ticker_symbols(df)
print('Found tickers for:', len(df[df.ticker != np.NaN]), 'of:', len(df.index), 'total datasets.')

# get stock price info
df = add_stock_info(df)
print('Datasets count:', len(df.index), 
	  'Pos:', len(df[df.label == 1]), 
	  'Neg:', len(df[df.label == 2]), 
	  'Neutral:', len(df[df.label == 0]))

# save to json file
with open('stock_dgap_adhoc2_with_price.json', 'w') as fp:
    json.dump(json.loads(df.to_json(orient = 'index')), fp, indent = 4, sort_keys = True)
	
# save datasets with errors to a different file
df = df[np.isnan(df['perf'])]
with open('stock_dgap_adhoc2_unknown.json', 'w') as fp:
    json.dump(json.loads(df.to_json(orient = 'index')), fp, indent = 4, sort_keys = True)
