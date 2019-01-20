import pandas as pd
import numpy as np
import json
from datetime import datetime
import requests
from requests.exceptions import ProxyError
import pandas_datareader as pdr
import time

with open('stock_dgap_adhoc.json') as fp:
	data = json.load(fp)
	
df = pd.DataFrame(data)

print(df.country.unique())

with open('country.txt', 'w') as fp:
	for country in df.country.unique():
		fp.write(country)
		fp.write('\n')
