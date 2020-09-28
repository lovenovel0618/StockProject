import pandas_datareader as pdr
from datetime import datetime, timedelta

sid = '2303'
start = datetime.now() - timedelta(days=360*5)
end = datetime.now()
stock_dr = pdr.get_data_yahoo(f'{sid}.TW', start, end)
print(stock_dr)