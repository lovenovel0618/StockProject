import yfinance as yf

sid = '2302'
start = '2015-01-01'
# stock = yf.download(f"{sid}.TW", start=start)
stocks = yf.Ticker(f"{sid}.TW")
# df = pro.index_daily(ts_code=f'{sid}.TW', start_date='20180101', end_date='20181010')
print(stocks.history(period="max"))
# print(stocks.info)
# print(stocks.financials)
