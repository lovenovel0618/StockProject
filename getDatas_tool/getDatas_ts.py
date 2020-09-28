import tushare as ts

token = "aeb486faf185df4d85b3a1d6aa4aa4e6379a03181a6a6cc7eaf31f85"

ts.set_token(token)
sid = '2303'
api = ts.pro_api()
api.us_basic()
data = api.stock_basic(fields='ts_code,name,list_date')
twdata = data[data['ts_code'].apply(lambda x: "TW" in x)]
print(twdata)
print(type(data))