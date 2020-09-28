from numpy.random import uniform
from dateutil.relativedelta import relativedelta
import pandas_datareader as pdr
from datetime import datetime
from config import Config
import pandas as pd
import requests
import time
import re


def scrawlUrl(Urls, Date, SID):         # 爬取網址
    scrawldate = Date.strftime('%Y%m%d')
    # print(scrawldate)
    url = Urls.format(scrawldate=scrawldate, sid=SID)
    # print(url)
    count = 0

    while True:
        try:
            req = requests.get(url=url, headers=Config.HEADERS, timeout=10)
            time.sleep(round(uniform(3, 6), 1))
        except Exception as e:
            print("Error:", e)
            return None

        if req.status_code != 200:
            return None
        else:
            try:
                content = req.json()
                if content["data"]:
                    return content
            except Exception as e:
                print("Error:", e)
                time.sleep(10)
                count += 1

        if count > 10:
            return None


# 原始資料數值 轉成 正式資料數值
def trans_Value(Values: str, byTpye: str):
    checkV = Values.replace(" ", "")

    if (Values is None) or ("None" in Values) or ("none" in Values) or ("" == checkV):
        return None

    regular = re.compile('[0-9]+')
    check = regular.findall(Values)

    if check:
        if byTpye == "f":
            value = Values.replace(",", "")
            return round(float(value), 2)

        if byTpye == "i":
            value = Values.replace(",", "")
            return int(float(value))
        if byTpye == 's':
            return Values
    else:
        return None


# 原始資料日期 轉成 統一正式資料日期
def trans_Date(dateV):
    date_ls = re.findall(r"\d+\.?\d*", dateV)
    if int(date_ls[0]) > 500:
        print(date_ls)
    date_ls[0] = str(int(date_ls[0]) + 1911)
    return "-".join(date_ls)


# 個股日成交資訊
def scrawl_stock_day(Date, SID: int, isPrint=False):
    info = list()
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={scrawldate}&stockNo={sid}"
    content = scrawlUrl(Urls=url, Date=Date, SID=SID)
    # print(content)
    # datas = pd.DataFrame(content.get('data'), columns=content.get('fields'))
    # print(datas)

    datas = pd.DataFrame()
    if content is not None:
        if content['data']:
            for data in content['data']:
                record = {
                    'date': data[0],
                    'svolume': data[1],          # 成交股數
                    'amount': data[2],        # 成交金額
                    'open': data[3],
                    'high': data[4],
                    'low': data[5],
                    'close': data[6],
                    'margin': data[6],          # 漲跌價差
                    'volume': data[8]            # 成交筆數
                }
                info.append(record)
            datas = pd.DataFrame(info)
            datas["date"] = datas["date"].apply(lambda x: trans_Date(x))
            datas.loc[:, ["svolume", "amount", "volume"]] = datas.loc[:, ["svolume", "amount", "volume"]].applymap(
                lambda x: trans_Value(Values=x, byTpye='i')
            )
            datas.iloc[:, 3:8] = datas.iloc[:, 3:8].applymap(lambda x: trans_Value(Values=x, byTpye='f'))
            datas.set_index(["date"], inplace=True)

    if isPrint:
        print(datas)
    return datas


# 個股日本益比、殖利率及股價淨值比
def scrawl_stock_BWIBBU(Date, SID: int, isPrint=False):
    url = "https://www.twse.com.tw/exchangeReport/BWIBBU?response=json&date={scrawldate}&stockNo={sid}"
    content = scrawlUrl(Urls=url, Date=Date, SID=SID)
    # print(content)
    datas = pd.DataFrame()
    if content is not None:
        if content['data']:
            datas = pd.DataFrame(content.get('data'), columns=content.get('fields'))
            datas = datas.loc[:, ['日期', '殖利率(%)', '本益比', '股價淨值比']]
            datas.rename(columns={'日期': 'date', '殖利率(%)': 'yield', '本益比': 'per', '股價淨值比': 'pbr'}, inplace=True)
            # print(datas)
            datas["date"] = datas["date"].apply(lambda x: trans_Date(x))
            datas.loc[:, ["yield", "per", "pbr"]] = datas.loc[:, ["yield", "per", "pbr"]].applymap(
                lambda x: trans_Value(Values=x, byTpye='f')
            )
            datas.set_index(["date"], inplace=True)
    if isPrint:
        print(datas)
    return datas


# 取得 yahoo 資料
def scrawl_date_pdr(Date, SID: int, isPrint=False):
    now = datetime.now()

    if (Date.year == now.year) and (Date.month == now.month):
        end = datetime.now()
    else:
        end = Date + relativedelta(months=1) - relativedelta(days=1)

    df = pdr.get_data_yahoo(symbols=f'{SID}.TW', start=Date, end=end)
    df.reset_index(inplace=True)
    df.rename(columns={'Adj Close': 'Adj_Close', 'Date': 'date'}, inplace=True)

    df["Adj_Close"] = df["Adj_Close"].apply(lambda x:  round(x, 2))
    df["date"] = df["date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    # print(df)

    df.set_index(["date"], inplace=True)
    if isPrint:
        print(df)
    return df.loc[:, ["Adj_Close"]]


def scrawl_date_info(Date, SID: int, isPrint=False):
    print(f"=====> {SID} 更新 {Date.year}-{Date.month}")
    price_df = scrawl_stock_day(Date=Date, SID=SID)
    bwibbu_df = scrawl_stock_BWIBBU(Date=Date, SID=SID)
    pdr_df = scrawl_date_pdr(Date=Date, SID=SID)
    if (len(price_df) > 0) and (len(bwibbu_df) > 0):
        data_df = pd.concat([price_df, bwibbu_df], axis=1)
        data_df = pd.concat([data_df, pdr_df], axis=1, join='inner')
        data_df.reset_index(inplace=True)
        data_df.rename(columns={'index': 'date'}, inplace=True)
    else:
        print(f"{Date} 無檔案!!")
        return None

    if isPrint:
        print(data_df)
        # print(data_df.columns)
    return data_df


def scrawler_date_infos(Date, SID: int, isPrint=False):
    count = 0
    while True:
        try:
            df = scrawl_date_info(Date=Date, SID=SID, isPrint=isPrint)
            time.sleep(3)
            return df
        except Exception as e:
            print("休息一下")
            time.sleep(60)
            count += 1
        if count > 5:
            return pd.DataFrame()


if __name__ == "__main__":
    date = datetime.strptime("2010-06", "%Y-%m")
    sid = 3686
    scrawl_date_info(Date=date, SID=sid, isPrint=True)
    # scrawl_stock_BWIBBU(Date=date, SID=sid, isPrint=True)
