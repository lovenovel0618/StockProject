"""
下載行情
"""
from dateutil.relativedelta import relativedelta
import pandas_datareader as pdr
import yfinance as yf
from datetime import datetime
from bin import get_info_path, get_price_path
from bin.scrawler import scrawler_date_infos
from bin.sqltools import *


info_path = get_info_path()
date_now = datetime.now()


# 更新個股公司資訊
def update_class_info(NewClass: dict = None):
    if NewClass:
        if not check_data_exists(info_path):
            data = [{"cname": "半導體", "cid": 71}, {"cname": "電子", "cid": 20}, {"cname": "電腦", "cid": 72},
                    {"cname": "光電", "cid": 73}, {"cname": "通信", "cid": 74}, {"cname": "電子零件", "cid": 75}]

            df = pd.DataFrame(data)
            save_database(DF=df, TableN="classID", Paths=info_path, IFExist='replace')

        else:
            df = pd.DataFrame([NewClass])
            save_database(DF=df, TableN="classID", Paths=info_path, IFExist='append')


# 更新 每日行情
def update_cStock_info(NewStock: dict = None) -> None:
    # data = [{"sid": 2302, "sname": "麗正", "cid": 71}, {"sid": 2303, "sname": "聯電", "cid": 71}]

    df = pd.DataFrame([NewStock])
    save_database(DF=df, TableN="cStockID", Paths=info_path, IFExist='append')

    sid = NewStock.get("sid")
    cid = NewStock.get("cid")
    stock_path = get_price_path(SC=cid, SID=sid)
    info_df = get_stock_info(SID=sid)
    save_database(DF=info_df, TableN="infos", Paths=stock_path, IFExist="replace")


def get_stock_info(SID: int):     # 回傳 yfinance 之 股票ID 最大歷史行情
    stocks = yf.Ticker(f"{SID}.TW")
    info = pd.DataFrame([stocks.info]).T
    info.reset_index(inplace=True)
    info.rename(columns={'index': 'item_en', 0: 'describe'}, inplace=True)
    info["item_en"] = info["item_en"].apply(lambda x: str(x))
    info["describe"] = info["describe"].apply(lambda x: str(x))
    return info


def get_price_pdr(SID: int, StartDate):     # 回傳 pandas_datareader 之 股票ID 範圍至今行情
    end = datetime.now()
    df = pdr.get_data_yahoo(symbols=f'{SID}.TW', start=StartDate, end=end)
    df.reset_index(inplace=True)
    return df


def date_price_update(Date, Paths: str, SID: int, isPrint=False):
    date_info_df = scrawler_date_infos(Date=Date, SID=SID, isPrint=False)

    if isPrint:
        print(date_info_df)
    if date_info_df is not None:
        save_database(DF=date_info_df, TableN="dprice", Paths=Paths, IFExist="append")


def update_price_datas():
    stockIDs = get_database(Paths=info_path, TableN='cStockID').values

    for sid, sname, cid in stockIDs:
        print(f"================ {sid} {sname} 開始下載 ================")
        stock_path = get_price_path(SC=cid, SID=sid)
        flag = False            # 判斷是否已存在資料之開關

        if check_data_exists(Paths=stock_path):
            if table_exists(Paths=stock_path, TableN="dprice"):
                flag = True

        if flag:
            # data_df = reset_database(Paths=stock_path, TableN='dprice')
            # start_date = datetime.strptime(data_df.iat[0, 0], "%Y-%m-%d")
            # end_date = datetime.strptime(data_df.tail(1).values[0][0], "%Y-%m-%d")
            # first_date = datetime.strptime("2010-01-04", "%Y-%m-%d")            # 證交所最初之日期
            #
            # # 判斷 是否更新最新資訊
            # if ((date_now-end_date).days > 2 and date_now.isoweekday() == 1) or \
            #         ((date_now-end_date).days > 1 and date_now.isoweekday() != 1) and (date_now.isoweekday() != 7):
            #     if date_now.year == end_date.year:
            #         for month in range(end_date.month, date_now.month+1):
            #             date = datetime.strptime(f"{date_now.year}-{month}-01", "%Y-%m-%d")
            #             date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
            #     elif date_now.year > end_date.year:
            #         for month in range(end_date.month, 13):
            #             date = datetime.strptime(f"{end_date.year}-{month}-01", "%Y-%m-%d")
            #             date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
            #         for year in range(end_date.year+1, date_now.year+1):
            #             if year == date_now.year:
            #                 for month in range(1, date_now.month+1):
            #                     date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            #                     date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
            #             else:
            #                 for month in range(1, 13):
            #                     date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            #                     date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
            #     else:
            #         print("Error Datas")
            #
            # # 判斷 舊資訊是否都更新齊全
            # if (start_date - first_date).days != 0:
            #     for year in range(first_date.year, start_date.year+1):
            #         if year == start_date.year:
            #             for month in range(1, start_date.month+1):
            #                 date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            #                 date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
            #         else:
            #             for month in range(1, 13):
            #                 date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            #                 date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
            pass
        else:
            # 添加 股票公司 基本資訊
            info_df = get_stock_info(SID=sid)
            save_database(DF=info_df, TableN="infos", Paths=stock_path, IFExist="replace")

            # yRange = date_now.year-99+1-1911
            # for year in range(yRange):
            #     if year == 0:
            #         for month in range(date_now.month):
            #             date = date_now - relativedelta(years=year) - relativedelta(months=month)
            #             start_date = datetime.strptime(f"{date.year}-{date.month}", "%Y-%m")
            #             date_price_update(Date=start_date, Paths=stock_path, SID=sid, isPrint=False)
            #     else:
            #         for month in range(12):
            #             date = datetime.strptime(f"{date_now.year}-12-01", "%Y-%m-%d")
            #             date = date - relativedelta(years=year) - relativedelta(months=month)
            #             start_date = datetime.strptime(f"{date.year}-{date.month}", "%Y-%m")
            #             if start_date.year >= 2010:
            #                 date_price_update(Date=start_date, Paths=stock_path, SID=sid, isPrint=False)
            # reset_database(Paths=stock_path, TableN='dprice')
        print(f"================ {sid} {sname} 下載完畢 ================")


if __name__ == '__main__':
    # 新類別
    # newClass = dict(cname='電機', cid=13)
    # update_info(newClass)

    # 新股市公司
    # newStock = dict(sid=8261, sname='富鼎', cid=71)
    # update_cStock_info(newStock)

    # 下載每日行情
    update_price_datas()
    pass
