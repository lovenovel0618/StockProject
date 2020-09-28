"""
下載行情
"""
import pandas_datareader as pdr
from datetime import datetime
from bin import get_info_path, get_price_path
from bin.scrawler import scrawler_date_infos, scrawl_stock_company_info
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


# 更新 股市公司資訊
def update_cStock_info(NewStock: dict = None) -> None:
    # data = [{"sid": 2302, "sname": "麗正", "cid": 71}, {"sid": 2303, "sname": "聯電", "cid": 71}]

    df = pd.DataFrame([NewStock])
    save_database(DF=df, TableN="cStockID", Paths=info_path, IFExist='append')

    if table_exists(Paths=info_path, TableN="company"):
        companies_info = get_database(Paths=info_path, TableN="company")
        save_date = companies_info.iat[0, 0].split("/")
        save_date[0] = str(int(save_date[0]) + 1911)
        save_date = "-".join(save_date)
        save_date = datetime.strptime(save_date, "%Y-%m-%d")
        if save_date < date_now:
            companies_info = scrawl_stock_company_info()
            save_database(DF=companies_info, TableN="company", Paths=info_path, IFExist="replace")
    else:
        companies_info = scrawl_stock_company_info()
        save_database(DF=companies_info, TableN="company", Paths=info_path, IFExist="replace")

    sid = NewStock.get("sid")
    cid = NewStock.get("cid")
    info_df = get_stock_info(sid)
    # print(info_df)
    stock_path = get_price_path(SC=cid, SID=sid)
    save_database(DF=info_df, TableN="infos", Paths=stock_path, IFExist="replace")


def get_stock_info(SID: int):     # 取得各公司之資訊
    companies_info = get_database(Paths=info_path, TableN="company")
    info_df = companies_info[companies_info["公司代號"].isin([SID])]
    info_df.reset_index(drop=True, inplace=True)
    info_df = info_df.T
    info_df.reset_index(inplace=True)
    info_df.rename(columns={'index': 'item_en', 0: 'describe'}, inplace=True)
    save_date = info_df.iat[0, 1].split("/")
    save_date[0] = str(int(save_date[0]) + 1911)
    info_df.iat[0, 1] = "-".join(save_date)
    return info_df


def get_price_pdr(SID: int, StartDate):     # 回傳 pandas_datareader 之 股票ID 範圍至今行情
    end = datetime.now()
    df = pdr.get_data_yahoo(symbols=f'{SID}.TW', start=StartDate, end=end)
    df.reset_index(inplace=True)
    return df


# 更新 每日行情
def date_price_update(Date, Paths: str, SID: int, isPrint=False):
    date_info_df = scrawler_date_infos(Date=Date, SID=SID, isPrint=False)

    if isPrint:
        print(date_info_df)
    if date_info_df is not None:
        save_database(DF=date_info_df, TableN="dprice", Paths=Paths, IFExist="append")


def update_price_datas():
    stockIDs = get_database(Paths=info_path, TableN='cStockID').values
    rule_date = datetime.strptime("2010-01-04", "%Y-%m-%d")            # 證交所最初之日期

    for sid, sname, cid in stockIDs:
        print(f"================ {sid} {sname} 開始下載 ================")
        stock_path = get_price_path(SC=cid, SID=sid)
        flag = False            # 判斷是否已存在資料之開關

        if check_data_exists(Paths=stock_path):
            if table_exists(Paths=stock_path, TableN="dprice"):
                flag = True

        companyOpenDate = datetime.strptime(get_database(Paths=stock_path, TableN="infos").iat[15, 1], "%Y%m%d")
        if flag:
            data_df = reset_database(Paths=stock_path, TableN='dprice')
            end_date = datetime.strptime(data_df.tail(1).values[0][0], "%Y-%m-%d")

            # 判斷 是否更新最新資訊
            if ((date_now-end_date).days > 2 and date_now.isoweekday() == 1) or \
                    ((date_now-end_date).days > 1 and date_now.isoweekday() != 1) and (date_now.isoweekday() != 7):
                if date_now.year == end_date.year:
                    for month in range(end_date.month, date_now.month+1):
                        date = datetime.strptime(f"{date_now.year}-{month}-01", "%Y-%m-%d")
                        date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
                elif date_now.year > end_date.year:
                    for month in range(end_date.month, 13):
                        date = datetime.strptime(f"{end_date.year}-{month}-01", "%Y-%m-%d")
                        date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
                    for year in range(end_date.year+1, date_now.year+1):
                        if year == date_now.year:
                            for month in range(1, date_now.month+1):
                                date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
                                date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
                        else:
                            for month in range(1, 13):
                                date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
                                date_price_update(Date=date, Paths=stock_path, SID=sid, isPrint=False)
                else:
                    print("Error Datas")
            pass
        else:
            yRange = rule_date.year if companyOpenDate <= rule_date else companyOpenDate.year
            for year in range(yRange, date_now.year+1):
                if year == date_now.year:
                    for month in range(1, date_now.month+1):
                        start_date = datetime.strptime(f"{year}-{month}", "%Y-%m")
                        date_price_update(Date=start_date, Paths=stock_path, SID=sid, isPrint=False)
                elif year == companyOpenDate.year:
                    for month in range(companyOpenDate.month, 13):
                        start_date = datetime.strptime(f"{year}-{month}", "%Y-%m")
                        date_price_update(Date=start_date, Paths=stock_path, SID=sid, isPrint=False)
                else:
                    for month in range(1, 13):
                        start_date = datetime.strptime(f"{year}-{month}", "%Y-%m")
                        date_price_update(Date=start_date, Paths=stock_path, SID=sid, isPrint=False)
            reset_database(Paths=stock_path, TableN='dprice')
            pass
        print(f"================ {sid} {sname} 下載完畢 ================")


if __name__ == '__main__':
    # 新類別
    # newClass = dict(cname='電機', cid=13)
    # update_info(newClass)

    # 新股市公司
    # newStock = dict(sid=8261, cid=71)
    # update_cStock_info(newStock)

    # 下載每日行情
    update_price_datas()
    pass
