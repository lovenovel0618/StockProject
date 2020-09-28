from bin import Path
import sqlite3
import pandas as pd
import re


def check_data_exists(Paths: str):
    return Path(Paths).exists()


def table_exists(Paths: str, TableN: str):        # 这个函数用来判断表是否存在
    _conn = sqlite3.connect(Paths)
    cursorObj = _conn.cursor()
    cursorObj.execute('SELECT name from sqlite_master where type= "table"')

    tableNs = [tn[0] for tn in cursorObj.fetchall()]
    return TableN in tableNs


def save_database(DF: pd.DataFrame, Paths, TableN, IFExist: str) -> None:
    _conn = sqlite3.connect(Paths)

    try:
        DF.to_sql(name=TableN, con=_conn, if_exists=IFExist, index=False)
        print("存檔完畢!!")
    except sqlite3.IntegrityError:
        print(f"{DF.iat[0, 0]} 已存在!!")


def get_database(Paths: str, TableN: str) -> pd.DataFrame:
    _conn = sqlite3.connect(Paths)
    sql_cmd = f"SELECT * FROM {TableN}"
    return pd.read_sql(sql=sql_cmd, con=_conn)


def reset_database(Paths: str, TableN: str) -> pd.DataFrame:        # 重新 整理資瞭
    df = get_database(Paths=Paths, TableN=TableN)
    # print(df)
    df.drop_duplicates(keep='first', inplace=True)
    df.sort_values(axis=0, by='date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    # print(df)
    save_database(DF=df, TableN=TableN, Paths=Paths, IFExist='replace')
    return df


if __name__ == '__main__':
    pass
