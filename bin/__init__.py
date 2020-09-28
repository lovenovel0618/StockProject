from config import Config
from pathlib import Path


def get_price_path(SC: str, SID) -> str:     # 取得股票之路徑
    """
    :param SC: 股類ID
    :param SID: 股票代號 stock id
    :return:
    """
    _path = Path(Config.ROOT) / 'databases' / f'c{SC}/{SID}TW'
    _path.mkdir(parents=True, exist_ok=True)
    _path = _path / 'price.sqlite3'
    return str(_path)


def get_info_path() -> str:     # 取得基本資料之路徑
    _path = Path(Config.ROOT) / 'databases'
    _path.mkdir(parents=True, exist_ok=True)
    _path = _path / 'info.sqlite3'
    return str(_path)
