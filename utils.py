import asyncio
import datetime

import records
import tushare as ts

db_conn = records.Database('mysql+pymysql://root:rootROOT123@106.55.234.173:3306/stock_db?charset=utf8',
                           pool_size=50, pool_pre_ping=True, encoding='utf-8')
tushare_api = ts.pro_api('9d08ef105cb484c3fb9ad5495942e92d34e3f1caa4df2fa7fb2d4d0f')


def trade_calender():
    _df = tushare_api.trade_cal(end_date=datetime.datetime.now().strftime('%Y%m%d'), is_open='1')
    # 获取交易日历
    _df_date = _df.sort_values('cal_date', ascending=False)
    return list(map(lambda x: getattr(x, 'cal_date'), _df_date.itertuples()))


def make_it_async(func, *args):
    loop = asyncio.get_event_loop() or asyncio.new_event_loop()
    return loop.run_in_executor(None, func, *args)


def query_or_execute(sql, params=None, db=db_conn, as_dict=True):
    if params is None:
        params = {}
    _sql = sql.strip().lower()
    if _sql.startswith('select'):
        return db.get_connection().query(sql, **params).as_dict() \
            if as_dict else db.get_connection().query(sql, **params)
    else:
        return db.get_connection().query(sql, **params)


def async_query_or_execute(sql, params=None, db=db_conn):
    return make_it_async(query_or_execute, sql, params, db)
