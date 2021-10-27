import pandas as pd
import tushare

from utils import tushare_api, async_query_or_execute, trade_calender


async def task_get_st():
    df1 = tushare_api.stock_basic(**{"market": "中小板", "list_status": "L"},
                                  fields=["ts_code", "name", "industry", "market"])
    df2 = tushare_api.stock_basic(**{"market": "主板", "list_status": "L"},
                                  fields=["ts_code", "name", "industry", "market"])
    all_stocks = pd.concat([df1, df2])
    sql = '''
    insert into stock(ts_code, name, industry, market) values 
    '''
    param_list = []
    for s in all_stocks.itertuples():
        param_list.append(f"('{getattr(s, 'ts_code')}', '{getattr(s, 'name')}', "
                          f"'{getattr(s, 'industry')}', '{getattr(s, 'market')}')")
    sql += ','.join(param_list)
    sql += '''
    on duplicate key update
    name = values(name),
    industry = values(industry),
    market = values(market)
    '''
    await async_query_or_execute(sql)


async def task_get_st_daily_data():
    date_list = trade_calender()
    sql = 'select trade_date from stock_daily order by trade_date desc limit 1'
    last = await async_query_or_execute(sql)
    last = last[0] if len(last) > 0 else '20181228'
    insert_list = date_list[:date_list.index(last)]
    sql = 'select ts_code from stock'
    stocks = await async_query_or_execute(sql)
    stocks = list(map(lambda x: x['ts_code'], stocks))
    db_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', ('close', 'close_x'), 'pre_close', ('chg', 'change'),
                  'pct_chg', 'vol', 'amount', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb',
                  'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share', 'float_share', 'free_share', 'total_mv',
                  'circ_mv']
    insert_list.reverse()

    async def _(ts_code_lst):
        for date in insert_list:
            df_base = tushare_api.daily(ts_code=','.join(ts_code_lst), start_date=date, end_date=date)
            df_more = tushare_api.daily_basic(ts_code=','.join(ts_code_lst), trade_date=date)
            df = pd.merge(df_base, df_more, on=['ts_code', 'trade_date'])
            df.fillna(0, inplace=True)
            sql_ = f"insert into stock_daily " \
                   f"({','.join([x if isinstance(x, str) else x[0] for x in db_columns])}) " \
                   f"values "
            value_list = []
            for daily_data in df.itertuples():
                params = map(lambda x: "'" + str(getattr(daily_data, x if isinstance(x, str) else x[1])) + "'",
                             db_columns)
                value_list.append("(" + ','.join(params) + ")")
            sql_ += ','.join(value_list)
            print(f'insert daily data {date}')
            await async_query_or_execute(sql_)

    while len(stocks) > 0:
        await _(stocks[:100])
        stocks = stocks[100:]


async def _update_ma():
    sql = 'select ts_code from stock'
    stocks = await async_query_or_execute(sql)
    stocks = list(map(lambda x: x['ts_code'], stocks))
    ma_list = [5, 10, 20, 30, 40, 60, 120, 255]
    update_sql = f'insert into stock_daily ' \
                 f'(ts_code, trade_date, {",".join(map(lambda x: f"m{x}", ma_list))}) values '
    value_list = []
    for s in stocks:
        sql = '''
        select * from stock_daily where ts_code=:ts_code order by trade_date desc
        '''
        st_data = await async_query_or_execute(sql, {'ts_code': s})
        df = tushare.pro_bar(ts_code=s, adj='qfq',
                             start_date=st_data[-1]['trade_date'],
                             end_date=st_data[0]['trade_date'],
                             ma=ma_list,
                             api=tushare_api)
        if df is not None:
            df.fillna(0, inplace=True)
            for d in df.itertuples():
                pass
