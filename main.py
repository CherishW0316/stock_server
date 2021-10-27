from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sanic import Sanic

from task import task_get_st, task_get_st_daily_data

app = Sanic(__name__)


@app.listener('before_server_start')
async def before_server_start(_app, loop):
    pass


@app.listener('after_server_start')
async def after_server_start(_app, loop):
    _app.scheduler = AsyncIOScheduler()
    _app.scheduler.add_job(task_get_st, 'cron', id='每日更新stock列表', hour=15, minute=10)
    _app.scheduler.add_job(task_get_st_daily_data, 'cron', id='每日更新stock_daily数据', hour=23, minute=10)
    _app.scheduler.start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8910)
