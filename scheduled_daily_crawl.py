#  -*- coding: utf-8 -*-

import schedule
from daily_crawler import Crawler
import time
# from datetime import datetime
from datetime import datetime as dt, timedelta 

"""
每天下午20:30执行抓取，只有周一到周五才真正执行抓取任务
"""


def crawl_daily():
    """
    每日的定时抓取
    """

    # 初始化抓取日线数据类
    dc = Crawler()
    # 获取当前时间
    now_date = dt.now()
    previous_now_date = dt.now() + timedelta(days=-1) 
    #时间格式转为字符串格式
    now = now_date.strftime('%Y-%m-%d')
    previous_now =  previous_now_date.strftime('%Y-%m-%d')
    
    # 获取今天星期几，周日-周六：0-6
    weekday = int(now_date.strftime('%w'))
      
    # 只有周一到周五执行
    if 0 < weekday < 6:
        # 当前日期         
        c = Crawler()
        l = ['L1','L2','L3']
        for i in l: #抓取当日申万指数行情信息
            c.crawl_index_sw(begin_date=previous_now, end_date=previous_now,level=i) #抓取前一天的申万指数行情信息
        c.crawl_etf(begin_date=now, end_date=now) #抓取当日ETF行情信息
        c.crawl_index(begin_date=now, end_date=now) #抓取当日指数行情信息
        c.crawl(begin_date=now, end_date=now) #抓取当日股票行情信息


# 定时任务的启动入口
if __name__ == '__main__':
    # 设定每天下午20:30执行抓取任务
    schedule.every().day.at("20:30").do(crawl_daily)
    # 通过无限循环，执行任务检测
    while True:
        # 每10秒检测一次
        schedule.run_pending()
        time.sleep(10)