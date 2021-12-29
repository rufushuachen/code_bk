#  -*- coding: utf-8 -*-


import traceback
from datetime import datetime, timedelta
import tushare as ts
from pandas.io import json
from pymongo import UpdateOne
from base import DB_CONN,ts_pro
# from stock_util import get_trading_dates

"""
从tushare获取股票基础数据，保存到本地的MongoDB数据库中
"""
class Crawl_basic(object):
    def __init__(self):
        self.pro = ts_pro()
        

    def crawl_basic(self,begin_date=None, end_date=None):
        """
        抓取指定时间范围内的股票基础信息
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """
        # 如果没有指定开始日期，则默认为前一日
        if begin_date is None:
            begin_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        # 如果没有指定结束日期，则默认为前一日
        if end_date is None:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        #通过tushare接口，获取交易日历
        df = self.pro.trade_cal(exchange='SSE', is_open='1', 
                                start_date=begin_date, 
                                end_date=end_date, 
                                fields='cal_date')
        all_dates = list(df.cal_date)
        # 按照每个交易日抓取
        for date in all_dates:
            try:
                # 抓取当日的基本信息
                self.crawl_basic_at_date(date)
            except:
                print('抓取股票基本信息时出错，日期：%s' % date, flush=True)

    def crawl_basic_at_date(self,date):
        """
        从Tushare抓取指定日期的股票基本信息
        :param date: 日期
        """
        
        df = self.pro.bak_basic(trade_date=date)
        print(df)
        # 初始化更新请求列表
        update_requests = []
        for i in list(df.index):
            #获取一只股票的数据
            doc = dict(df.loc[i])
            try:
                # API返回的数据中，上市日期是一个int类型。将上市日期，20180101转换为2018-01-01的形式
                time_to_market = doc['list_date']
                # 将总股本和流通股本转为数字
                totals = doc['total_share']
                outstanding = doc['float_share']
                code = doc['ts_code']
                 # 组合成基本信息文档
                doc.update({
                    # 股票代码
                    'code': code,
                    # 日期
                    'date': date,
                    # 上市日期
                    'timeToMarket': time_to_market,
                    # 流通股本
                    'outstanding': outstanding,
                    # 总股本
                    'totals': totals
                })
                # 生成更新请求，需要按照code和date创建索引
                # tushare
                # numpy.int64/numpy.float64等数据类型，保存到mongodb时无法序列化。
                # 解决办法：这里使用pandas.json强制转换成json字符串，然后再转换成dict。int64/float64转换成int,float
                #创建索引，db.basic.createIndex({'code':1,'date':1,'index':1},{'background':true})
                update_requests.append(
                UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': json.loads(json.dumps(doc))}, upsert=True))                
            
            except:
                print('发生异常，股票代码：%s，日期：%s' % (code, date), flush=True)
                print(doc, flush=True)
                print(traceback.print_exc())
                    
        # 如果抓到了数据
        print(update_requests)
        if len(update_requests) > 0:
            update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)

            print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %
                (date, update_result.upserted_count, update_result.modified_count), flush=True)
          
if __name__ == '__main__':
    cb = Crawl_basic()
    cb.crawl_basic('20181115','20211220')

        
    