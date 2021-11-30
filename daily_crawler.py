#  -*- coding: utf-8 -*-

from typing_extensions import TypeGuard, TypedDict
from pymongo import UpdateOne
#from tushare.stock.fundamental import get_stock_basics
from base import DB_CONN,ts_pro
import tushare as ts
from datetime import datetime
from time import sleep 
from pymongo import errors
import pandas as pd 
from abc import ABCMeta, abstractmethod

"""
从tushare获取日K数据，保存到本地的MongoDB数据库中
"""

class DailyCrawler(object):
    def __init__(self):
         # 创建daily数据集
        self.daily = DB_CONN['daily']
        # 创建daily_hfq数据集
        self.daily_hfq = DB_CONN['daily_hfq']
        self.daily_sw1 = DB_CONN['daily_sw_1']
        self.daily_sw2 = DB_CONN['daily_sw_2']
        self.daily_sw3 = DB_CONN['daily_sw_3']
        self.pro = ts_pro()

    @abstractmethod
    def crawl_index_sw(self):
        raise NotImplemntedError('Should implement generate sw data !')       
 
    @abstractmethod
    def get_sw_list(self):
        raise NotImplemntedError('Should implement generate sw list !')

    def get_daily_sw(self,code,begin_date=None,end_date=None):
        for _ in range(3):
            try:
                if code:
                    df_daily = self.pro.sw_daily(ts_code=code, start_date=begin_date, end_date=end_date)                     
                    
            except:
                sleep(1)
            else:
                return df_daily
    
    def crawl_index(self,begin_date=None,end_date=None):
        """
        抓取指数的日K数据。
        指数行情的主要作用：
        1. 用来生成交易日历
        2. 回测时做为收益的对比基准
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """       
        index_codes = ['000001.SH', '000300.SH'] 
        # 当前日期
        now = datetime.now().strftime('%Y%m%d')
        # 如果没有指定开始，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日，则默认为当前日期
        if end_date is None:
            end_date = now

        # 按照指数的代码循环，抓取所有指数信息
        for code in index_codes:
            # 抓取一个指数的在时间区间的数据
            df_daily = self.get_daily(code,type='I',begin_date=begin_date,end_date=end_date)
            if (isinstance(df_daily,pd.DataFrame)) and (not df_daily.empty):
                # 保存数据
                self.save_data(code, df_daily, self.daily, {'index': True,'ETF':False})
            else:
                print(code)
                print('data is empty')
            


    def get_daily(self,code,type,begin_date,end_date,adj=None):
        for _ in range(3):
            try:
                if code:
                    df_daily = ts.pro_bar(ts_code=code, asset=type, start_date=begin_date, end_date=end_date,adj=adj)                     
                    
            except:
                sleep(1)
            else:
                return df_daily
            
        
    def crawl(self,begin_date=None,end_date=None):
        """
        抓取股票的日K数据，主要包含了不复权和后复权两种
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """
        stock_list = self.get_stock_list()
        # 当前日期
        now = datetime.now().strftime('%Y%m%d')
        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now
        # print(stock_list[0])
         
        for code in stock_list:            
            # 抓取不复权的价格
            df_daily = self.get_daily(code,type='E',begin_date=begin_date,end_date=end_date)
            if (isinstance(df_daily,pd.DataFrame)) and (not df_daily.empty):
                
                self.save_data(code, df_daily, self.daily, {'index':False,'ETF':False})
            else:
                print(code)
                print('data is empty')

            #抓取后复权的价格
            df_daily_hfq = self.get_daily(code,type='E',begin_date=begin_date,end_date=end_date,adj='hfq')
            #数据框不为空，同时返回值是数据框类型
            if (isinstance(df_daily_hfq,pd.DataFrame)) and (not df_daily_hfq.empty):
                self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False,'ETF':False})
            else:
                print(code)
                print('data is empty')
                    
       
    def get_stock_list(self):
        #查询当前所有正常上市交易的股票列表
        for _ in range(3):
            try:
                stock_list = list(self.pro.stock_basic(exchange='', list_status='L', fields='ts_code').ts_code)                     
                    
            except:
                sleep(1)
            else:
                return stock_list

    def crawl_etf(self,begin_date=None,end_date=None):
        etf_list = self.get_etf_list()
        # 当前日期
        now = datetime.now().strftime('%Y%m%d')
        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now
               
        for etf in etf_list:                       
            df_daily = self.get_daily(etf,type='FD',begin_date=begin_date,end_date=end_date)
            if (isinstance(df_daily,pd.DataFrame)) and (not df_daily.empty):            
                self.save_data(etf, df_daily, self.daily, {'index':False,'ETF':True})
            else:
                print(etf)
                print('data is empty')
            
       
    def get_etf_list(self):
        for _ in range(3):
            try:                
                df = self.pro.fund_basic(market='E')
            except:
                sleep(1)
            else:
                res = df[df.name.str.contains('ETF')]
                etf_list = list(res.ts_code)        
                return etf_list       
                
            
    def save_data(self,code,df_daily,collection,extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """       
        # 数据更新的请求列表
        update_requests = []
        for df_index in df_daily.index:
            #将DataFrame中的一行数据转dict
            doc =  dict(df_daily.loc[df_index])
            doc['code'] = code 
            #如果指定了其他字段，则更新dict
            if extra_fields is not None:
                doc.update(extra_fields)
            #生成一条数据库的更新请求
            # 注意：
            # 需要在code、date、index三个字段上增加索引，否则随着数据量的增加，
            # 写入速度会变慢，需要创建索引。创建索引需要在MongoDB-shell中执行命令式：
            # db.daily.createIndex({'code':1,'date':1,'index':1},{'background':true})
            # db.daily_hfq.createIndex({'code':1,'date':1,'index':1},{'background':true})
            # db.daily_sw_2.createIndex({'code':1,'date':1,'index':1},{'background':true})
            #查看索引db.daily_sw.getIndexes()
            update_requests.append(
                UpdateOne(
                    {'code':doc['code'],'date':doc['trade_date'],'index':doc['index'],'ETF':doc['ETF']},
                    {'$set':doc},
                    upsert=True                    
                )
            )
        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            try:
                update_result = collection.bulk_write(update_requests, ordered=False)
                 
            except errors.BulkWriteError as e:
                print(e.detals)

            else:
                print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
                    (code, update_result.upserted_count, update_result.modified_count),
                    flush=True)  

class Crawler(DailyCrawler):
    def __init__(self,level='L1'):
        super(Crawler,self).__init__()
        self.level = level    
    def get_sw_list(self):
        level = self.level
        for _ in range(3):
            try:
                df = self.pro.index_classify(level=level, src='SW2021')
                index_codes = list(df.index_code)
            except:
                sleep(1)
            else:
                return index_codes

    def crawl_index_sw(self,begin_date=None,end_date=None):
        """
        获取申万指数
        """
        index_codes = self.get_sw_list()        
        # 当前日期
        now = datetime.now().strftime('%Y%m%d')
        # 如果没有指定开始，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日，则默认为当前日期
        if end_date is None:
            end_date = now
        level = self.level 
        if level == 'L1':
            # 按照指数的代码循环，抓取所有指数信息
            for code in index_codes:
                # 抓取一个指数的在时间区间的数据
                df_daily = self.get_daily_sw(code,begin_date=begin_date,end_date=end_date)
                # 保存数据
                if (isinstance(df_daily,pd.DataFrame)) and not (df_daily.empty):
                    self.save_data(code, df_daily, self.daily_sw1, {'index': True,'ETF':False})
        elif level == 'L2':
             # 按照指数的代码循环，抓取所有指数信息
            for code in index_codes:
                # 抓取一个指数的在时间区间的数据
                df_daily = self.get_daily_sw(code,begin_date=begin_date,end_date=end_date)
                # 保存数据
                if (isinstance(df_daily,pd.DataFrame)) and not (df_daily.empty):
                    self.save_data(code, df_daily, self.daily_sw2, {'index': True,'ETF':False})             
        elif level == 'L3':
             # 按照指数的代码循环，抓取所有指数信息
            for code in index_codes:
                # 抓取一个指数的在时间区间的数据
                df_daily = self.get_daily_sw(code,begin_date=begin_date,end_date=end_date)
                # 保存数据
                if (isinstance(df_daily,pd.DataFrame)) and not (df_daily.empty):
                    self.save_data(code, df_daily, self.daily_sw3, {'index': True,'ETF':False})     
    



if __name__ == '__main__':
    # crawl =  DailyCrawler()
    # crawl.crawl_index_sw('20140101','20210820')
    # crawl.crawl_index('20140101','20210820')
    # crawl.crawl('20210826','20210826')
    # crawl.crawl_etf('20140101','20210820')
    c = Crawler(level='L3')
    c.crawl_index_sw('20211129','20211129')
    # print(c.get_sw_list())
