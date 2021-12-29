#  -*- coding: utf-8 -*-
"""
数据库连接
"""

from pymongo import MongoClient


# 指定数据库的连接，quant_01是数据库名
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_02']

def ts_pro():
    import tushare as ts 
    token='84c17c550c7e71523a11cee789600d34641a3660dacc22741c734caf'
    ts.set_token(token)
    pro=ts.pro_api()
    pro_bar = ts.pro_bar
    return pro,pro_bar
if __name__ == '__main__':
    pro, pro_bar = ts_pro()
    df =  pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011')
    print(df)