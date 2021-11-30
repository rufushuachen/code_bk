#  -*- coding: utf-8 -*-
"""
数据库连接
"""

from pymongo import MongoClient


# 指定数据库的连接，quant_01是数据库名
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_01']

def ts_pro():
    import tushare as ts 
    token='84c17c550c7e71523a11cee789600d34641a3660dacc22741c734caf'
    pro=ts.pro_api(token)
    return pro