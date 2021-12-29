import tushare as ts 
import pandas as pd
import numpy as np 
from datetime import datetime,timedelta
from abc import ABCMeta, abstractmethod

def get_code(date):
    #date = (datetime.now() + timedelta(days=-3)).strftime('%Y%m%d') #当前日期的前一天，转为字符串 
    pro = ts.pro_api()
    data = pd.DataFrame()   
   
    df_db= pro.query('daily_basic', ts_code='', trade_date= date,fields='ts_code,trade_date,\
    turnover_rate,volume_ratio,pe,pb,total_share,free_share,total_mv')
    data =data.append(df_db)
    df_db =   data.copy() 
    
    df_db['share_ratio'] = df_db.free_share / df_db.total_share * 100 #计算自由流通股占比
    df_db['total_mv'] = df_db.total_mv  / 10000 #把总市值单位转为亿元
    filt = (df_db['total_mv'] > 50 ) & (df_db['share_ratio'] > 20)  #总市值 > 100亿,流通股占比大于20%
    df_db =  df_db[filt] 
    c_1 = set(df_db.ts_code.to_list()) #取set
    
    df = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    year = (datetime.now() +timedelta(days=-365*3)).strftime('%Y%m%d') #上市时间为三年前的今天
    #保留上市时间大于三年个股数据
    df=df[df.list_date<year]
    #排除银行、保险、多元金融公司
    df=df[-df.industry.isin(['银行','保险','多元金融'])]
    #排除st和*ST股
    df=df[-df.name.str.startswith(('ST'))]
    df=df[-df.name.str.startswith(('*'))] 
    c_2 = set(df.ts_code.to_list())

    c = c_1 & c_2 #求交集
    df = df[df.ts_code.isin(c)]
    code=df.ts_code.values
    name=df.name.values
#         print(dict(zip(name,code)))
    return dict(zip(name,code))
if __name__ == '__main__':
    print(get_code('20211227'))
    