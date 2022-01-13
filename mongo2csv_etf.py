import pandas as pd 
from base import DB_CONN,ts_pro
from pymongo import ASCENDING, UpdateOne
from datetime import datetime as dt,timedelta

 
def mongo2csv(collection,begin_date,end_date,fil_name):
    """
    创建索引也不难，
    db.你的collection.createIndex({“你的字段”: -1})，此处 -1 代表倒序，1 代表正序；
    db.你的collecton.getIndexes();
    """
    
    daily_cursor = collection.find(
        {'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
        sort=[('date', ASCENDING)],
        projection={'date': True,'code':True,'close_hfq': True, '_id': False,'open':True,'high':True,'low':True,'pct_chg':True,'vol':True,'amount':True}
    )

    df_daily = pd.DataFrame([daily for daily in daily_cursor])
    df_daily.to_csv( fil_name + '.csv')

def commit_on_friday():
    """
    获取从今天至5天前的申万1,2,3级日线、指数日线数据，导出为csv数据
    """
    now_date = dt.now().strftime('%Y%m%d')
    previous_date = (dt.now() + timedelta(-4)).strftime('%Y%m%d')
    # print(now_date,previous_date)
    begin_date =  previous_date
    end_date =  now_date
    collection = DB_CONN['daily_ETF_hfq']
    fil_name =     '/home/rufus/quant/data/ETF/daily_ETF'
    fil_name =  fil_name + now_date 
    mongo2csv(collection,begin_date,end_date,fil_name) 

def commit_on_random_day(begin_date):
    """
      获取从指定开始日期到今天的申万1,2,3级日线、指数日线数据,导出为csv数据
    """
    now_date = dt.now().strftime('%Y%m%d')    
    end_date = now_date
    collection = DB_CONN['daily_ETF_hfq']
    fil_name =     '/home/rufus/quant/data/ETF/daily_ETF'
    fil_name =  fil_name + now_date    
    
    
    mongo2csv(collection,begin_date,end_date,fil_name)   

if __name__ == '__main__':
    commit_on_random_day('20181115') 
