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
        projection={'date': True,'code':True,'close': True, '_id': False,'open':True,'high':True,'low':True,'pct_chg':True,'vol':True,'amount':True}
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
    collections = [DB_CONN['daily_sw_1'],DB_CONN['daily_sw_2'],DB_CONN['daily_sw_3'],DB_CONN['daily_index']]
    fil_names = ['/home/rufus/quant/data/sw_1_index/daily_sw_1','/home/rufus/quant/data/sw_2_index/daily_sw_2','/home/rufus/quant/data/sw_3_index/daily_sw_3',
    '/home/rufus/quant/data/index/daily_index']
    fil_names = [i + '_' + now_date for i in fil_names ]
    # print(fil_names)
    col_tuple = zip(collections,fil_names)
    for col,fil_name in col_tuple:
        mongo2csv(col,begin_date,end_date,fil_name)
def commit_on_random_day(begin_date):
    """
      获取从指定开始日期到今天的申万1,2,3级日线、指数日线数据,导出为csv数据
    """
    now_date = dt.now().strftime('%Y%m%d')    
    end_date = now_date
    collections = [DB_CONN['daily_sw_1'],DB_CONN['daily_sw_2'],DB_CONN['daily_sw_3'],DB_CONN['daily_index']]
    fil_names = ['/home/rufus/quant/data/sw_1_index/daily_sw_1','/home/rufus/quant/data/sw_2_index/daily_sw_2','/home/rufus/quant/data/sw_3_index/daily_sw_3',
    '/home/rufus/quant/data/index/daily_index']
    fil_names = [i + '_' + now_date for i in fil_names ]
    
    col_tuple = zip(collections,fil_names)
    for col,fil_name in col_tuple:
        mongo2csv(col,begin_date,end_date,fil_name)   

if __name__ == '__main__':
    commit_on_random_day('20211227') 
