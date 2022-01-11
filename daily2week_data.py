import pandas as pd 
import numpy as np 
import re 
from datetime import datetime as dt,timedelta
import os 

class Daily2WeekData(object):
    """
    把日线数据降频成周线数据，写入文件
    适用于ETF、申万L1-3级指数和股票日线数据
    """
    def __init__(self,file_list):
        self.file_list = file_list
        self.df = pd.DataFrame()
        self.codes = list()
        self.get_data_accu()



    def get_data_accu(self):
        #拼接每周的增量数据
        df_list = []
        
        for i in self.file_list:
            df = pd.read_csv(i)
            df_list.append(df)
        df = pd.concat(df_list,axis=0)
        cols = ['date','code','open','high','low','close','pct_chg','vol','amount']
        df = df[cols].copy()
       
        df['date'] = df['date'].astype('str')  #把日期转为字符串格式
        
        df.sort_values(['date'],inplace=True)
        df['date'] = df['date'].apply(lambda x: self.format_date_str(x)) #把字符串格式标准化
        df['date'] =  pd.to_datetime(df['date']) #把字符串转为日期格式

        codes = list(df['code'].unique())
        df = df.reset_index(drop=True)
        df = df.set_index('date').copy()
        self.df = df.copy()
        self.codes = codes
        return self.codes, self.df

    def format_date_str(self,date_str):
        # 格式转换
        pattern = re.compile(r"(\d{4})(\d{2})(\d{2})")
        return pattern.sub(date_str[-8:-4]+'-'+date_str[-4:-2]+'-'+date_str[-2:], date_str, 0)
        # return re.sub(r"(\d{4})(\d{2})(\d{2})", date_str[-8:-4]+'-'+date_str[-4:-2]+'-'+date_str[-2:], date_str, 0, re.IGNORECASE)

    def daily2week(self,path):
        #把累计每周的日线数据降频为周线数据
        
        df_bar_list = list()
        for code in self.codes:
            df_t = self.df[self.df['code']==code]
            df_bar = df_t.resample('W',label='left', closed='right').agg({'close':['max', 'min', 'mean', 'first', 'last'],
                                                                'pct_chg':['max', 'min', 'mean', 'first', 'last'],'vol':['sum'],'amount':['sum']
                                                     }                                 
                                                   ).ffill()
            cols = [('close','max'),('close','first'),('close','min'),('close','last'),('vol','sum'),('amount','sum'),('pct_chg','last')]
            df_bar = df_bar[cols]
            df_bar.columns = ['_'.join(col) for col in df_bar.columns.values]
            df_bar['code'] = code
            df_bar_list.append(df_bar)
        df = pd.concat(df_bar_list,axis=0)
        codes = list(df['code'].unique())
        df.to_csv('/home/rufus/quant/data/index_weekly/' + path) #把周线数据存到指定的目录

        return codes,df

if __name__ == '__main__': 
     
    
    # file_list = ['/home/rufus/quant/data/index/daily_index.csv']
    file_list =  os.listdir('/home/rufus/quant/data/index') #获取文件列表
    file_l = ['/home/rufus/quant/data/index/' + i for i in file_list]
    
    now_date = dt.now().strftime('%Y%m%d')
    file_path = 'weekly_index_'+ now_date +'.csv'   #拼接文件名
    

    d = Daily2WeekData(file_l)
    d.get_data_accu() 
    d.daily2week(file_path)
   