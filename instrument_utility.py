from base import DB_CONN,ts_pro
import pandas as pd 
import numpy as np 
import json 
from datetime import datetime,timedelta

class InstrumentUtility(object):
    def __init__(self):
        self.pro, self.pro_bar = ts_pro()
        self.index = self.read_file('D:\\Quant_Code\\data\\ETF\\final_index_code.txt')
        self.stocks = dict()
        self.date = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')
        
         
    def read_file(self,path):
        with open(path,'r',encoding='gbk') as f:
            file_str = f.read()
            dic = json.loads(file_str)        
        return dic
    
    def get_code(self,name):
        """
        获取代码
        """
        df =  self.pro.stock_basic(exchange='')
        
        codes = df.ts_code.values
        names = df.name.values 
        stock = dict(zip(names,codes))
        #合并指数和个股成一个字典
        self.stocks = dict(stock,**self.index)
        return self.stocks[name]

    def get_filt_code(self,date):
        #date = (datetime.now() + timedelta(days=-3)).strftime('%Y%m%d') #当前日期的前一天，转为字符串 
        
        data = pd.DataFrame()   
    
        df_db= self.pro.query('daily_basic', ts_code='', trade_date= date,fields='ts_code,trade_date,\
        turnover_rate,volume_ratio,pe,pb,total_share,free_share,total_mv')
        data =data.append(df_db)
        df_db =   data.copy() 
        print(df_db.head())
        
        df_db['share_ratio'] = df_db.free_share / df_db.total_share * 100 #计算自由流通股占比
        df_db['total_mv'] = df_db.total_mv  / 10000 #把总市值单位转为亿元
        filt = (df_db['total_mv'] > 50 ) & (df_db['share_ratio'] > 20)  #总市值 > 100亿,流通股占比大于20%
        df_db =  df_db[filt] 
        c_1 = set(df_db.ts_code.to_list()) #取set
        
        df = self.pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
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

    def get_daily_basic(self,ts_code,date):
        df = self.pro.query('daily_basic', ts_code=ts_code, trade_date=date,fields='ts_code,trade_date,close,pe_ttm,pb,ps')#         
        return df['pe_ttm'][0],df['close'][0],df['pb'][0],df['ps'][0]

    def get_fina_indicator(self,ts_code):
        df = self.pro.query('fina_indicator', ts_code=ts_code, start_date='20210101', end_date='20211229')
        cols = ['ts_code','ann_date','end_date','eps','bps','revenue_ps']
        df = df.loc[:,cols].copy()
        df.drop_duplicates(inplace = True)
        
        eps_score = (df['eps'][-3:] * [12/9,12/6,12/3]).mean()
        bps_score = (df['bps'][-3:] * [1,1,1]).mean()
        ps_score = (df['revenue_ps'][-3:] * [12/9,12/6,12/3]).mean()
        return eps_score,bps_score,ps_score

    def stock_price_eval(self,date,name):
        ts_code = self.get_code(name)
        eval_dict = dict()
        pe_ttm,close,pb,ps = self.get_daily_basic(ts_code,self.date)
        eps_score,bps_score,ps_score =  self.get_fina_indicator(ts_code)
        if pd.isna(pe_ttm):
                if pd.isna(pb):
                    eval_price =  round(ps_score * ps,3)
                else:
                    eval_price =  round(bps_score * pb,3)
        else:
            eval_price = round(eps_score * pe_ttm,3) 

        eval_dict[ts_code] = {'eval':eval_price,'close':close}
        return eval_dict[ts_code]
    

if __name__ == '__main__':
    i = InstrumentUtility()
    print(i.stock_price_eval('20211230','牧原股份'))
    print(i.get_filt_code('20211229'))


