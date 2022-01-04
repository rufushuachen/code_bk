import pandas as pd 
import numpy as np 
import re 


class Daily2WeekData(object):
    """
    把日线数据降频成周线数据，写入文件
    适用于ETF、申万L1-3级指数和股票日线数据
    """
    def __init__(self,file_list):
        self.file_list = file_list
        print(self.file_list)
        self.df = pd.DataFrame()
        self.codes = list()
        self.get_data_accu()



    def get_data_accu(self):
        #拼接每天的增量数据
        df_list = []
        
        for i in self.file_list:
            df = pd.read_csv(i)
            df_list.append(df)
        df = pd.concat(df_list,axis=0)
        cols = ['date','code','open','high','low','close','vol']
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
        df_bar_list = list()
        for code in self.codes:
            df_t = self.df[self.df['code']==code]
            df_bar = df_t.resample('W',label='left', closed='right').agg({'close':['max', 'min', 'mean', 'first', 'last'],
                                                                'vol':['sum']
                                                     }                                 
                                                   ).ffill()
            cols = [('close','max'),('close','first'),('close','min'),('close','last'),('vol','sum')]
            df_bar = df_bar[cols]
            df_bar.columns = ['_'.join(col) for col in df_bar.columns.values]
            df_bar['code'] = code
            df_bar_list.append(df_bar)
        df = pd.concat(df_bar_list,axis=0)
        codes = list(df['code'].unique())
        df.to_csv('/home/rufus/quant/data/index/' + path)

        return codes,df

if __name__ == '__main__': 
    # d = Daily2WeekData(['D:\E-BOOK\daily_stock.csv'])  
    
    file_list = ['/home/rufus/quant/data/index/daily_sw_3.csv']
    file_path = 'weekly_sw_3.csv'
    
 
    d = Daily2WeekData(file_list)
    d.get_data_accu() 
    d.daily2week(file_path)
    # codes,df = d.get_data_accu()
    # codes,df =d.daily2week()
    # print(df.tail())
    # d.daily2week()  