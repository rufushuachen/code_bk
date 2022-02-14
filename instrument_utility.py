from numpy.lib import index_tricks
from base import DB_CONN,ts_pro
import pandas as pd 
import numpy as np 
import json 
from datetime import datetime,timedelta
import re
from time import sleep 

class InstrumentUtility(object):
    def __init__(self):
        self.pro, self.pro_bar = ts_pro()
        self.index = self.read_file('D:\\Quant_Code\\data\\ETF\\final_index_code.txt') #获取指数代码
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

    def get_filt_code(self,date,total_mv=50,share_ratio=20):
        #筛选上市时间大于三年，总市值大于50亿，流通股占比大于20%的个股
        #date = (datetime.now() + timedelta(days=-3)).strftime('%Y%m%d') #当前日期的前一天，转为字符串 
        
        # data = pd.DataFrame()   
    
        df_db= self.pro.query('daily_basic', ts_code='', trade_date= date,fields='ts_code,trade_date,\
        turnover_rate,volume_ratio,pe,pb,total_share,free_share,total_mv,close')
        
        
        # data =data.append(df_db)
        # df_db =   data.copy() 
        
        
        df_db['share_ratio'] = df_db.free_share / df_db.total_share * 100 #计算自由流通股占比
        df_db['total_mv'] = df_db.total_mv  / 10000 #把总市值单位转为亿元
        filt = (df_db['total_mv'] > 50 ) & (df_db['share_ratio'] > 20) & (df_db['close'] >= 20)  #总市值 >50亿,流通股占比大于20%,收盘价大于20元
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
        #个股估值
        ts_code = self.get_code(name)
        eval_dict = dict()
        pe_ttm,close,pb,ps = self.get_daily_basic(ts_code,date)
        eps_score,bps_score,ps_score =  self.get_fina_indicator(ts_code)
        if pd.isna(pe_ttm):
                if pd.isna(pb):
                    eval_price =  round(ps_score * ps,3)
                    
                else:
                    eval_price =  round(bps_score * pb,3)
                    
        else:
            eval_price = round(eps_score * pe_ttm,3) 
            
        ps_price = round(ps_score * ps,3)
        pb_price = round(bps_score * pb,3)
        pe_price = round(eps_score * pe_ttm,3)
        eval_dict[ts_code] = {'eval':eval_price,'close':close,'pe_price':pe_price,'pb_price':pb_price,'ps_price':ps_price}
        return eval_dict[ts_code]
    
    
    def format_str(self,se):
        return re.sub(r'[\%]', "", str(se))

    def get_ETF_filt(self,path='D:\\Quant_Code\\data\\ETF\\ETF_filt.csv'):
        #获取规模大于2亿，净值大于1，管理费小于等于0.5的ETF
        df = pd.read_csv(path,encoding='utf-8')
        
        df['fee'] = df['fee'].apply(lambda x: self.format_str(x))
        df['fee'] = df['fee'].astype('float')
        names = df['name'].tolist()
        codes = df['code'].tolist()
        index_names = df['index_name'].tolist()
        index_codes = df['index_code'].tolist()
        ETF_dict = dict()
        
        for i in range(len(names)):
            # print(names[i],codes[i])
            ETF_dict[names[i]] = [codes[i],index_names[i],index_codes[i]]

                        
        return ETF_dict 

    def get_sw_list(self,level):
            
        for _ in range(3):
            try:
                df = self.pro.index_classify(level=level, src='SW2021')
                index_codes = list(df.index_code)
                index_names = list(df.industry_name)
                
            except:
                sleep(1)
            else:
                return dict(zip(index_codes,index_names))
    def index_member(self,name,level='L3'):
        #获取三级申万指数对应的行业名称和代码
        df_sw3 = self.pro.index_classify(level=level, src='SW2021')
        sw_index_code = df_sw3['index_code'].tolist()
        sw_industry_name = df_sw3['industry_name'].tolist()
        sw_index_dict = dict()
        sw_index_dict = dict(zip(sw_industry_name,sw_index_code))
        if name in list(sw_index_dict.keys()):
            ts_code = sw_index_dict[name]
        else:
            return print('请检查申万指数名称是否正确！')
        #获取申万三级指数对应的成分股
        df = self.pro.index_member(index_code=ts_code)
        index_code = df['con_code'].tolist()
        # print(index_code)
        #把成分股代码转为字典
        member_dict = dict()
        if len(index_code) >0:            
            for code in index_code:                
                df_t =  self.pro.stock_basic(exchange='',ts_code=code)
                if not df_t.empty:
                    name = df_t['name'][0]
                    member_dict[name] = code 
        else:            
            print('找不到成分股')        
        return member_dict
      
    def swcode2name(self,level='L3'):
        #获取三级申万指数对应的行业名称和代码
        for _ in range(3):
            try:
                df_sw3 = self.pro.index_classify(level=level, src='SW2021')
            except:
                sleep(1)
            else:
                sw_index_code = df_sw3['index_code'].tolist()
                sw_industry_name = df_sw3['industry_name'].tolist()
                sw_index_dict = dict()
                sw_index_dict = dict(zip(sw_index_code,sw_industry_name))
                return sw_index_dict

    def get_ETF_list(self,days=-400):
        list_date = (datetime.now() + timedelta(days=days)).strftime('%Y%m%d')
        
        for _ in range(3):
            try:                
                df = self.pro.fund_basic(market='E',status='L') #获取上市中的ETF                      
            except:
                sleep(1)
            else:
                df.sort_values('list_date',inplace=True,ascending=True)
                #获取上市满一年，管理费小于等于0.5，托管费小于等于0.1的ETF
                filt = (df['list_date'] <= list_date) & (df['m_fee']<= 0.5) & (df['c_fee']<= 0.1)
                df = df[filt].copy()
                res = df[df.name.str.contains('ETF')]
                codes = list(res.ts_code) 
                names = list(res.name)               
                return   codes,names
    
    def get_ETF_names(self):
                
        for _ in range(3):
            try:                
                df = self.pro.fund_basic(market='E',status='L') #获取上市中的ETF                      
            except:
                sleep(1)
            else:
                res = df[df.name.str.contains('ETF')]
                codes = list(res.ts_code) 
                names = list(res.name)               
                return   dict(zip(codes,names))

    def get_ETF_scale(self):
        """
        筛选规模大于2亿的资产
        返回ETF代码和名字
        """
        ts_codes,names =  self.get_ETF_list()
        codes_list = list()
        fd_share_list =  list()
        name_list = list()
        for code,name in zip(ts_codes,names):     
            df = self.pro.fund_share(ts_code= code)
            df_t = self.pro.fund_nav(ts_code=code)
            try:
                fd_share = df.iloc[0]['fd_share']                
            except:
                fd_share = np.nan

            try:
                unit_nav = df_t.iloc[0]['unit_nav']
            except:
                unit_nav = np.nan
            
            scale_amount = round(fd_share  * unit_nav,1)
            if scale_amount > 20000: #规模大于2亿
                print(code,scale_amount)
                codes_list.append(code)
                name_list.append(name)               
        return  codes_list,name_list

    def get_ETF_filt(self):
        """
        用符合条件的ETF，筛选周线行情数据
        """
        codes,names = self.get_ETF_scale()
        names_dict = dict(zip(codes,names))
        path = 'D:\\Quant_Code\\data\\ETF_weekly\\weekly_ETF_hfq20220113.csv'
        df_list =list()
        df = pd.read_csv(path)
        cols =['date','code','close_hfq_last']
        df_t = df[cols].copy()
        for code in codes:
            df_s = df_t[df_t['code']==code].copy()           
            df_list.append(df_s)
        
        df_res =pd.concat(df_list,axis=0)
        df_res.sort_values('date',ascending=True,inplace=True)
        return df_res,names_dict

    def get_ETF_SHARP(self,m=1.0):
        """
        根据筛选后的周线行情数据，计算资产的年化回报，波动率，sharp ratio
        返回sharp ratio大于阈值的资产代码

        """
        df, names_dict= self.get_ETF_filt()
        codes = df['code'].unique().tolist()
        item_list = list()
        for code in codes:
            df_s = df[df['code']==code].copy()  
            df_returns = np.log(df_s['close_hfq_last']/df_s['close_hfq_last'].shift(1))
            df_returns.dropna(inplace=True)
            annual_ret = df_returns.mean() * 52 *100
            annula_volatility = df_returns.std() * np.sqrt(52)*100
            sharp = round(annual_ret / annula_volatility,3)
            item_dic = {'code':code,'annual_ret':annual_ret,
                    'annula_volatility':annula_volatility,'sharp':sharp}
            item_list.append(item_dic)

        res = pd.DataFrame(item_list)
        res.sort_values('sharp',ascending=False,inplace=True)
        fil = res['sharp'] > m
        res = res[fil].copy()
        res['name'] = [ names_dict[i] for i in res['code']]
        return list(res['code']),list(res['name']),res 

    def ret_df(self,w):
        """
        1.计算指数收益率，形成序列
        2.对每个指数收益率序列计算中位数
        3.对中位数进行排序
        4.排序后的指数列表[{指数代码:中位数}，{指数代码:中位数}，...]
        5.指数收益率时间范围从2018年底到2021年底
        """   
        path = 'D:\\Quant_Code\\data\\index\\weekly_index.csv'
       
        df = pd.read_csv(path)
        df_list =  list()
        codes = df['code'].unique().tolist()
        for i in codes:
            df_t = df[df['code'] == i]
            df_t.reset_index(drop=True,inplace=True)
            df_t['ret'] =(df_t['close_last']/df_t['close_last'].shift(w)-1)    #计算收益率     
            df_t= df_t.iloc[w:].fillna(0)     
            df_list.append(np.median(df_t['ret'])) #计算中位数
    
        res = sorted(dict(zip(codes,df_list)).items(),key=lambda x:x[1],reverse=True) #排序
        codes_sorted = list()
        for i in res:
            codes_sorted.append(i[0])
            
        return codes_sorted

    def format_str(self,se):
        return re.sub(r'[\%]', "", str(se)) #替换字符'%'
    def format_str_code(self,se):
        return re.sub(r'[\D]', "", str(se)) #替换非数字的字符,去除ETF代码中的特殊字符

    def get_ETF_code_df(self,path='D:\\Quant_Code\\data\\ETF\\ETF_filt.csv'): 
        """
        获取ETF资产的DataFrame
        ETF_filt.csv 是经过筛选的ETF资产
        把格式化'fee'字段，用正则表达式替换把'%'替换掉
        """ 
        df = pd.read_csv(path,encoding='utf-8')
        df['fee'] = df['fee'].apply(lambda x: self.format_str(x))
        df['fee'] = df['fee'].astype('float')
        return df

    def get_ETF_codes(self,w=1,m=30,n=60):
        """
        1.拿到符合条件的指数代码(根据中位数排序)
        2.获取对应的ETF资产的DataFrame
        3.根据指数代码，获取对应的ETF资产代码(ETF资产跟踪对应的指数)
        4.去除ETF资产代码中非数字的字符
        5.参数w表示计算多长时间的收益率
        """ 
        p3 =  round(len(self.ret_df(w)) * 0.75)
        codes = self.ret_df(w)[p3:] #选取排名靠前的33个指数
        df = self.get_ETF_code_df()
        codes_list = list()
        names_list = list()
        for code in codes:
            df_t = df[df['index_code']==code]
            df_t.sort_values(by=['fee'],ascending=True,inplace=True)
            if df_t['name'].tolist()[0:2]:
                codes_list.append(df_t['name'].tolist()[0])
                names_list.append(df_t['code'].tolist()[0])
        #[format_str_code(i) for i in names_list]
        return [self.format_str_code(i) for i in names_list],codes_list # names,codes

    def get_ETF_features(self):
        """
        1.获取根据指数中位数排序后对应的ETF资产代码
        2.根据ETF资产代码，获取对应的上市时间、管理费、托管费、资产规模等信息
        3.返回股票代码、股票名称       
        """
        codes,names = self.get_ETF_codes(1) #参数1，表示计算一周的收益率
        codes_dict = dict(zip(codes,names))
        for _ in range(3):
            try:                
                df = self.pro.fund_basic(market='E',status='L') #获取上市中的ETF                      
            except:
                sleep(1)
            else: 
                break
        df['code'] = df['ts_code'].apply(lambda x: self.format_str_code(x))
        
        df_t = df[df['code'].isin(codes)]
        codes = list(df_t['ts_code'])
       
        cols = ['ts_code','code','name','list_date','m_fee','c_fee','fund_type','scale']
        scale_list= list()
        for code,name in zip(codes,names):     
            df_share = self.pro.fund_share(ts_code= code)
            df_nav = self.pro.fund_nav(ts_code=code)
            try:
                fd_share = df_share.iloc[0]['fd_share']                
            except:
                fd_share = np.nan

            try:
                unit_nav = df_nav.iloc[0]['unit_nav']
            except:
                unit_nav = np.nan            
            
            scale_amount = round(fd_share  * unit_nav,1)
            scale_list.append({'ts_code':code,'scale':scale_amount})
        df_scale = pd.DataFrame(scale_list)  
        res = df_t.merge(df_scale,how='left',on='ts_code')  
        res['name'] = res['code'].apply(lambda x: codes_dict[x])
        return res[cols],res['ts_code'],res['name']
        

                

if __name__ == '__main__':
    i = InstrumentUtility()
    # print(i.stock_price_eval('20211231','美的集团'))
    
    # print(i.get_filt_code('20211229'))
    # print(i.get_ETF_filt())
    # print(i.get_sw_list('L3'))
    # print(i.index_member('金属包装','L3'))
    # print(i.swcode2name())
    # print(i.read_file('sw_3_dict.txt'))
    # codes,names = i.get_ETF_list()
   
    # print(dict(zip(codes,names)))
    
    codes = i.ret_df(1)
    p3 =  round(len(codes) * 0.75)
    print(p3)
    
  
