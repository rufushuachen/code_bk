import requests
from bs4 import BeautifulSoup
from urllib.parse import  urljoin
from queue import Queue,LifoQueue,PriorityQueue
from collections import deque
from multiprocessing import Process  # 进程
from hashlib import md5
import pandas as pd
import time
import schedule
import os
import re

class Chaojiying_Client(object):

    def __init__(self, username, password, soft_id):
        self.username = username
        password =  password.encode('utf8')
        self.password = md5(password).hexdigest()
        self.soft_id = soft_id
        self.base_params = {
            'user': self.username,
            'pass2': self.password,
            'softid': self.soft_id,
        }
        self.headers = {
            'Connection': 'Keep-Alive',
            'User-Agent': 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)',
        }

    def PostPic(self, im, codetype):
        """
        im: 图片字节
        codetype: 题目类型 参考 http://www.chaojiying.com/price.html
        """
        params = {
            'codetype': codetype,
        }
        params.update(self.base_params)
        files = {'userfile': ('ccc.jpg', im)}
        r = requests.post('http://upload.chaojiying.net/Upload/Processing.php', data=params, files=files, headers=self.headers)
        return r.json()

    def ReportError(self, im_id):
        """
        im_id:报错题目的图片ID
        """
        params = {
            'id': im_id,
        }
        params.update(self.base_params)
        r = requests.post('http://upload.chaojiying.net/Upload/ReportError.php', data=params, headers=self.headers)
        return r.json()

def login_tushare():
    # 1.创建一个session
    session = requests.session()
    # 2.可以提前给session设置好请求头或者cookie
    session.headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    }
    # 可用, 可不用
    # session.cookies = {
    #     # 可以把一些cookie的内容塞进来, 这里要的是字典
    # }
    url = "https://tushare.pro/login"
    # 3. 发请求
    res_1 = session.get(url)
    main_soup = BeautifulSoup(res_1.text, "html.parser")
    url_tag = main_soup.select_one("img#login-captcha-img")
    child_url = url_tag.get("src")
    child_url = urljoin(url, child_url)
    img = session.get(child_url)

    with open("login-captcha-img.png", mode="wb") as f:
        f.write(img.content)
    with open("login-captcha-img.png", mode="rb") as f:
        img_content = f.read()
    # 交给超级鹰来进行识别
    chaojiying = Chaojiying_Client('18614075987', 'q6035945', '931774')
    dic = chaojiying.PostPic(img_content, 1004)  # 把图片的字节传递进去即可
    code = dic['pic_str']  # 获取识别结果
    print(code)
    # print(res_1.text)
    # 登录
    data = {
        "_xsrf": "4e782e3d-cd10-438c-a320-2537198b9975",
        "account": "170407065@qq.com",
        "password": "tgb7uj9K",
        "captcha": code
    }
    # requests.post(url, data=data)
    res = session.post(url, data=data)  # resp.header set-cookie
    # print(res.headers['Set-Cookie'])
    print(res)
    return session

def in_data_once(session):
    """
    :param session 把登陆后的session对象传入，供调取数据使用:
    :return 当24小时周期结束后，释放set资源,删除文件:
    1.每隔一秒钟查询一次数据
    2.遍历数据，发现是新数据添加进set，写入文件
    3.如果是旧数据跳过写入文件，继续隔一秒查询数据，重复步骤1
    4.系统时间满足条件时（24小时周期），删除文件，函数返回，释放set资源
    """
    r_lst =list()
    res_set = set()
    while True:
        url = "https://tushare.pro/news#9"
        print("查询网站数据更新！requests.get")
        resp = session.get(url)
        child_soup = BeautifulSoup(resp.text, "html.parser")
        key_news = child_soup.select("div#news_9>div.key_news")
        for i in key_news:
            news_datetime = i.select_one(".news_datetime").text
            news_datetime = news_datetime.strip()
            news_content = i.select_one(".news_content").text
            news_content  = news_content.strip()
            if news_content in res_set:
                break
            else:
                res_set.add(news_content)
                dic = {"news_datetime": news_datetime, "news_content": news_content}
                print("插入一条新增数据！")
                write_file(f"{news_datetime}|{news_content}\n")
                r_lst.append(dic)
        t = time.strftime('%H:%M', time.localtime())
        if t == "23:38": #结束时间必须小于schedule开始的时间，这是第二天的时间
            print("时间已到，函数结束！")
            rm_file()
            #此处应是删除文件的函数
            #......
            return #函数的出口，释放集合资源
        elif t == "09:30":  #时间必须小于schedule开始的时间，这是第二天的时间
            t_flag = "morning"
            r1_lst = read_file(t_flag)
            print(r1_lst)
            time.sleep(70)
            continue
        elif t== "12:00":  #时间必须小于schedule开始的时间，这是第二天的时间
            print("开始读取文件")
            t_flag = "noon"
            r2_lst = read_file(t_flag)
            print(r2_lst)
            time.sleep(70)
            continue
        elif t== "18:30":  #时间必须小于schedule开始的时间，这是第二天的时间
            t_flag = "noon"
            r3_lst = read_file(t_flag)
            print(r3_lst)
            time.sleep(70)
            continue
        elif t== "22:00":  #时间必须小于schedule开始的时间，这是第二天的时间
            t_flag = "evening"
            r4_lst = read_file(t_flag)
            print(r4_lst)
            time.sleep(70)
            continue
        time.sleep(1)

def write_file(str_):
    filename= "records.csv"
    with open("./"+filename,encoding="utf-8",mode="a") as f:
        f.write(str_)
def read_file(t):
    re_str = r".*新闻联播.*\n$|.*今日财经TOP10.*\n$|.*制造业采购经理指数.*\n$|.*制造业采购经理指数.*\n$|.*制造业PMI.*\n$|.*美股盘前消息速报.*\n$|.*央视新闻.*|.*中共中央政治局.*\n$|.*央视财经.*|.*中国基金报.*|.*第一财经.*|.*中国人民银行.*\n$|.*人民日报.*"
    obj_1 = re.compile(re_str, re.S)
    # obj_2 = re.compile(r".*新闻联播.*\n$|.*今日财经TOP10.*\n$", re.S)
    # obj_3 = re.compile(r".*今日财经TOP10.*\n$", re.S)
    # # obj_4 = re.compile(r".*制造业采购经理指数.*\n$", re.S)
    # # obj_5 = re.compile(r".*制造业PMI.*\n$", re.S)
    # # obj_6 = re.compile(r".*美股盘前消息速报.*\n$", re.S)
    # # obj_7 = re.compile(r".*央视新闻.*\n$", re.S)
    # # obj_8 = re.compile(r".*中共中央政治局\n$", re.S)
    # filename= "records.csv"
    with open("./records.csv",encoding="utf-8",mode="r") as f:
        news_filter_lst = list()
        line = f.readline()
        while line  :
            r_1 = obj_1.search(line)
            if r_1:
                #print(r_1.group())
                news_filter_lst.append(r_1.group())
            line = f.readline()
        #print(news_filter_lst)
    morning_lst = list()
    noon_lst = list()
    afternoon_lst = list()
    evening_lst = list()
    for i in news_filter_lst:
        data = i.split("|")
        if data:
            str_time = data[0]
        else:
            str_time =""
        if str_time > "00:00" and str_time <= "08:00":
            morning_lst.append(i)
        elif str_time > "08:00" and str_time <= "12:00":
            noon_lst.append(i)
        elif str_time > "12:00" and str_time <= "18:30":
            afternoon_lst.append(i)
        elif str_time > "18:30" and str_time <= "23:59":
            evening_lst.append(i)
    print("删除文件！")
    rm_file()
    if t == "morning":
        return  morning_lst
    if t == "noon":
        return  noon_lst
    if t == "afternoon":
        return  afternoon_lst
    if t == "evening":
        return  evening_lst
def rm_file():
    path = os.getcwd()
    file_name = os.path.join(path,"records.csv")
    try:
        os.remove(file_name)
    except Exception as e:
        print("文件已删除过！")

if __name__ == '__main__':
    session = login_tushare()
    # 设定每天下午15:30执行抓取任务
    schedule.every().day.at("10:08").do(in_data_once,session) #函数的入口，23:39执行此程序
    #此处应该是定时查询文件，进行判断，提取信息，在信息提取完成后，删除文件的函数
    # 通过无限循环，执行任务检测
    while True:
        # 每10秒检测一次
        schedule.run_pending()
        time.sleep(10)

