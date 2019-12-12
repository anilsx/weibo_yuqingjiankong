#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import json
import math
import random
import sys
import traceback
from collections import OrderedDict
from datetime import date, datetime, timedelta
from time import sleep
from dateutil import parser
import requests
from lxml import etree
from tqdm import tqdm


class Weibo(object):
    def __init__(self, config):  #8
        """Weibo类初始化"""
        since_date = str(config['since_date']) #9
        if since_date.isdigit():#10
            since_date = str(date.today() - timedelta(int(since_date)))
        self.since_date = since_date  # 起始时间，即爬取发布日期从该值到现在的微博，形式为yyyy-mm-dd  #11
        user_id_list = config['user_id_list'] #12
        if not isinstance(user_id_list, list):#13
            user_id_list = self.get_user_list(user_id_list) #14
        self.user_id_list = user_id_list  # 要爬取的微博用户的user_id列表 #22
        self.user_id = ''  # 用户id,如昵称为"Dear-迪丽热巴"的id为'1669879400' #23
        self.user = {}  # 存储目标微博用户信息 #24
        self.got_count = 0  # 存储爬取到的微博数 #25
        self.weibo = []  # 存储爬取到的所有微博信息#26
        self.weibo_id_list = []  # 存储爬取到的所有微博id#27

    """获取网页中json数据"""
    def get_json(self, params): #37
        url = 'https://m.weibo.cn/api/container/getIndex?'
        r = requests.get(url, params=params)
        return r.json()
    """获取网页中微博json数据"""
    def get_weibo_json(self, page):
        params = {'containerid': '107603' + str(self.user_id), 'page': page}
        js = self.get_json(params)
        return js
    """将爬取的用户信息写入MongoDB数据库"""
    def user_to_mongodb(self): #42
        user_list = [self.user]
        self.info_to_mongodb('user', user_list) #43
        print(u'%s信息写入MongoDB数据库完毕' % self.user['screen_name'])
    """获取用户信息"""
    def get_user_info(self):
        params = {'containerid': '100505' + str(self.user_id)} #35
        js = self.get_json(params) #36
        if js['ok']:
            info = js['data']['userInfo'] #38
            user_info = {}
            user_info['id'] = self.user_id
            user_info['screen_name'] = info.get('screen_name', '')
            user_info['gender'] = info.get('gender', '')
            user_info['statuses_count'] = info.get('statuses_count', 0)
            user_info['followers_count'] = info.get('followers_count', 0)
            user_info['follow_count'] = info.get('follow_count', 0)
            user_info['description'] = info.get('description', '')
            user_info['profile_url'] = info.get('profile_url', '')
            user = user_info #39
            self.user = user #40
            self.user_to_mongodb() #41
            return user #45
    """获取长微博"""
    def get_long_weibo(self, id):
        url = 'https://m.weibo.cn/detail/%s' % id
        html = requests.get(url).text
        html = html[html.find('"status":'):]
        html = html[:html.rfind('"hotScheme"')]
        html = html[:html.rfind(',')]
        html = '{' + html + '}'
        js = json.loads(html, strict=False)
        weibo_info = js.get('status')
        if weibo_info:
            weibo = self.parse_weibo(weibo_info)
            return weibo
    """字符串转换为整数"""
    def string_to_int(self, string):
        if isinstance(string, int):
            return string
        elif string.endswith(u'万+'):
            string = int(string[:-2] + '0000')
        elif string.endswith(u'万'):
            string = int(string[:-1] + '0000')
        return int(string)
    """时间处理"""
    def standardize_date(self, created_at):
        if u"刚刚" in created_at:
            created_at = datetime.now().strftime("%Y-%m-%d")
        elif u"分钟" in created_at:
            minute = created_at[:created_at.find(u"分钟")]
            minute = timedelta(minutes=int(minute))
            created_at = (datetime.now() - minute).strftime("%Y-%m-%d")
        elif u"小时" in created_at:
            hour = created_at[:created_at.find(u"小时")]
            hour = timedelta(hours=int(hour))
            created_at = (datetime.now() - hour).strftime("%Y-%m-%d")
        elif u"昨天" in created_at:
            day = timedelta(days=1)
            created_at = (datetime.now() - day).strftime("%Y-%m-%d")
        elif created_at.count('-') == 1:
            year = datetime.now().strftime("%Y")
            created_at = year + "-" + created_at
        elif "+" in created_at:
            created_at = parser.parse(created_at)
            created_at = datetime.strftime(created_at,"%Y-%m-%d")
        return created_at
    def parse_weibo(self, weibo_info):
        weibo = OrderedDict()
        weibo['id'] = int(weibo_info['id'])
        weibo['user_id'] = self.user_id
        text_body = weibo_info['text']
        weibo['text'] = etree.HTML(text_body).xpath('string(.)')
        weibo['attitudes_count'] = self.string_to_int(
            weibo_info['attitudes_count'])
        weibo['comments_count'] = self.string_to_int(
            weibo_info['comments_count'])
        weibo['reposts_count'] = self.string_to_int(
            weibo_info['reposts_count'])
        weibo['created_at'] = self.standardize_date(
            weibo_info['created_at'])
        return weibo
    """打印用户信息"""
    def print_user_info(self):
        print('+' * 100)
        print(u'用户信息')
        print(u'用户id：%s' % self.user['id'])
        print(u'用户昵称：%s' % self.user['screen_name'])
        gender = u'女' if self.user['gender'] == 'f' else u'男'
        print(u'性别：%s' % gender)
        print(u'微博数：%d' % self.user['statuses_count'])
        print(u'粉丝数：%d' % self.user['followers_count'])
        print(u'关注数：%d' % self.user['follow_count'])
        if self.user.get('verified_reason'):
            print(self.user['verified_reason'])
        print(self.user['description'])
        print('+' * 100)
    """打印一条微博"""
    def print_one_weibo(self, weibo):
        print(u'微博id：%d' % weibo['id'])
        print(u'微博正文：%s' % weibo['text'])
        print(u'发布时间：%s' % weibo['created_at'])
        print(u'点赞数：%d' % weibo['attitudes_count'])
        print(u'评论数：%d' % weibo['comments_count'])
        print(u'转发数：%d' % weibo['reposts_count'])
    """打印微博，若为转发微博，会同时打印原创和转发部分"""
    def print_weibo(self, weibo):
        if weibo.get('retweet'):
            print('*' * 100)
            print(u'转发部分：')
            self.print_one_weibo(weibo['retweet'])
            print('*' * 100)
            print(u'原创部分：')
            self.print_one_weibo(weibo)
        print('-' * 120)
    """获取一条微博的全部信息"""
    def get_one_weibo(self, info):
        try:
            weibo_info = info['mblog']
            weibo_id = weibo_info['id']
            retweeted_status = weibo_info.get('retweeted_status')
            is_long = weibo_info['isLongText']
            if retweeted_status:  # 转发
                retweet_id = retweeted_status['id']
                is_long_retweet = retweeted_status['isLongText']
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
                if is_long_retweet:
                    retweet = self.get_long_weibo(retweet_id)
                    if not retweet:
                        retweet = self.parse_weibo(retweeted_status)
                else:
                    retweet = self.parse_weibo(retweeted_status)
                retweet['created_at'] = self.standardize_date(
                    retweeted_status['created_at'])
                weibo['retweet'] = retweet
            else:  # 原创
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
            return weibo
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()
    """判断微博是否为置顶微博"""
    def is_pinned_weibo(self, info):
        weibo_info = info['mblog']
        title = weibo_info.get('title')
        if title and title.get('text') == u'置顶':
            return True
        else:
            return False
    """获取一页的全部微博"""
    def get_one_page(self, page):
        try:
            js = self.get_weibo_json(page)
            if js['ok']:
                weibos = js['data']['cards']
                for w in weibos:
                    if w['card_type'] == 9:
                        wb = self.get_one_weibo(w)
                        if wb:
                            if wb['id'] in self.weibo_id_list:
                                continue
                            created_at = datetime.strptime(
                                wb['created_at'], "%Y-%m-%d")
                            since_date = datetime.strptime(
                                self.since_date, "%Y-%m-%d")
                            if created_at < since_date:
                                if self.is_pinned_weibo(w):
                                    continue
                                else:
                                    return True
                            self.weibo.append(wb)
                            self.weibo_id_list.append(wb['id'])
                            self.got_count = self.got_count + 1
                            self.print_weibo(wb)
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()
    """获取微博页数"""
    def get_page_count(self):
        weibo_count = self.user['statuses_count']
        page_count = int(math.ceil(weibo_count / 10.0))
        return page_count
    """获取要写入的微博信息"""
    def get_write_info(self, wrote_count):
        write_info = []
        for w in self.weibo[wrote_count:]:
            wb = OrderedDict()
            for k, v in w.items():
                if k not in ['user_id', 'screen_name', 'retweet']:
                    if 'unicode' in str(type(v)):
                        v = v.encode('utf-8')
                    wb[k] = v
            write_info.append(wb)
        return write_info
    """将爬取的信息写入MongoDB数据库"""
    def info_to_mongodb(self, collection, info_list): #44
        try:
            import pymongo
        except ImportError:
            sys.exit(u'系统中可能没有安装pymongo库，请先运行 pip install pymongo ，再运行程序')
        try:
            from pymongo import MongoClient

            client = MongoClient()
            db = client['weibo']
            collection = db[collection]
            for info in info_list:
                if not collection.find_one({'id': info['id']}):
                    collection.insert_one(info)
                else:
                    collection.update_one({'id': info['id']}, {'$set': info})
        except pymongo.errors.ServerSelectionTimeoutError:
            sys.exit(u'系统中可能没有安装或启动MongoDB数据库，请先根据系统环境安装或启动MongoDB，再运行程序')
    """将爬取的微博信息写入MongoDB数据库"""
    def weibo_to_mongodb(self, wrote_count):
        self.info_to_mongodb('weibo', self.weibo[wrote_count:])
        print(u'%d条微博写入MongoDB数据库完毕' % self.got_count)
    """将爬到的信息写入文件或数据库"""
    def write_data(self, wrote_count):
        if self.got_count > wrote_count:
            self.weibo_to_mongodb(wrote_count)
    """获取全部微博"""
    def get_pages(self):

        self.get_user_info() #34
        page_count = self.get_page_count() #46
        wrote_count = 0
        self.print_user_info()
        page1 = 0
        random_pages = random.randint(1, 5)
        for page in tqdm(range(1, page_count + 1), desc='Progress'):
            print(u'第%d页' % page)
            is_end = self.get_one_page(page)
            if is_end:
                break

            if page % 20 == 0:  # 每爬20页写入一次文件
                self.write_data(wrote_count)
                wrote_count = self.got_count

            # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
            # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
            # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
            if page - page1 == random_pages and page < page_count:
                sleep(random.randint(6, 10))
                page1 = page
                random_pages = random.randint(1, 5)

        self.write_data(wrote_count)  # 将剩余不足20页的微博写入文件
        print(u'微博爬取完成，共爬取%d条微博' % self.got_count)
    """获取文件中的微博id信息"""
    def get_user_list(self, file_name): #15
        with open(file_name, 'rb') as f: #16
            lines = f.read().splitlines()   #17
            lines = [line.decode('utf-8') for line in lines] #18
            user_id_list = [
                line.split(' ')[0] for line in lines #19
                if len(line.split(' ')) > 0 and line.split(' ')[0].isdigit() #20
            ]

        return user_id_list #21
    """初始化爬虫信息"""
    def initialize_info(self, user_id): #32
        self.weibo = []
        self.user = {}
        self.got_count = 0
        self.user_id = user_id
        self.weibo_id_list = []
    """运行爬虫"""
    def start(self): #29
        try: #30
            for user_id in self.user_id_list:
                self.initialize_info(user_id) #31
                self.get_pages() #33
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

def main():#3
    try:  #4
        with open('./config.json') as f:  #5
            config = json.loads(f.read())
        #     print(config)
        #     print(type(config))#6
        # from pymongo import MongoClient
        # import pandas as pd
        # client = MongoClient('localhost', 27017)
        # db = client['weibo']
        # col = db['config']
        # df = pd.DataFrame(list(col.find()))
        # config = df.to_dict(orient='records')
        # for i in config:
        #     config = i
        wb = Weibo(config)  #7
        wb.start()  # 爬取微博信息#28
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

if __name__ == '__main__':   #1
    main()                      #2
