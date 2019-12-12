import traceback

import requests
from tqdm import tqdm


class Weibo_id(object):
    def __init__(self,config):
        user_id = config['user_id']
        count = config['count']
        fans = config['fans']
        self.user_id = user_id
        self.count = count
        self.fans = fans
        self.got_count = 0
        self.write_count = 0
        self.weibo_id_list = []



    def get_weibo_json(self,page,User_id):
        url = 'https://m.weibo.cn/api/container/getIndex?'
        params = {'containerid': '231051' +'_-_followers_-_'+ str(User_id),'page': page}
        content = requests.get(url, params=params)
        js = content.json()
        return js

    def save_to_txt(self,user_id_list):
        for i in user_id_list:
            with open(str(self.user_id) + '.txt','a')as f:
                f.write(str(i))
                f.write('\n')

    def user_to_mongodb(self): #42
        user_list = self.weibo_id_list
        self.save_to_txt(user_list) #43
        print(u'%s条信息写入txt完毕' % len(self.weibo_id_list))

    def initialize_info(self,user_id): #32
        self.user_id = user_id
        self.got_count = 0
        self.write_count = 0
        self.weibo_id_list = []

    def string_to_int(self,string):
        if isinstance(string, int):
            return string
        elif string.endswith(u'万+'):
            string = int(string[:-2] + '0000')
        elif string.endswith(u'万'):
            string = int(string[:-1] + '0000')
        elif string.endswith(u'亿'):
            string = round(float(string[:-1])*100000000)
        return int(string)

    def get_user_id(self,page,User_id):
        # params = {'containerid': '231051' +'_-_followers_-_'+ str(self.user_id),'page': page}
        js = self.get_weibo_json(page,User_id)
        if js['ok']:
            data = js['data']['cards'][0]['card_group']
            for i in data:
                user_id = i['user']['id']    #获取用户id
                fans = i['desc2'].split('：')[1]  # 获取粉丝数量
                # fans = i.get('desc2', 0)
                fans = self.string_to_int(fans)
                try:
                    if fans > self.fans:
                        if len(self.weibo_id_list) < self.count:
                            self.weibo_id_list.append(user_id)
                    else:
                        return True
                except Exception as e:
                    print('Error: ', e)
                    traceback.print_exc()
            self.user_to_mongodb()
                # weibo_id_list = user_id_list
                # self.weibo_id_list = weibo_id_list
                # return weibo_id_list

    def get_pages(self,User_id):
        for page in tqdm(range(2, 11), desc='Progress'):
            print(u'第%d页' % page)
            is_end = self.get_user_id(page,User_id)
            if is_end:
                break
        self.user_to_mongodb()

    def start(self):
        try:
            self.initialize_info(self.user_id)
            self.get_pages(self.user_id)
            for user_id in self.weibo_id_list:
                self.get_pages(user_id)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

def main():#3
    try:  #4
        # with open('./config.json') as f:  #5
        #     config = json.loads(f.read())
        #     print(config)
        #     print(type(config))#6
        from pymongo import MongoClient
        import pandas as pd
        client = MongoClient('localhost', 27017)
        db = client['weibo']
        col = db['config']
        df = pd.DataFrame(list(col.find()))
        config = df.to_dict(orient='records')
        for i in config:
            config = i
        wb = Weibo_id(config)  #7
        wb.start()  # 爬取微博id#28
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

if __name__ == '__main__':   #1
    main()