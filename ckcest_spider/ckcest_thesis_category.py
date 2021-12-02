import json
import time
import urllib.parse
from datetime import datetime

import pymongo
import redis
import pymysql
import requests

"""
论文分类

"""


class SpiderCkcest():

    def __init__(self):
        self.mongo_url = "mongodb://192.168.12.187:27017"
        self.mongo_db = "ckcest"
        self.mongo_table = "ckcest_type"
        self.mongo_user = "ckcest"
        self.mongo_pwd = "123456"
        self.client = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db = self.client[self.mongo_db]
        self.db.authenticate(self.mongo_user, self.mongo_pwd)

        # MYSQL
        self.sql_conn = pymysql.connect(host='192.168.12.240',
                                        user='root',
                                        password='123456',
                                        db='usppa_dev')
        self.sql_cursor = self.sql_conn.cursor()

    def ckcest_spider(self):

        header = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36 Edg/86.0.622.69"
        }
        session = requests.session()
        session.DEFAULT_RETRIES = 5
        session.keep_alive = False
        # table = self.db[self.mongo_table]

        # 页
        url = 'http://www.ckcest.cn/default/es3/search/postContent'
        for new_energy_keywords in open("../spider_keywords.txt", encoding="UTF-8"):
            new_energy_keywords = new_energy_keywords.replace('\n', '')
            new_energy_keywords_quote = urllib.parse.quote(new_energy_keywords)

            request_data = {
                "dbId": "1002",
                "text": "",
                "express": "TI==" + new_energy_keywords + " OR KY=" + new_energy_keywords,
                "secondSearchExpress": "RESTY==1002",
                "order": "101",
                "startYear": '1700',
                "endYear": '2099',
                "page": '1',
                "limit": "10",
                "isadvanced": "1",
                "uuid": "",
                "country": "China",
                "province": "Beijing",
                "city": "Beijing"
            }

            get = session.post(url, headers=header, data=request_data)
            datas_temp = json.loads(get.text)
            results_datas = datas_temp['results']['aggs']

            # 获取每个关键字下的一级分类
            for item in results_datas:
                if item['showName'] == '中国工程科技分类':
                    parm_list = []
                    for x in item['datas']:
                        if x.keys() != 'showName':
                            parm = [y for y in list(x.keys()) if y != 'showName']
                            parm_list.append(''.join(parm))
                            if ''.join(parm) in x:
                                # 每个分类标识 中文名
                                classify_name = x['showName']
                                # 每个分类标识 类型
                                classify_label = ''.join(parm)
                                # all_one_classify_name.append(classify_name)
                                # all_one_classify_label.append(classify_label)

                                # 获取二级分类
                                request_data = {
                                    "dbId": "1002",
                                    "text": "",
                                    "express": "TI%3D%3D" + new_energy_keywords_quote + "%20OR%20KY%3D%3D" + new_energy_keywords_quote,
                                    "secondSearchExpress": "RESTY%3D%3D1002%20AND%20LANG%3D%3D1001%20AND%20CCC1%3D%3D" + urllib.parse.quote(
                                        classify_label),
                                    "order": "101",
                                    "startYear": '1700',
                                    "endYear": '2099',
                                    "page": '1',
                                    "limit": "10",
                                    "isadvanced": "1",
                                    "uuid": "",
                                    "country": "China",
                                    "province": "Beijing",
                                    "city": "Beijing"
                                }
                                get = session.post(url, headers=header, data=request_data)
                                results_datas = get.json()['results']['aggs']
                                print(results_datas)
                                for item in results_datas:
                                    if item['showName'] == '中国工程科技分类':
                                        parm_list = []
                                        for x in item['datas']:
                                            if x.keys() != 'showName':
                                                parm = [y for y in list(x.keys()) if y != 'showName']
                                                parm_list.append(''.join(parm))
                                                if ''.join(parm) in x:
                                                    # 每个分类标识 中文名
                                                    classify_name_two = x['showName']
                                                    # 每个分类标识 类型
                                                    classify_label_two = ''.join(parm)
                                                    print(classify_name)
                                                    print(classify_label)
                                                    print(classify_name_two)
                                                    print(classify_label_two)
                                                    sql = 'insert into ckcest_category (first_category,first_category_id,second_category,second_category_id,category_resource) values (%s, %s, %s, %s, %s)'
                                                    valuse = (classify_name, classify_label, classify_name_two,
                                                              classify_label_two, 'ckcest_thesis')
                                                    self.sql_cursor.execute(sql, valuse)
                                                    self.sql_conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sql_conn.close()


if __name__ == '__main__':
    ckcest = SpiderCkcest()
    ckcest.ckcest_spider()
