import json
import time
from datetime import datetime
from urllib.request import urlretrieve

import pymongo
import redis

import requests

'''发明专利'''


class SpiderCkcest():

    def __init__(self):
        self.mongo_url = "mongodb://192.168.12.199:27017"
        self.mongo_db = "ckcest"
        self.mongo_table = "ckcest_patent"
        # self.mongo_user = "ckcest"
        # self.mongo_pwd = "123456"
        self.client = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db = self.client[self.mongo_db]
        # self.db.authenticate(self.mongo_user, self.mongo_pwd)

        self.redis_con = redis.Redis(host='192.168.12.100', port='6379', db=1)
        # self.redis_con.flushdb()
        self.file_write = open("error_url.txt", "w+")

    def ckcest_spider(self):
        insert_num = 1
        header = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        session = requests.session()
        session.DEFAULT_RETRIES = 5
        session.keep_alive = False
        table = self.db[self.mongo_table]

        # 专利类型
        for patty in ["4|6|17|8|1|15|3", "63", "70", "5"]:
            # 国家和地区
            for country in ["17", "5", "22", "72", "58"]:
                # 公开年
                for year in range(1966, 2020):
                    # 页
                    for num in range(1, 101):
                        url = 'http://mall.ckcest.cn/mall/listContent.ilf?dbId=3001&text=&express=&secondSearchExpress=YEAR' \
                              '%3D%3D'+str(year)+'+AND+PATTY%3D%3D'+patty+'+AND+COUNTRY%3D%3D'+country+'&' \
                               'secondSearchFlag=0' \
                              '&fieldSearchExpress=&fieldSearchEntity=&order=3&startYear=1700&endYear=2099&page=' + str(
                            num) + '&limit=10&model=1&isadvanced=0&uuid='

                        try:
                            get = session.get(url, headers=header)
                        except:
                            self.file_write.write(url+'\n')
                            continue

                        try:
                            print(url)
                            datas_temp = json.loads(get.text)
                            datas = datas_temp['results']['datas']
                        except Exception as e:
                            print(datas_temp)
                            break
                        for item in datas:
                            print(item)

                            if self.is_parse_id(item['_id']):
                                continue

                            picture_urls = item['picture_url'] if "picture_url" in item else None,

                            picture_name_one = []
                            if picture_urls[0]:
                                for index, picture_url in enumerate(str(picture_urls[0]).split(';')):
                                    try:
                                        urlretrieve(picture_url.replace('增量数据', '%e5%a2%9e%e9%87%8f%e6%95%b0%e6%8d%ae').replace('世界著录项目数据','%e4%b8%96%e7%95%8c%e8%91%97%e5%bd%95%e9%a1%b9%e7%9b%ae%e6%95%b0%e6%8d%ae'),
                                                filename='I:\Softwares\JetBrains\PythonWorkSpace\CkcestSpider\\resources\pictures\\' +
                                                         item['_id'] + "&id=" + str(index + 1) + ".jpg")
                                    except:
                                        self.file_write.write(url + '\n')
                                        continue
                                    picture_name_one.append(item['_id'] + "&id=" + str(index + 1) + ".jpg")

                            # 相关专利
                            related_patent = session.get('http://www.ckcest.cn/default/es/search/getLiterature22?type=3001&text=' + item['TI'] if "TI" in item else None, headers=header)
                            try:
                                related_patent_datas = json.loads(related_patent.text)
                                related_datas = related_patent_datas['results']['datas']
                            except:
                                continue

                            related_ids = []
                            for related_item in related_datas:
                                if self.is_parse_id(related_item['_id']):
                                    continue
                                picture_urls = related_item['picture_url'] if "picture_url" in related_item else None,

                                picture_name_two = []
                                if picture_urls[0]:
                                    for index, picture_url in enumerate(str(picture_urls[0]).split(';')):
                                        try:
                                            urlretrieve(
                                                picture_url.replace('增量数据', '%e5%a2%9e%e9%87%8f%e6%95%b0%e6%8d%ae').replace('世界著录项目数据','%e4%b8%96%e7%95%8c%e8%91%97%e5%bd%95%e9%a1%b9%e7%9b%ae%e6%95%b0%e6%8d%ae'),
                                                    filename='I:\Softwares\JetBrains\PythonWorkSpace\CkcestSpider\\resources\pictures\\' +
                                                             related_item['_id'] + "&id=" + str(index + 1) + ".jpg")
                                            picture_name_two.append(related_item['_id'] + "&id=" + str(index + 1) + ".jpg")
                                        except:
                                            self.file_write.write(picture_url+'\n')

                                sponsor_strftime = datetime.strptime(str(related_item['publish_date']), '%Y%m%d')
                                format_sponsor_time = datetime.strftime(sponsor_strftime, '%Y-%m-%d')
                                related_dict_data = {
                                    # 出版年份
                                    "application_year": related_item[
                                        'application_year'] if "application_year" in related_item else None,
                                    # 公布日
                                    "publish_date": format_sponsor_time,
                                    # 专利权人
                                    "patentee_list": related_item['patentee_list'] if "patentee_list" in related_item else None,
                                    # 申请日期
                                    "application_date": related_item[
                                        'application_date'] if "application_date" in related_item else None,
                                    # 公布号
                                    "publication_number": related_item[
                                        'publication_number'] if "publication_number" in related_item else None,
                                    # 专利类型
                                    "patent_type": related_item['patent_type'] if "patent_type" in related_item else None,
                                    # 标题语言
                                    "title_language": related_item['title_language'] if "title_language" in related_item else None,
                                    # 数据来源
                                    "datasource": related_item['datasource'] if "datasource" in related_item else None,
                                    # 标题
                                    "title": related_item['TI'] if "TI" in related_item else None,
                                    # 国家
                                    "country_code": related_item['country_code'] if "country_code" in related_item else None,
                                    # 关键词
                                    "key_words": related_item['KY'] if "KY" in related_item else None,
                                    # 资源类型
                                    "resource_type": related_item['resource_type'] if "resource_type" in related_item else None,
                                    # 申请人
                                    "application_list": related_item[
                                        'application_list'] if "application_list" in related_item else None,
                                    # 代理机构
                                    "agency": related_item['agency'] if "agency" in related_item else None,
                                    # 摘要
                                    "summary": related_item['AB'] if "AB" in related_item else None,
                                    # ipc
                                    "ipc": related_item['ipc'] if "ipc" in related_item else None,
                                    # 发明人
                                    "inventor": related_item['inventor'] if "inventor" in related_item else None,
                                    # 申请号
                                    "application_no": related_item['application_no'] if "application_no" in related_item else None,
                                    # 数据表名
                                    "source_table_name": related_item[
                                        'source_table_name'] if "source_table_name" in related_item else None,
                                    # 网页URL
                                    "url": "http://www.ckcest.cn/default/es3/detail?md5=" + related_item[
                                        '_id'] + "&tablename=" + related_item[
                                               'source_table_name'] if "source_table_name" in related_item else None,
                                    "picture_urls": related_item['picture_url'] if "picture_url" in related_item else None,
                                    "picture_name": picture_name_two if picture_name_two else None,
                                    # 插入时间
                                    "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),
                                }
                                related_ids.append(''.join(related_item['_id']))
                                insert_num += 1
                                table.insert_one(related_dict_data)

                            sponsor_strftime = datetime.strptime(str(item['publish_date']), '%Y%m%d')
                            format_sponsor_time = datetime.strftime(sponsor_strftime, '%Y-%m-%d')
                            dict_data = {
                                # 出版年份
                                "application_year": item['application_year'] if "application_year" in item else None,
                                # 公布日
                                "publish_date": format_sponsor_time,
                                # 专利权人
                                "patentee_list": item['patentee_list'] if "patentee_list" in item else None,
                                # 申请日期
                                "application_date": item['application_date'] if "application_date" in item else None,
                                # 公布号
                                "publication_number": item['publication_number'] if "publication_number" in item else None,
                                # 专利类型
                                "patent_type": item['patent_type'] if "patent_type" in item else None,
                                # 标题语言
                                "title_language": item['title_language'] if "title_language" in item else None,
                                # 数据来源
                                "datasource": item['datasource'] if "datasource" in item else None,
                                # 标题
                                "title": item['TI'] if "TI" in item else None,
                                # 国家
                                "country_code": item['country_code'] if "country_code" in item else None,
                                # 关键词
                                "key_words": item['KY'] if "KY" in item else None,
                                # 资源类型
                                "resource_type": item['resource_type'] if "resource_type" in item else None,
                                # 申请人
                                "application_list": item['application_list'] if "application_list" in item else None,
                                # 代理机构
                                "agency": item['agency'] if "agency" in item else None,
                                # 摘要
                                "summary": item['AB'] if "AB" in item else None,
                                # ipc
                                "ipc": item['ipc'] if "ipc" in item else None,
                                # 发明人
                                "inventor": item['inventor'] if "inventor" in item else None,
                                # 申请号
                                "application_no": item['application_no'] if "application_no" in item else None,
                                # 数据表名
                                "source_table_name": item['source_table_name'] if "source_table_name" in item else None,
                                # 网页URL
                                "url": "http://www.ckcest.cn/default/es3/detail?md5=" + item['_id'] + "&tablename="+item['source_table_name'] if "source_table_name" in item else None,
                                # 相关专利的ID
                                "related_ids": related_ids if related_ids else None,
                                # 图片
                                "picture_urls": item['picture_url'] if "picture_url" in item else None,
                                "picture_name":picture_name_one if picture_name_one else None,
                                # 插入时间
                                "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),
                            }
                            insert_num += 1
                            table.insert_one(dict_data)

                        print("insert num" + str(insert_num))
        self.file_write.close()

    def is_parse_id(self, ckcest_id):
        """
        判断是否已经爬取过该id
        :return:
        """
        try:
            if self.redis_con.hexists('already_parse', ckcest_id):
                return True
            else:
                self.redis_con.hset('already_parse', ckcest_id, 1)
                return False
        except Exception as err:
            return False


if __name__ == '__main__':
    ckcest = SpiderCkcest()
    ckcest.ckcest_spider()
