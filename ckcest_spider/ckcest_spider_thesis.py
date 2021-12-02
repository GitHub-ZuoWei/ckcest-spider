import json
import time
import urllib.parse
from datetime import datetime

import pymongo
import redis

import requests
from opencc import OpenCC
cc = OpenCC('t2s')

'''期刊论文'''
"""
1002   期刊论文

"""

class SpiderCkcest():

    def __init__(self):
        self.mongo_url = "mongodb://192.168.12.187:27017"
        self.mongo_db = "ckcest"
        self.mongo_table = "ckcest_thesis_temp"
        self.mongo_user = "ckcest"
        self.mongo_pwd = "123456"
        self.client = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db = self.client[self.mongo_db]
        self.db.authenticate(self.mongo_user, self.mongo_pwd)

        self.redis_con = redis.Redis(host='192.168.12.187', port='6379', db=1)
        # self.redis_con.flushdb()
        self.file_write = open("error_url.txt", "w")

    def ckcest_spider(self):
        # 本次爬取数量
        insert_num = 1
        # 本次爬取重复数量
        spider_num = 1

        header = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36 Edg/86.0.622.69"
        }
        session = requests.session()
        session.DEFAULT_RETRIES = 5
        session.keep_alive = False
        table = self.db[self.mongo_table]

        # 页
        url = 'http://www.ckcest.cn/default/es3/search/postContent'
        for new_energy_keywords in open("../spider_keywords.txt", encoding="UTF-8"):
            new_energy_keywords = new_energy_keywords.replace('\n', '')
            new_energy_keywords_quote = urllib.parse.quote(new_energy_keywords)

            request_data = {
                "dbId": "1002, 1007, 3001",
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

            # all_one_classify_name = []
            # all_one_classify_label = []
            # all_two_classify_name = []
            # all_two_classify_label = []

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
                                    "dbId": "1002, 1007, 3001",
                                    "text": "",
                                    "express": "TI%3D%3D" + new_energy_keywords_quote + "%20OR%20KY%3D%3D" + new_energy_keywords_quote,
                                    "secondSearchExpress": "RESTY%3D%3D1002%20AND%20LANG%3D%3D1001%20AND%20CCC1%3D%3D" + urllib.parse.quote(classify_label),
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
                                                    print(classify_name_two)
                                                    print(classify_label_two)

                                                    for num in range(1, 101):
                                                        request_data = {
                                                            "dbId": "1002, 1007, 3001",
                                                            "text": '',
                                                            "express": "TI%3D%3D" + new_energy_keywords_quote + "%20OR%20KY%3D%3D" + new_energy_keywords_quote,
                                                            # "express": "TI=新能源 OR KY=新能源",
                                                            # "secondSearchExpress": "CCC2==" + label_two[:2] + "-%3E" + label_two + " AND LANG==1001",
                                                            "secondSearchExpress": "RESTY%3D%3D1002%20AND%20LANG%3D%3D1001%20AND%20CCC2%3D%3D" + classify_label_two[:2] + "-%3E" + classify_label_two,
                                                            "order": "101",
                                                            "startYear": '1700',
                                                            "endYear": '2099',
                                                            "page": str(num),
                                                            "limit": "10",
                                                            "isadvanced": "1",
                                                            "uuid": '',
                                                            "country": "China",
                                                            "province": "Beijing",
                                                            "city": "Beijing"
                                                        }

                                                        get = session.post(url, headers=header, data=request_data)
                                                        try:
                                                            results_datas = get.json()['results']['datas']
                                                        except:
                                                            break
                                                        print(classify_name)
                                                        print(classify_label)
                                                        print(classify_name_two)
                                                        print(classify_label_two)
                                                        print(request_data)
                                                        # print(results_datas)

                                                        for item in results_datas:

                                                            if self.is_parse_id(item['_id']):
                                                                spider_num += 1
                                                                print("spider_num: "+str(spider_num))
                                                                continue

                                                            # 相似文献
                                                            related_patent = session.get(
                                                                'http://www.ckcest.cn/default/es/search/getLiterature22?type=1002&text=' +
                                                                urllib.parse.quote(urllib.parse.quote(item['TI'])) if "TI" in item else None, headers=header)

                                                            related_patent_datas = related_patent.json()

                                                            related_ids = []
                                                            if related_patent_datas and related_patent_datas['code'] == 10000:
                                                                for related_item in related_patent_datas['results']['datas']:
                                                                    if self.is_parse_id(related_item['_id']):
                                                                        spider_num += 1
                                                                        print("spider_num: "+str(spider_num))
                                                                        continue
                                                                    sponsor_strftime = datetime.strptime(str(related_item['publish_date']), '%Y%m%d')
                                                                    format_sponsor_time = datetime.strftime(sponsor_strftime,'%Y-%m-%d')

                                                                    related_author_organization = []
                                                                    if "author_organization" in related_item:
                                                                        try:
                                                                            for x in list(set(related_item['author_organization'])):
                                                                                if x:
                                                                                    related_author_organization.append(x)
                                                                        except:
                                                                            continue
                                                                    else:
                                                                        related_author_organization = None
                                                                    related_dict_data = {
                                                                        "id": related_item['_id'],
                                                                        # 发表时间
                                                                        "publish_date": format_sponsor_time,
                                                                        # 发表年份
                                                                        "publish_year": related_item['publish_year'],
                                                                        # 期刊名称
                                                                        "journal_title": related_item[
                                                                            'journal_title'] if "journal_title" in related_item else None,
                                                                        # 作者
                                                                        "author": related_item[
                                                                            'author'] if "author" in related_item else None,
                                                                        # 单位
                                                                        "author_organization": related_author_organization,
                                                                        # 标题语言 1
                                                                        "title_language": related_item[
                                                                            'title_language'] if "title_language" in related_item else None,
                                                                        # 数据来源 1
                                                                        "datasource": related_item[
                                                                            'datasource'] if "datasource" in related_item else None,
                                                                        # 标题 1
                                                                        "title": cc.convert(''.join(str(related_item['TI'])).replace('<em>','').replace('</em>','')) if "TI" in related_item else None,
                                                                        # 国家 1
                                                                        "country_code": related_item[
                                                                            'country_code'] if "country_code" in related_item else None,
                                                                        # 关键词 1
                                                                        "key_words": [''.join(x).replace('<em>','').replace('</em>','') for x in related_item['KY']] if "KY" in related_item else None,
                                                                        # 资源类型 1
                                                                        "resource_type": related_item[
                                                                            'resource_type'] if "resource_type" in related_item else None,
                                                                        # 摘要
                                                                        "summary": cc.convert(''.join(related_item['AB']).replace('<em>','').replace('</em>','')) if "AB" in related_item else None,
                                                                        # 分类编号
                                                                        "ccc": item['ccc'] if "ccc" in item else None,
                                                                        # 一级分类
                                                                        "ccc_l1": item['ccc_l1'] if "ccc_l1" in item else None,
                                                                        # 二级分类
                                                                        "ccc_l2": item['ccc_l2'] if "ccc_l2" in item else None,
                                                                        # 数据表名
                                                                        "source_table_name": related_item[
                                                                            'source_table_name'] if "source_table_name" in related_item else None,
                                                                        # 网页URL
                                                                        "url": "http://www.ckcest.cn/default/es3/detail?md5=" +
                                                                               related_item[
                                                                                   '_id'] + "&tablename=" + related_item[
                                                                                   'source_table_name'] if "source_table_name" in related_item else None,
                                                                        # 插入时间
                                                                        "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),
                                                                        # 搜索关键词
                                                                        "search_key_words": new_energy_keywords,
                                                                        # 父ID
                                                                        "related_parent_id": item['_id']
                                                                    }
                                                                    related_ids.append(''.join(related_item['_id']))
                                                                    insert_num += 1
                                                                    table.insert_one(related_dict_data)


                                                            sponsor_strftime = datetime.strptime(str(item['publish_date']), '%Y%m%d')
                                                            format_sponsor_time = datetime.strftime(sponsor_strftime, '%Y-%m-%d')

                                                            author_organization = []
                                                            if "author_organization" in item:
                                                                for y in list(set(item['author_organization'])):
                                                                    if y:
                                                                        author_organization.append(y)
                                                            else:
                                                                author_organization = None
                                                            dict_data = {
                                                                "id": item['_id'],
                                                                # 发表时间
                                                                "publish_date": format_sponsor_time,
                                                                # 发表年份
                                                                "publish_year": item['publish_year'],
                                                                # 期刊名称
                                                                "journal_title": item['journal_title'] if "journal_title" in item else None,
                                                                # 作者
                                                                "author": item['author'] if "author" in item else None,
                                                                # 单位
                                                                "author_organization": author_organization,
                                                                # 标题语言 1
                                                                "title_language": item['title_language'] if "title_language" in item else None,
                                                                # 数据来源 1
                                                                "datasource": item['datasource'] if "datasource" in item else None,
                                                                # 标题 1
                                                                "title": cc.convert(str(item['TI']).replace('<em>','').replace('</em>','')) if "TI" in item else None,
                                                                # 国家 1
                                                                "country_code": item['country_code'] if "country_code" in item else None,
                                                                # 关键词 1
                                                                "key_words": [x.replace('<em>','').replace('</em>','') for x in item['KY']] if "KY" in item else None,
                                                                # 资源类型 1
                                                                "resource_type": item['resource_type'] if "resource_type" in item else None,
                                                                # 摘要
                                                                "summary": cc.convert(''.join(str(item['AB'])).replace('<em>','').replace('</em>','')) if "AB" in item else None,
                                                                # 分类编号
                                                                "ccc": item['ccc'] if "ccc" in item else None,
                                                                # 一级分类
                                                                "ccc_l1": item['ccc_l1'] if "ccc_l1" in item else None,
                                                                # 二级分类
                                                                "ccc_l2": item['ccc_l2'] if "ccc_l2" in item else None,
                                                                # 数据表名
                                                                "source_table_name": item['source_table_name'] if "source_table_name" in item else None,
                                                                # 网页URL
                                                                "url": "http://www.ckcest.cn/default/es3/detail?md5=" + item[
                                                                    '_id'] + "&tablename=" + (item['source_table_name'] if "source_table_name" in item else None) + "&year=&dbid=1002",
                                                                # 插入时间
                                                                "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),
                                                                # 搜索关键词
                                                                "search_key_words": new_energy_keywords,
                                                                # 一级类别
                                                                "first_category": classify_name,
                                                                # 二级类别
                                                                "second_category": classify_name_two,
                                                                # 相关项目ID
                                                                "related_ids": related_ids
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
            if self.redis_con.hexists('already_parse_thesis', ckcest_id):
                return True
            else:
                self.redis_con.hset('already_parse_thesis', ckcest_id, 1)
                return False
        except Exception as err:
            return False


if __name__ == '__main__':
    ckcest = SpiderCkcest()
    ckcest.ckcest_spider()
