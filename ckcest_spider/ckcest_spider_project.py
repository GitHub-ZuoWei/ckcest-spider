import json
import re
import sys
import time
import urllib.parse
from datetime import datetime
from urllib.request import urlretrieve
from utils.ckcest_cookies import get_ckcest_cookies
from loguru import logger

import pymongo
import redis
import urllib.parse
import redisbloomfilter
import requests
from utils.ckcest_cookies import get_ckcest_specialist_cookies
from opencc import OpenCC

cc = OpenCC('t2s')

'''科研项目'''


class SpiderCkcest:

    def __init__(self):
        logger.add(sys.stderr, format="{time} {level} {message}", filter="ckcest_module", level="INFO")
        self.mongo_url = "mongodb://192.168.12.199:27017"
        self.mongo_db = "ckcest"
        # self.mongo_table = "ckcest_specialist"
        # self.mongo_user = "ckcest"
        # self.mongo_pwd = "123456"

        # 插入成功条数
        self.insert_num = 1
        # redis 键的名字
        bloomfilter_ckcest = "bloomfilter_ckcest"
        # # 专家
        # bloomfilter_specialist = "bloomfilter_specialist"
        # # 项目
        # bloomfilter_project = "bloomfilter_project"
        # # 论文
        # bloomfilter_thesis = "bloomfilter_thesis"

        # 预先估计要去重的数量
        number_of_insertion = 100000000
        # 错误率
        error_rate = 0.00001

        self.client = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db = self.client[self.mongo_db]
        self.table = self.db['ckcest_project']
        # self.db.authenticate(self.mongo_user, self.mongo_pwd)

        # 布隆过滤器
        self.redis_client = redis.StrictRedis(host='192.168.12.100', port=6379, db=6, decode_responses=True)
        self.bloom_filter = redisbloomfilter.RedisBloomFilter(bloomfilter_ckcest, number_of_insertion, error_rate,
                                                              self.redis_client)
        self.bloom_filter.initialize()

        self.header = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36 Edg/86.0.622.69",
            # "Host": "www.ckcest.cn",
            # "Referer": "http://mall.ckcest.cn/mall/list.html?typeId=2003",
        }
        self.proxies = {'http': '192.168.17.243:10809', 'https': '192.168.17.243:10809'}
        self.session = requests.session()
        self.session.DEFAULT_RETRIES = 3
        self.session.keep_alive = False

        # 本次爬取数量
        self.insert_num = 1
        # 本次爬取重复数量
        self.spider_num = 1

    def ckcest_spider(self):
        url = 'http://www.ckcest.cn/default/es3/search/postContent'
        for new_energy_keywords in open("../spider_keywords.txt", encoding="UTF-8"):
            new_energy_keywords = new_energy_keywords.replace('\n', '')
            new_energy_keywords_quote = urllib.parse.quote(new_energy_keywords)
            request_data = {
                "dbId": "1007",
                "text": "",
                "express": f"TI=={new_energy_keywords} OR KY={new_energy_keywords} OR AB={new_energy_keywords} OR LDROG={new_energy_keywords}" ,
                "secondSearchExpress": "RESTY==1007",
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

            json_datas = self.session.post(url, headers=self.header, data=request_data)
            if json_datas.status_code == 200:
                results_datas = json_datas.json()['results']['aggs']
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
                                        "dbId": "1007",
                                        "text": "",
                                        "express": f"TI%3D%3D{new_energy_keywords_quote}%20OR%20KY%3D{new_energy_keywords_quote}%20OR%20AB%3D{new_energy_keywords_quote}%20OR%20LDROG%3D{new_energy_keywords_quote}",
                                        "secondSearchExpress": "RESTY%3D%3D1007%20AND%20LANG%3D%3D1001%20AND%20CCC1%3D%3D" + urllib.parse.quote(classify_label),
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
                                    json_datas = self.session.post(url, headers=self.header, data=request_data)
                                    if json_datas.status_code == 200:
                                        results_datas = json_datas.json()['results']['aggs']

                                    for item in results_datas:
                                        if item['showName'] == '中国工程科技分类':
                                            print(item['datas'])
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
                                                        # print(classify_name_two)
                                                        # print(classify_label_two)
                                                        logger.info(classify_name)
                                                        for num in range(1, 101):
                                                            request_data = {
                                                                "dbId": "1007",
                                                                "text": '',
                                                                "express": f"TI%3D%3D{new_energy_keywords_quote}%20OR%20KY%3D{new_energy_keywords_quote}%20OR%20AB%3D{new_energy_keywords_quote}%20OR%20LDROG%3D{new_energy_keywords_quote}",
                                                                # "express": "TI=新能源 OR KY=新能源",
                                                                # "secondSearchExpress": "CCC2==" + label_two[:2] + "-%3E" + label_two + " AND LANG==1001",
                                                                "secondSearchExpress": "RESTY%3D%3D1007%20AND%20LANG%3D%3D1001%20AND%20CCC2%3D%3D" + classify_label_two[:2] + "-%3E" + classify_label_two + "%20AND%20LANG%3D%3D1001",
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

                                                            json_data = self.session.post(url, headers=self.header,
                                                                                          data=request_data)
                                                            try:
                                                                results_datas = json_data.json()['results']['datas']
                                                            except Exception as e:
                                                                logger.warning('break', e)
                                                                break
                                                            # print(classify_name)
                                                            # print(classify_label)
                                                            # print(classify_name_two)
                                                            # print(classify_label_two)
                                                            # print(request_data)
                                                            for item in results_datas:
                                                                data_id = item['_id']
                                                                if self.is_parse(data_id):
                                                                    logger.warning(f'已经解析过项目 : {data_id}')
                                                                    continue
                                                                sponsor_strftime = datetime.strptime(
                                                                    str(item['publish_date']), '%Y%m%d')
                                                                format_sponsor_time = datetime.strftime(
                                                                    sponsor_strftime, '%Y-%m-%d')
                                                                dict_data = {
                                                                    # 文章ID
                                                                    "id": item['_id'] if "_id" in item else None,
                                                                    # 标题
                                                                    "title": cc.convert(
                                                                        ''.join(item['TIO']).replace('<em>',
                                                                                                     '').replace(
                                                                            '</em>', '')) if "TIO" in item else None,
                                                                    # 标题英文翻译
                                                                    "title_translate": ''.join(item['TIT']).replace(
                                                                        '<em>', '').replace('</em>',
                                                                                            '') if "TIT" in item else None,
                                                                    # 出版年份
                                                                    "publish_year": item[
                                                                        'publish_year'] if "publish_year" in item else None,
                                                                    # 发布时间
                                                                    "publish_date": format_sponsor_time,
                                                                    # 数据源类别
                                                                    "datasource_class": item[
                                                                        'datasourceclass'] if "datasourceclass" in item else None,
                                                                    # 语言
                                                                    "language": item[
                                                                        'language'] if "language" in item else None,
                                                                    # 项目区域
                                                                    "project_area": item[
                                                                        'project_area'] if "project_area" in item else None,
                                                                    # 机标关键词
                                                                    "key_words_other": [cc.convert(
                                                                        ''.join(x).replace('<em>', '').replace('</em>',
                                                                                                               '')) for
                                                                        x in item[
                                                                            'KYA']] if "KYA" in item else None,
                                                                    # 关键词
                                                                    "key_words": [cc.convert(
                                                                        ''.join(x).replace('<em>', '').replace('</em>',
                                                                                                               '')) for
                                                                        x in item[
                                                                            'KYO']] if "KYO" in item else None,
                                                                    # 分类编号
                                                                    "ccc": item['ccc'] if "ccc" in item else None,
                                                                    # 一级分类
                                                                    "ccc_l1": item[
                                                                        'ccc_l1'] if "ccc_l1" in item else None,
                                                                    # 二级分类
                                                                    "ccc_l2": item[
                                                                        'ccc_l2'] if "ccc_l2" in item else None,
                                                                    # 资助机构
                                                                    "funder_organization": item[
                                                                        'funder_organization'] if "funder_organization" in item else None,
                                                                    # 项目类型
                                                                    "project_type": cc.convert(item[
                                                                                                   'project_type']) if "project_type" in item else None,
                                                                    # 项目参与者
                                                                    "participant": [cc.convert(x) for x in item[
                                                                        'author']] if "author" in item else None,
                                                                    # 项目负责人
                                                                    "responsibler": [cc.convert(x) for x in item[
                                                                        'responsibler']] if "responsibler" in item else None,
                                                                    # 资源类型
                                                                    "resource_type": item[
                                                                        'resource_type'] if "resource_type" in item else None,
                                                                    # 项目金额
                                                                    "funding_usd": item[
                                                                        'funding_usd'] if "funding_usd" in item else None,
                                                                    # 金额类型
                                                                    "fund_code": item[
                                                                        'fund_code'] if "fund_code" in item else None,
                                                                    # 承担机构所在国家
                                                                    "funder_organization_country": item[
                                                                        'funder_organization_country'] if "funder_organization_country" in item else None,
                                                                    # 中文摘要
                                                                    "zh_summary": cc.convert(
                                                                        ''.join(item['ABO']).replace('<em>',
                                                                                                     '').replace(
                                                                            '</em>', '')) if "ABO" in item else None,
                                                                    # 英文摘要
                                                                    "en_summary": ''.join(item['ABT']).replace('<em>',
                                                                                                               '').replace(
                                                                        '</em>', '') if "ABT" in item else None,
                                                                    # 承担机构
                                                                    "leader_organization": [cc.convert(x) for x in item[
                                                                        'leader_organization']] if "leader_organization" in item else None,
                                                                    # 承担机构国家
                                                                    "leader_organization_country": item[
                                                                        'leader_organization_country'] if "leader_organization_country" in item else None,
                                                                    # 来源
                                                                    "detail_name": str(item['detail_url']).split('=')[
                                                                        0] if "detail_url" in item else None,
                                                                    # 来源URL
                                                                    "detail_url": str(item['detail_url']).split('=')[
                                                                        1] if "detail_url" in item else None,
                                                                    # 网页URL
                                                                    "url": f"http://www.ckcest.cn/default/es3/detail?md5={data_id}&tablename=dw_project2020&year=&dbid=1007",
                                                                    # 插入时间
                                                                    "insert_time": str(
                                                                        time.strftime('%Y-%m-%d %H:%M:%S')),
                                                                    # 搜索关键词
                                                                    "search_key_words": new_energy_keywords,
                                                                    # 一级类别
                                                                    "first_category": classify_name,
                                                                    # 二级类别
                                                                    "second_category": classify_name_two,
                                                                }

                                                                self.table.insert_one(dict_data)
                                                                self.insert_num += 1

                                                                logger.info(f"INSERT NUM : {self.insert_num}")

    # 布隆过滤器
    def is_parse(self, ckcest_id):
        """
        判断是否已经爬取过该id
        :return:
        """
        # 包含
        if self.bloom_filter.contains(ckcest_id):
            return True
        # 不包含
        else:
            self.bloom_filter.put(ckcest_id)
            return False


if __name__ == '__main__':
    ckcest = SpiderCkcest()
    ckcest.ckcest_spider()
