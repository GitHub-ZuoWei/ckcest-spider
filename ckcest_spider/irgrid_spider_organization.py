import json
import time
from datetime import datetime

import pymongo

import requests

'''机构'''


class SpiderCkcest():

    def __init__(self):
        self.mongo_url = "mongodb://192.168.10.223:27017"
        self.mongo_db = "ckcest"
        self.mongo_table = "irgrid_organization"
        self.mongo_user = "ckcest"
        self.mongo_pwd = "123456"
        self.client = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db = self.client[self.mongo_db]
        self.db.authenticate(self.mongo_user, self.mongo_pwd)

    def CkcestSpider(self):
        insert_num = 1
        header = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        session = requests.session()
        session.DEFAULT_RETRIES = 5
        session.keep_alive = False
        table = self.db[self.mongo_table]
        for num in range(1, 13):
            url = 'http://www.irgrid.ac.cn/repository-list?fragment=repository&page=' + str(num) + '&sort_by=1&province=all'
            get = session.get(url, headers=header)
            try:
                datas_temp = json.loads(get.text)
                print(datas_temp)
                datas = datas_temp['results']['datas']
            except Exception as e:
                print(datas_temp)
                continue
            for item in datas:
                sponsor_strftime = datetime.strptime(str(item['publish_date']), '%Y%m%d')
                format_sponsor_time = datetime.strftime(sponsor_strftime, '%Y-%m-%d')

                dict_data = {
                    # 出版年份
                    "publish_year": item['publish_year'] if "publish_year" in item else None,
                    # 数据源类别
                    "datasource_class": item['datasourceclass'] if "datasourceclass" in item else None,
                    # 语言
                    "language": item['language'] if "language" in item else None,
                    # 项目区域
                    "project_area": item['project_area'] if "project_area" in item else None,
                    # 机标关键词
                    "KYA": item['KYA'] if "KYA" in item else None,
                    # 关键词
                    "KYO": item['KYO'] if "KYO" in item else None,
                    # 知识中心分类号
                    "ccc_l2": item['ccc_l2'] if "ccc_l2" in item else None,
                    # 资助机构
                    "funder_organization": item['funder_organization'] if "funder_organization" in item else None,
                    # 项目类型
                    "project_type": item['project_type'] if "project_type" in item else None,
                    # 项目负责人
                    "author": item['author'] if "author" in item else None,
                    # 资源类型
                    "resource_type": item['resource_type'] if "resource_type" in item else None,
                    # 项目金额
                    "funding_usd": item['funding_usd'] if "funding_usd" in item else None,
                    # 承担机构所在国家
                    "funder_organization_country": item[
                        'funder_organization_country'] if "funder_organization_country" in item else None,
                    # 标题
                    "title": item['TIO'] if "TIO" in item else None,
                    # 文章ID
                    "id": item['_id'] if "_id" in item else None,
                    # 摘要
                    "ABO": item['ABO'] if "ABO" in item else None,
                    # 摘要
                    "ABT": item['ABT'] if "ABT" in item else None,
                    # 承担机构
                    "leader_organization": item['leader_organization'] if "leader_organization" in item else None,
                    # 发布时间
                    "publish_date": format_sponsor_time,
                    # 来源
                    "detail_name": str(item['detail_url']).split('=')[0] if "detail_url" in item else None,
                    # 来源URL
                    "detail_url": str(item['detail_url']).split('=')[1] if "detail_url" in item else None,
                    # 标题英文翻译
                    "title_translate": item['TIT'] if "TIT" in item else None,
                    # 网页URL
                    "url": "http://www.ckcest.cn/default/es3/detail?md5=" + item['_id'] + "&tablename=dw_project2020",
                    # 插入时间
                    "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),

                }
                insert_num += 1
                one = table.insert_one(dict_data)

            print("insert num" + str(insert_num))


if __name__ == '__main__':
    ckcest = SpiderCkcest()
    ckcest.CkcestSpider()
