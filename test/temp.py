# -*- coding: utf-8 -*-
#
# CkcestSpider
# Author: ZuoWei
# Email: zuowei@yuchen.com
# Created Time: 2021/11/30 16:46
import time

import pymongo

mongo_url = "mongodb://192.168.12.199:27017"
mongo_db = "ckcest"
client = pymongo.MongoClient(mongo_url, connect=False)
db = client[mongo_db]

one = db['ckcest_org_source']
two = db['ckcest_org_source_copy2']

all_org = []
find = one.find()
for _ in find:
    two.insert_one({
        "name": _.get('name', ''),
        "affiliations": _.get('affiliations', ''),
        "arwuW": _.get('arwuW', ''),
        "staff": _.get('staff', ''),
        "established": _.get('established', ''),
        "country": _.get('country', ''),
        "orgType": _.get('orgType', ''),
        "short_abstracts_zh": _.get('short_abstracts_zh', ''),
        "long_abstracts_zh": _.get('long_abstracts_zh', ''),
        "short_abstracts_en": _.get('short_abstracts_en', ''),
        "long_abstracts_en": _.get('long_abstracts_en', ''),
        "keyWords": _.get('keyWords', ''),
        "dataSource": _.get('dataSource', ''),
        "img_new": _.get('img_new', ''),
        "create_time": _.get('create_time', str(time.strftime('%Y-%m-%d %H:%M:%S'))),
        "insertTime": str(time.strftime('%Y-%m-%d %H:%M:%S')),
    })
    # num += 1
