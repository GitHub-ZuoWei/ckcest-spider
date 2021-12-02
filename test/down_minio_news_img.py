# -*- coding: utf-8 -*-
#
# CkcestSpider
# Author: ZuoWei
# Email: zuowei@yuchen.com
# Created Time: 2021/11/18 11:31
import pymongo
import redis

# redis_client = redis.StrictRedis(host='192.168.12.100', port=6379, db=1, decode_responses=True)

# num = redis_client.scard('proxy_ip')

# print(num)


mongo_url = "mongodb://192.168.12.199:27017"
mongo_db = "data_collect_news"
client = pymongo.MongoClient(mongo_url, connect=False)
db = client[mongo_db]

data_collect_news = db['data_collect_news']

find = data_collect_news.find({"task_id": "2e1478b0c6e34dcb92ef1e9c295d9a90"})

all_img = []
for item in find:
    if item['local_img_url']:
        for _ in item['local_img_url']:
            all_img.append(_)

from minio import Minio

minio = Minio(endpoint="192.168.12.196:8888", access_key="minio", secret_key="admin123456", secure=False)
for _ in all_img:
    get_object = minio.fget_object(bucket_name="newsfiles", object_name=_,file_path=f'D:\Resource\\news\{_}')
