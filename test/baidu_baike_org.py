import json
import time
from urllib.request import urlretrieve
from bson.objectid import ObjectId

import pymongo, requests
from requests_html import HTMLSession

session = HTMLSession()

mongo_url = "mongodb://192.168.12.199:27017"
mongo_db = "ckcest"
client = pymongo.MongoClient(mongo_url, connect=False)
db = client[mongo_db]

data_collect_news = db['ckcest_specialist']
org_mongo = db['ckcest_org_source_copy2']
find = data_collect_news.find({})

num = 0
all_org = set()
for item in find:
    if item['organization']:
        for _ in item['organization']:
            all_org.add(_)
            num += 1
            print(num)

with open('org.txt', 'w+', encoding='UTF-8') as f:
    f.write(json.dumps(list(all_org), ensure_ascii=False))

print(f'len: {len(all_org)}')

import pymysql

connect = pymysql.connect(host='192.168.12.186', user='root', password='mysqlpass@1234', db='baidu_baike')

cursor = connect.cursor(pymysql.cursors.DictCursor)

execute = cursor.execute(f'select *  from lemmas where title in %s group by title', (all_org,))

fetchall = cursor.fetchall()
for _ in fetchall:
    title = _.get('title')
    abstract = _.get('abstract')
    infobox = json.dumps(json.loads(_.get('infobox')), ensure_ascii=False)
    pic = _.get('interPic')
    subject = _.get('subject')

    org_count = org_mongo.count({'name': title})
    if org_count > 0:
        print('跳过')
        continue

    if pic:
        if pic.startswith('http'):
            suffix = pic.split('.')[-1]
            try:
                img_name = f"ckcest_org_{ObjectId()}.{suffix}"

                response = session.get(f'http://baike.baidu.com/item/{title}')
                response.encoding = 'utf-8'
                img_url = response.html.xpath("//div[@class='summary-pic']//img/@src", first=True)
                if img_url:
                    urlretrieve(img_url, filename=fr"D:\Resource\baidu\\{img_name}")
                    pic = img_name
                else:
                    pic = ''
            except Exception as e:
                pic = ''
                print("error")

    org_mongo.insert_one({
        "name": title,
        "short_abstracts_zh": abstract,
        "long_abstracts_zh": abstract,
        "infobox": infobox,
        "img_new": pic,
        "insertTime": str(time.strftime('%Y-%m-%d %H:%M:%S')),
    })
