import json
import re
import time
import pymongo
import requests

'''万方数据'''


class SpiderWanFang():

    def __init__(self):
        self.mongo_url = "mongodb://192.168.10.223:27017"
        self.mongo_db = "ckcest"
        self.mongo_table = "wanfang_result"
        self.mongo_user = "ckcest"
        self.mongo_pwd = "123456"
        self.client = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db = self.client[self.mongo_db]
        self.db.authenticate(self.mongo_user, self.mongo_pwd)
        self.facet_filed_dict = {
            "T": '工业技术',
            "R": '医药、卫生',
            "U": '交通运输',
            "X": '环境科学、安全科学',
            "S": '农业科学',
            "E": '军事',
            "P": '天文学、地球科学',
            "O": '数理科学和化学',
            "F": '经济',
            "V": '航空、航天',
            "Q": '生物科学',
            "G": '文化、科学、教育、体育'
        }

    def WanFangSpider(self):
        insert_num = 1
        ids_num = 1
        header = {
            "Accept": "text/html, */*; q=0.01",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36"
        }
        session = requests.session()
        session.DEFAULT_RETRIES = 5
        session.keep_alive = False
        table = self.db[self.mongo_table]

        result_ids = {}
        # for facet_field in ["T"]:
        for facet_field in ["T", "R", "U", "X", "S", "E", "P", "O", "F", "V", "Q", "G"]:
            result_list = []
            for num in range(1, 99999):
                url = 'http://www.wanfangdata.com.cn/search/searchList.do?beetlansyId=aysnsearch&searchType=tech_result&pageSize=20&page=' + str(
                    num) + '&searchWord=%E5%90%8E%E5%8B%A4%20%E5%86%9B&order=correlation&showType=detail&isCheck=check&isHit=&isHitUnit=&facetField=%24subject_classcode_level%3A%E2%88%B7%2F' + facet_field + '&facetName=' + \
                      self.facet_filed_dict[
                          facet_field] + ':$subject_classcode_level&firstAuthor=false&corePerio=false&alreadyBuyResource=false&rangeParame=&navSearchType=tech_result'
                get_cotent = session.get(url, headers=header)
                if get_cotent.status_code == 200:
                    text = get_cotent.text
                    pattern = re.compile("this.id,'(.*?)'", re.S)
                    ids = pattern.findall(text)
                    if not ids:
                        break
                    for item in ids:
                        result_list.append(item)
                        ids_num += 1
            result_ids[facet_field] = result_list

        for result_key in result_ids:
            for ids in result_ids[result_key]:
                # url
                url = 'http://d.wanfangdata.com.cn/cstad/' + ids
                # 详情
                detail_url = 'http://d.wanfangdata.com.cn/Detail/Cstad/'
                # 相关主题
                keywords_url = 'http://d.wanfangdata.com.cn/Detail/RelatedDetail'
                # 相关文献
                related_articles_url = 'http://d.wanfangdata.com.cn/Detail/RelatedPaper'
                detail_data = {'Id': ids}

                result_get = session.post(detail_url, headers=header, json=detail_data)
                if not result_get.status_code == 200:
                    continue
                final_dumps = json.loads(result_get.text)
                detail = final_dumps['detail'][0]['Cstad']
                keywords = final_dumps['detail'][0]['Cstad']['Keywords']
                keywords_list = []
                related_articles = []
                if keywords:
                    keywords_data = {'KeyWord': keywords, "RelatedType": "TOPIC", "ResourceType": "Cstad"}
                    related_articles_data = {'KeyWord': keywords, "Id": ids, "RelatedType": "SimilarPaper",
                                             "ResourceType": "Cstad"}

                    keywords_result_get = session.post(keywords_url, headers=header, json=keywords_data)
                    if keywords_result_get.status_code == 200:
                        dumps = json.loads(keywords_result_get.text)
                        try:
                            keywords_list = dumps['Result']['Keywords']['list']
                        except:
                            keywords_list = []

                    related_articles_get = session.post(related_articles_url, headers=header,
                                                        json=related_articles_data)
                    if related_articles_get.status_code == 200:
                        dumps = json.loads(related_articles_get.text)
                        related_articles = dumps['detail']

                detail['related_subject '] = keywords_list
                detail['related_articles '] = related_articles
                detail['subject_type '] = self.facet_filed_dict[result_key]
                detail['url '] = url
                detail['insert_time '] = str(time.strftime('%Y-%m-%d %H:%M:%S'))
                del detail['Transaction']
                table.insert_one(detail)
                insert_num += 1

                print("insert num" + str(insert_num))

        self.client.close()


if __name__ == '__main__':
    wanfang = SpiderWanFang()
    wanfang.WanFangSpider()
