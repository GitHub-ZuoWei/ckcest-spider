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
import redisbloomfilter

import requests
from lxml import etree
from opencc import OpenCC



cc = OpenCC('t2s')

'''专家学者'''


class SpiderCkcest:

    def __init__(self):
        logger.add(sys.stderr, format="{time} {level} {message}", filter="ckcest_module", level="INFO")
        self.mongo_url = "mongodb://192.168.12.199:27017"
        self.mongo_db = "ckcest"
        # self.mongo_table = "ckcest_specialist"
        # self.mongo_user = "ckcest"
        # self.mongo_pwd = "123456"

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
        # self.db.authenticate(self.mongo_user, self.mongo_pwd)

        self.client1 = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db1 = self.client[self.mongo_db]
        # self.db1.authenticate(self.mongo_user, self.mongo_pwd)

        self.client2 = pymongo.MongoClient(self.mongo_url, connect=False)
        self.db2 = self.client[self.mongo_db]
        # self.db2.authenticate(self.mongo_user, self.mongo_pwd)

        # 布隆过滤器
        redis_client = redis.StrictRedis(host='192.168.12.100', port=6379, db=1)
        self.bloom_filter = redisbloomfilter.RedisBloomFilter(bloomfilter_ckcest, number_of_insertion, error_rate,
                                                              redis_client)
        self.bloom_filter.initialize()

        self.file_write = open("error_url.txt", "a+", encoding='utf8')

        # 获取ckcest请求cookies
        self.ckcest_cookies = get_ckcest_cookies()

        self.tmp_cookies = {
            '_Collect_ISNEW': '1637138587749',
            '_Collect_UD': 'e8518435-6de4-49bd-a556-589419f5239c',
            '_uab_collina': '163713858783894397595782',
            'Hm_lvt_789fd650fa0be6a2a064d019d890b87f': '1635917333,1636091311',
            'JSESSIONID': 'AFEE8E7296059F5C929004E2E2867DCB',
            'acw_tc': '3ccdc15916372246468745023e6cd16079a65c7f4bfa8f0d18afbd972fadeb',
            'acw_sc__v2': '619614812e5b6b338b207a5d43f068da8d1ee9ef',
            '__session:0.11493427349697694:': 'http:',
            '__session:0.39083174526785647:': 'http:',
            'Hm_lpvt_789fd650fa0be6a2a064d019d890b87f': '1637225610',
            'ssxmod_itna': 'eqIx0D97i=0=qD5GQDXDnQAqeEopd3DgGAixrAx0y04PGzDAxn40iDtr=kxOioODmxY=W60ifxq=0beiFDO4WE3IqCSdqGLDmKDyYWOiPDx1q0rD74irDDxD3mxneD+D0bMgnkqi3DhxDODWKDXgas=nIP5nK1CxGC4407Di5xn=0D6xG1DQ5Ds60vKnKD0xfAKG51HIgQbnDDUDWj3ox4c4i3fCKhe40OD0IG6xibzoOj0EHUV7h3b7xPOhDPd04xrY+PYhGnee94+CGP=BR75cGPYCmY8QZMKDDWZlwK4D',
            'ssxmod_itna2': 'eqIx0D97i=0=qD5GQDXDnQAqeEopd3DgGAixrDnFS94DsF+eDLGnjQyS3=qnRDNrUSZOKxCFiYm9NeuDgRGNzqKec56=U9Y6DmCej777jxTP2wDA2bMtmgDEXSglH0hcyQL2irTXVwWeoPeqZPrl0HN54RQ4cTqTOhwKtam3tgNsZYtVB54mguDTKyilnW=qO9NO=9N+toA4tDt4=dtl=LOP45=418Cu0LNBWBEtaBfn0vRg0INTIlWE3YabCPrulRfy=dc+3Q9IOdBqmdBbDSfIO2tpVCN2+ZmmT577M5x7VaE4/EE+fah8rE3kx+bKnN3+o3WaedCKeOudC4RHTji48b3fO7ediCamR3alkZ0+OmmWxzobCgiFGMCOMHXL1rfdFoTjuP13pUGolDWDwbLT7EDDwxh4nTE7hKBekeomfN40UIFwKeo4DGcDG7KiDD==',
        }

    def ckcest_spider(self):

        insert_num = 0

        header = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36 Edg/86.0.622.69",
            "Host": "www.ckcest.cn"
        }

        session = requests.session()
        session.DEFAULT_RETRIES = 3
        session.keep_alive = False

        ckcest_specialist_table = self.db['ckcest_specialist']
        ckcest_project_table = self.db1['ckcest_project']
        ckcest_thesis_table = self.db2['ckcest_thesis']

        url = 'http://www.ckcest.cn/default/es3/search/postContent'
        for new_energy_keywords in open("../spider_keywords.txt", encoding="UTF-8"):
            new_energy_keywords = new_energy_keywords.replace('\n', '')
            keywords_quote = urllib.parse.quote(new_energy_keywords)
            request_data = {
                "dbId": "2003",
                "text": "",
                "express": "AB=" + keywords_quote + " OR OG=" + keywords_quote,
                "secondSearchExpress": "",
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
            json_datas = session.post(url, headers=header, data=request_data).json()
            results_datas = json_datas['results']['aggs']
            # 获取每个关键字下的一级分类
            for item in results_datas:
                if item['showName'] == '学科领域':
                    parm_list = []
                    for x in item['datas']:
                        if x.keys() != 'showName':
                            parm = [y for y in list(x.keys()) if y != 'showName']
                            parm_list.append(''.join(parm))
                            if ''.join(parm) in x:
                                # 每个分类标识 中文名
                                classify_name = x['showName']
                                # 每个分类标识 类型
                                # classify_label = ''.join(parm)
                                print(classify_name)
                                for num in range(1, 101):
                                    request_data = {
                                        "dbId": "2003",
                                        "text": "",
                                        "express": "AB=" + keywords_quote + " OR OG=" + keywords_quote,
                                        "secondSearchExpress": "RD%3D%3D" + urllib.parse.quote(classify_name),
                                        "order": "101",
                                        "startYear": '1700',
                                        "endYear": '2099',
                                        "page": num,
                                        "limit": "10",
                                        "isadvanced": "1",
                                        "uuid": "",
                                        "country": "China",
                                        "province": "Beijing",
                                        "city": "Beijing"
                                    }
                                    get = session.post(url, headers=header, data=request_data)
                                    try:
                                        results_datas = get.json()['results']['datas']
                                    except Exception as e:
                                        logger.error(e)
                                        self.file_write.write(url + '\n')
                                        self.file_write.flush()
                                        break
                                    for item in results_datas:
                                        data_id = item['_id']
                                        if self.is_parse(data_id):
                                            logger.warning(f'已经解析过专家 : {data_id}')
                                            continue

                                        achievement = session.get(
                                            'http://www.ckcest.cn/default/es3/detail/1000/dw_expert_2020_merge/' + data_id,
                                            # cookies=self.tmp_cookies, headers=header)
                                            cookies=self.ckcest_cookies, headers=header)
                                        html_element = etree.HTML(achievement.text)
                                        content_element = html_element.xpath('//script[@type="text/javascript"]')
                                        # img_url_link = html.xpath('//*[@class="lf ebf-img"]/img/@src')

                                        detailStr = {}
                                        expertResultStr = {}
                                        for item_element in content_element:
                                            item_data = re.findall(
                                                "var expertResultStr = '\s|[\r\n](.*?)';\s|[\r\n]        var md5 =",
                                                item_element.xpath('string(.)'))
                                            for content in item_data:
                                                unquote = urllib.parse.unquote(content)
                                                if unquote.startswith('        var expertResultStr = '):
                                                    replace_text = unquote.replace('        var expertResultStr = \'',
                                                                                   '')
                                                    if replace_text.strip():
                                                        try:
                                                            expertResultStr = json.loads(replace_text)
                                                        except Exception as e:
                                                            logger.error(e)
                                                if unquote.startswith('        var detailStr = '):
                                                    detailStr = json.loads(
                                                        unquote.replace('        var detailStr = \'', ''))
                                                    if 'ArticleMd5' in detailStr:
                                                        del detailStr['ArticleMd5']

                                        item['id'] = data_id
                                        # item['person_detail'] = detailStr['results']['data']
                                        # item['category_professional'] = acadetit
                                        item['category_subject'] = classify_name  # 搜索条件的领域
                                        # item['category_id'] = enum_id  # 八大领域ID

                                        # 刪除无用字段
                                        del item['_id']
                                        del item['_score']
                                        del item['quality']
                                        if 'keywords_other' in item:
                                            del item['keywords_other']
                                        if '_index' in item:
                                            del item['_index']
                                        if 'organization_zh_other' in item:
                                            del item['organization_zh_other']
                                        if 'email' in item:
                                            del item['email']

                                        # 格式化字段及字段名
                                        if 'publish_date' in item:
                                            item['publish_date'] = datetime.strftime(
                                                datetime.strptime(str(item['publish_date']), '%Y%m%d'), '%Y-%m-%d')
                                        if 'KY' in item:
                                            item['key_words'] = item['KY']
                                            del item['KY']
                                        if 'academic_title' in item:
                                            item['academic_title'] = self.format_string_data(item['academic_title'])
                                        # 研究领域
                                        item['research_field'] = self.format_string_data(
                                            item['RD'] if 'RD' in item else '')
                                        del item['RD']
                                        # 个人简介
                                        item['summary'] = self.format_string_data(item['AB'] if 'AB' in item else '')
                                        if 'AB' in item:
                                            del item['AB']
                                        # if 'gender' in item:
                                        #     item['gender'] = '男' if item['gender'] == 2 else '女'

                                        # 专家相关项目
                                        related_project = []
                                        # 专家相关论文
                                        related_thesis = []
                                        # 科研项目
                                        if 'project' in expertResultStr:
                                            for expertResult in expertResultStr['project']:
                                                project_id = expertResult['_id']
                                                related_project.append(project_id)
                                                if self.is_parse(project_id):
                                                    logger.warning(f'已经解析过项目 : {project_id}')
                                                    continue
                                                sponsor_strftime = datetime.strptime(str(expertResult['publish_date']),
                                                                                     '%Y%m%d')
                                                format_sponsor_time = datetime.strftime(sponsor_strftime, '%Y-%m-%d')

                                                dict_data = {
                                                    # 文章ID
                                                    "id": expertResult['_id'] if "_id" in expertResult else None,
                                                    # 标题
                                                    "title": cc.convert(
                                                        ''.join(expertResult['TI']).replace('<em>', '').replace('</em>',
                                                                                                                '')) if "TI" in expertResult else None,
                                                    # 标题英文翻译
                                                    "title_translate": ''.join(
                                                        expertResult['title_translate_en']).replace('<em>',
                                                                                                    '').replace(
                                                        '</em>', '') if "title_translate_en" in expertResult else None,
                                                    # 出版年份
                                                    "publish_year": expertResult[
                                                        'publish_year'] if "publish_year" in expertResult else None,
                                                    # 发布时间
                                                    "publish_date": format_sponsor_time,
                                                    # 数据源类别
                                                    "datasource_class": expertResult[
                                                        'datasourceclass'] if "datasourceclass" in expertResult else None,
                                                    # 语言
                                                    "language": expertResult[
                                                        'language'] if "language" in expertResult else None,
                                                    # 项目区域
                                                    "project_area": expertResult[
                                                        'project_area'] if "project_area" in expertResult else None,
                                                    # 机标关键词
                                                    "key_words_other": [
                                                        cc.convert(''.join(x).replace('<em>', '').replace('</em>', ''))
                                                        for x in expertResult['KY']] if "KY" in expertResult else None,
                                                    # 关键词
                                                    "key_words": [
                                                        cc.convert(''.join(x).replace('<em>', '').replace('</em>', ''))
                                                        for x
                                                        in expertResult[
                                                            'keywords_auto_zh']] if "keywords_auto_zh" in expertResult else None,
                                                    # 分类编号
                                                    "ccc": expertResult['ccc'] if "ccc" in expertResult else None,
                                                    # 一级分类
                                                    "ccc_l1": expertResult[
                                                        'ccc_l1'] if "ccc_l1" in expertResult else None,
                                                    # 二级分类
                                                    "ccc_l2": expertResult[
                                                        'ccc_l2'] if "ccc_l2" in expertResult else None,
                                                    # 资助机构
                                                    "funder_organization": expertResult[
                                                        'funder_organization'] if "funder_organization" in expertResult else None,
                                                    # 项目类型
                                                    "project_type": cc.convert(
                                                        expertResult[
                                                            'project_type']) if "project_type" in expertResult else None,
                                                    # 项目参与者
                                                    "participant": [cc.convert(x) for x in
                                                                    expertResult[
                                                                        'author']] if "author" in expertResult else None,
                                                    # 项目负责人
                                                    "responsibler": [cc.convert(x) for x in expertResult[
                                                        'responsibler']] if "responsibler" in expertResult else None,
                                                    # 资源类型
                                                    "resource_type": expertResult[
                                                        'resource_type'] if "resource_type" in expertResult else None,
                                                    # 项目金额
                                                    "funding_usd": expertResult[
                                                        'funding_usd'] if "funding_usd" in expertResult else None,
                                                    # 金额类型
                                                    "fund_code": expertResult[
                                                        'fund_code'] if "fund_code" in expertResult else None,
                                                    # 承担机构所在国家
                                                    "funder_organization_country": expertResult[
                                                        'funder_organization_country'] if "funder_organization_country" in expertResult else None,
                                                    # 中文摘要
                                                    "zh_summary": cc.convert(
                                                        ''.join(expertResult['AB']).replace('<em>', '').replace('</em>',
                                                                                                                '')) if "AB" in expertResult else None,
                                                    # 英文摘要
                                                    "en_summary": ''.join(
                                                        expertResult['abstract_translate_zh_other']).replace('<em>',
                                                                                                             '').replace(
                                                        '</em>',
                                                        '') if "abstract_translate_zh_other" in expertResult else None,
                                                    # 承担机构
                                                    "leader_organization": [cc.convert(x) for x in expertResult[
                                                        'leader_organization']] if "leader_organization" in expertResult else None,
                                                    # 承担机构国家
                                                    "leader_organization_country": expertResult[
                                                        'leader_organization_country'] if "leader_organization_country" in expertResult else None,
                                                    # 来源
                                                    "detail_name": str(expertResult['detail_url']).split('=')[
                                                        0] if "detail_url" in expertResult else None,
                                                    # 来源URL
                                                    "detail_url": str(expertResult['detail_url']).split('=')[
                                                        1] if "detail_url" in expertResult else None,
                                                    # 网页URL
                                                    "url": "http://www.ckcest.cn/default/es3/detail?md5=" +
                                                           expertResult[
                                                               '_id'] + "&tablename=dw_project2020&year=&dbid=1007",
                                                    # 插入时间
                                                    "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),
                                                }

                                                ckcest_project_table.insert_one(dict_data)
                                        # 期刊论文
                                        if 'journal_article' in expertResultStr:
                                            for expertResult1 in expertResultStr['journal_article']:
                                                journal_article_id = expertResult1['_id']
                                                related_thesis.append(journal_article_id)
                                                if self.is_parse(journal_article_id):
                                                    logger.warning(f'已经解析过论文 : {journal_article_id}')
                                                    continue
                                                sponsor_strftime = datetime.strptime(str(expertResult1['publish_date']),
                                                                                     '%Y%m%d')
                                                format_sponsor_time = datetime.strftime(sponsor_strftime, '%Y-%m-%d')

                                                author_organization = []
                                                if "author_organization" in expertResult1:
                                                    for y in list(set(expertResult1['author_organization'])):
                                                        if y:
                                                            split = re.split('[；;，、,: ]', y)
                                                            for i in split:
                                                                if len(i) <= 3 or i.isdigit():
                                                                    continue
                                                                if '!' in y:
                                                                    i = y.split('!')[0]
                                                                author_organization.append(i)
                                                else:
                                                    author_organization = None

                                                thesis_key_words = []
                                                if "KY" in expertResult1:
                                                    for x in expertResult1['KY']:
                                                        if x:
                                                            if x.replace('<em>', '').replace('</em>', '').isalpha():
                                                                split_en_keywords = x.split(' ')
                                                                for _ in split_en_keywords:
                                                                    thesis_key_words.append(_)
                                                                continue
                                                            thesis_key_words.append(x)
                                                else:
                                                    thesis_key_words = None

                                                related_dict_data = {
                                                    "id": expertResult1['_id'],
                                                    # 发表时间
                                                    "publish_date": format_sponsor_time,
                                                    # 发表年份
                                                    "publish_year": expertResult1['publish_year'],
                                                    # 期刊名称
                                                    "journal_title": expertResult1[
                                                        'journal_title'] if "journal_title" in expertResult1 else None,
                                                    # 作者
                                                    "author": [_.split('\n')[0] for _ in expertResult1[
                                                        'author']] if "author" in expertResult1 else None,
                                                    # 单位
                                                    "author_organization": list(
                                                        set(author_organization)) if author_organization else None,
                                                    # 标题语言 1
                                                    "title_language": expertResult1[
                                                        'title_language'] if "title_language" in expertResult1 else None,
                                                    # 数据来源 1
                                                    "datasource": expertResult1[
                                                        'datasource'] if "datasource" in expertResult1 else None,
                                                    # 标题 1
                                                    "title": cc.convert(
                                                        str(expertResult1['TI']).replace('<em>', '').replace('</em>',
                                                                                                             '')) if "TI" in expertResult1 else None,
                                                    # 国家 1
                                                    "country_code": expertResult1[
                                                        'country_code'] if "country_code" in expertResult1 else None,
                                                    # 关键词 1
                                                    "key_words": list(
                                                        set(thesis_key_words)) if thesis_key_words else None,
                                                    # 资源类型 1
                                                    "resource_type": expertResult1[
                                                        'resource_type'] if "resource_type" in expertResult1 else None,
                                                    # 摘要
                                                    "summary": cc.convert(
                                                        ''.join(str(expertResult1['AB'])).replace('<em>', '').replace(
                                                            '</em>',
                                                            '')) if "AB" in expertResult1 else None,
                                                    # 分类编号
                                                    "ccc": expertResult1['ccc'] if "ccc" in expertResult1 else None,
                                                    # 一级分类
                                                    "ccc_l1": expertResult1[
                                                        'ccc_l1'] if "ccc_l1" in expertResult1 else None,
                                                    # 二级分类
                                                    "ccc_l2": expertResult1[
                                                        'ccc_l2'] if "ccc_l2" in expertResult1 else None,
                                                    # 数据表名
                                                    "source_table_name": expertResult1[
                                                        'source_table_name'] if "source_table_name" in expertResult1 else None,
                                                    # 网页URL
                                                    "url": "http://www.ckcest.cn/default/es3/detail?md5=" +
                                                           expertResult1[
                                                               '_id'] + "&tablename=" + (expertResult1[
                                                                                             'source_table_name'] if "source_table_name" in expertResult1 else None) + "&year=&dbid=1002",
                                                    # 插入时间
                                                    "insert_time": str(time.strftime('%Y-%m-%d %H:%M:%S')),

                                                }
                                                try:
                                                    ckcest_thesis_table.insert_one(related_dict_data)
                                                except:
                                                    print(thesis_key_words)

                                        item['related_project'] = related_project
                                        item['related_thesis'] = related_thesis
                                        item['insert_time'] = str(time.strftime('%Y-%m-%d %H:%M:%S'))

                                        if 'picture_url' in item:
                                            picture_url = item['picture_url']
                                            if picture_url.startswith('http'):
                                                suffix = picture_url.split('.')[-1]
                                                try:
                                                    logger.info(f'正在下载专家图片 URL:{picture_url}')
                                                    img_name = f"{item['_id']}&id=1.{suffix}"
                                                    urlretrieve(picture_url, filename=fr"D:\Resource\\{img_name}")
                                                    item['picture_name'] = img_name
                                                except Exception as e:
                                                    logger.error('PICTURE DOWNLOAD FAILED URL: ', e)
                                            del item['picture_url']
                                        ckcest_specialist_table.insert_one(item)
                                        insert_num += 1
                                        logger.info(f'INSERT NUM : {insert_num}')

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

    # 移除多余字符
    def format_string_data(self, string_data):
        if isinstance(string_data, list):
            return [''.join(x).replace('<em>', '').replace('</em>', '') for x in string_data]
        return string_data.replace('<em>', '').replace('</em>', '')

    def __del__(self):
        self.file_write.close()


if __name__ == '__main__':
    ckcest = SpiderCkcest()
    ckcest.ckcest_spider()
