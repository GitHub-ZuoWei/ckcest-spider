# -*- coding: utf-8 -*-
#
# CkcestSpider
# Author: ZuoWei
# Email: zuowei@yuchen.com
# Created Time: 2021/11/24 10:35
import time

import redis
import requests


class ProxyIP:
    def __init__(self):
        self.redis_client = redis.StrictRedis(host='192.168.12.100', port=6379, db=1, decode_responses=True)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36'}
        self.url_for_test = 'http://httpbin.org/ip'

    def get_proxy_list(self):
        proxy_response = requests.get(
            'http://gev.qydailiip.com/api/?apikey=622f40f01006e3141bfc02fe1e8a3a8521e02dcd&num=5&type=json&line=win&proxy_type=secret',
            headers=self.headers)
        print('获取proxy list')
        return proxy_response.json()

    def test_proxy_ip(self):
        '''
        测试爬取到的ip，测试成功则存入MongoDB
        '''
        for ip_for_test in self.get_proxy_list():
            # 设置代理
            proxies = {
                'http': 'http://' + ip_for_test,
                'https': 'http://' + ip_for_test,
            }
            while True:
                time.sleep(1)
                if self.redis_client.scard('proxy_ip') <= 10:
                    break
            # print(ip_for_test)
            try:
                response = requests.get(self.url_for_test, headers=self.headers, proxies=proxies, timeout=3)
                if response.status_code == 200:
                    print(f'测试通过: {response.json()["origin"]}')
                    self.redis_client.sadd('proxy_ip', ip_for_test)
            except Exception as e:
                continue

    def get_random_ip(self):
        '''
        随机取出一个ip
        '''
        random_ip = self.redis_client.srandmember('proxy_ip')
        proxy = {
            'http': 'http://' + random_ip,
            'https': 'http://' + random_ip,
        }
        try:
            response = requests.get(self.url_for_test, headers=self.headers, proxies=proxy, timeout=3)
            if response.status_code == 200:
                return random_ip
        except Exception as e:
            print(f'{random_ip}已失效')
            self.redis_client.srem('proxy_ip', random_ip)
            print('已经从Redis移除')
            return self.get_random_ip()


if __name__ == '__main__':
    ip = ProxyIP()
    while True:
        ip.test_proxy_ip()
