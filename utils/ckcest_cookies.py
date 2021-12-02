# -*- coding: utf-8 -*-
#
# CkcestSpider
# Author: ZuoWei
# Email: zuowei@yuchen.com
# Created Time: 2021/11/8 11:22
import time

from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions


def get_ckcest_cookies():
    chrome_opt = ChromeOptions()
    chrome_opt.add_argument("--proxy-server=http://192.168.17.243:10809")
    driver = Chrome(chrome_options=chrome_opt)
    # driver = Chrome()
    # driver.maximize_window()
    # 设置模拟浏览器最长等待时间
    driver.set_page_load_timeout(10)
    driver.set_script_timeout(10)
    url = 'http://www.ckcest.cn/default/es3/detail?tablename=dw_expert_2020_merge&md5=a4db119c154248fa92d092943523c473&dbid=1000'
    driver.get(url=url)
    time.sleep(2)
    c = driver.get_cookies()
    selenium_cookies = {}
    # 获取cookie中的name和value,转化成requests可以使用的形式
    for cookie in c:
        selenium_cookies[cookie['name']] = cookie['value']
    if driver:
        driver.quit()
    return selenium_cookies


# 根据分类查询   获取专家列表页Cookies
def get_ckcest_specialist_cookies():
    chrome_opt = ChromeOptions()
    chrome_opt.add_argument("--proxy-server=http://192.168.17.243:10809")
    driver = Chrome(chrome_options=chrome_opt)
    # driver = Chrome()
    # driver.maximize_window()
    # 设置模拟浏览器最长等待时间
    driver.set_page_load_timeout(10)
    driver.set_script_timeout(10)
    url = 'http://mall.ckcest.cn/mall/listContent.ilf?dbId=2003&text=&express=&secondSearchExpress=EXPTY%3D%3D1+AND+RD%3D%3D%E8%AE%A1%E7%AE%97%E6%9C%BA%E7%A7%91%E5%AD%A6%E6%8A%80%E6%9C%AF&fieldSearchExpress=&fieldSearchEntity=&order=1&startYear=1700&endYear=2099&page=1&limit=10&model=1&isadvanced=0&picture=&uuid='
    driver.get(url=url)
    time.sleep(2)
    c = driver.get_cookies()
    selenium_cookies = {}
    # 获取cookie中的name和value,转化成requests可以使用的形式
    for cookie in c:
        selenium_cookies[cookie['name']] = cookie['value']
    if driver:
        driver.quit()
    return selenium_cookies
