import json
import re
import urllib
import requests
from lxml import etree

url = 'http://www.ckcest.cn/default/es3/detail?tablename=dw_expert_2020_20200620&md5=7b23c921815d4a189f908d0057a76364'
get = requests.get(url)

html = etree.HTML(get.text)

xpath1 = html.xpath('//script[@type="text/javascript"]')
detailStr = {}
expertResultStr = {}
for item in xpath1:
    xpath12 = item.xpath('string(.)')
    findall1 = re.findall("var expertResultStr = '\s|[\r\n](.*?)';\s|[\r\n]        var md5 =", xpath12)
    for adsf in findall1:
        unquote = urllib.parse.unquote(adsf)
        if unquote.startswith('        var expertResultStr = '):
            expertResultStr = json.loads(unquote.replace('        var expertResultStr = \'', ''))
        if unquote.startswith('        var detailStr = '):
            detailStr = json.loads(unquote.replace('        var detailStr = \'', ''))

print(detailStr)
print(expertResultStr)
