import requests
import time
import os
from lxml import etree
import json
import re
#from openpyxl import Workbook
# url
url='http://45.79.87.129/bbs/index.php?board=1928'
# 目录每一页的帖子数量
PAGETOPICS = 30


header = {
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
'Connection': 'keep-alive',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
}

if __name__ == '__main__':
    process_pages = int(input("请输入要更新的目录页数："))
    # 读取Cookie
    if os.path.exists("./Cookie.txt"):
        with open("./Cookie.txt",mode="r",encoding='UTF-8') as _f:
            header["Cookie"] = _f.read()
    else:
        print("缺少Cookie.txt")
        os._exit()
    # 读取已保存的数据
    data = {}
    if not os.path.exists("./Data.json"):
        with open("./Data.json",mode="w",encoding='UTF-8') as _f:
            _f.write("{}")
    else:
        with open("./Data.json",mode="r",encoding='UTF-8') as _f:
            data_text = _f.read()
            data = json.loads(data_text)
    # 读取分类表数据
    catalogues = {}
    if os.path.exists("./Catalogues.json"):
        with open("./Catalogues.json",mode="r",encoding='UTF-8') as _f:
            data_text = _f.read()
            catalogues = json.loads(data_text)
    now_page = 0
    end = False
    
    '''
    # 创建表格
    wb = Workbook()
    wb.create_sheet(index=1, title="私设区")
    ws = wb.active
    ws.append(["标题","副标题","链接","作者"])
    '''
    
    deck = data.keys()
    

    # 收集内容
    while(now_page < process_pages):
        # 当前页面的url
        s_url = url + "." + str(now_page * PAGETOPICS)
        res = requests.get(s_url,headers=header) #发起请求
        res.encoding='UTF-8' #设置编码格式
        text = res.text
        html = etree.HTML(text)
        items = html.xpath('//div[@id="topic_container"]/div[contains(@class, "windowbg")]')
        # print(res.status_code)
        for item in items:
            sub_data = {
                "Title" : "",
                "SubTitle" : "",
                "Author" : "",
                "Catalogue" : ""
            }
            # 获取数据
            linkid = ""
            # 新结构：标题在 div.info > div > div.message_index_title > span.preview > span > a
            topic = item.xpath('.//div[@class="message_index_title"]//span[contains(@class,"preview")]/span/a')
            # 副标题在 div.message_index_title > p[style]
            topic_subtitle = item.xpath('.//div[@class="message_index_title"]/p[@style]')
            # 作者在 p.floatleft > a
            topic_author = item.xpath('.//p[@class="floatleft"]/a')
            if len(topic) > 0:
                title_text = topic[0].text
                if title_text:
                    sub_data["Title"] = title_text.strip().replace("\\","\\\\").replace("\"","\\\"")
                # 从href提取topic ID
                href = topic[0].get("href", "")
                match = re.search(r'topic=(\d+)', href)
                if match:
                    linkid = match.group(1)
            if len(topic_subtitle) > 0 and topic_subtitle[0].text:
                sub_data["SubTitle"] = topic_subtitle[0].text.strip().replace("\\","\\\\").replace("\"","\\\"")
            if len(topic_author) > 0 and topic_author[0].text:
                sub_data["Author"] = topic_author[0].text.strip().replace("\\","\\\\").replace("\"","\\\"")
            # print(sub_data)
            # ws.append(sub_data)
            # 写入数据
            if linkid not in deck:
                print("发现新帖 "+linkid+" - "+sub_data["Title"])
                data[linkid] = sub_data
                # 寻找分类
                match_text = sub_data["Title"]+"\n"+sub_data["SubTitle"]
                found = False
                for key in catalogues.keys():
                    for cat in catalogues[key]:
                        if cat in match_text:
                            data[linkid]["Catalogue"] = key
                            print("已为 "+linkid+" 分配分类\""+key+"\"")
                            found = True
                            break
                    if found:
                        break
            else:
                if data[linkid]["Title"] != sub_data["Title"]:
                    print("检测到主题 "+linkid+" 已改名：")
                    print("    - 原: "+data[linkid]["Title"])
                    print("    + 新: "+sub_data["Title"])
                    data[linkid]["Title"] = sub_data["Title"]
                if data[linkid]["SubTitle"] != sub_data["SubTitle"]:
                    print("检测到主题 "+linkid+" 副标题有所修改：")
                    print("    - 原: "+data[linkid]["SubTitle"])
                    print("    + 新: "+sub_data["SubTitle"])
                    data[linkid]["SubTitle"] = sub_data["SubTitle"]
                if data[linkid]["Author"] != sub_data["Author"]:
                    print("检测到主题 "+linkid+" 作者已改名：")
                    print("    - 原: "+data[linkid]["Author"])
                    print("    + 新: "+sub_data["Author"])
                    data[linkid]["Author"] = sub_data["Author"]
                    for _linkid in data:
                        if data[_linkid]["Author"] == data[linkid]["Author"]:
                            data[_linkid]["Author"] = sub_data["Author"]
        now_page += 1
        time.sleep(1)

    # wb.save('Data.xlsx')
    with open("./Data.json",mode="w",encoding='UTF-8') as _f:
        _f.write(json.dumps(data,ensure_ascii=False,indent=4,separators=(',', ': ')))
    print("爬虫完毕，欢迎下次使用")