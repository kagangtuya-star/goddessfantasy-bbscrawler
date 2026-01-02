import json
import os
import datetime

README = [
    "[color=red][b][size=18pt]以下内容没有经过任何审查，可能存在严重违背平衡性的私设，使用前请先与DM达成一致！[/size][/b][/color]",
    "",
    "请各位私设写手在自己的标题中注明好自己发的内容是什么，参考以下标签：",
    "【邪术师】【异界宗主】【邪术师-异界宗主】【职业】【种族】【亚种】【背景】【专长】【法术】【拓展书】【重置】",
    "",
    "如果可以，请尽可能将单品分开发（不然会比较麻烦）；由于对原版内容进行的修改&重置重复数量以及\"创作\"程度各不相同，因此被独立到了一个分类中（不论实际与什么有关），以方便查阅。"
]

BIGTITLE = "[color=brown][size=24pt][b]创作区大目录[/b][/size][/color]"

TOPIC = "[li][url=http://www.GoddessFantasy.net/bbs/index.php?topic={0}.0][b]{1}[/b] - by {2}[/url][/li]"

if __name__ == '__main__':
    # 读取已保存的数据
    data = {}
    if not os.path.exists("./Data.json"):
        print("数据不存在！")
    else:
        with open("./Data.json",mode="r",encoding='UTF-8') as _f:
            data_text = _f.read()
            data = json.loads(data_text)
    total = len(data.keys())
    
    catalogues = []
    output_catalogues = ["奇械师","野蛮人","吟游诗人","牧师","德鲁伊","战士","武僧","圣武士","游侠","游荡者","术士","邪术师","法师","秘术师","新职业","种族","专长","背景","怪物","法术","物品","规则","修改&重置","杂谈","其他&未整理"]
    output_list = {}
    # 数据录入列表
    for key in sorted(data.keys(),reverse=True):
        if "Nickname" in data[key].keys():
            text = TOPIC.format(key,data[key]["Nickname"],data[key]["Author"])
        else:
            text = TOPIC.format(key,data[key]["Title"],data[key]["Author"])
        cat = data[key]["Catalogue"] if data[key]["Catalogue"] != "" else "其他&未整理"
        if cat == "忽略":
            total -= 1
        elif cat == "特殊":
            if "Split" in data[key].keys():
                split_cats = data[key]["Split"]
                for sub_cat in split_cats.keys():
                    if sub_cat not in catalogues:
                        catalogues.append(sub_cat)
                        output_list[sub_cat] = []
                    output_list[sub_cat].append(TOPIC.format(key,split_cats[sub_cat],data[key]["Author"]))
        else:
            if cat not in catalogues:
                catalogues.append(cat)
                output_list[cat] = []
            output_list[cat].append(text)
    #列表做成大目录
    statistics = ["[b]"+str(len(output_list[cat]))+"[/b]件"+cat+"私设" for cat in output_catalogues if cat in catalogues]
    
    statistic_text = "截止 [b]"+str(datetime.date.today())+"[/b] ，果园5E创作区已有[b]"+str(total)+"[/b]张私设帖。其中（部分帖在目录中被拆分）：\n" + "、".join(statistics)+"。"
    
    output = "\n".join(README)+"\n"+statistic_text+"\n[hr]\n"+BIGTITLE+"\n\n"
    
    for cat in output_catalogues:
        if cat in catalogues:
            prefix = "[color=teal][size=16pt][b]"+cat+"[/b][/size][/color]\n[spoiler][list]\n"
            suffix = "\n[/list][/spoiler]\n\n"
            output += prefix + "\n".join(output_list[cat]) + suffix
    with open("./OutputBBCode.txt",mode="w",encoding='UTF-8') as _f:
        _f.write(output)
    print("生成完毕！")