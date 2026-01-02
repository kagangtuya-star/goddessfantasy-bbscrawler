import json
import os

if __name__ == '__main__':
    if not os.path.exists("./Data.json"):
        print("Data.json 不存在，请先运行 Grab.py")
        exit(1)

    with open("./Data.json", mode="r", encoding='UTF-8') as f:
        data = json.load(f)

    output_lines = []
    for topic_id, info in data.items():
        title = info.get("Title", "")
        author = info.get("Author", "")
        url = f"http://www.GoddessFantasy.net/bbs/index.php?topic={topic_id}.0"
        line = f"[url={url}][b]{title}[/b] - by {author}[/url]"
        output_lines.append(line)

    with open("./ExportBBCode.txt", mode="w", encoding='UTF-8') as f:
        f.write("\n".join(output_lines))

    print(f"已导出 {len(output_lines)} 条记录到 ExportBBCode.txt")
