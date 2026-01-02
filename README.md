# GoddessFantasy 论坛爬虫工具

抓取纯美苹果园论坛讨论区帖子目录的工具集。

## 文件说明

| 文件 | 功能 |
|------|------|
| `Grab.py` | 爬取论坛帖子目录，保存到 Data.json |
| `ExportBBCode.py` | 将 Data.json 导出为无分类的 BBCode 格式 |
| `MakeBBCode.py` | 按分类生成 BBCode 目录 |
| `Cookie.txt` | 论坛登录 Cookie（需自行配置） |
| `Catalogues.json` | 分类关键词配置 |
| `Data.json` | 爬取的帖子数据 |

## 使用方法

### 1. 配置 Cookie

按f12打开控制台，从浏览器获取论坛登录后的 Cookie，保存到 `Cookie.txt`。
![PixPin_2026-01-02_18-55-21](https://github.com/user-attachments/assets/43cdc6bb-19e8-4f50-927f-ceb857275c50)

### 2. 爬取帖子目录

```bash
python Grab.py
```

输入要爬取的页数（每页30帖），程序会：
- 抓取帖子标题、副标题、作者
- 自动根据 `Catalogues.json` 分配分类
- 检测已有帖子的改名/修改
- 保存到 `Data.json`

### 3. 导出 BBCode

**全量导出（无分类）：**
```bash
python ExportBBCode.py
```
输出到 `ExportBBCode.txt`

**按分类导出：**
```bash
python MakeBBCode.py
```
输出到 `OutputBBCode.txt`

## 依赖

```bash
pip install requests lxml
```

## 注意事项

- Cookie 过期需重新获取
- 爬取间隔 1 秒，避免频繁请求
