import os
import re
import time
import requests
import xlrd
from datetime import datetime
from notion_client import Client

# ---------------- 配置区 ----------------
# 确保仓库名正确（根据你提供的代码，数据存放在 cme-tracker 的 data 目录下）
GITHUB_REPO = "Curarpikt0000/cme-tracker" 
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
notion = Client(auth=NOTION_TOKEN)

# 建议保持 "latest"，但我优化了 "latest" 的定义
SYNC_MODE = "latest" 

CONFIG = {
    "Gold": {
        "filename": "Gold_Stocks.xls",
        "db_id": "2bc47eb5fd3c8083966eecfd9f396b44",
        "reg_coords": (121, 7),  
        "elig_coords": (123, 7)  
    },
    "Silver": {
        "filename": "Silver_stocks.xls", # 修正：与下载端的 s 小写保持一致
        "db_id": "2bc47eb5fd3c80f3a71ad8de149a4943",
        "reg_coords": (72, 7),   
        "elig_coords": (73, 7)   
    },
    "Pt": {
        "filename": "PA-PL_Stck_Rprt.xls", # 修正：对应下载端的铂金文件名
        "db_id": "2d647eb5fd3c801a9ce5d5db4d0b961a",
        "reg_coords": (71, 7),   
        "elig_coords": (72, 7)   
    }
}

def get_target_folders(repo, mode="latest"):
    """获取并按时间严格排序文件夹"""
    api_url = f"https://api.github.com/repos/{repo}/contents/data"
    # 如果有 GitHub Token 建议加上，防止 API 限流
    headers = {"Authorization": f"token {os.getenv('GH_PERSONAL_TOKEN')}"} if os.getenv('GH_PERSONAL_TOKEN') else {}
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    
    # 提取符合日期格式的文件夹，并转换为 datetime 对象排序
    folder_items = [
        item['name'] for item in response.json() 
        if item['type'] == 'dir' and re.match(r'\d{4}-\d{2}-\d{2}', item['name'])
    ]
    
    if not folder_items:
        return []

    # 按照真实日期数值排序，而不是字符串排序
    sorted_folders = sorted(folder_items, key=lambda x: datetime.strptime(x, "%Y-%m-%d"))
    
    if mode == "latest":
        return [sorted_folders[-1]] # 取日期最大的那一个
    return sorted_folders

def parse_cme_excel(filepath, reg_coords, elig_coords):
    """解析 Excel 获取数据"""
    try:
        book = xlrd.open_workbook(filepath, ignore_workbook_corruption=True)
        sheet = book.sheet_by_index(0)
        
        activity_date = None
        for row_idx in range(min(150, sheet.nrows)):
            for col_idx in range(min(10, sheet.ncols)):
                cell_val = str(sheet.cell_value(row_idx, col_idx))
                if "Activity Date:" in cell_val:
                    match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', cell_val)
                    if match:
                        date_obj = datetime.strptime(match.group(1), "%m/%d/%Y")
                        activity_date = date_obj.strftime("%Y-%m-%d")
                        break
            if activity_date: break

        reg_val = sheet.cell_value(reg_coords[0], reg_coords[1])
        elig_val = sheet.cell_value(elig_coords[0], elig_coords[1])
        return activity_date, float(reg_val), float(elig_val)
    except Exception as e:
        print(f"解析文件 {filepath} 失败: {e}")
        return None, 0, 0

def push_to_notion(metal_type, db_id, date_str, reg_val, elig_val):
    """写入 Notion 并包含查重逻辑"""
    date_prop = f"{metal_type}日期"
    
    # 显式查重
    exists = notion.databases.query(
        database_id=db_id,
        filter={"property": date_prop, "date": {"equals": date_str}}
    ).get("results")

    if exists:
        print(f"[{metal_type}] 跳过: {date_str} 数据已存在")
        return

    notion.pages.create(
        parent={"database_id": db_id},
        properties={
            "Name": {"title": [{"text": {"content": f"{metal_type} {date_str}"}}]},
            date_prop: {"date": {"start": date_str}},
            f"{metal_type} Reg库存": {"number": reg_val},
            f"{metal_type} Elig库存": {"number": elig_val},
            "市场": {"select": {"name": "CME"}}
        }
    )
    print(f"[{metal_type}] 成功同步: {date_str}")

def main():
    target_folders = get_target_folders(GITHUB_REPO, mode=SYNC_MODE)
    print(f"准备处理文件夹: {target_folders}")
    
    for folder in target_folders:
        for metal, config in CONFIG.items():
            filename = config["filename"]
            raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/data/{folder}/{filename}"
            
            try:
                # 下载临时文件
                r = requests.get(raw_url)
                if r.status_code != 200:
                    print(f"跳过: {metal} 在文件夹 {folder} 中不存在")
                    continue
                
                with open(filename, 'wb') as f:
                    f.write(r.content)
                
                date_str, reg_val, elig_val = parse_cme_excel(filename, config["reg_coords"], config["elig_coords"])
                
                if date_str:
                    push_to_notion(metal, config["db_id"], date_str, reg_val, elig_val)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"处理 {metal} 时出错: {e}")

if __name__ == "__main__":
    main()
