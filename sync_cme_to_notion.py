import os
import re
import time
import requests
import xlrd
from datetime import datetime
from notion_client import Client

# ---------------- 配置区 ----------------
GITHUB_REPO = "Curarpikt0000/cme-tracker"
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
notion = Client(auth=NOTION_TOKEN)

# 运行模式设置：
# "latest" -> 只扫描 GitHub 中最新的一天（用于每日定时任务）
# "all"    -> 扫描 GitHub 中所有的历史日期（用于补录 3-14 到 3-17 的数据）
SYNC_MODE = "latest" 

CONFIG = {
    "Gold": {
        "filename": "Gold_Stocks.xls",
        "db_id": "2bc47eb5fd3c8083966eecfd9f396b44",
        "reg_coords": (121, 7),  
        "elig_coords": (123, 7)  
    },
    "Silver": {
        "filename": "Silver_Stocks.xls",
        "db_id": "2bc47eb5fd3c80f3a71ad8de149a4943",
        "reg_coords": (72, 7),   
        "elig_coords": (73, 7)   
    },
    "Pt": {
        "filename": "Platinum_Palladium_Stocks.xls",
        "db_id": "2d647eb5fd3c801a9ce5d5db4d0b961a",
        "reg_coords": (71, 7),   
        "elig_coords": (72, 7)   
    }
}

def get_target_folders(repo, mode="latest"):
    """根据模式获取 GitHub data 目录下的有效日期文件夹"""
    api_url = f"https://api.github.com/repos/{repo}/contents/data"
    response = requests.get(api_url)
    response.raise_for_status()
    
    # 过滤出符合 YYYY-MM-DD 格式的文件夹名，确保排序准确
    folders = [
        item['name'] for item in response.json() 
        if item['type'] == 'dir' and re.match(r'\d{4}-\d{2}-\d{2}', item['name'])
    ]
    sorted_folders = sorted(folders) 
    
    if mode == "latest":
        return [sorted_folders[-1]] if sorted_folders else []
    return sorted_folders

def download_file(url, save_path):
    """下载文件"""
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, 'wb') as f:
        f.write(response.content)

def parse_cme_excel(filepath, reg_coords, elig_coords):
    """解析 Excel 获取 Date, Reg, Elig"""
    book = xlrd.open_workbook(filepath, ignore_workbook_corruption=True)
    sheet = book.sheet_by_index(0)
    
    activity_date = None
    # 增加搜索深度，防止大型表格漏掉日期行
    for row_idx in range(min(150, sheet.nrows)):
        for col_idx in range(min(10, sheet.ncols)):
            cell_val = str(sheet.cell_value(row_idx, col_idx))
            if "Activity Date:" in cell_val:
                match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', cell_val)
                if match:
                    raw_date = match.group(1)
                    date_obj = datetime.strptime(raw_date, "%m/%d/%Y")
                    activity_date = date_obj.strftime("%Y-%m-%d")
                    break
        if activity_date:
            break

    # 获取库存数值
    reg_val = sheet.cell_value(reg_coords[0], reg_coords[1])
    elig_val = sheet.cell_value(elig_coords[0], elig_coords[1])
    
    return activity_date, float(reg_val), float(elig_val)

def check_if_date_exists(db_id, date_prop, date_str):
    """【修复版】使用显式参数调用，适配新版 SDK"""
    try:
        response = notion.databases.query(
            database_id=db_id,
            filter={
                "property": date_prop,
                "date": {
                    "equals": date_str
                }
            }
        )
        return len(response.get("results", [])) > 0
    except Exception as e:
        print(f"查询数据库验证去重时发生错误: {e}")
        return False

def push_to_notion(metal_type, db_id, date_str, reg_val, elig_val):
    """将数据推送到对应的 Notion Database"""
    if not date_str:
        print(f"[{metal_type}] 警告: 未找到有效日期，跳过。")
        return

    date_prop = f"{metal_type}日期"
    reg_prop = f"{metal_type} Reg库存"
    elig_prop = f"{metal_type} Elig库存"

    if check_if_date_exists(db_id, date_prop, date_str):
        print(f"[{metal_type}] 跳过: Notion 已存在 {date_str}")
        return

    try:
        notion.pages.create(
            parent={"database_id": db_id},
            properties={
                "Name": {"title": [{"text": {"content": f"{metal_type} {date_str}"}}]},
                date_prop: {"date": {"start": date_str}},
                reg_prop: {"number": reg_val},
                elig_prop: {"number": elig_val},
                "市场": {"select": {"name": "CME"}}
            }
        )
        print(f"[{metal_type}] 成功: 新增记录 {date_str} (市场: CME)")
    except Exception as e:
        print(f"[{metal_type}] 失败: 写入 Notion 时报错: {e}")

def main():
    try:
        target_folders = get_target_folders(GITHUB_REPO, mode=SYNC_MODE)
        print(f"发现 {len(target_folders)} 个目标日期文件夹准备处理。")
    except Exception as e:
        print(f"获取 GitHub 目录失败: {e}")
        return
    
    for folder in target_folders:
        print(f"\n--- 正在处理目录: {folder} ---")
        for metal, config in CONFIG.items():
            filename = config["filename"]
            raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/data/{folder}/{filename}"
            
            try:
                download_file(raw_url, filename)
                date_str, reg_val, elig_val = parse_cme_excel(
                    filename, config["reg_coords"], config["elig_coords"]
                )
                
                push_to_notion(metal, config["db_id"], date_str, reg_val, elig_val)
                time.sleep(0.5)
                
            except Exception as e:
                print(f"处理 {folder} 下的 {metal} 时发生错误: {e}")

if __name__ == "__main__":
    main()
