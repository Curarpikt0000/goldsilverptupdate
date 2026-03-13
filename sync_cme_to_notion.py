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

# 目标设定：文件、Notion DB ID、以及行列坐标(0-based index)
# H列是第8列，对应索引为7。行数减1即为索引。
CONFIG = {
    "Gold": {
        "filename": "Gold_Stocks.xls",
        "db_id": "2bc47eb5fd3c8083966eecfd9f396b44",
        "reg_coords": (121, 7),  # H122
        "elig_coords": (123, 7)  # H124
    },
    "Silver": {
        "filename": "Silver_Stocks.xls",
        "db_id": "2bc47eb5fd3c80f3a71ad8de149a4943",
        "reg_coords": (72, 7),   # H73
        "elig_coords": (73, 7)   # H74
    },
    "Pt": {
        "filename": "Platinum_Palladium_Stocks.xls",
        "db_id": "2d647eb5fd3c801a9ce5d5db4d0b961a",
        "reg_coords": (71, 7),   # H72
        "elig_coords": (72, 7)   # H73
    }
}

def get_latest_folder(repo):
    """获取 GitHub data 目录下最新日期的文件夹"""
    api_url = f"https://api.github.com/repos/{repo}/contents/data"
    response = requests.get(api_url)
    response.raise_for_status()
    folders = [item['name'] for item in response.json() if item['type'] == 'dir']
    latest_folder = sorted(folders)[-1] 
    return latest_folder

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
    # 扫描前50行，前10列寻找 Activity Date
    for row_idx in range(min(50, sheet.nrows)):
        for col_idx in range(min(10, sheet.ncols)):
            cell_val = str(sheet.cell_value(row_idx, col_idx))
            if "Activity Date:" in cell_val:
                # 提取日期如 03/12/2026
                match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', cell_val)
                if match:
                    raw_date = match.group(1)
                    # 无论 Notion UI 如何显示，此处必须转换为标准的 YYYY-MM-DD 交给 API
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
    """向 Notion 发送 Query 请求，检查特定日期是否已存在"""
    try:
        response = notion.databases.query(
            **{
                "database_id": db_id,
                "filter": {
                    "property": date_prop,
                    "date": {
                        "equals": date_str
                    }
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

    # 执行去重检查
    if check_if_date_exists(db_id, date_prop, date_str):
        print(f"[{metal_type}] 拦截: Notion 中已存在 {date_str} 的数据，终止写入以防重复。")
        return

    try:
        notion.pages.create(
            parent={"database_id": db_id},
            properties={
                "Name": {"title": []}, 
                date_prop: {"date": {"start": date_str}},
                reg_prop: {"number": reg_val},
                elig_prop: {"number": elig_val}
            }
        )
        print(f"[{metal_type}] 成功: 已添加 {date_str} 的新记录。")
    except Exception as e:
        print(f"[{metal_type}] 失败: 写入 Notion 时报错: {e}")

def main():
    try:
        latest_folder = get_latest_folder(GITHUB_REPO)
        print(f"发现最新数据目录: {latest_folder}")
    except Exception as e:
        print(f"获取 GitHub 目录失败: {e}")
        return
    
    for metal, config in CONFIG.items():
        filename = config["filename"]
        raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/data/{latest_folder}/{filename}"
        
        try:
            download_file(raw_url, filename)
            date_str, reg_val, elig_val = parse_cme_excel(
                filename, config["reg_coords"], config["elig_coords"]
            )
            print(f"[{metal}] Excel 解析完毕 -> 日期: {date_str}, Reg: {reg_val}, Elig: {elig_val}")
            
            push_to_notion(metal, config["db_id"], date_str, reg_val, elig_val)
            
            # 引入停顿，防止触发 Notion API 的速率限制
            time.sleep(1)
            
        except Exception as e:
            print(f"处理 {metal} 任务时发生错误: {e}")

if __name__ == "__main__":
    main()
