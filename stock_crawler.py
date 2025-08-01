#!/usr/bin/env python3
"""
股市爬蟲 - 成交量排名前10名股票
"""

from bs4 import BeautifulSoup
import requests
import json
import datetime
import time
import sys
import traceback
from datetime import datetime, timezone, timedelta


def log_status(success, error_msg=None):
    # 設定台灣時區 (UTC+8)
    tw_timezone = timezone(timedelta(hours=8))
    current_time = datetime.now(tw_timezone).strftime('%Y-%m-%d %H:%M:%S')
    log_file = "stock_crawler_log.txt"
    
    try:
        # 如果文件不存在，則建立檔案
        with open(log_file, "a+", encoding="utf-8") as f:
            if success:
                log_message = f"[O] [{current_time}] 完成爬取台灣股市前 10 大交易量資料。\n"
            else:
                log_message = f"[X] [{current_time}] 爬取失敗，Error: {error_msg}\n"
            
            f.write(log_message)
            print(f"已記錄執行狀態至 {log_file}")
    except Exception as e:
        print(f"無法寫入日誌文件: {e}")

def fetch_stock_data():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://tw.stock.yahoo.com/'
    }
    
    url = "https://tw.stock.yahoo.com/rank/volume?exchange=TAI"
    
    try:
        print(f"正在從 {url} 抓取資料...")
        response = requests.get(url, headers=headers, timeout=30)
        
        # 檢查回應狀態
        if response.status_code == 200:
            print(f"成功取得頁面！內容長度: {len(response.text)} 字元")
            return response.text
        else:
            print(f"請求失敗，狀態碼: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"抓取資料時發生錯誤: {e}")
        return None

def parse_stock_data(html):
    # 檢查 HTML 內容是否為空
    if not html or len(html.strip()) == 0:
        print("錯誤: 提供的 HTML 內容為空！")
        return []
        
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        print("正在分析 HTML 結構...")
        stock_rows = soup.find_all('li', class_='List(n)')
        
        if not stock_rows:
            print("警告: 使用標準方法找不到股票列表，嘗試其他方法...")
            stock_rows = soup.select('div[class*="table-body"] li')
            
            if not stock_rows:
                print("警告: 找不到任何股票資料列。HTML 結構可能已變更。")

                debug_file = "stock_page_debug.html"
                try:
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(html[:100000])
                    print(f"已將部分頁面內容保存至 '{debug_file}' 以供分析")
                except Exception as debug_err:
                    print(f"無法保存調試文件: {debug_err}")
                return []
    except Exception as e:
        print(f"解析 HTML 時發生錯誤: {e}")
        traceback.print_exc()
        return []
    
    print(f"找到 {len(stock_rows)} 筆股票資料")
    parsed_data = []
    
    for i, row in enumerate(stock_rows):
        try:
            left_panel = row.find('div', class_='D(f) Start(0)')
            
            if left_panel is None:
                # 嘗試其他可能的選擇器
                left_panel = row.select_one('div[class*="Start(0)"]')
            
            # 如果仍然找不到左側面板，則跳過此筆資料
            if left_panel is None:
                print(f"警告: 在第 {i+1} 筆資料中找不到左側面板，跳過此筆資料")
                continue
                
            # --- 提取排名 ---
            rank_div = left_panel.find('div', class_='W(40px)')
            if rank_div is None:
                rank_div = left_panel.select_one('div[class*="W(40px)"]')
            
            rank = "0"
            if rank_div:
                rank_span = rank_div.find('span')
                if rank_span:
                    rank = rank_span.text.strip()
                else:
                    rank = rank_div.text.strip()
            
            # --- 提取股名和股號 ---
            name_div = left_panel.find('div', class_='Lh(20px)')
            if name_div is None:
                name_div = left_panel.select_one('div[class*="Fw(600)"]')
                
            stock_name = name_div.text.strip() if name_div else 'N/A'
            
            # 尋找股票代號
            symbol_span = None
            if left_panel:
                symbol_span = left_panel.find('span', class_='Fz(14px)')
                if not symbol_span:
                    symbol_span = left_panel.select_one('span[class*="C(#979ba7)"]')
                    
            stock_symbol = symbol_span.text.strip().replace('.TW', '') if symbol_span else 'N/A'
            
            # --- 提取右側數據欄位 ---
            data_divs = row.find_all('div', class_='Fxg(1)')
            
            if not data_divs:
                # 嘗試其他可能的選擇器
                data_divs = row.select('div[class*="Fxg(1)"]')
                
            # 定義一個函數來安全地從元素中提取文字
            def safe_extract_text(div_element):
                if div_element:
                    span = div_element.find('span')
                    if span:
                        return span.text.strip()
                    return div_element.text.strip()
                return 'N/A'
                
            # 根據索引提取各欄位數據
            price = safe_extract_text(data_divs[0]) if len(data_divs) > 0 else 'N/A'
            change = safe_extract_text(data_divs[1]) if len(data_divs) > 1 else 'N/A'
            change_percent = safe_extract_text(data_divs[2]) if len(data_divs) > 2 else 'N/A'
            high = safe_extract_text(data_divs[3]) if len(data_divs) > 3 else 'N/A'
            low = safe_extract_text(data_divs[4]) if len(data_divs) > 4 else 'N/A'
            price_diff = safe_extract_text(data_divs[5]) if len(data_divs) > 5 else 'N/A'
            volume = safe_extract_text(data_divs[6]) if len(data_divs) > 6 else 'N/A'
            turnover = safe_extract_text(data_divs[7]) if len(data_divs) > 7 else 'N/A'

            # 嘗試將 rank 轉換為整數，如果失敗則使用索引值+1作為排名
            try:
                rank_int = int(rank)
            except (ValueError, TypeError):
                rank_int = i + 1
                
            # 清理數據中的特殊字符
            change = change.replace('▲', '').replace('▼', '').strip()
            change_percent = change_percent.replace('▲', '').replace('▼', '').strip()
            
            # 創建股票資訊字典並添加到結果列表中
            stock_info = {
                "rank": rank_int,
                "name": stock_name,
                "symbol": stock_symbol,
                "price": price,
                "change": change,
                "change_percent": change_percent,
                "high": high,
                "low": low,
                "price_diff": price_diff,
                "volume_shrs": volume,
                "turnover_B_NTD": turnover
            }
            
            parsed_data.append(stock_info)
            
        except Exception as e:
            print(f"處理第 {i+1} 筆資料時出錯: {e}")
            continue
        
    # 依照排名或索引值排序
    parsed_data.sort(key=lambda x: x["rank"])
    return parsed_data

def process_and_save_data(ranking_data, top_n=10, execution_count=0):
    if not ranking_data:
        error_msg = "沒有可用的排名數據"
        print(f"❌ {error_msg}")
        log_status(False, error_msg)
        return False
        
    # 使用台灣時區
    tw_timezone = timezone(timedelta(hours=8))
    current_time = datetime.now(tw_timezone).strftime('%Y-%m-%d %H:%M:%S')
    success = True
    error_messages = []
    
    output_data = {
        "source": "Finance Taiwan (Parsed from HTML)",
        "last_updated_cst": current_time,
        "execution_count": execution_count,
        "ranking_data": ranking_data
    }

    file_name = "stock_top10_volume.json"
    
    try:
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
            
        print(f"✅ 成功解析 {len(ranking_data)} 筆資料，並已儲存至 '{file_name}'")
    except Exception as e:
        error_msg = f"儲存完整排名資料時發生錯誤: {e}"
        print(f"❌ {error_msg}")
        success = False
        error_messages.append(error_msg)
    
    print("\n前 5 筆資料預覽:")
    for i, stock in enumerate(ranking_data[:5]):
        print(f"{i+1}. {stock['name']} ({stock['symbol']}): 股價 {stock['price']}, 成交量 {stock['volume_shrs']}")
    
    top_data = []
    max_count = min(top_n, len(ranking_data))
    
    for i in range(max_count):
        stock = ranking_data[i]
        simplified_stock = {
            "rank": stock["rank"],
            "symbol": stock["symbol"],
            "name": stock["name"],
            "price": stock["price"],
            "volume": stock["volume_shrs"]
        }
        top_data.append(simplified_stock)
    
    top_file_name = f"stock_top{top_n}_volume.json"
    top_output = {
        "source": f"Finance Taiwan (Top {top_n} Volume Ranking)",
        "last_updated_cst": current_time,
        "execution_count": execution_count,
        "data": top_data
    }
    
    try:
        with open(top_file_name, 'w', encoding='utf-8') as f:
            json.dump(top_output, f, ensure_ascii=False, indent=4)
            
        print(f"\n✅ 已擷取成交量排行前 {max_count} 名資料，並儲存至 '{top_file_name}'")
    except Exception as e:
        error_msg = f"儲存前 {top_n} 名資料時發生錯誤: {e}"
        print(f"❌ {error_msg}")
        success = False
        error_messages.append(error_msg)
    
    print(f"\n前 {top_n} 名成交量排行資料:")
    for stock in top_data[:10]:
        print(f"{stock['rank']}. {stock['symbol']} {stock['name']}: 股價 {stock['price']}, 成交量 {stock['volume']}")
    
    if top_n > 10:
        print("... 更多資料請參考 JSON 檔案 ...")
    
    if success:
        log_status(True)
    else:
        log_status(False, "; ".join(error_messages))
    
    return success

def crawl_once():
    # 使用台灣時區
    tw_timezone = timezone(timedelta(hours=8))
    print(f"\n[{datetime.now(tw_timezone).strftime('%Y-%m-%d %H:%M:%S')}] 執行爬蟲...")
    
    try:
        html_content = fetch_stock_data()
        
        if not html_content:
            error_msg = "無法取得 HTML 內容，本次爬蟲終止"
            print(error_msg)
            log_status(False, error_msg)
            return False, None
            
        print("開始解析 HTML 內容...")
        ranking_data = parse_stock_data(html_content)
        
        if not ranking_data or len(ranking_data) == 0:
            error_msg = "解析結果為空，本次爬蟲終止"
            print(error_msg)
            log_status(False, error_msg)
            return False, None
            
        return True, ranking_data
    
    except Exception as e:
        error_msg = f"爬蟲過程發生未預期錯誤: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        log_status(False, error_msg)
        return False, None

def main():
    test_mode = False
    for arg in sys.argv:
        if arg.lower() in ('--test', '-t'):
            test_mode = True
    
    INTERVAL_SECONDS = 5 if test_mode else 0  # 改為 0，不需要間隔
    MAX_EXECUTIONS = 3 if test_mode else 1   # 改為 1，每次只執行一次爬蟲
    TOP_N = 10
    
    print("\n" + "="*60)
    print(f"股市前 {TOP_N} 大成交量爬蟲開始執行")
    if test_mode:
        print(f"【測試模式】將執行 {MAX_EXECUTIONS} 次爬蟲，間隔 {INTERVAL_SECONDS} 秒")
    else:
        print(f"程式將執行 {MAX_EXECUTIONS} 次爬蟲，每次間隔 {INTERVAL_SECONDS} 秒")
    print("可透過 Ctrl+C 終止程式")
    print("="*60 + "\n")
    
    execution_count = 0
    start_time_total = time.time()
    
    try:
        while execution_count < MAX_EXECUTIONS:
            start_time = time.time()
            execution_count += 1
            
            print(f"\n===== 第 {execution_count}/{MAX_EXECUTIONS} 次爬蟲 =====")
            try:
                success, ranking_data = crawl_once()
                
                if not success:
                    print(f"第 {execution_count} 次爬蟲失敗，等待 {INTERVAL_SECONDS} 秒後重試...")
                    time.sleep(INTERVAL_SECONDS)
                    continue
                
                process_result = process_and_save_data(ranking_data, top_n=TOP_N, execution_count=execution_count)
                if not process_result:
                    print("處理數據失敗")
            except Exception as loop_err:
                error_msg = f"第 {execution_count} 次爬蟲執行過程中發生錯誤: {loop_err}"
                print(error_msg)
                traceback.print_exc()
                log_status(False, error_msg)
                time.sleep(INTERVAL_SECONDS)
                continue
            
            elapsed_time = time.time() - start_time
            wait_time = max(0, INTERVAL_SECONDS - elapsed_time)
            
            if execution_count < MAX_EXECUTIONS:
                print(f"\n等待 {wait_time:.1f} 秒後進行下一次爬蟲...")
                time.sleep(wait_time)
        
        total_time = time.time() - start_time_total
        print("\n" + "="*60)
        print(f"完成 {execution_count}/{MAX_EXECUTIONS} 次爬蟲任務")
        print(f"總執行時間: {total_time:.1f} 秒")
        print(f"最終結果已儲存於 stock_top{TOP_N}_volume.json 檔案")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\n\n使用者中斷程式執行")
        print(f"已完成 {execution_count}/{MAX_EXECUTIONS} 次爬蟲")
        log_status(False, "使用者中斷程式執行")
        
    except Exception as e:
        error_msg = f"\n發生未預期的錯誤: {e}"
        print(error_msg)
        traceback.print_exc()
        log_status(False, error_msg)
        
    finally:
        print("\n程式執行結束")
        return 0

if __name__ == "__main__":
    sys.exit(main())
