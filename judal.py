import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import re
from pathlib import Path
import os
import time
import json
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor
from check_krx import get_krx_status, KRXStatus

def init_db():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stock_data.db3")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 테이블 생성
    cursor.execute('''
      CREATE TABLE IF NOT EXISTS net_buy (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, -- 날짜
        stock_name TEXT, -- 종목
        code TEXT, -- 종목코드
        foreigner INTEGER, -- 외국인 순매수(억원)
        foreigner_continuous  INTEGER, -- 외국인 연속 매수/매도 일수
        institution INTEGER, -- 기관 순매수(억원)
        institution_continuous  INTEGER, -- 기관 연속 매수/매도 일수
        current_price INTEGER, -- 현재가격
        change_rate TEXT, --전일비
        candle TEXT DEFAULT(''), -- 캔들정보
        UNIQUE(date, stock_name)
      );
    ''')

    cursor.execute('''
      -- 종목별 테마 관계 테이블
      CREATE TABLE IF NOT EXISTS stock_themes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          stock_name TEXT, -- 종목
          theme_name TEXT, -- 테마
          UNIQUE(stock_name, theme_name) -- 중복 방지
      );
    ''')
    conn.commit()
    return conn

def save_to_db_FundBuy(current_date, df):
    if df is None or df.empty:
        return

    conn = init_db()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            # insert
            cursor.execute('''
              INSERT INTO net_buy (date, stock_name, code, foreigner, foreigner_continuous, institution, institution_continuous, current_price, change_rate)
              VALUES (?, ?, ?, 0, 0, ?, ?, ?, ?)
              ON CONFLICT(date, stock_name) DO UPDATE SET
                institution = excluded.institution,
                institution_continuous = excluded.institution_continuous
            ''', (current_date, row['종목명'], row['종목코드'], row['매수금액(억)'], row['연속매수일'], row['현재가격'], row['전일비']))

            # 테마 설정
            themes = row['테마'].split(',')
            for theme_name in themes:
              cursor.execute('''
                INSERT OR IGNORE INTO stock_themes (stock_name, theme_name)
                VALUES (?, ?)
                ''', (row['종목명'], theme_name))
        except Exception as e:
            print(f"Error: {e}")
    conn.commit()
    conn.close()

def save_to_db_ForeignerBuy(current_date, df):
    if df is None or df.empty:
        return

    conn = init_db()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            # insert
            cursor.execute('''
              INSERT INTO net_buy (date, stock_name, code, foreigner, foreigner_continuous, institution, institution_continuous, current_price, change_rate)
              VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
              ON CONFLICT(date, stock_name) DO UPDATE SET
                foreigner = excluded.foreigner,
                foreigner_continuous = excluded.foreigner_continuous
            ''', (current_date, row['종목명'], row['종목코드'], row['매수금액(억)'], row['연속매수일'], row['현재가격'], row['전일비']))

            # 테마 설정
            themes = row['테마'].split(',')
            for theme_name in themes:
              cursor.execute('''
                INSERT OR IGNORE INTO stock_themes (stock_name, theme_name)
                VALUES (?, ?)
                ''', (row['종목명'], theme_name))
        except Exception as e:
            print(f"Error: {e}")
    conn.commit()
    conn.close()

def get_name_and_code(row):
  try:
    name = row.select_one("th a b").get_text(strip=True)
    code = row.select_one("th a span").get_text(strip=True)
    match = re.search(r"(KOSPI|KOSDAQ) (.*)", code)
    if match:
        code = match.group(2)
    else:
        code = None
    return [name, code]
  except Exception as e:
    print(e)

# multiplier
# - 1 : 매수(증가)
# - -1 : 매도(감소)
def get_judal_stock_data(url, multiplier=1):
    #url = "https://www.judal.co.kr/?view=stockList&type=fundBuy"

    # 웹페이지 요청 (User-Agent 설정으로 브라우저인 척 접근)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("페이지를 불러오는데 실패했습니다.")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # 기준일
    elems = soup.select("html > body > div.container-fluid > div.row > div.col-lg-10.p-2 > div.container-fluid > div.row > div.col.p-1 > h1.fs-5.fw-bold > span")
    text = elems[0].text.strip()
    match = re.search(r'\d{4}-\d{2}-\d{2}', text)
    current_date = match.group() if match else datetime.now().strftime('%Y-%m-%d')

    # 데이터가 포함된 테이블 행(tr) 찾기
    # 주식달의 리스트는 통상 'table' 태그 내 'tr' 구조로 되어 있습니다.
    rows = soup.select("table.table-hover tbody tr")

    stock_list = []

    for row in rows:
        name, code = get_name_and_code(row)
        #print(name, code)
        cols = row.find_all("td")
        if len(cols) > 5:  # 데이터가 있는 유효한 행인지 확인
            try:
                text = cols[0].select_one("b").text.strip() # ~억원
                buy_amount = text.replace("억원", "").replace(",", "") # 매수금액(문자열)
                continuous_buy = cols[0].select_one(":scope > span") # 연속매수
                if continuous_buy:
                    text = continuous_buy.text.strip()
                    match = re.search(r"\d+", text)
                    continuous_buy = int(match.group()) if match else 0
                else:
                    continuous_buy = 1
                current_price = cols[1].select_one("b").text.strip()
                change_rate = cols[2].select_one("span")
                if change_rate is None: # span 없는 경우
                    change_rate = cols[2]
                change_rate = change_rate.text.strip()

                #print(name, code, buy_amount, continuous_buy, current_price, change_rate)

                # 테마 csv
                a_tags = cols[19].select(":scope > a")
                themes = [a.get_text(strip=True) for a in a_tags]
                theme_text = ",".join(themes)

                stock_list.append({
                    "종목명": name,
                    "종목코드": code,
                    "매수금액(억)": int(buy_amount) * multiplier,
                    "연속매수일": int(continuous_buy) * multiplier,
                    "현재가격": int(current_price.replace(',', '')),
                    "전일비": change_rate,
                    "테마": theme_text
                })
            except IndexError:
                continue
            except Exception as e:
                print(e)

    # 결과 출력 (데이터프레임 형태)
    df = pd.DataFrame(stock_list)
    return [current_date, df]

def crawl_data():
  # 연기금 순매수
  current_date, df = get_judal_stock_data(url = "https://www.judal.co.kr/?view=stockList&type=fundBuy")
  if df is not None:
      df.to_excel(f"{current_folder}/tmp/fundBuy.xlsx", index=False)
      save_to_db_FundBuy(current_date, df)

  # current_date를 DAY 파일에 저장 => github actions에서 사용 예정
  with open(f"{current_folder}/DAY", "w") as f:
    f.write(current_date)

  # 연기금 순매도
  current_date, df = get_judal_stock_data(url = "https://www.judal.co.kr/?view=stockList&type=fundSell", multiplier=-1)
  if df is not None:
      df.to_excel(f"{current_folder}/tmp/fundSell.xlsx", index=False)
      save_to_db_FundBuy(current_date, df)

  # 외국인 순매수
  current_date, df = get_judal_stock_data(url = "https://www.judal.co.kr/?view=stockList&type=foreignerBuy")
  if df is not None:
      df.to_excel(f"{current_folder}/tmp/foreignerBuy.xlsx", index=False)
      save_to_db_ForeignerBuy(current_date, df)

  # 외국인 순매도
  current_date, df = get_judal_stock_data(url = "https://www.judal.co.kr/?view=stockList&type=foreignerSell", multiplier=-1)
  if df is not None:
      df.to_excel(f"{current_folder}/tmp/foreignerSell.xlsx", index=False)
      #print(df)
      save_to_db_ForeignerBuy(current_date, df)

  return current_date


# 외국인/기관 연속 매수일이 너무 큰 것은 당일 이익실현으로 하락 가능성 있다.
# SQL 쿼리: 외국인/기관 연속 매수일이 모두 (1, 2)인 종목 추출
def find_both_buy_12_12(date):
    conn = init_db()
    query = """
      WITH recent_stats AS (
        SELECT
            date,
            stock_name,
            -- 최근 5개 행(거래일) 동안의 외국인 매수 합계
            SUM(foreigner) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as foreign_sum_5d,
            -- 최근 5개 행 동안의 기관 매수 합계
            SUM(institution) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as inst_sum_5d
        FROM net_buy
      )
      SELECT
        nb.code as 종목코드,
        nb.stock_name as 종목명,
        nb.current_price as 현재가격,
        nb.change_rate as 전일비,
        nb.foreigner as 외인매수금액,
        nb.institution as 기관매수금액,
        (nb.foreigner + nb.institution) as 당일매수금액,
        rs.foreign_sum_5d as "5일_외인누적",
        rs.inst_sum_5d as "5일_기관누적",
        (rs.foreign_sum_5d + rs.inst_sum_5d) as "5일매수금액"
      FROM net_buy as nb
      JOIN recent_stats rs ON nb.date = rs.date AND nb.stock_name = rs.stock_name
      WHERE nb.foreigner_continuous IN (1, 2)
        AND nb.institution_continuous IN (1, 2)
        AND nb.date=?
      ORDER BY "5일매수금액" DESC, nb.foreigner_continuous DESC, nb.institution_continuous DESC
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    return ["외12기12", df]

def find_both_buy_2_2(date):
    conn = init_db()
    query = """
      WITH recent_stats AS (
        SELECT
            date,
            stock_name,
            -- 최근 5개 행(거래일) 동안의 외국인 매수 합계
            SUM(foreigner) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as foreign_sum_5d,
            -- 최근 5개 행 동안의 기관 매수 합계
            SUM(institution) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as inst_sum_5d
        FROM net_buy
      )
      SELECT
        nb.code as 종목코드,
        nb.stock_name as 종목명,
        nb.current_price as 현재가격,
        nb.change_rate as 전일비,
        nb.foreigner as 외인매수금액,
        nb.institution as 기관매수금액,
        (nb.foreigner + nb.institution) as 당일매수금액,
        rs.foreign_sum_5d as "5일_외인누적",
        rs.inst_sum_5d as "5일_기관누적",
        (rs.foreign_sum_5d + rs.inst_sum_5d) as "5일매수금액"
      FROM net_buy as nb
      JOIN recent_stats rs ON nb.date = rs.date AND nb.stock_name = rs.stock_name
      WHERE nb.foreigner_continuous > 2
        AND nb.institution_continuous > 2
        AND nb.date=?
      ORDER BY "5일매수금액" DESC, nb.foreigner_continuous DESC, nb.institution_continuous DESC
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    return ["외2기2", df]

# SQL 쿼리: 외국인 연속 매수일이 모두 (1, 2)인 종목 추출
def find_both_buy_12_0(date):
    conn = init_db()
    query = """
      WITH recent_stats AS (
        SELECT
            date,
            stock_name,
            -- 최근 5개 행(거래일) 동안의 외국인 매수 합계
            SUM(foreigner) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as foreign_sum_5d,
            -- 최근 5개 행 동안의 기관 매수 합계
            SUM(institution) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as inst_sum_5d
        FROM net_buy
      )
      SELECT
        nb.code as 종목코드,
        nb.stock_name as 종목명,
        nb.current_price as 현재가격,
        nb.change_rate as 전일비,
        nb.foreigner as 외인매수금액,
        nb.institution as 기관매수금액,
        (nb.foreigner + nb.institution) as 당일매수금액,
        rs.foreign_sum_5d as "5일_외인누적",
        rs.inst_sum_5d as "5일_기관누적",
        (rs.foreign_sum_5d + rs.inst_sum_5d) as "5일매수금액"
      FROM net_buy as nb
      JOIN recent_stats rs ON nb.date = rs.date AND nb.stock_name = rs.stock_name
      WHERE nb.foreigner_continuous IN (1, 2)
        AND nb.institution_continuous == 0
        AND nb.date=?
      ORDER BY "5일매수금액" DESC, nb.foreigner_continuous DESC, nb.institution_continuous DESC
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    return ["외12기0", df]

# SQL 쿼리: 외국인 연속 매수일이 모두 (1, 2)인 종목 추출
def find_both_buy_12_2(date):
    conn = init_db()
    query = """
      WITH recent_stats AS (
        SELECT
            date,
            stock_name,
            -- 최근 5개 행(거래일) 동안의 외국인 매수 합계
            SUM(foreigner) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as foreign_sum_5d,
            -- 최근 5개 행 동안의 기관 매수 합계
            SUM(institution) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as inst_sum_5d
        FROM net_buy
      )
      SELECT
        nb.code as 종목코드,
        nb.stock_name as 종목명,
        nb.current_price as 현재가격,
        nb.change_rate as 전일비,
        nb.foreigner as 외인매수금액,
        nb.institution as 기관매수금액,
        (nb.foreigner + nb.institution) as 당일매수금액,
        rs.foreign_sum_5d as "5일_외인누적",
        rs.inst_sum_5d as "5일_기관누적",
        (rs.foreign_sum_5d + rs.inst_sum_5d) as "5일매수금액"
      FROM net_buy as nb
      JOIN recent_stats rs ON nb.date = rs.date AND nb.stock_name = rs.stock_name
      WHERE nb.foreigner_continuous IN (1, 2)
        AND nb.institution_continuous > 2
        AND nb.date=?
      ORDER BY "5일매수금액" DESC, nb.foreigner_continuous DESC, nb.institution_continuous DESC
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    return ["외12기>2", df]

# SQL 쿼리: 기관 연속 매수일이 모두 (1, 2)인 종목 추출
def find_both_buy_0_12(date):
    conn = init_db()
    query = """
      WITH recent_stats AS (
        SELECT
            date,
            stock_name,
            -- 최근 5개 행(거래일) 동안의 외국인 매수 합계
            SUM(foreigner) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as foreign_sum_5d,
            -- 최근 5개 행 동안의 기관 매수 합계
            SUM(institution) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as inst_sum_5d
        FROM net_buy
      )
      SELECT
        nb.code as 종목코드,
        nb.stock_name as 종목명,
        nb.current_price as 현재가격,
        nb.change_rate as 전일비,
        nb.foreigner as 외인매수금액,
        nb.institution as 기관매수금액,
        (nb.foreigner + nb.institution) as 당일매수금액,
        rs.foreign_sum_5d as "5일_외인누적",
        rs.inst_sum_5d as "5일_기관누적",
        (rs.foreign_sum_5d + rs.inst_sum_5d) as "5일매수금액"
      FROM net_buy as nb
      JOIN recent_stats rs ON nb.date = rs.date AND nb.stock_name = rs.stock_name
      WHERE nb.foreigner_continuous == 0
        AND nb.institution_continuous IN (1, 2)
        AND nb.date=?
      ORDER BY "5일매수금액" DESC, nb.foreigner_continuous DESC, nb.institution_continuous DESC
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    return ["외0기12", df]

# SQL 쿼리: 기관 연속 매수일이 모두 (1, 2)인 종목 추출
def find_both_buy_2_12(date):
    conn = init_db()
    query = """
      WITH recent_stats AS (
        SELECT
            date,
            stock_name,
            -- 최근 5개 행(거래일) 동안의 외국인 매수 합계
            SUM(foreigner) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as foreign_sum_5d,
            -- 최근 5개 행 동안의 기관 매수 합계
            SUM(institution) OVER (
                PARTITION BY stock_name
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as inst_sum_5d
        FROM net_buy
      )
      SELECT
        nb.code as 종목코드,
        nb.stock_name as 종목명,
        nb.current_price as 현재가격,
        nb.change_rate as 전일비,
        nb.foreigner as 외인매수금액,
        nb.institution as 기관매수금액,
        (nb.foreigner + nb.institution) as 당일매수금액,
        rs.foreign_sum_5d as "5일_외인누적",
        rs.inst_sum_5d as "5일_기관누적",
        (rs.foreign_sum_5d + rs.inst_sum_5d) as "5일매수금액"
      FROM net_buy as nb
      JOIN recent_stats rs ON nb.date = rs.date AND nb.stock_name = rs.stock_name
      WHERE nb.foreigner_continuous > 2
        AND nb.institution_continuous IN (1, 2)
        AND nb.date=?
      ORDER BY "5일매수금액" DESC, nb.foreigner_continuous DESC, nb.institution_continuous DESC
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    return ["외>2기12", df]

def filter_df_for_excel(df):
    df1 = df[['종목코드', '종목명']].copy()
    df1['매입단가'] = ''
    df1['매입수량'] = ''
    df1['메모'] = ''
    return df1

def save_to_excel(current_date):
  file_name = f"stock_interest_{current_date}.xlsx"
  try:
      # 종목코드, 종목명, 매입단가, 매입수량, 메모
      with pd.ExcelWriter(f"{current_folder}/tmp/{file_name}", engine='openpyxl') as writer:
          # 데이터가 비어있지 않은 경우에만 시트 생성
          if not df1.empty:
              df = filter_df_for_excel(df1)
              df.to_excel(writer, sheet_name=title1, index=False)
          if not df2.empty:
              df = filter_df_for_excel(df2)
              df.to_excel(writer, sheet_name=title2, index=False)
          if not df3.empty:
              df = filter_df_for_excel(df3)
              df.to_excel(writer, sheet_name=title3, index=False)
          if not df4.empty:
              df = filter_df_for_excel(df4)
              df.to_excel(writer, sheet_name=title4, index=False)
          if not df5.empty:
              df = filter_df_for_excel(df5)
              df.to_excel(writer, sheet_name=title5, index=False)
          if not df6.empty:
              df = filter_df_for_excel(df6)
              df.to_excel(writer, sheet_name=title6, index=False)
      print(f"✅ 엑셀 파일이 생성되었습니다: {file_name}")
  except Exception as e:
      print(f"❌ 엑셀 생성 중 오류 발생: {e}")

def save_to_md(current_date):
  file_name_md = f"stock_interest_{current_date}.md" # 하나증권_관심종목

  # 출력할 데이터프레임과 제목 매칭
  outputs = [
      (df1, "🔥 외인 / 기관 동시 매수 (1-2일)"),
      (df2, "🔥 외인(> 2일) / 기관(> 2일)"),
      (df3, "🔥 외인 위주 매수 (1, 2일)"),
      (df4, "🔥 외인(1,2) / 기관(> 2일)"),
      (df5, "🔥 기관 위주 매수 (1, 2일)"),
      (df6, "🔥 기관(1,2) / 외인(> 2일)"),
  ]

  try:
    with open(f"{current_folder}/{file_name_md}", "w", encoding="utf-8") as f:
        # 파일 최상단 제목
        f.write(f"# 📈 관심종목 추출 결과 ({current_date})\n\n")
        f.write(f"> 추출 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n\n")

        for df, title in outputs:
            if not df.empty:
                f.write(f"## {title}\n")
                # tablefmt="github"를 사용하면 깃허브나 노션 스타일의 깔끔한 테이블이 생성됩니다.
                f.write(df.to_markdown(index=False, tablefmt="github"))
                f.write("\n\n") # 섹션 간 간격
            else:
                f.write(f"## {title}\n")
                f.write("*해당 조건에 맞는 종목이 없습니다.*\n\n")

    print(f"✅ 마크다운 파일이 생성되었습니다: {file_name_md}")
  except Exception as e:
      print(f"❌ 마크다운 생성 중 오류 발생: {e}")

import naver
def make_candle(date):
    print("📢 모든 종목의 캔들 업데이트 프로세스가 시작되었습니다.")
    conn = init_db()

    # 1. 해당 날짜에 candle 정보가 비어있는 종목 코드들 가져오기
    query = """
      SELECT code, stock_name
      FROM net_buy
      WHERE date = ?
        AND (candle = '' OR candle IS NULL)
    """
    df = pd.read_sql_query(query, conn, params=(date,))
    if df.empty:
        print(f"{date} 자에 업데이트할 캔들 데이터가 없습니다.")
        conn.close()
        return

    cursor = conn.cursor()

    # 2. 각 종목별로 루프 돌며 업데이트
    for _, row in df.iterrows():
        code = row['code']
        name = row['stock_name']

        try:
            # 네이버에서 캔들 객체 가져오기
            candle_obj = naver.get_today_candle(code)
            #time.sleep(0.1)

            if candle_obj:
                # 데이터클래스를 JSON 문자열로 변환
                candle_json = json.dumps(asdict(candle_obj))

                # DB 업데이트
                update_query = """
                    UPDATE net_buy
                    SET candle = ?
                    WHERE date = ? AND code = ?
                """
                cursor.execute(update_query, (candle_json, date, code))
                print(f"✅ {name}({code}) 캔들 업데이트 완료")
            else:
                print(f"⚠️ {name}({code}) 데이터를 가져오지 못했습니다.")

        except Exception as e:
            print(f"❌ {name}({code}) 처리 중 에러: {e}")

    conn.commit()
    conn.close()
    print("📢 모든 종목의 캔들 업데이트 프로세스가 종료되었습니다.")

# 성능을 높이기 위해서는 데이터를 리스트에 다 모은 뒤, executemany()를 사용해 한 번에 업데이트
def make_candle_optimized(date):
    conn = init_db()

    # 1. 대상 종목 가져오기
    query = "SELECT code, stock_name FROM net_buy WHERE date = ? AND (candle = '' OR candle IS NULL)"
    df = pd.read_sql_query(query, conn, params=(date,))

    if df.empty:
        conn.close()
        return

    update_data = [] # 업데이트할 데이터를 모을 리스트

    print(f"🚀 {len(df)}개 종목 데이터 수집 시작...")

    # 2. 모든 종목의 캔들 데이터를 먼저 수집
    for _, row in df.iterrows():
        code = row['code']
        name = row['stock_name']

        try:
            candle_obj = naver.get_today_candle(code)
            if candle_obj:
                candle_json = json.dumps(asdict(candle_obj))
                # (업데이트할 값, 조건1:날짜, 조건2:코드) 순서의 튜플로 저장
                update_data.append((candle_json, date, code))
                print(f"✔️ 수집 완료: {name}({code})")
        except Exception as e:
            print(f"❌ {name} 수집 실패: {e}")

    # 3. DB에 한 번에 업데이트 (Bulk Update)
    if update_data:
        try:
            cursor = conn.cursor()
            update_query = """
                UPDATE net_buy
                SET candle = ?
                WHERE date = ? AND code = ?
            """
            # executemany는 리스트 안의 튜플들을 한 번의 트랜잭션으로 처리합니다.
            cursor.executemany(update_query, update_data)
            conn.commit()
            print(f"✨ 총 {len(update_data)}개 종목 DB 반영 완료!")
        except Exception as e:
            print(f"❌ DB 업데이트 중 오류: {e}")
            conn.rollback()

    conn.close()


def fetch_candle_data(row, date):
    """멀티스레딩으로 실행될 개별 수집 함수"""
    code = row['code']
    name = row['stock_name']
    try:
        # 네이버에서 데이터 가져오기
        candle_obj = naver.get_today_candle(code)
        if candle_obj:
            candle_json = json.dumps(asdict(candle_obj))
            print(f"✔️ 수집 완료: {name}({code})")
            return (candle_json, date, code) # 성공 시 튜플 반환
    except Exception as e:
        print(f"❌ {name}({code}) 수집 실패: {e}")
    return None

def make_candle_fast(date):
    conn = init_db()

    # 1. 대상 종목 가져오기
    query = "SELECT code, stock_name FROM net_buy WHERE date = ? AND (candle = '' OR candle IS NULL)"
    df = pd.read_sql_query(query, conn, params=(date,))

    if df.empty:
        print("업데이트할 종목이 없습니다.")
        conn.close()
        return

    print(f"🚀 {len(df)}개 종목 멀티스레드 수집 시작 (병렬: 5)...")

    # 2. ThreadPoolExecutor를 이용한 병렬 수집
    update_data = []
    # max_workers를 5~10 정도로 설정하는 것이 차단 방지에 좋습니다.
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 각 행(row)에 대해 fetch_candle_data 함수를 실행
        results = list(executor.map(lambda r: fetch_candle_data(r[1], date), df.iterrows()))

        # None(실패)을 제외한 결과만 리스트에 담기
        update_data = [r for r in results if r is not None]

    # 3. DB에 일괄 업데이트 (Bulk Update)
    if update_data:
        try:
            cursor = conn.cursor()
            update_query = """
                UPDATE net_buy
                SET candle = ?
                WHERE date = ? AND code = ?
            """
            cursor.executemany(update_query, update_data)
            conn.commit()
            print(f"✨ 총 {len(update_data)}개 종목 DB 반영 완료!")
        except Exception as e:
            print(f"❌ DB 업데이트 중 오류: {e}")
            conn.rollback()

    conn.close()

if __name__ == "__main__":
  # Run Test
  global current_folder, current_date
  current_folder = Path(__file__).resolve().parent
  current_date = datetime.now().strftime('%Y-%m-%d')
  current_date = crawl_data()
  #yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

  [title1, df1] = find_both_buy_12_12(current_date)
  [title2, df2] = find_both_buy_2_2(current_date)
  [title3, df3] = find_both_buy_12_0(current_date)
  [title4, df4] = find_both_buy_12_2(current_date)
  [title5, df5] = find_both_buy_0_12(current_date)
  [title6, df6] = find_both_buy_2_12(current_date)
  #save_to_excel(current_date)
  save_to_md(current_date)
  krx_status = get_krx_status()
  if krx_status == KRXStatus.CLOSED:
    # KRX 장이 끝난 경우만 candle 정보 업데이트
    # make_candle(current_date)
    # make_candle_optimized(current_date)
    make_candle_fast(current_date) # 그닥 빠르진 않네
