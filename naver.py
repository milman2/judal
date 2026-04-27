import json
import requests
from bs4 import BeautifulSoup
from dataclasses import asdict, dataclass
import traceback

@dataclass
class Candle:
  start: int
  end: int
  low: int
  high: int


def get_today_candle(code):
  headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
  }

  url = f"https://finance.naver.com/item/main.nhn?code={code}#"
  response = requests.get(url, headers=headers)
  if response.status_code != 200:
        print("페이지를 불러오는데 실패했습니다.")
        return

  soup = BeautifulSoup(response.text, "html.parser")

  try:
    rate_info_krx = soup.find(id="rate_info_krx")

    end_price = rate_info_krx.select_one(":scope > div.today > p.no_today > em > span.blind").text.strip().replace(",", "")
    #prev_end_price = rate_info_krx.select_one(":scope > table.no_info > tr > td.first > em > span.blind").text.strip().replace(",", "")
    high_price = rate_info_krx.select_one(":scope > table.no_info > tr > td > em > span.blind").text.strip().replace(",", "")
    start_price = rate_info_krx.select_one(":scope > table.no_info > tr > td.first > em > span.blind").text.strip().replace(",", "")
    low_price = rate_info_krx.select_one(":scope > table.no_info > tr > td:not(.first) > em > span.blind").text.strip().replace(",", "")
    # print(end_price, high_price, start_price, low_price)
    candle = Candle(start=int(start_price), end=int(end_price), low=int(low_price), high=int(high_price))
    return candle
  except Exception as e:
    print(e)
    traceback.print_exc()

if __name__ == "__main__":
  candle = get_today_candle('0088M0')
  print(candle)
  candle_json = json.dumps(asdict(candle))
  print(candle_json)