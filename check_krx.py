# pip install exchange_calendars

import exchange_calendars as xcals
from datetime import datetime
import pytz
import pandas as pd
from enum import Enum

# 1. 장 상태를 정의하는 Enum 클래스
class KRXStatus(Enum):
    OPEN = "운영 중"
    CLOSED = "마감됨"
    HOLIDAY = "휴장일"

"""
정규 시장: 09:00 ~ 15:30
장후 시간외: 15:40 ~ 16:00
시간외 단일가: 16:00 ~ 18:00
"""

def get_krx_status():
    # KRX 거래소 캘린더 가져오기 (XKRX)
    krx = xcals.get_calendar("XKRX")

    # 한국 시간 기준으로 현재 시각 및 날짜 설정
    seoul_tz = pytz.timezone('Asia/Seoul')
    now_seoul = datetime.now(seoul_tz)
    today = now_seoul.date()

    # 해당 날짜가 거래일(session)인지 확인
    if not krx.is_session(today):
        return KRXStatus.HOLIDAY

    # 마감 시간 확인 (UTC -> 서울 시간 변환)
    close_time = krx.session_close(today).astimezone(seoul_tz)

    # 현재 시간이 마감 시간 이후인지 확인
    if now_seoul >= close_time:
        return KRXStatus.CLOSED
    else:
        # 개장 시간(09:00) 이전일 수도 있지만,
        # 여기서는 단순화를 위해 마감 전이면 OPEN으로 간주합니다.
        return KRXStatus.OPEN

if __name__ == "__main__":
    status = get_krx_status()

    # 2. Enum을 활용한 조건 분기
    if status == KRXStatus.OPEN:
        print(f"오늘({datetime.now().date()})은 KRX 개장일이며 현재 운영 중입니다. 📈")

    elif status == KRXStatus.CLOSED:
        print(f"오늘({datetime.now().date()})은 KRX 영업일이지만, 장이 마감되었습니다. 🏁")

    elif status == KRXStatus.HOLIDAY:
        print(f"오늘({datetime.now().date()})은 KRX 휴장일입니다. ☕")