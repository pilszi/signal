import json
import logging

import requests
import numpy as np
import yfinance as yf
import pandas as pd
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from config import Config
from db import get_db
from sqlalchemy import text
import random
# 전역 변수 유지
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logging.getLogger("elasticsearch").setLevel(logging.WARNING)  # ES 내부 로그 숨기기
logging.getLogger("elastic_transport").setLevel(logging.WARNING) # 통신 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING) # 네트워크 요청 로그 숨기기
cny_key_index = 0


def get_cny_rate_with_rotation():
    """위안화 API 로테이션 수집 (금액 반환)"""
    global cny_key_index
    for _ in range(len(Config.CNY_API_KEYS)):
        api_key = Config.CNY_API_KEYS[cny_key_index]
        url = f"https://v6.exchangerate-api.com/v6/{api_key}/pair/CNY/KRW"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if data.get('result') == 'success':
                rate = data['conversion_rate']
                # logging.info(f"✅ [API #{cny_key_index + 1}] 위안화: {rate}")
                return rate
            elif data.get('result') == 'error':
                cny_key_index = (cny_key_index + 1) % len(Config.CNY_API_KEYS)
                continue
        except Exception as e:
            cny_key_index = (cny_key_index + 1) % len(Config.CNY_API_KEYS)
            continue
    return None


def collect_market_data_job():
    """60분 간격으로 실행될 수집 및 DB 저장 작업"""
    logging.info(f"\n🚀 [수집 시작] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ticker_to_no = {
        "USDKRW=X": 1, "EURKRW=X": 2, "JPYKRW=X": 3, "CNY=X": 4,
        "GC=F": 5, "SI=F": 6, "HG=F": 7, "CL=F": 8, "BZ=F": 9, "NG=F": 10, "QM=F": 11
    }

    tickers = {
        "환율": ["USDKRW=X", "EURKRW=X", "JPYKRW=X", "CNY=X"],
        "원자재": ["GC=F", "SI=F", "HG=F", "CL=F", "BZ=F", "NG=F", "QM=F"]
    }

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # db.py의 get_db() 사용
    with get_db() as session:
        for cat, t_list in tickers.items():
            for t in t_list:
                try:
                    price = None
                    if t == "CNY=X":
                        price = get_cny_rate_with_rotation()
                        if price is None:
                            data = yf.download("CNYKRW=X", period="1d", interval="1m", progress=False)
                            if not data.empty: price = data['Close'].iloc[-1]
                    else:
                        data = yf.download(t, period="1d", interval="1m", progress=False)
                        if not data.empty:
                            last_val = data['Close'].iloc[-1]
                            price = float(last_val) if not isinstance(last_val, (pd.Series, pd.DataFrame)) else float(
                                last_val.iloc[0])

                    if price is not None:
                        final_price = round(price, 4)
                        i_no = ticker_to_no.get(t)

                        # SQLAlchemy의 session.execute 사용 (SQL문 작성)
                        # :variable 형식을 사용하여 SQL 인젝션 방지
                        query = text("""
                                INSERT INTO indicator_data (indicator_no, gathering_time, price)
                                VALUES (:no, :time, :price)
                            """)
                        session.execute(query, {"no": i_no, "time": current_time, "price": final_price})

                        logging.info(f"  ✅ [DB저장] {t:10} (No.{i_no}) | {final_price}")
                    else:
                        logging.info(f"  ⚠️ [데이터없음] {t}")

                except Exception as e:
                    logging.error(f"  ❌ [오류] {t}: {e}")

        # yield가 끝나면 get_db 내부에서 자동으로 commit()이 호출됩니다.

    logging.info(f"💤 수집 완료. 60분 대기...\n")

# --- 스케줄러 설정 ---

scheduler = BackgroundScheduler()
# 60분(hours=1) 간격으로 실행
random_second = random.randint(0, 59)
scheduler.add_job(collect_market_data_job,"cron", minute="0", second=random_second, id='indicator_crawling')

if __name__ == "__main__":
    # 실행 즉시 한 번 수집 시작
    collect_market_data_job()

    # 스케줄러 시작
    scheduler.start()
    logging.info("⏰ APScheduler 가동 중... (Ctrl+C로 종료)")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("정지되었습니다.")