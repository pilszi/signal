import json
import requests
from db import SessionLocal
import yfinance as yf
import pandas as pd
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from config import Config
from logger import get_logger
from sqlalchemy import text
import random
# 전역 변수 유지
cny_key_index = 0

logger = get_logger(__name__)

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
                logger.info(f"✅ [위안화 API] 키 인덱스 {cny_key_index} 사용 | 결과: {rate}")
                return rate
            else:
                logger.warning(f"⚠️ [위안화 API] 실패: {data.get('error-type')}")
                continue
        except Exception as e:
            logger.error(f"❌ [위안화 API] 통신 에러: {e}")
        cny_key_index = (cny_key_index + 1) % len(Config.CNY_API_KEYS)

    return None

# 환율/원자재 라벨링 1: 지표 실시간 수치 가져와서 db에 저장 -> 그 다음 main
def collect_market_data_job():
    """60분 간격으로 실행될 수집 및 DB 저장 작업"""
    print(f"\n🚀 [수집 시작] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ticker_to_no = {
        "USDKRW=X": 1, "EURKRW=X": 2, "JPYKRW=X": 3, "CNY=X": 4,
        "GC=F": 5, "SI=F": 6, "HG=F": 7, "CL=F": 8, "BZ=F": 9, "NG=F": 10, "QM=F": 11
    }

    tickers = {
        "환율": ["USDKRW=X", "EURKRW=X", "JPYKRW=X", "CNY=X"],
        "원자재": ["GC=F", "SI=F", "HG=F", "CL=F", "BZ=F", "NG=F", "QM=F"]
    }

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # DB 세션 생성
    session = SessionLocal()

    try:
        for cat, t_list in tickers.items():
            for t in t_list:
                try:
                    price = None

                    if t == "CNY=X":
                        # 1. USD/KRW (달러당 원화) 가져오기
                        data_krw = yf.download("USDKRW=X", period="1d", interval="1m", progress=False)
                        # 2. USD/CNY (달러당 위안화) 가져오기
                        data_cny = yf.download("USDCNY=X", period="1d", interval="1m", progress=False)

                        if not data_krw.empty and not data_cny.empty:
                            # iloc[-1]만으로는 Series가 나올 수 있으므로, .item()을 사용하거나
                            # 값을 확실하게 추출합니다.
                            try:
                                # .values[0]을 쓰면 확실하게 숫자만 뽑아낼 수 있습니다.
                                usd_krw = float(data_krw['Close'].iloc[-1].values[0])
                                usd_cny = float(data_cny['Close'].iloc[-1].values[0])

                                price = usd_krw / usd_cny
                                logger.info(f"💾 [직접 계산] 위안화 환율: {price:.4f}")
                            except Exception as inner_e:
                                # 혹시 iloc[-1]이 이미 숫자라면 .values[0]에서 에러가 날 수 있으므로 예외 처리
                                usd_krw = float(data_krw['Close'].iloc[-1])
                                usd_cny = float(data_cny['Close'].iloc[-1])
                                price = usd_krw / usd_cny
                                logger.info(f"💾 [직접 계산 - 백업방식] 위안화 환율: {price:.4f}")
                        else:
                            logger.info(f"  ⚠️ [데이터없음] 위안화 계산을 위한 데이터 부족")

                    else:
                        # 나머지 지표들은 기존 로직대로 yfinance 다운로드
                        data = yf.download(
                            t,
                            period="1d",
                            interval="1m",
                            progress=False
                        )

                        if not data.empty:
                            last_val = data['Close'].iloc[-1]

                            price = (
                                float(last_val)
                                if not isinstance(last_val, (pd.Series, pd.DataFrame))
                                else float(last_val.iloc[0])
                            )

                    if price is not None:
                        final_price = round(price, 4)
                        i_no = ticker_to_no.get(t)

                        query = text("""
                            INSERT INTO indicator_data
                            (indicator_no, gathering_time, price)
                            VALUES (:no, :time, :price)
                        """)

                        session.execute(
                            query,
                            {
                                "no": i_no,
                                "time": current_time,
                                "price": final_price
                            }
                        )

                        logger.info(f"  ✅ [DB저장] {t:10} (No.{i_no}) | {final_price}")
                    else:
                        logger.info(f"  ⚠️ [데이터없음] {t}")

                except Exception as e:
                    logger.info(f"  ❌ [오류] {t}: {e}")

        # 최종 커밋
        session.commit()

    finally:
        # 세션 종료
        session.close()

    print(
        f"📊 [INDICATOR] "
        f"{datetime.now().strftime('%H:%M:%S')} 기준 "
        f"11종 지표 업데이트 완료"
    )
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
    print("⏰ APScheduler 가동 중... (Ctrl+C로 종료)")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("정지되었습니다.")