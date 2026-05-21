from db import SessionLocal
import yfinance as yf
import pandas as pd
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from logger import get_logger
from sqlalchemy import text
import random


logger = get_logger(__name__)


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
                        try:
                            # 1순위: CNYKRW=X 직접 수집 (1위안 = ?원)
                            data = yf.download("CNYKRW=X", period="1d", interval="1m", progress=False)
                            if not data.empty:
                                price = float(data['Close'].iloc[-1].values[0]) if hasattr(
                                    data['Close'].iloc[-1], 'values') else float(data['Close'].iloc[-1])
                                logger.info(f"💾 [CNYKRW=X 직접] 위안화 환율: {price:.4f}")

                            else:
                                # 2순위: USDKRW / USDCNY 계산
                                data_usdkrw = yf.download("USDKRW=X", period="1d", interval="1m", progress=False)
                                data_usdcny = yf.download("USDCNY=X", period="1d", interval="1m", progress=False)

                                if not data_usdkrw.empty and not data_usdcny.empty:
                                    usd_krw = float(data_usdkrw['Close'].iloc[-1].values[0]) if hasattr(
                                        data_usdkrw['Close'].iloc[-1], 'values') else float(
                                        data_usdkrw['Close'].iloc[-1])
                                    usd_cny = float(data_usdcny['Close'].iloc[-1].values[0]) if hasattr(
                                        data_usdcny['Close'].iloc[-1], 'values') else float(
                                        data_usdcny['Close'].iloc[-1])

                                    # 1위안당 원화 = (1달러당 원화) / (1달러당 위안화)
                                    price = usd_krw / usd_cny
                                    logger.info(
                                        f"💾 [계산 방식] USDKRW({usd_krw:.2f}) / USDCNY({usd_cny:.4f}) = {price:.4f}")
                                else:
                                    logger.warning("⚠️ [위안화] 모든 수집 방법 실패")

                        except Exception as e:
                            logger.error(f"❌ [위안화 수집 오류] {e}")

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

if __name__ == "__main__":
    # 실행 즉시 한 번 수집 시작
    collect_market_data_job()

    scheduler = BackgroundScheduler()
    random_second = random.randint(0, 59)    # 60분(hours=1) 간격으로 실행
    scheduler.add_job(collect_market_data_job, "cron", minute="0", second=random_second, id='indicator_crawling')
    scheduler.start() # 스케줄러 시작
    print("⏰ APScheduler 가동 중... (Ctrl+C로 종료)")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("정지되었습니다.")