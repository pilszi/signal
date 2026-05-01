import logging
import os
from dotenv import load_dotenv
load_dotenv()
import random
import time
from datetime import datetime
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler


# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
# 시끄러운 라이브러리 로그 차단
os.environ['WDM_LOG_LEVEL'] = '0'
for name in ['elasticsearch', 'elastic_transport', 'urllib3', 'webdriver_manager', 'selenium']:
    logging.getLogger(name).setLevel(logging.ERROR)

# 각 파일에서 함수 임포트 (파일명이 각각 yna.py, global_rss.py, naver.py 라고 가정)
# 실제 파일명에 맞춰 수정하세요.
from yna import article_process as yna_crawl
from RSS import crawl_job as global_crawl
from naver import bulk_search_naver_news as naver_crawl

app = FastAPI()
scheduler = BackgroundScheduler(timezone="Asia/Seoul")


def run_integrated_crawling():
    """세 가지 크롤링 소스를 순차적으로 실행하고 통합 리포트를 출력합니다."""
    start_time = datetime.now()
    logging.info(f"🚀 [통합 수집 사이클 시작] {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    total_stats = {"success": 0, "failed": 0, "skipped": 0}

    # 1. 연합뉴스 수집 (yna.py)
    try:
        logging.info("--- (1/3) 연합뉴스 수집 중... ---")
        keywords = ["유가", "중국", "미국", "전쟁", "중동", "환율", "수입", "수출"]
        # yna의 article_process는 통계를 리턴하도록 수정되어야 함
        res = yna_crawl(keywords, 2)
        if isinstance(res, dict):
            total_stats["success"] += res.get("success", 0)
            total_stats["skipped"] += res.get("skipped", 0)
            total_stats["failed"] += res.get("failed", 0)
    except Exception as e:
        logging.error(f"연합뉴스 수집 실패: {e}")

    # 2. 글로벌 RSS 수집 (global_rss.py)
    try:
        logging.info("--- (2/3) 글로벌 RSS 수집 중... ---")
        res = global_crawl()
        if isinstance(res, dict):
            total_stats["success"] += res.get("SUCCESS", 0)
            total_stats["skipped"] += res.get("EXIST", 0)
            total_stats["failed"] += res.get("FAILED", 0) + res.get("ERROR", 0)
    except Exception as e:
        logging.error(f"글로벌 RSS 수집 실패: {e}")

    # 3. 네이버 뉴스 수집 (naver.py)
    try:
        logging.info("--- (3/3) 네이버 뉴스 수집 중... ---")
        res = naver_crawl()
        if isinstance(res, dict):
            total_stats["success"] += res.get("newly_saved", 0)
            # 네이버 코드에서 skip 데이터가 있다면 합산
            total_stats["skipped"] += res.get("already_exists", 0)
    except Exception as e:
        logging.error(f"네이버 뉴스 수집 실패: {e}")

    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    logging.info(f"""
{'=' * 45}
📊 [통합 수집 리포트 완료]
- 총 신규 저장: {total_stats['success']}건
- 총 중복 제외: {total_stats['skipped']}건
- 총 실패/에러: {total_stats['failed']}건
- 전체 소요 시간: {duration}초
{'=' * 45}
    """)


@app.on_event("startup")
def setup_scheduler():
    """서버 시작 시 스케줄러 설정 (자동 실행은 하지 않고 대기)"""
    if not scheduler.running:
        scheduler.start()
        logging.info("⏰ 스케줄러 인스턴스 준비 완료")


@app.get("/crawl")
def crawl():
    """
    API 호출 시 스케줄러에 작업을 추가하여 즉시 + 주기적으로 실행
    """
    job_id = "integrated_news_job"
    random_second = random.randint(0, 59)

    # 이미 등록된 작업이 있다면 삭제 후 재등록 (중복 실행 방지)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    # 15분 주기로 설정 (0, 15, 30, 45분)
    scheduler.add_job(
        run_integrated_crawling,
        "cron",
        minute="0,15,30,45",
        second=random_second,
        id=job_id,
        next_run_time=datetime.now()  # 호출 즉시 첫 실행
    )

    logging.info(f"✅ 뉴스 수집 엔진 가동됨 (15분 주기, {random_second}초 기준)")
    return {
        "status": "started",
        "message": "Integrated news crawling engine is now running every 15 minutes.",
        "next_run": datetime.now().strftime('%H:%M:%S')
    }


@app.get("/stop")
def stop_crawl():
    """수집 중단"""
    scheduler.remove_all_jobs()
    return {"status": "stopped", "message": "All crawling jobs removed."}

