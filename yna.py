import logging
import random
import time
import traceback
from elasticsearch import Elasticsearch, helpers
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import pandas as pd
from selenium.webdriver.chromium.options import ChromiumOptions
from apscheduler.schedulers.background import BackgroundScheduler
from utils import find_target_country
from utils import extract_keywords
from utils import generate_article_id
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
from config import Config
from logger import get_logger
from utils import is_noise_article

logger = get_logger(__name__)

# yna.py의 흐름
# run_yna_collect() -> article_process() -> article_crawling() -> article_save()

# webdriver-manager 로그 끄기
os.environ['WDM_LOG_LEVEL'] = '0'
options = webdriver.ChromeOptions()
options.add_argument("--remote-allow-origins=*")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")

# 1. 최신 헤드리스 모드 (봇 감지 회피율이 훨씬 높습니다)
options.add_argument("--headless=new")

# 2. 유저 에이전트 추가 (사람이 크롬을 쓰는 것처럼 위장)
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# 3. 자동화 제어 메시지 제거 및 탐지 회피
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

# 4. 이미지 로딩 끄기 (연합뉴스 페이지의 무거운 사진들을 안 읽어서 속도가 3배 빨라집니다)
options.add_argument("--blink-settings=imagesEnabled=false")

# 5. 페이지 로딩 전략 (eager: DOM은 로드됐지만 광고/이미지가 덜 떴어도 진행)
options.page_load_strategy = 'eager'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logging.getLogger("elasticsearch").setLevel(logging.WARNING)  # ES 내부 로그 숨기기
logging.getLogger("elastic_transport").setLevel(logging.WARNING) # 통신 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING) # 네트워크 요청 로그 숨기기

def get_es():
    return Elasticsearch("http://100.123.232.79:9200")


def close_es(es):
    es.close()


def fetch_content_single(article):
    """별도의 가벼운 드라이버나 requests를 쓰면 좋지만,
    현재 구조 유지를 위해 세부 정보 검증용으로만 사용합니다."""
    # 이 함수는 article_crawling 내부에서 효율적으로 호출됩니다.
    return article


# 1. 스케줄러 및 시작
def run_yna_collect():
    """main.py에서 10분마다 호출하는 메인 함수"""
    logging.info("📡 [연합뉴스 통합 수집 시작]")

    # 딕셔너리에 있는 모든 키워드를 리스트 하나로 합치기
    selected_keywords = get_random_strategic_keywords()
    logging.info(f"🎯 이번 회차 선정 키워드: {selected_keywords}")

    total_pages = 1  # 테스트 시(1페이지), 운영 시(2페이지)

    # 뽑힌 키워드를 실제 크롤링 함수로 전달
    return article_process(selected_keywords, total_pages)


# 1-1. 키워드를 하나로 합치고 랜덤으로 8~10개를 뽑는 보조 함수
def get_random_strategic_keywords():
    # 딕셔너리의 모든 리스트를 하나로 합침
    all_flat_keywords = [kw for sublist in Config.STRATEGIC_KEYWORDS.values() for kw in sublist]
    # 8~10개 사이로 랜덤 추출
    sample_size = min(len(all_flat_keywords), random.randint(8, 10))
    return random.sample(all_flat_keywords, sample_size)


# 2. 크롤링 루프
def article_process(keywords, total_pages):
    logger.info("----- 연합뉴스 수집 시작 -----")
    final_stats = {"success": 0, "failed": 0, "skipped": 0}

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)  # 페이지 로딩 30초 제한
    driver.implicitly_wait(5)  # 요소 찾기 5초 제한

    try:
        for key in keywords:
            # 현재 수집 중인 키워드
            logger.info(f"🔍 [YNA] 수집 중인 키워드: {key}")

            for p in range(1, total_pages + 1):
                articles = article_crawling(driver, p, key)
                if isinstance(articles, list) and len(articles) > 0:
                    # article_save가 리턴하는 {success, failed, skipped}를 받음
                    res = article_save(articles)

                    # res가 딕셔너리인지 확인하여 안전하게 더함
                    if isinstance(res, dict):
                        final_stats["success"] += res.get("success", 0)
                        final_stats["failed"] += res.get("failed", 0)
                        final_stats["skipped"] += res.get("skipped", 0)
                    else:
                        # 만약 res가 숫자(int)라면 success에만 더함
                        # (혹시라도 예전 버전과 꼬일까 봐 넣는 안전장치)
                        if isinstance(res, int):
                            final_stats["success"] += res
                        logger.warning(f"⚠️ article_save 반환값이 예상과 다릅니다: {type(res)}")

                time.sleep(1)  # 키워드 간 짧은 휴식
    except Exception as e:
        logger.error(f"❌ [YNA] 수집 중 치명적 오류 발생: {e}")
        logger.error(traceback.format_exc())
    finally:
        driver.quit()
        logger.info(f"📊 연합뉴스 수집 요약: 신규 저장 {final_stats['success']}건 / 중복 제외 {final_stats['skipped']}건")
        logger.info("--- 연합뉴스 수집 시퀀스 종료 ---")
    return final_stats



# 3. 검색 결과 추출
def article_crawling(driver, p: int, keyword):
    press = '연합뉴스'
    article_list = []
    es = get_es()  # 중복 체크용

    start_date = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")
    url = (
        f'https://www.yna.co.kr/search/index'
        f'?query={keyword}'
        f'&ctype=A'
        f'&from={start_date}'
        f'&to={end_date}'
        f'&page_no={p}'
    )
    # logging.info(f'url = {url}')

    try:
        # [수정 포인트] 기존 driver.get(url)과 time.sleep(2)를 이 블록으로 교체합니다.
        try:
            driver.get(url)
            time.sleep(2)  # 로딩 대기
        except Exception as e:
            logging.warning(f"⚠️ 페이지 로딩 타임아웃 발생(30초 초과): {url}")
            return []  # 현재 페이지/키워드는 포기하고 즉시 종료 (함수 밖으로 나감)
        try:
            temp_press = driver.find_element(By.CSS_SELECTOR, "a.logo-yna03").get_attribute("aria-label")
            if temp_press: press = temp_press
        except:
            pass

        elements = driver.find_elements(By.CSS_SELECTOR, "div.list-type501 ul.list01 li")

        temp_list = []
        for elem in elements:
            try:
                # 1. 제목,링크 우선 추출 및 사전 중복 체크
                title = elem.find_element(By.CSS_SELECTOR, "strong.tit-news").text
                link = elem.find_element(By.CSS_SELECTOR, "div.item-box01 a").get_attribute("href")
                if not title: continue

                doc_id = generate_article_id(title)

                # 이미 있는 기사는 본문 페이지에 들어가지도 않음
                if es.exists(index="news_origin", id=doc_id):
                    continue

                # 2. 이미지 체크
                photo_el = elem.find_element(By.CSS_SELECTOR, "figure.img-con11 img")
                photo = photo_el.get_attribute("src")
                if not photo or "data:image" in photo: continue

                # 3. 날짜
                published_date = elem.find_element(By.CSS_SELECTOR, "span.txt-time").text

                if not title or not published_date: continue

                # 1차 문자열 필터
                # 연도가 문자열에 아예 없으면 제거
                if "2026" not in published_date:
                    continue

                # 2차 날짜 필터링
                try:
                    parsed_date = pd.to_datetime(published_date, errors='coerce')
                    if pd.isna(parsed_date):
                        continue
                    if parsed_date.year < 2026:
                        continue
                except Exception as e:
                    logger.warning(f"⚠️ 날짜 파싱 실패: {published_date} | {e}")
                    continue

                temp_list.append({
                    "title": title, "photo": photo, "url": link,
                    "published_date": published_date, "press": press
                })
            except:
                continue

        # 4. 본문 추출 (Selenium은 순차 처리가 안전하므로 유지하되 중복이 제거되어 훨씬 빠름)
        for article in temp_list:
            try:
                driver.get(article["url"])
                time.sleep(0.8)  # 최적화된 대기 시간
                contents = driver.find_elements(By.CSS_SELECTOR, "article#articleWrap div.story-news.article p")
                if contents:
                    content_text = " ".join([c.text for c in contents if c.text.strip()])
                    if not content_text or len(content_text.strip()) < 10:
                        continue  # 내용이 없거나 너무 부실하면 저장하지 않고 패스

                    if is_noise_article(article["title"], content_text, article["url"]):
                        continue

                    article["content"] = content_text
                    article_list.append(article)
            except:
                continue

    except Exception as e:
        logging.error(f"크롤링 에러: {e}")
    finally:
        es.close()
    return article_list


# 4. 저장 및 분석
def article_save(news_list):
    if len(news_list) > 0:
        es = get_es()
        logging.info(f'저장 대상 {len(news_list)}개 처리 시작')
        try:
            df = pd.DataFrame(news_list)
            df = df[df["content"].str.len() > 0]
            if df.empty: return {}

            # # --- [추가] 날짜 필터링 로직 ---
            # # 1. 일단 날짜형으로 변환 (에러 방지를 위해 errors='coerce' 사용)
            # df['temp_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            #
            # # 2. 2026년 이후 기사만 남기고 나머지는 버림
            # original_count = len(df)
            # df = df[df['temp_date'].dt.year >= 2026]
            #
            # if len(df) < original_count:
            #     logging.info(f"⏭️ [날짜 스킵] {original_count - len(df)}건의 오래된 기사가 필터링되었습니다.")
            #
            # if df.empty: return {}
            # # ---------------------------

            # 분석 로직
            df["extracted_keywords"] = df.apply(lambda x: extract_keywords(x["title"], x["content"]), axis=1)
            df["country_name"] = df.apply(lambda x: find_target_country(x["title"], x["content"]), axis=1)
            df["is_processed"] = False

            df['published_date'] = (
                pd.to_datetime(df['published_date'], errors='coerce')
                .fillna(pd.Timestamp.now())
                .dt.strftime('%Y-%m-%dT%H:%M:%S')
            )

            actions = [
                {
                    "_op_type": "create",
                    "_index": "news_origin",
                    "_id": generate_article_id(row["title"]),
                    "_source": {
                        "title": row["title"], "content": row["content"],
                        "published_date": row["published_date"],
                        "is_processed": bool(row["is_processed"]), "url": row["url"],
                        "press_name": row["press"], "extracted_keyword": row["extracted_keywords"],
                        "country_name": row["country_name"], "main_image": row["photo"],
                    }
                }
                for _, row in df.iterrows()
            ]
            success, failed = helpers.bulk(es, actions, raise_on_error=False)
            failed_count = len(failed)
            logging.info(
                f"✅ 연합뉴스 저장 완료: 신규 {success}건 / 실패 {failed_count}건"
            )
            return {"success": success, "failed": failed_count, "skipped": 0}
        except Exception as e:
            logging.error(f"ES 저장 에러: {e}")
            return {"success": 0, "failed": len(news_list), "skipped": 0}
        finally:
            close_es(es)
    return {"success": 0, "failed": 0, "skipped": 0}


# 스케줄 알람: 시스템이 알아서 정해진 시간에 run_yna_collect를 호출하도록 명령을 내리는 것
def get_scheduler():
    # job_defaults 설정을 추가하여 인스턴스 제한을 풀기
    job_defaults = {
        'coalesce': False,
        'max_instances': 3  # 동시에 최대 3개까지 실행 허용 (기본값은 1)
    }
    sch = BackgroundScheduler(job_defaults=job_defaults)

    random_second = random.randint(0, 59)
    sch.add_job(
        run_yna_collect, "cron", minute="0,10,20,30,40,50",
        second=random_second,id='yna_crawling_job', next_run_time=datetime.now()
    )
    return sch



if __name__ == '__main__':
    sch = get_scheduler()
    try:
        sch.start()
        logging.info("⏰ 연합뉴스 고속 수집 엔진(중복 필터링 적용) 가동")
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        sch.shutdown()
        logging.info("👋 프로그램 종료")