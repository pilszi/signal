import hashlib
import logging
import random
import time
from elasticsearch import Elasticsearch, helpers
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
import pandas as pd
from selenium.webdriver.chromium.options import ChromiumOptions
from apscheduler.schedulers.background import BackgroundScheduler
from utils import find_target_country
from utils import extract_keywords
from utils import generate_article_id
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
from concurrent.futures import ThreadPoolExecutor

# webdriver-manager 로그 끄기
os.environ['WDM_LOG_LEVEL'] = '0'
options = ChromiumOptions()
options.add_argument("--remote-allow-origins=*")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--start-maximized")
options.add_argument("--headless")  # 속도 향상을 위해 Headless 권장

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logging.getLogger("elasticsearch").setLevel(logging.WARNING)  # ES 내부 로그 숨기기
logging.getLogger("elastic_transport").setLevel(logging.WARNING) # 통신 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING) # 네트워크 요청 로그 숨기기

def get_es():
    return Elasticsearch("http://localhost:9200")


def close_es(es):
    es.close()


def fetch_content_single(article):
    """별도의 가벼운 드라이버나 requests를 쓰면 좋지만,
    현재 구조 유지를 위해 세부 정보 검증용으로만 사용합니다."""
    # 이 함수는 article_crawling 내부에서 효율적으로 호출됩니다.
    return article


def article_crawling(driver, p: int, keyword):
    press = '연합뉴스'
    article_list = []
    es = get_es()  # 중복 체크용

    url = f'https://www.yna.co.kr/search/index?query={keyword}&ctype=A&page_no={p}'
    logging.info(f'url = {url}')

    try:
        driver.get(url)
        time.sleep(2)

        try:
            temp_press = driver.find_element(By.CSS_SELECTOR, "a.logo-yna03").get_attribute("aria-label")
            if temp_press: press = temp_press
        except:
            pass

        elements = driver.find_elements(By.CSS_SELECTOR, "div.list-type501 ul.list01 li")

        temp_list = []
        for elem in elements:
            try:
                # 1. 링크 우선 추출 및 사전 중복 체크 (가장 중요)
                link = elem.find_element(By.CSS_SELECTOR, "div.item-box01 a").get_attribute("href")
                doc_id = generate_article_id(link)

                # [최적화] 이미 있는 기사는 본문 페이지에 들어가지도 않음
                if es.exists(index="news_origin_es2", id=doc_id):
                    continue

                # 2. 이미지 체크
                photo_el = elem.find_element(By.CSS_SELECTOR, "figure.img-con11 img")
                photo = photo_el.get_attribute("src")
                if not photo or "data:image" in photo: continue

                # 3. 제목 및 날짜
                title = elem.find_element(By.CSS_SELECTOR, "strong.tit-news").text
                published_date = elem.find_element(By.CSS_SELECTOR, "span.txt-time").text

                if not title or not published_date: continue

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
                    if content_text and len(content_text.strip()) > 10:
                        article["content"] = content_text
                        article_list.append(article)
            except:
                continue

    except Exception as e:
        logging.error(f"크롤링 에러: {e}")
    finally:
        es.close()

    return article_list


def article_save(news_list):
    if len(news_list) > 0:
        es = get_es()
        logging.info(f'저장 대상 {len(news_list)}개 처리 시작')
        try:
            df = pd.DataFrame(news_list)
            df = df[df["content"].str.len() > 0]
            if df.empty: return {}

            # 분석 로직
            df["extracted_keywords"] = df.apply(lambda x: extract_keywords(x["title"], x["content"]), axis=1)
            df["country_name"] = df.apply(lambda x: find_target_country(x["title"], x["content"]), axis=1)
            df["is_processed"] = False

            try:
                df['published_date'] = pd.to_datetime(df['published_date']).dt.strftime('%Y-%m-%dT%H:%M:%S')
            except:
                df['published_date'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

            actions = [
                {
                    "_op_type": "create",
                    "_index": "news_origin_es2",
                    "_id": generate_article_id(row["url"]),
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
            logging.info(f"✅ 연합뉴스 저장 완료: 신규 {success}건")
        except Exception as e:
            logging.error(f"ES 저장 에러: {e}")
        finally:
            close_es(es)
    return {}


def article_process(keywords, total_pages):
    logging.info("----- 연합뉴스 수집 시작 -----")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        for key in keywords:
            for p in range(1, total_pages + 1):
                articles = article_crawling(driver, p, key)
                if articles:
                    article_save(articles)
                time.sleep(1)  # 키워드 간 짧은 휴식
    finally:
        driver.quit()
        logging.info("----- 연합뉴스 수집 종료 -----")


def get_scheduler():
    # job_defaults 설정을 추가하여 인스턴스 제한을 풉니다.
    job_defaults = {
        'coalesce': False,
        'max_instances': 3  # 동시에 최대 3개까지 실행 허용 (기본값은 1)
    }
    sch = BackgroundScheduler(job_defaults=job_defaults)
    keywords = ["유가", "중국", "미국", "전쟁", "중동", "환율", "수입", "수출"]
    random_second = random.randint(0, 59)
    sch.add_job(
        article_process, "cron", minute="0,15,30,45", second=random_second,
        args=[keywords, 2], id='yna_crawling_job', replace_existing=True,
        next_run_time=datetime.now()
    )
    return sch


if __name__ == '__main__':
    sch = get_scheduler()
    try:
        sch.start()
        print("⏰ 연합뉴스 고속 수집 엔진(중복 필터링 적용) 가동")
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        sch.shutdown()
        print("👋 프로그램 종료")