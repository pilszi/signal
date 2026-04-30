import requests
from bs4 import BeautifulSoup
from config import Config
import urllib.request
import json
import hashlib
import html
from utils import find_target_country
from utils import extract_keywords
import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from elasticsearch import Elasticsearch
import random
from utils import generate_article_id
from dateutil import parser as date_parser
from concurrent.futures import ThreadPoolExecutor  # 멀티스레딩용

es = Elasticsearch(["http://localhost:9200"])
INDEX_NAME = "news_origin_es2"


def get_detailed_news(url):
    """네이버 뉴스 본문 및 상세 수집 (결측치 검증 포함)"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }
        res = requests.get(url, headers=headers, timeout=5)  # 타임아웃 단축
        soup = BeautifulSoup(res.text, "html.parser")

        image_tag = soup.find("meta", property="og:image")
        main_image = image_tag["content"] if image_tag else None

        press_name = None
        site_meta = soup.find("meta", property="og:site_name")
        if site_meta and site_meta["content"] != "네이버 뉴스":
            press_name = site_meta["content"]
        else:
            logo_tag = soup.select_one(".media_end_head_top_logo img")
            if logo_tag:
                press_name = logo_tag.get("title") or logo_tag.get("alt")

        content_tags = ["#dic_area", "#articleBodyContents", "#article_body", ".article_body"]
        full_content = None
        for tag in content_tags:
            content = soup.select_one(tag)
            if content:
                full_content = content.get_text(strip=True)
                break

        # 결측치 체크
        if not main_image or not press_name or not full_content:
            return None

        return {"main_image": main_image, "press_name": press_name, "content": full_content}
    except:
        return None


def process_single_article(item):
    """개별 기사를 상세 수집하고 ES에 저장하는 단위 작업 (스레드에서 실행)"""
    try:
        # 1. 상세 수집 실행
        details = get_detailed_news(item['link'])
        if not details:
            return False

        # 2. 날짜 파싱
        try:
            dt_obj = date_parser.parse(item['pubDate'])
            final_pub_date = dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
        except:
            return False

        # 3. 국가 및 키워드 분석
        target_country = find_target_country(item['title'], details['content'])

        doc = {
            "title": html.unescape(item['title'].replace('<b>', '').replace('</b>', '')),
            "content": html.unescape(details['content']),
            "press_name": details['press_name'],
            "published_date": final_pub_date,
            "main_image": details['main_image'],
            "url": item['link'],
            "extracted_keyword": extract_keywords(item['title'], details['content']),
            "country_name": target_country,
            "is_processed": False
        }

        # 4. 개별 저장
        doc_id = generate_article_id(item['link'])
        es.index(index=INDEX_NAME, id=doc_id, document=doc)
        return True
    except:
        return False


def bulk_search_naver_news():
    """멀티스레딩과 사전 중복체크를 적용한 고속 수집 루프"""
    newly_saved = 0
    already_exists = 0
    tasks = []  # 상세 수집 대상 기사들을 담을 리스트

    for group, keywords in Config.STRATEGIC_KEYWORDS.items():
        for kw in keywords:
            encText = urllib.parse.quote(kw)
            url = f"https://openapi.naver.com/v1/search/news.json?query={encText}&display=30&sort=date"
            req = urllib.request.Request(url)
            req.add_header("X-Naver-Client-Id", Config.NAVER_CLIENT_ID)
            req.add_header("X-Naver-Client-Secret", Config.NAVER_CLIENT_SECRET)

            try:
                res = json.loads(urllib.request.urlopen(req).read().decode('utf-8'))
                for item in res['items']:
                    if not item.get('pubDate') or not item.get('title') or not item.get('link'):
                        continue

                    doc_id = generate_article_id(item['link'])

                    # [최적화 1] 상세 페이지 접속 전 ES 중복 체크
                    if es.exists(index=INDEX_NAME, id=doc_id):
                        already_exists += 1
                        continue

                    # 중복이 아닌 기사만 작업 목록에 추가
                    tasks.append(item)

                time.sleep(0.1)
            except Exception as e:
                print(f"⚠️ [{kw}] API 호출 오류: {e}")

    # [최적화 2] 멀티스레딩(ThreadPoolExecutor) 적용
    # 동시에 10개의 상세 페이지를 긁어옵니다.
    if tasks:
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(process_single_article, tasks))
            newly_saved = results.count(True)

    print(f"📊 수집 요약: 신규 저장 {newly_saved}건 / 중복 제외 {already_exists}건")
    return {"status": "success", "newly_saved": newly_saved}


# --- 이후 스케줄러 설정 및 시스템 가동 코드는 이전과 동일하게 유지 ---
scheduler = BackgroundScheduler(timezone="Asia/Seoul")
random_second = random.randint(0, 59)


@scheduler.scheduled_job("cron", minute="0,15,30,45", second=random_second, id='collect_and_update_task')
def auto_collect_and_market_update():
    print(f"\n🚀 [통합 정기 사이클 시작] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        collect_result = bulk_search_naver_news()
        print(f"✅ 수집 완료: 새 뉴스 {collect_result.get('newly_saved', 0)}건 확보")
    except Exception as e:
        print(f"❌ 수집 단계 오류 발생: {e}")
    print(f"🏁 [사이클 종료] {datetime.now()}")


if __name__ == '__main__':
    try:
        if not scheduler.running:
            scheduler.start()
            print("⏰ [시스템] 백그라운드 스케줄러 가동 시작 (15분 주기)")

        print("🚀 [시스템] 초기 데이터 확보를 위해 첫 번째 분석을 즉시 실행합니다...")
        auto_collect_and_market_update()

        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        if scheduler.running:
            scheduler.shutdown()
            print("\n👋 [시스템] 스케줄러를 정지하고 서버를 안전하게 종료합니다.")