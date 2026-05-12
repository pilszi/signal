import time
import random
import re
import datetime
import json
import hashlib
import sys
import logging
import feedparser
import cloudscraper
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from dateutil import parser as date_parser
from config import Config as AppConfig

# --- 0. Python 3.14+ 호환성 패치 ---
try:
    import six
except ImportError:
    import types

    six = types.ModuleType("six")
    six.moves = types.ModuleType("moves")
    import _thread

    six.moves._thread = _thread
    sys.modules["six"] = six
    sys.modules["six.moves"] = six.moves

from newspaper import Article, Config as NewsConfig
from elasticsearch import Elasticsearch
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup

# --- 1. 초기 설정 및 최적화된 소스 ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

es = Elasticsearch(
    ["http://100.123.232.79:9200"],
    request_timeout=30
)
INDEX_NAME = "news_en"
scraper = cloudscraper.create_scraper()

RSS_FEEDS = [
    "https://abcnews.go.com/abcnews/businesstimes",
    "https://abcnews.go.com/abcnews/internationalheadlines",
    "https://finance.yahoo.com/news/rssindex",
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "https://www.investing.com/rss/news_25.rss",
    "https://www.investing.com/rss/market_overview.rss",
    "http://feeds.marketwatch.com/marketwatch/topstories/",
    "https://www.theguardian.com/world/rss",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "http://feeds.feedburner.com/zerohedge/feed",
    "https://www.scmp.com/rss/91/feed",
    "https://www.ft.com/?format=rss"
]


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
]


def get_config():
    cfg = NewsConfig()
    cfg.browser_user_agent = random.choice(USER_AGENTS)
    cfg.request_timeout = 20
    cfg.memoize_articles = False
    return cfg


# --- 2. 보조 함수 ---

def get_source_name(url):
    domain = urlparse(url).netloc.lower()
    if 'abcnews' in domain: return "ABC News"
    if 'reuters' in domain: return "Reuters"
    if 'cnbc' in domain: return "CNBC"
    if 'investing' in domain: return "Investing.com"
    if 'theguardian' in domain: return "The Guardian"
    if 'aljazeera' in domain: return "Al Jazeera"
    if 'zerohedge' in domain: return "ZeroHedge"
    if 'yahoo' in domain: return "Yahoo Finance"
    if 'marketwatch' in domain: return "MarketWatch"
    return "Global News"


def extract_exact_date(html, article_obj):
    """
    [방법 B 강화] 주요 외신별 맞춤형 시간 파싱 로직
    """
    soup = BeautifulSoup(html, 'html.parser')

    # 1. CNBC 전용: 'last_updated_datetime' 또는 'published_datetime' 메타 데이터
    cnbc_meta = soup.find('meta', attrs={'name': 'last-modified'}) or \
                soup.find('meta', attrs={'property': 'article:published_time'})
    if cnbc_meta and cnbc_meta.get('content'):
        try:
            return date_parser.parse(cnbc_meta['content'])
        except:
            pass

    # 2. Al Jazeera 전 "@type": "NewsArticle" 내의 datePublished 검색
    import json
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get('@type') == 'NewsArticle':
                return date_parser.parse(data.get('datePublished'))
            elif isinstance(data, list):  # 가끔 리스트 형태로 들어있음
                for item in data:
                    if item.get('@type') == 'NewsArticle':
                        return date_parser.parse(item.get('datePublished'))
        except:
            pass

    # 3. The Guardian 전용: 'article:published_time' 메타 태그
    guardian_meta = soup.find('meta', attrs={'property': 'article:published_time'})
    if guardian_meta and guardian_meta.get('content'):
        try:
            return date_parser.parse(guardian_meta['content'])
        except:
            pass

    # 4. 공통: newspaper3k 결과가 있다면 활용
    if article_obj.publish_date:
        return article_obj.publish_date

    return None


def fallback_extract(html):
    soup = BeautifulSoup(html, 'html.parser')
    meta_desc = soup.find('meta', attrs={'name': 'description'}) or \
                soup.find('meta', attrs={'property': 'og:description'})
    summary = ""
    if meta_desc:
        summary = meta_desc.get('content', '').strip()
    paragraphs = soup.find_all('p')
    body_text = "\n".join([p.get_text() for p in paragraphs if len(p.get_text()) > 30])
    if len(body_text) > len(summary):
        return body_text.strip()
    return summary


# --- 3. 메인 수집 함수 ---

def fetch_and_save(data):
    link, rss_pub_date = data

    if not link.startswith(('http://', 'https://')):
        full_url = "https://" + link.lstrip('/')
    else:
        full_url = link

    clean_url = full_url.split('?')[0].split('#')[0].strip().rstrip('/')
    doc_id = hashlib.sha256(clean_url.encode('utf-8')).hexdigest()

    try:
        # [최적화 1] 네트워크 요청 전 ES 중복 체크로 불필요한 트래픽 차단
        if es.exists(index=INDEX_NAME, id=doc_id):
            return "EXIST"

        # 병렬 처리 시 서버 차단 방지를 위한 미세 대기
        time.sleep(random.uniform(0.1, 0.5))

        response = scraper.get(clean_url, timeout=20)
        if response.status_code != 200:
            return "FAILED"

        html = response.text
        article = Article(clean_url, config=get_config())
        article.download(input_html=html)
        article.parse()

        content = article.text.strip()
        bad_keywords = ["subscribe to read", "register now", "standard: thomson reuters"]
        is_bad_content = any(kw in content.lower() for kw in bad_keywords)

        if len(content) < 300 or is_bad_content:
            content = fallback_extract(html)

        # [결측치 체크 강화]
        press_name = get_source_name(clean_url)
        main_image = article.top_image

        if not article.title or len(article.title.strip()) < 5:
            return "FAILED"
        if not content or len(content.strip()) < 100:
            return "FAILED"
        if not main_image or len(main_image.strip()) < 5:
            return "FAILED"

        # [방법 B 적용] 상세 페이지에서 다시 한번 정교하게 날짜 추출
        exact_date = extract_exact_date(html, article)
        raw_date = exact_date or rss_pub_date

        if not raw_date:
            return "FAILED"

        try:
            if isinstance(raw_date, str):
                dt_obj = date_parser.parse(raw_date)
            else:
                dt_obj = raw_date
            final_pub_date = dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
        except:
            return "FAILED"

        doc = {
            'title_en': article.title,
            'content_en': content,
            'press_name': press_name,
            'published_date': final_pub_date,
            'main_image': main_image,
            'url': clean_url,
            'is_translated': False,
        }

        es.index(index=INDEX_NAME, id=doc_id, document=doc)
        logging.info(f"✅ [GLOBAL-RSS] 저장 완료: {article.title[:30]}...")
        return "SUCCESS"

    except Exception as e:
        logging.error(f"❌ [GLOBAL-RSS] 수집 실패 ({clean_url}): {e}")
        return "ERROR"


# --- 4. 실행부 ---

def crawl_job(keywords):
    logging.info("🌍 [GLOBAL-RSS] === 글로벌 뉴스 수집 프로세스 시작 ===")
    link_data = {}

    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                title = entry.title.lower()
                summary = entry.get('summary', '').lower()
                search_text = (title + " " + summary)

                # --- [블랙리스트 필터링 추가] ---
                # 1. 제목/요약에 매수 유도 키워드가 있으면 즉시 패스
                if any(bl in search_text for bl in AppConfig.BLACKLIST):
                    continue

                # 2. 티커(Ticker) 패턴 감지: (MSTR), (AAPL) 등 대문자 괄호 제외
                import re
                if re.search(r"\([A-Z]{1,5}\)", entry.title):
                    continue

                # [중요] 전역 변수 TARGET_KEYWORDS 대신, 인자로 받은 keywords를 사용합니다.
                if any(kw in search_text for kw in keywords):
                    raw_url = entry.link
                    norm_url = raw_url.split('?')[0].split('#')[0].strip().rstrip('/')

                    if norm_url not in link_data:
                        # RSS 데이터에 상세 시간이 있으면 최대한 보존
                        link_data[norm_url] = entry.get('published') or entry.get('updated')
        except Exception as e:
            logging.error(f"Feed error ({feed_url}): {e}")

    tasks = list(link_data.items())
    if not tasks:
        logging.info("🌍 [GLOBAL-RSS] 탐색 결과, 키워드와 일치하는 새 기사가 없습니다.")
    else:
        logging.info(f"🌍 [GLOBAL-RSS] 총 {len(tasks)}개의 분석 대상 기사를 발견했습니다.")
    stats = {"SUCCESS": 0, "EXIST": 0, "FAILED": 0, "ERROR": 0}

    # [최적화 2] 해외 사이트 지연을 고려하여 max_workers를 10으로 확장
    if tasks:
        logging.info(f"📂 [GLOBAL-RSS] 총 {len(tasks)}개의 후보 기사 처리 중...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_and_save, tasks))
            for res in results:
                stats[res] = stats.get(res, 0) + 1


    # 신규 저장 건수가 있을 때만 강조 표시
    if stats['SUCCESS'] > 0:
        logging.info(f"✅ [GLOBAL-RSS] 저장 완료: 신규 {stats['SUCCESS']}건 (중복 제외: {stats['EXIST']}건)")
    else:
        logging.info(f"💤 [GLOBAL-RSS] 업데이트 없음 (탐색: {len(tasks)}건, 신규: 0건)")


def run_reuters_collect():
    """main.py의 스케줄러와 연결되는 글로벌 뉴스 수집 메인 함수"""
    logging.info("📡 [글로벌 뉴스 수집 시작] RSS 피드 탐색 중...")

    # 영문 키워드 전체를 하나의 리스트로 통합 (필터링용)
    TARGET_KEYWORDS = [kw.lower() for sublist in AppConfig.STRATEGIC_KEYWORDS_EN.values() for kw in sublist]

    # 이 키워드들을 가지고 수집 로직 실행
    return crawl_job(TARGET_KEYWORDS)


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    random_second = random.randint(0, 59)

    # 혼자 실행할 때도 키워드가 필요하므로 args를 추가해줘야 에러가 x
    # Config에서 키워드를 미리 뽑아서 전달
    initial_keywords = [kw.lower() for sublist in AppConfig.STRATEGIC_KEYWORDS_EN.values() for kw in sublist]

    scheduler.add_job(
        crawl_job, "cron", minute="0,10,20,30,40,50", second=random_second,
        args=[initial_keywords],
        next_run_time=datetime.datetime.now())
    try:
        logging.info("⏰ Signal 뉴스 엔진 가동 (고속 병렬 수집 모드)")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("👋 종료 중...")