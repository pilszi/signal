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

from newspaper import Article, Config
from elasticsearch import Elasticsearch
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup

# --- 1. 초기 설정 및 최적화된 소스 ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

es = Elasticsearch(
    ["http://localhost:9200"],
    request_timeout=30
)
INDEX_NAME = "news_en_es1"
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

TARGET_KEYWORDS = [
    'korea', 'china', 'taiwan', 'russia', 'ukraine', 'middle east', 'israel', 'iran',
    'sanction', 'conflict', 'geopolitical', 'military', 'security', 'war',
    'economy', 'fed', 'fomc', 'interest rate', 'inflation', 'cpi', 'recession',
    'semiconductor', 'nvidia', 'supply chain', 'tariff', 'trade war', 'price', 'stock', 'bank', 'rate',
    'breaking', 'urgent', 'alert', 'exclusive',
    'oil', 'gas', 'energy', 'battery', 'ev', 'lithium', 'crude', 'biden', 'trump',
    'market', 'crash', 'yield', 'bond'
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
]


def get_config():
    cfg = Config()
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

        raw_date = article.publish_date or rss_pub_date
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
        return "SUCCESS"

    except Exception as e:
        logging.error(f"Error processing {clean_url}: {e}")
        return "ERROR"


# --- 4. 실행부 ---

def crawl_job():
    logging.info(f"🚀 Signal 글로벌 뉴스 수집 가동: {len(RSS_FEEDS)}개 피드 탐색")
    link_data = {}

    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                search_text = (entry.title + " " + entry.get('summary', '')).lower()
                if any(kw in search_text for kw in TARGET_KEYWORDS):
                    raw_url = entry.link
                    norm_url = raw_url.split('?')[0].split('#')[0].strip().rstrip('/')

                    if norm_url not in link_data:
                        link_data[norm_url] = entry.get('published') or entry.get('updated')
        except Exception as e:
            logging.error(f"Feed error ({feed_url}): {e}")

    tasks = list(link_data.items())
    stats = {"SUCCESS": 0, "EXIST": 0, "FAILED": 0, "ERROR": 0}

    # [최적화 2] 해외 사이트 지연을 고려하여 max_workers를 10으로 확장
    if tasks:
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_and_save, tasks))
            for res in results:
                stats[res] = stats.get(res, 0) + 1

    logging.info(
        f"📊 수집 리포트 | 신규: {stats['SUCCESS']} | 중복: {stats['EXIST']} | 실패: {stats['FAILED']} | 에러: {stats['ERROR']}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    random_second = random.randint(0, 59)
    scheduler.add_job(crawl_job, "cron", minute="10,25,40,55", second=random_second,
                      next_run_time=datetime.datetime.now())
    try:
        logging.info("⏰ Signal 뉴스 엔진 가동 (고속 병렬 수집 모드)")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("👋 종료 중...")