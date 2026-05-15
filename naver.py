import requests
from bs4 import BeautifulSoup
from config import Config
import urllib.request
import json
import logging
import html
from utils import find_target_country, extract_keywords, generate_article_id
import time
from datetime import datetime
from elasticsearch import Elasticsearch
import random
from dateutil import parser as date_parser
from concurrent.futures import ThreadPoolExecutor
from utils import is_noise_article

es = Elasticsearch(["http://localhost:9200"])
INDEX_NAME = "news_origin"


def get_detailed_news(url):
    """네이버 뉴스 본문 및 상세 수집 """
    # 네이버 뉴스 링크(n.news.naver.com)가 아니면 본문 수집이 어려우므로 패스
    if "news.naver.com" not in url:
        return None
    try:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ]
        headers = {"User-Agent": random.choice(user_agents)}

        res = requests.get(url, headers=headers, timeout=10)  # 타임아웃 단축
        res.raise_for_status()  # 응답 에러 시 바로 except로 이동
        soup = BeautifulSoup(res.text, "html.parser")

        # 이미지, 언론사, 본문 추출
        image_tag = soup.find("meta", property="og:image")
        main_image = image_tag["content"] if image_tag else None

        # 언론사 이름 추출
        press_name = None
        logo_tag = soup.select_one(".media_end_head_top_logo img")
        if logo_tag:
            press_name = logo_tag.get("title") or logo_tag.get("alt")

        if not press_name:
            site_meta = soup.find("meta", property="og:site_name")
            press_name = site_meta["content"] if site_meta else "언론사 미상"

        # 본문 셀렉터 최신화
        content_tags = ["#newsct_article", "#dic_area", "#articleBodyContents", "#article_body", ".article_body"]
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

    except Exception as e:
        logging.warning(f"⚠️ 상세 페이지 수집 에러 ({url}): {e}")
        return None




def bulk_search_naver_news():
    """네이버 API 응답 체크 및 수집 로직"""
    newly_saved = 0
    already_exists = 0
    tasks = []  # 상세 수집 대상 기사들을 담을 리스트

    # 전략 키워드 중 랜덤하게 일부만 선택 (API 할당량 및 속도 관리)
    all_flat_keywords = [kw for sublist in Config.STRATEGIC_KEYWORDS.values() for kw in sublist]
    selected_kws = random.sample(all_flat_keywords, min(len(all_flat_keywords), 5))
    logging.info(f"🚀 네이버 수집 시작 키워드: {selected_kws}")

    for kw in selected_kws:
        encText = urllib.parse.quote(kw)
        # sort=sim(유사도순) 보다는 최신 리스크 감지를 위해 sort=date(날짜순) 유지
        url = f"https://openapi.naver.com/v1/search/news.json?query={encText}&display=20&sort=date"

        req = urllib.request.Request(url)
        req.add_header("X-Naver-Client-Id", Config.NAVER_CLIENT_ID)
        req.add_header("X-Naver-Client-Secret", Config.NAVER_CLIENT_SECRET)

        try:
            response = urllib.request.urlopen(req)
            if response.getcode() != 200:
                logging.error(f"❌ 네이버 API 호출 실패: {response.getcode()}")
                continue

            res = json.loads(response.read().decode('utf-8'))

            # API가 준 기사가 아예 없을 때 로그
            if not res['items']:
                logging.info(f"ℹ️ [{kw}] 검색 결과가 없습니다.")
                continue

            for item in res['items']:
                # [수정 4] 네이버 뉴스(n.news.naver.com) 링크만 수집 대상으로 선정
                # API에서 'link'는 네이버 뉴스 링크, 'originallink'는 언론사 직링크입니다.
                target_link = item.get('link')
                if "news.naver.com" not in target_link:
                    continue

                raw_title = item['title'].replace('<b>', '').replace('</b>', '')
                clean_title = html.unescape(raw_title)

                # 1차 노이즈 필터링: 제목만 가지고 스포츠/연예/노이즈 체크
                # 상세 페이지에 들어가기 전(리소스 소모 전)에 미리 거릅니다.
                is_noise_title = any(nk.lower() in clean_title.lower() for nk in
                                     Config.SPORTS_KEYWORDS +
                                     Config.ENTERTAINMENT_KEYWORDS +
                                     Config.RECOMMENDATION_KEYWORDS +
                                     Config.skip_keywords)

                if is_noise_title:
                    # 노이즈 기사는 skip하고 로그는 남기지 않아 가독성을 유지합니다.
                    continue

                # Elasticsearch 중복 체크
                doc_id = generate_article_id(clean_title)
                if es.exists(index=INDEX_NAME, id=doc_id):
                    already_exists += 1
                    continue

                tasks.append(item)

            # API 과부하 방지를 위한 미세 대기
            time.sleep(0.1)

        except Exception as e:
            logging.error(f"⚠️ [{kw}] API 호출 중 심각한 에러: {e}")

    # 상세 수집 진행 (멀티스레딩)
    if tasks:
        logging.info(f"📦 상세 수집 시작: {len(tasks)}건 (ThreadPoolExecutor 가동)")
        with ThreadPoolExecutor(max_workers=5) as executor:  # 안정성을 위해 스레드 수 조절
            results = list(executor.map(process_single_article, tasks))
            newly_saved = results.count(True)

    logging.info(f"📊 네이버 수집 요약: 신규 저장 {newly_saved}건 / 중복 제외 {already_exists}건")
    logging.info(f"--- 네이버 수집 시퀀스 종료 ---")

    return {"status": "success", "newly_saved": newly_saved}



def process_single_article(item):
    """
    개별 기사를 상세 수집하고, 노이즈 필터링(2차)을 거쳐 Elasticsearch에 저장하는 단위 작업
    """
    try:
        # 1. 상세 수집 실행 (본문, 언론사, 이미지 등)
        details = get_detailed_news(item['link'])
        if not details:
            return False

        # 제목 정제 (HTML 태그 제거 및 특수문자 복원)
        raw_title = item['title'].replace('<b>', '').replace('</b>', '')
        clean_title = html.unescape(raw_title)

        # 2. 🔥 [2차 필터링] 팀원의 노이즈 필터링 함수 적용
        # 제목, 수집된 본문, URL을 모두 체크하여 100자 미만이거나 노이즈 키워드 포함 시 제외
        if is_noise_article(clean_title, details['content'], item['link']):
            logging.info(f"🚫 [Noise Filtered] {clean_title[:20]}... (필터링됨)")
            return False

        # 3. 날짜 파싱 (Elasticsearch 저장용 ISO 포맷 변환)
        try:
            dt_obj = date_parser.parse(item['pubDate'])
            final_pub_date = dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
        except Exception as date_err:
            logging.warning(f"⚠️ 날짜 파싱 에러: {date_err}")
            return False

        # 4. 국가 및 키워드 분석 (AI/Rule 기반)
        # 본문 전체 내용을 바탕으로 타겟 국가와 키워드를 추출합니다.
        target_country = find_target_country(clean_title, details['content'])
        extracted_kws = extract_keywords(clean_title, details['content'])

        # 5. 저장용 문서 구조 생성
        doc = {
            "title": clean_title,
            "content": html.unescape(details['content']),
            "press_name": details['press_name'],
            "published_date": final_pub_date,
            "main_image": details['main_image'],
            "url": item['link'],
            "extracted_keyword": extracted_kws,
            "country_name": target_country,
            "is_processed": False
        }

        # 4. 개별 저장
        doc_id = generate_article_id(clean_title)
        es.index(index=INDEX_NAME, id=doc_id, document=doc)
        return True

    except Exception as e:
        logging.error(f"❌ process_single_article 실행 중 오류: {e}")
        return False


def run_naver_collect():
    """main.py의 스케줄러와 연결되는 네이버 뉴스 수집 메인 함수"""
    print(f"📡 [네이버 뉴스 수집 시작] {datetime.now().strftime('%H:%M:%S')}")
    # 실제 수집 로직인 bulk_search_naver_news를 호출합니다.
    return bulk_search_naver_news()


