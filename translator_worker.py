import time
import random
from elasticsearch import Elasticsearch
from deep_translator import GoogleTranslator
from konlpy.tag import Okt
from collections import Counter
import re
from utils import extract_keywords, find_target_country
from concurrent.futures import ThreadPoolExecutor
from utils import generate_article_id
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logging.getLogger("elasticsearch").setLevel(logging.WARNING)  # ES 내부 로그 숨기기
logging.getLogger("elastic_transport").setLevel(logging.WARNING) # 통신 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING) # 네트워크 요청 로그 숨기기


okt = Okt()
es = Elasticsearch(["http://localhost:9200"],
    request_timeout=30,  # 30초까지 기다려줌
    retry_on_timeout=True # 타임아웃 나면 한 번 더 시도함
)
TARGET_INDICES = ["news_en"]


def translate_chunk(chunk):
    """조각 하나를 번역하는 단위 함수"""
    if not chunk or not chunk.strip():
        return ""
    try:
        # 병렬 처리 시 구글 차단을 막기 위해 약간의 지연시간 추가
        time.sleep(random.uniform(0.1, 0.4))
        result = GoogleTranslator(source='en', target='ko').translate(chunk)
        # 번역 결과가 None이면 빈 문자열 반환 (중요!)
        return result if result else ""
    except Exception:
        # 에러 발생 시 None이 아닌 문자열을 반환하여 join 에러 방지
        return "[번역 실패]"


def translate_full_text_fast(text, limit=1500):
    """본문을 쪼갠 뒤 여러 스레드로 동시에 번역"""
    if not text: return ""

    chunks = [text[i:i + limit] for i in range(0, len(text), limit)]

    # max_workers를 4 정도로 제한하여 구글 차단을 방지
    with ThreadPoolExecutor(max_workers=4) as executor:
        translated_chunks = list(executor.map(translate_chunk, chunks))

    # 리스트 안에 None이 있을 경우를 대비해 한 번 더 필터링
    # 모든 요소를 문자열로 강제 변환(str)하여 join 에러를 원천 차단
    safe_chunks = [str(c) if c is not None else "" for c in translated_chunks]

    return "".join(safe_chunks)


def process_translation():
    """
    APScheduler에 의해 주기적으로 실행될 번역 및 분석 함수입니다.
    기존 start_worker의 무한 루프 로직에서 '1회 분량'만 수행합니다.
    """
    logging.info(f"[{time.strftime('%H:%M:%S')}] 스케줄러 호출: 번역 작업 시작...")
    logging.info(f"대상 인덱스: {TARGET_INDICES} -> 목적지: news_origin")

    found_any_job = False

    for index_name in TARGET_INDICES:
        try:
            # 1. ES1에서 번역되지 않은 기사 검색 (한 번에 1개씩 처리)
            query = {"query": {"term": {"is_translated": False}}}
            res = es.search(index=index_name, body={**query, "size": 1}, ignore_unavailable=True)
            hits = res['hits']['hits']

            if not hits:
                continue

            found_any_job = True
            doc_id = hits[0]['_id']
            source = hits[0]['_source']

            print(f"[{index_name}] 작업 시작: {source.get('title_en', 'No Title')[:30]}...")

            # 2. 번역 진행
            translator = GoogleTranslator(source='en', target='ko')

            # 제목 번역
            raw_title = source.get('title_en', '')
            ko_title = translator.translate(raw_title) if raw_title else ""
            if not ko_title: ko_title = ""

            # 본문 번역 (병렬 처리)
            ko_content = translate_full_text_fast(source.get('content_en', ''))

            # 3. 키워드 및 국가 추출
            extracted_ks = extract_keywords(ko_title, ko_content)
            target_country = find_target_country(ko_title, ko_content)

            # 4. [ES1 업데이트] 번역 완료 상태로 변경
            es.update(index=index_name, id=doc_id, body={
                "doc": {"is_translated": True}
            }, refresh=True)

            # 5. [ES2 저장]
            analysis_doc = {
                "title": ko_title,
                "content": ko_content,
                "published_date": source.get('published_date'),
                "is_processed": False,
                "url": source.get('url'),
                "main_image": source.get('main_image'),
                "press_name": source.get('press_name'),
                "extracted_keyword": extracted_ks,
                "country_name": target_country
            }

            es.index(index="news_origin", id=doc_id, document=analysis_doc)
            logging.info(f"✅ ES2 저장 완료 및 상태 업데이트 성공")

        except Exception as e:
            print(f"❌ [{index_name}] 에러 발생: {e}")
            continue

    if not found_any_job:
        print(f"[{time.strftime('%H:%M:%S')}] 대기 중: 번역할 새 기사가 없습니다.")

if __name__ == "__main__":
    process_translation()