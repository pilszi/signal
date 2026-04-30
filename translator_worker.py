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
okt = Okt()
es = Elasticsearch(["http://localhost:9200"])
TARGET_INDICES = ["news_en_es1"]


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

    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        translated_chunks = list(executor.map(translate_chunk, chunks))

    # [수정 포인트] 리스트 안에 None이 있을 경우를 대비해 한 번 더 필터링
    # 모든 요소를 문자열로 강제 변환(str)하여 join 에러를 원천 차단합니다.
    safe_chunks = [str(c) if c is not None else "" for c in translated_chunks]

    return "".join(safe_chunks)


def start_worker():
    print(f"[{time.strftime('%H:%M:%S')}] 다중 인덱스 번역 및 분석 워커 가동 시작...")
    print(f"대상 인덱스: {TARGET_INDICES} -> 목적지: news_origin_es2")

    while True:
        found_any_job = False

        for index_name in TARGET_INDICES:
            try:
                # 1. ES1에서 번역되지 않은 기사 검색
                query = {"query": {"term": {"is_translated": False}}}
                res = es.search(index=index_name, body={**query, "size": 1}, ignore_unavailable=True)
                hits = res['hits']['hits']

                if not hits:
                    continue

                found_any_job = True
                doc_id = hits[0]['_id']
                source = hits[0]['_source']

                print(
                    f"\n[{time.strftime('%H:%M:%S')}] [{index_name}] 작업 시작: {source.get('title_en', 'No Title')[:30]}...")

                # 2. 번역 진행
                translator = GoogleTranslator(source='en', target='ko')

                # 제목 번역 (짧으므로 직접 실행)
                raw_title = source.get('title_en', '')
                ko_title = translator.translate(raw_title) if raw_title else ""
                if not ko_title: ko_title = ""  # None 방어

                # 본문 번역 (길어서 병렬 처리)
                ko_content = translate_full_text_fast(source.get('content_en', ''))

                # 3. 키워드 및 국가 추출 (한글 데이터 기준)
                extracted_ks = extract_keywords(ko_title, ko_content)
                target_country = find_target_country(ko_title, ko_content)

                print(f"🔍 추출 완료: 국가({target_country}), 키워드({len(extracted_ks)}개)")

                # 4. [ES1 업데이트] 번역 완료 상태로 변경 (refresh=True 필수)
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

                es.index(index="news_origin_es2", id=doc_id, document=analysis_doc)
                print(f"✅ ES2 저장 완료 및 상태 업데이트 성공")

            except Exception as e:
                print(f"❌ 에러 발생 ({index_name}): {e}")
                time.sleep(5)  # 에러 시 5초 대기
                continue

        if not found_any_job:
            time.sleep(10)  # 1분은 너무 기니 10초로 단축 제안


if __name__ == "__main__":
    start_worker()