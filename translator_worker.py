import time
import random
import html
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from elasticsearch import Elasticsearch
from deep_translator import GoogleTranslator
from konlpy.tag import Okt
from dateutil import parser as date_parser

import RSS
# 커스텀 유틸리티 함수 (기존 파일에서 불러오기)
from utils import extract_keywords, find_target_country, generate_article_id
# pytz를 지우고 zoneinfo를 사용합니다. (Python 3.9+ 표준)
from zoneinfo import ZoneInfo
from datetime import timezone
import pytz

# 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

okt = Okt()
es = Elasticsearch(
    ["http://100.123.232.79:9200"],
    request_timeout=30,
    retry_on_timeout=True
)

TARGET_INDICES = ["news_en"]
DEST_INDEX = "news_origin"


def translate_chunk(chunk):
    """조각 하나를 번역하는 단위 함수"""
    if not chunk or not chunk.strip():
        return ""
    try:
        time.sleep(random.uniform(0.1, 0.3))
        result = GoogleTranslator(source='en', target='ko').translate(chunk)
        return result if result else ""
    except Exception:
        return "[번역 실패]"


def translate_full_text_fast(text, limit=1500):
    """본문을 쪼갠 뒤 여러 스레드로 동시에 번역"""
    if not text:
        return ""

    chunks = [text[i:i + limit] for i in range(0, len(text), limit)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        translated_chunks = list(executor.map(translate_chunk, chunks))

    safe_chunks = [str(c) if c is not None else "" for c in translated_chunks]
    return "".join(safe_chunks)


def process_translation():
    """
    영어 기사를 번역하고, 시간을 KST(+9h)로 변환하여 저장하는 메인 함수
    연속 처리를 위해 while 루프를 추가함.
    1. 기사를 1개 가져온다.
    2. 가져오자마자 원본(news_en1)의 is_translated를 True로 바꾼다 (다른 일꾼이 못 잡게 함).
    3. 그 다음 여유롭게 번역과 분석을 진행한다.
    """
    logging.info(f"[{datetime.now().strftime('%H:%M:%S')}] 번역 및 시간 변환 작업 시작...")

    TZ_MAPPING = {
        "abcnews": ZoneInfo("America/New_York"),  # 뉴욕 (UTC-4 또는 -5)
        "scmp": ZoneInfo("Asia/Hong_Kong"),  # 홍콩 (UTC+8)
        "zerohedge": ZoneInfo("UTC"),  # 제로헤지는 아까 검증 시 UTC 숫정이었으므로 UTC 지정
        "guardian": ZoneInfo("Europe/London"),  # 런던 (UTC+0 또는 +1)
    }

    while True: # [수정] 번역 대상이 없을 때까지 무한 반복
        # [해결책] 일꾼들끼리 0~1초 사이로 엇박자를 줍니다.
        # 이렇게 하면 '동시 요청' 확률이 극적으로 낮아집니다.
        time.sleep(random.uniform(0.1, 0.5))
        found_job_in_this_turn = False

        for index_name in TARGET_INDICES:
            try:
                # 1. ES에서 번역되지 않은 기사 1개 가져오기
                query = {
                            "query": {
                                "bool": {
                                    "should": [
                                        {"term": {"is_translated": False}},
                                        {"bool": {"must_not": {"exists": {"field": "is_translated"}}}}
                                    ]
                                }
                            }
                        }
                res = es.search(index=index_name, body={**query, "size": 20}, ignore_unavailable=True)
                hits = res['hits']['hits']

                if not hits:
                    continue


                    # ---- 실제 작업 대상 ----
                hit = hits[0]
                source = hit["_source"]
                doc_id = hit["_id"]

                found_job_in_this_turn = True

                # [가장 중요 - 선점 로직]
                # 번역 시작하기 전에 일단 'True'로 업데이트해서 다른 스레드가 못 가져가게 막습니다.
                # refresh=True를 주어 즉시 반영되게 합니다.
                es.update(index=index_name, id=doc_id, body={
                    "doc": {"is_translated": True}
                }, refresh=True)

                # logging.info(f"[{index_name}] 선점 성공 & 작업 시작: {source.get('title_en', 'No Title')[:30]}...")

                # 2. 날짜 KST 변환 (서머타임 완벽 해결 및 자동 분기 버전)
                # 2. 날짜 KST 변환 (완전 가공 없는 현지 시간 매칭 버전)
                # 2. 날짜 KST 변환
                raw_date = source.get("published_date")
                final_pub_date = raw_date

                if raw_date:
                    try:
                        # 한국시간 변환
                        raw_date = source.get("published_date")

                        dt_obj = date_parser.parse(raw_date)

                        # DB에는 UTC만 저장했다고 가정
                        if dt_obj.tzinfo is None:
                            dt_obj = dt_obj.replace(tzinfo=timezone.utc)

                        kst_dt = dt_obj.astimezone(ZoneInfo("Asia/Seoul"))

                        final_pub_date = kst_dt.strftime('%Y-%m-%dT%H:%M:%S')

                        logging.info(f"""
                        raw_date={raw_date}
                        kst_date={final_pub_date}
                        """)

                    except Exception as date_err:
                        logging.warning(f"날짜 변환 오류 ({raw_date}) : {date_err}")
                # 3. 번역 진행
                translator = GoogleTranslator(source='en', target='ko')
                # 제목 번역
                raw_title = source.get('title_en', '')
                ko_title = translator.translate(raw_title) if raw_title else ""
                ko_title = html.unescape(ko_title) if ko_title else ""

                # 본문 번역 (멀티스레딩)
                raw_content = source.get('content_en', '')
                ko_content = translate_full_text_fast(raw_content)
                ko_content = html.unescape(ko_content)

                # 4. 키워드 및 국가 추출
                extracted_ks = extract_keywords(ko_title, ko_content)
                target_country = find_target_country(ko_title, ko_content)

                # 5. [ES1 업데이트] 원본 인덱스 상태 변경 (반드시 필요!)
                # 이 업데이트를 안 하면 다음 루프에서 똑같은 기사를 또 가져옵니다.
                es.update(index=index_name, id=doc_id, body={
                    "doc": {"is_translated": True,
                            "published_date_kst": final_pub_date}
                }, refresh=True)

                # 6. [ES2 저장]
                analysis_doc = {
                    "title": ko_title,
                    "content": ko_content,
                    "published_date": final_pub_date,
                    "is_processed": False,
                    "url": source.get('url'),
                    "main_image": source.get('main_image'),
                    "press_name": source.get('press_name'),
                    "extracted_keyword": extracted_ks,
                    "country_name": target_country
                }
                logging.info(f"press_name={source.get('press_name')}")
                es.index(index=DEST_INDEX, id=doc_id, document=analysis_doc)
                # logging.info(f"✅ 저장 완료 (KST: {final_pub_date})")


            except Exception as e:
                logging.error(f"❌ [{index_name}] 처리 중 오류 발생: {e}")
                # [안전장치] 만약 선점(is_translated=True)까지 성공했는데 그 뒤에서 에러가 났다면?
                # 다시 False로 돌려놓아서 다음 루프나 다른 일꾼이 긁어갈 수 있게 복구합니다.
                if doc_id:
                    try:
                        logging.info(f"🔄 [{index_name}] 에러로 인한 선점 해제 조치 (ID: {doc_id})")
                        es.update(index=index_name, id=doc_id, body={"doc": {"is_translated": False}}, refresh=True)
                    except Exception as re_err:
                        logging.error(f"선점 해제 실패: {re_err}")
                time.sleep(1)
                continue

        # 모든 인덱스를 확인했는데 할 작업이 없다면 루프 종료
        if not found_job_in_this_turn:
            logging.info("모든 번역 작업이 완료되었습니다.")
            return

if __name__ == "__main__":
    process_translation()