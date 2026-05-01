import torch
import numpy as np
import pandas as pd
import torch.nn.functional as F
import re
import json
import time
from datetime import datetime
from sqlalchemy import text
from elasticsearch import Elasticsearch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# [내 모듈 임포트]
from config import Config
from db import get_db
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logging.getLogger("elasticsearch").setLevel(logging.WARNING)  # ES 내부 로그 숨기기
logging.getLogger("elastic_transport").setLevel(logging.WARNING) # 통신 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING) # 네트워크 요청 로그 숨기기

# ==========================================
# 1. AI 모델 설정 (BERT & Gemini)
# ==========================================
MODEL_NAME = "klue/bert-base"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
bert_model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=3)
bert_model.eval()


es_url = Config.ES_HOST
if f":{Config.ES_PORT}" not in es_url:
    es_url = f"{es_url}:{Config.ES_PORT}"
# Elasticsearch 연결
es = Elasticsearch(
    es_url,
    # basic_auth는 최신 elasticsearch 라이브러리 권장 방식입니다.
    basic_auth=(Config.ES_USER, Config.ES_PWD) if Config.ES_USER else None
)


def get_bert_score(text_data):
    """문맥 파악 후 -1.0 ~ 1.0 사이 점수 산출"""
    try:
        inputs = tokenizer(text_data, return_tensors="pt", truncation=True, padding=True, max_length=512)
        with torch.no_grad():
            outputs = bert_model(**inputs)
        probs = F.softmax(outputs.logits, dim=-1)
        neg, neu, pos = probs[0].tolist()
        return (pos * 1.0) + (neg * -1.0)
    except Exception as e:
        logging.error(f"BERT 오류: {e}")
        return 0.0


def get_ai_prediction_report(risk_level, title, keywords, scores):
    """Gemini AI 활용 리포트 생성"""
    if risk_level != "심각":
        main_kw = ", ".join(keywords[:2]) if keywords else "주요 경제 지표"
        return {
            "prediction": f"{main_kw} 관련 지표 안정화에 따른 시장 회복세 전망",
            "reason": f"현재 {main_kw} 뉴스 심리 및 실시간 지표가 통계적 정상 범위 내에 머물고 있어 급격한 리스크 발생 가능성이 낮음"
        }

    prompt = f"""
    [Role] 수석 전략 분석가
    [Data] 제목: {title}, 키워드: {keywords}, 점수: {scores}
    분석 미션:
    1. 역사적 사건(오일쇼크, 금융위기 등)과 현재 상황의 유사성 분석
    2. 데이터 기반 브리핑 및 미래 구조 변화 예측
    출력 형식 (JSON):
    {{
      "prediction": "🚨 [요약]",
      "reason": "1. [유사성]\\n2. [변화]\\n3. [제언]"
    }}
    """
    for attempt in range(len(Config.GEMINI_API_KEYS)):
        try:
            client = Config.get_next_client()
            response = client.models.generate_content(model=Config.GEMINI_MODEL_ID, contents=prompt)
            res_text = response.text.strip()
            json_match = re.search(r'\{.*\}', res_text, re.DOTALL)
            return json.loads(json_match.group()) if json_match else json.loads(res_text)
        except Exception as e:
            logging.info(f"Gemini 키 교체 시도... ({e})")
            continue
    return {"prediction": "분석 지연", "reason": "API 할당량 초과로 인한 지연"}


# ==========================================
# 2. 통계 분석 로직 (Z-Score)
# ==========================================
def calculate_indicator_score(today_return, return_history_30d):
    if not return_history_30d: return 1.0
    mean_val, std_val = np.mean(return_history_30d), np.std(return_history_30d)
    if std_val == 0: return 1.0
    z_score = (today_return - mean_val) / std_val
    # 지표가 급등하거나 급락하면(절대값 2이상) 위험(-1.0) 판정
    return -1.0 if abs(z_score) >= 2.0 else 1.0


def aggregate_indicator(scores):
    valid = [s for s in scores if s is not None]
    if not valid: return 1.0
    neg_count = sum(1 for s in valid if s == -1.0)
    return -1.0 if neg_count >= len(valid) / 2 else 1.0


# ==========================================
# 3. 메인 파이프라인
# ==========================================
def run_analysis():
    logging.info(f"🚀 분석 시작: {datetime.now()}")

    # [STEP 1] DB에서 최근 30일 지표 가져오기
    with get_db() as session:
        query = text(
            "SELECT indicator_no, price FROM indicator_data WHERE gathering_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)")
        rows = session.execute(query).fetchall()

    if not rows:
        logging.info("❌ DB 데이터 부족")
        return

    df = pd.DataFrame(rows, columns=['no', 'price'])
    indicator_stats = {}
    for i in range(1, 12):
        prices = df[df['no'] == i]['price'].tolist()
        if len(prices) > 1:
            indicator_stats[i] = calculate_indicator_score(prices[-1], prices[:-1])
        else:
            indicator_stats[i] = 1.0

    # [STEP 2] ES에서 미처리 뉴스 가져오기
    search_query = {"query": {"term": {"is_processed": False}}, "size": 50}
    raw_news = es.search(index="news_origin_es2", body=search_query)
    docs = raw_news['hits']['hits']

    if not docs:
        logging.info("✅ 처리할 새 뉴스 없음")
        return

    for doc in docs:
        _id = doc['_id']
        data = doc['_source']

        # [STEP 3] 점수 계산
        sent_score = get_bert_score(data['title'])
        ex_score = aggregate_indicator([indicator_stats.get(i) for i in range(1, 5)])  # 환율
        ma_score = aggregate_indicator([indicator_stats.get(i) for i in range(5, 12)])  # 원자재

        # 최종 가중치 합산
        total = (sent_score * 0.4) + (ex_score * 0.3) + (ma_score * 0.3)

        if total <= -0.4:
            risk_lv = "심각"
        elif total <= 0.1:
            risk_lv = "주의"
        else:
            risk_lv = "안정"

        # [STEP 4] Gemini 리포트
        ai_rep = get_ai_prediction_report(risk_lv, data['title'], data.get('keywords', []),
                                          {"sent": sent_score, "ex": ex_score, "ma": ma_score})

        # [STEP 5] 결과 데이터 구성 (요청하신 매핑 구조)
        labelled_doc = {
            "analyzed_at": datetime.utcnow().isoformat(),
            "title": data['title'],
            "keywords": data.get('keywords', []),
            "url": data.get('url', ''),
            "press_name": data.get('press_name', ''),
            "main_image": data.get('main_image', ''),
            "prediction": ai_rep['prediction'],
            "prediction_reason": ai_rep['reason'],
            "risk_level": risk_lv,
            "final_total_score": {
                "total": round(total, 4),
                "sentiment_score": round(sent_score, 4),
                "exchange_score": float(ex_score),
                "raw_material_score": {
                    "gold": float(indicator_stats.get(5, 1.0)),
                    "silver": float(indicator_stats.get(6, 1.0)),
                    "copper": float(indicator_stats.get(7, 1.0)),
                    "wti_oil": float(indicator_stats.get(8, 1.0)),
                    "bc_oil": float(indicator_stats.get(9, 1.0)),
                    "dc_oil": float(indicator_stats.get(10, 1.0)),
                    "ng": float(indicator_stats.get(11, 1.0))
                }
            },
            "published_date": data.get('published_date'),
            "country_name": data.get('country_name', 'Global')
        }

        # [STEP 6] ES 저장 및 상태 업데이트
        es.index(index="news_labelling_es_3", body=labelled_doc)
        # es.update(index="news_origin_es2", id=_id, body={"doc": {"is_processed": True}})
        logging.info(f"📑 처리완료: {data['title'][:15]}... [{risk_lv}]")


if __name__ == "__main__":
    while True:
        run_analysis()
        logging.info("💤 10분 대기 후 다음 배치 시작...")
        time.sleep(600)