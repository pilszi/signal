import os
import traceback
import torch
import numpy as np
import pandas as pd
import torch.nn.functional as F
import re
import json
import time
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from elasticsearch import Elasticsearch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# [내 모듈 임포트]
from config import Config
from db import get_db
from logger import get_logger

# 로거 객체 생성
logger = get_logger(__name__)

# ==========================================
# 1. bert 모델 설정 및 es 연결
# ==========================================
MODEL_PATH = "./fine_tuned_finance_model_2"  # 학습시킨 모델 경로

# # 학습 완료된 모델 및 토크나이저 로드
# 1. 실행 장치 설정 (GPU가 있으면 사용, 없으면 CPU)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
try:
    # 경로 존재 여부 사전 체크
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"모델 폴더를 찾을 수 없습니다: {MODEL_PATH}")
    # 2. 토크나이저 로드 (아래의 get_weighted_keyword_score 함수 거치고 토크나이저 가져오기만 함)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    # 3. 모델 로드 및 장치 할당
    bert_model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
    bert_model.to(device)  # GPU/CPU 설정 적용
    bert_model.eval()
    logger.info(f"✅ [모델 로드] 커스텀 학습 모델 로드 완료: {MODEL_PATH}")
except FileNotFoundError as fe:
    logger.error(f"❌ [경로 에러] {fe}")
except Exception as e:
    # 상세한 에러 위치까지 로그에 남김
    err_msg = traceback.format_exc()
    logger.error(f"❌ [모델 로드 에러] 예상치 못한 오류 발생:\n{err_msg}")


es_url = Config.ES_HOST
if f":{Config.ES_PORT}" not in es_url:
    es_url = f"{es_url}:{Config.ES_PORT}"
# Elasticsearch 연결
es = Elasticsearch(
    es_url,
    basic_auth=(Config.ES_USER, Config.ES_PWD) if Config.ES_USER else None,
    request_timeout=30 # 타임아웃 방지
)

# ==========================================
# 2 분석 핵심 로직 (BERT, Z-Score, 제미나이)
# ==========================================
# 라벨링 학습한 bert가 긍정/부정 판단 - (위에서 tokenizer 가져온 이후 점수 매김)
def get_bert_score(analysis_text):
    """문맥 파악 후 -1.0 ~ 1.0 사이 점수 산출"""
    try:
        # 토크나이저로 get_weighted_keyword_score 함수에서 생성한 analysis_text인
        # bert에게 줄 요약문을 받아서 벡터화함
        inputs = tokenizer(
            analysis_text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512
        ).to(device)
        # 모델이 벡터를 보고 판단
        with torch.no_grad():
            outputs = bert_model(**inputs)
        # 모델의 예측값을 확률(0~1 사이)로 변환
        probs = F.softmax(outputs.logits, dim=-1)
        # 학습 라벨 순서에 맞춰 언패킹 (0:중립, 1:긍정, 2:부정)
        neu, pos, neg = probs[0].tolist() # train_model.py에서 이 순서로 매김
        # 긍정은 더하고(+), 부정은 빼서(-) -1.0 ~ 1.0 사이 점수 생성
        # 중립(neu)은 점수에 영향을 주지 않으므로 계산에서 제외
        return (pos * 1.0) + (neg * -1.0)
    except Exception as e:
        print(f"BERT 오류: {e}")
        return 0.0


# 사전 기반 위험도 측정 및 문맥 추출 - (utils.py에서 extract_keywords함수를 한 이후 실행)
def get_weighted_keyword_score(title, content):
    """
    4.1 요구사항: 제목 가중치(1.5배), 본문 등장 횟수 반영,
    bert가 읽기 전 키워드 포함 앞뒤 1문장 추출 (bert 분석용 텍스트 생성)
    """
    dict_score = 0
    relevant_sentences = set()  # 중복 문장 방지를 위해 set 사용

    # 문장 단위로 분리 (정규표현식 활용)
    sentences = re.split(r'(?<=[.!?])\s+', content)

    # Config에 정의된 위험 단어 사전을 순회
    for word, score in Config.DANGER_DICTIONARY.items():
        # [1] 제목 가중치 반영 (중요도 1.5배)
        if word in title:
            dict_score += (score * 1.5)

        # [2] 본문 등장 횟수 반영 및 주변 문장 추출
        if word in content:
            count = content.count(word)
            dict_score += (score * count)  # 많이 등장할수록 점수에 큰 영향

            # [3] 키워드가 포함된 문장과 그 전후 문장 추출
            for i, sentence in enumerate(sentences):
                if word in sentence:
                    relevant_sentences.add(sentence)  # 해당 문장
                    if i > 0: relevant_sentences.add(sentences[i - 1])  # 앞 문장
                    if i < len(sentences) - 1: relevant_sentences.add(sentences[i + 1])  # 뒤 문장

    # AI(BERT)에게 넘겨줄 요약본 생성 (추출된 문장이 없으면 본문 앞부분 사용)
    analysis_text = " ".join(list(relevant_sentences)) if relevant_sentences else content[:512]

    # 감성 점수 정규화 (-1.0 ~ 1.0)
    final_dict_score = max(-1.0, min(1.0, dict_score))

    return final_dict_score, analysis_text


# 제미나이 프롬프트
def get_ai_prediction_report(risk_level, title, keywords, scores):
    """Gemini AI 활용 리포트 생성"""
    if risk_level != "심각":
        main_kw = ", ".join(keywords[:2]) if keywords else "주요 경제 지표"
        return {
            "prediction": f"✅ {main_kw} 상황이 안정적입니다. 시장이 곧 회복될 것 같아요.",
            "reason": f"지금 {main_kw} 관련 뉴스나 수치들을 꼼꼼히 분석해 보니, 큰 문제 없이 정상 범위 안에 있어요. 당분간 급격한 위험은 없을 것으로 보이니 안심하셔도 좋습니다."
        }

    prompt = f"""
        [Role] 복잡한 경제 위기를 초보자도 알기 쉽게 설명해주는 친절한 경제 전문가
        [Data] 제목: {title}, 키워드: {keywords}, 점수: {scores}

        분석 미션:
        1. 지금 상황이 과거 어떤 사건(예: IMF, 오일쇼크 등)과 비슷한지 비유를 들어 아주 쉽게 설명해줘.
        2. 어려운 경제 용어 대신 일상적인 단어를 사용해서 현재 위험한 이유를 알려줘.
        3. 사용자가 앞으로 무엇을 주의 깊게 봐야 할지 '한 줄 조언'을 포함해줘.

        출력 형식 (JSON):
        {{
          "prediction": "🚨 [현재 상황 요약 - 한 줄로 아주 쉽게]",
          "reason": "1. [비유를 통한 상황 설명]\\n2. [우리에게 미치는 영향]\\n3. [앞으로의 주의점]"
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
            print(f"Gemini 키 교체 시도... ({e})")
            continue
    return {"prediction": "분석 지연", "reason": "API 할당량 초과로 인한 지연"}


# ==========================================
# 3. 지표 분석 로직 (Z-Score 산출)
# ==========================================
# 환율/원자재 라벨링 기준
def calculate_indicator_score(today_return, return_history_30d):
    if not return_history_30d: return 1.0
    mean_val, std_val = np.mean(return_history_30d), np.std(return_history_30d)
    if std_val == 0: return 1.0 # 변동이 전혀 없으면 안정으로 간주
    z_score = (today_return - mean_val) / std_val
    # 지표가 급등하거나 급락하면(절대값 2이상) 위험(-1.0) 판정
    return -1.0 if abs(z_score) >= 2.0 else 1.0

# (환율/원자재)그룹 점수를 각각 낼 때 사용
def aggregate_indicator(scores):
    valid = [s for s in scores if s is not None]
    if not valid: return 1.0
    # 7개 중 1개라도 위험(-1.0)이 있다면 위험으로 간주 (민감도 향상)
    neg_count = sum(1 for s in valid if s == -1.0)
    # 최소 1~2개 이상 문제 시 즉시 경보
    return -1.0 if neg_count >= 1 else 1.0


# ==========================================
# 4. 메인 분석 실행 함수 (run_analysis)
# ==========================================
def run_analysis():
    """
    [핵심 분석 엔진]
    1. DB에서 최근 30일 환율/원자재 지표를 로드하여 통계 분석(Z-Score) 수행
    2. ES_2(news_origin)에서 미처리 뉴스를 BERT 모델로 문맥 분석 및 감성 점수 산출
    3. 뉴스 점수 + 지표 점수를 가중 합산하여 최종 리스크 등급(안정/주의/심각) 결정
    4. Gemini AI를 통한 예측 리포트 생성 및 최종 결과를 news_labeling 인덱스에 저장
    5. 분석 완료된 원본 뉴스의 처리 상태(is_processed) 업데이트
    """
    logger.info("--------------------------------------------------")
    logger.info("🔍 [분석 엔진] 실전 분석 사이클 시작")

    # [STEP 1] DB에서 최근 30일 지표 가져오기
    with get_db() as session:
        query = text(
            "SELECT indicator_no, price FROM indicator_data WHERE gathering_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)")
        rows = session.execute(query).fetchall()

    if not rows:
        print("❌ DB 데이터 부족")
        return

    df = pd.DataFrame(rows, columns=['no', 'price'])
    # 실제 환율 가격 추출 (1:달러, 2:유로, 3:엔화, 4:위안화) -> 프론트
    try:
        exchange_prices = {
            "usd": float(df[df['no'] == 1]['price'].iloc[-1]),
            "eur": float(df[df['no'] == 2]['price'].iloc[-1]),
            "jpy": float(df[df['no'] == 3]['price'].iloc[-1]),
            "cny": float(df[df['no'] == 4]['price'].iloc[-1])
        }
    except Exception as e:
        logger.warning(f"⚠️ 일부 환율 가격을 가져오지 못했습니다: {e}")
        exchange_prices = {"usd": 0, "eur": 0, "jpy": 0, "cny": 0}

    indicator_stats = {}
    for i in range(1, 12):
        prices = df[df['no'] == i]['price'].tolist()
        if len(prices) > 1:
            indicator_stats[i] = calculate_indicator_score(prices[-1], prices[:-1])
        else:
            indicator_stats[i] = 1.0

    # [STEP 2] ES에서 미처리 뉴스 가져오기
    search_query = {"query": {"term": {"is_processed": False}}, "size": 50}
    raw_news = es.search(index="news_origin", body=search_query)
    docs = raw_news['hits']['hits']
    logger.info(f"📰 [ES] 분석 대기 중인 신규 기사: {len(docs)}건 발견")

    if not docs:
        print("✅ 처리할 새 뉴스 없음")
        return

    for doc in docs:
        _id = doc['_id']
        data = doc['_source']

        # [1] 가중 점수 계산 및 분석용 문장 추출
        keyword_score, target_text = get_weighted_keyword_score(data['title'], data['content'])
        # [2] 추출된 문장을 AI(BERT)로 분석
        ai_score = get_bert_score(target_text)
        # [3] 최종 기사 점수 산출 (AI 0.7 : 키워드 0.3)
        final_sent_score = round((ai_score * 0.7) + (keyword_score * 0.3), 4)

        # [4] 지표 점수 (Z-Score 활용)
        ex_score = aggregate_indicator([indicator_stats.get(i) for i in range(1, 5)])  # 환율
        ma_score = aggregate_indicator([indicator_stats.get(i) for i in range(5, 12)])  # 원자재

        # [5] 최종 가중치 합산 (0.5 : 0.35 : 0.15)
        total = (final_sent_score * 0.5) + (ex_score * 0.35) + (ma_score * 0.15)

        if total <= -0.1:
            risk_lv = "심각"
        elif total <= 0.4:
            risk_lv = "주의"
        else:
            risk_lv = "안정"

        # [STEP 4] Gemini 리포트
        ai_rep = get_ai_prediction_report(risk_lv, data['title'], data.get('keywords', []),
                                          {"sent": final_sent_score, "ex": ex_score, "ma": ma_score})

        # 한국 표준시(KST)로 정확하게 설정
        kst = timezone(timedelta(hours=9))  # 한국은 UTC보다 9시간 빠름
        now_kst = datetime.now(kst)

        # [STEP 5] 결과 데이터 구성
        labelled_doc = {
            "analyzed_at": now_kst.isoformat(),
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
                "sentiment_score": round(final_sent_score, 4),
                "exchange_score": float(ex_score),
                "exchange_details": exchange_prices,
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
            "publish_date": data.get('publish_date'),
            "country_name": data.get('country_name', 'Global')
        }

        # [STEP 6] ES 저장 및 상태 업데이트
        try:
            # 1. 분석 완료 데이터 저장 (ES_3)
            es.index(index="news_labeling", body=labelled_doc)
            # 2. 원본 데이터 처리 상태 업데이트 (ES_2)
            es.update(index="news_origin", id=_id, body={"doc": {"is_processed": True}})
            # 점수 산출 로그 추가
            logger.info(
                f"🎯 [분석 완료] {data['title'][:20]}...\n"
                f"   - AI 감성 점수: {final_sent_score}\n"
                f"   - 지표 합산 점수: {round(total, 4)}\n"
                f"   - 최종 위험 등급: [{risk_lv}]\n"
                f"   - 상태 업데이트: news_origin ID({_id}) -> is_processed: True"
            )
        except Exception as e:
            logger.error(f"❌ [저장 에러] ID {_id} 처리 중 오류 발생: {e}")
        # 모든 루프 종료 후 요약 로그
        logger.info(f"✅ 이번 배치 분석 완료 (총 {len(docs)}건 처리)")
        logger.info("--------------------------------------------------")


def get_latest_signals(size=10):
    """
    ES3 인덱스에서 라벨링이 완료된 모든 데이터를 가져와서 main으로 보내주는 함수
    """
    try:
        query = {
            "sort": [
                {"analyzed_at": {"order": "desc"}}
            ],
            "size": size
        }
        res = es.search(index="news_labeling", body=query)
        return [hit["_source"] for hit in res['hits']['hits']]
    except Exception as e:
        logger.error(f"ES 데이터 조회 중 오류: {e}")
        return []
        es.index(index="news_labelling", body=labelled_doc)
        # es.update(index="news_origin", id=_id, body={"doc": {"is_processed": True}})
        print(f"📑 처리완료: {data['title'][:15]}... [{risk_lv}]")


if __name__ == "__main__":
    while True:
        run_analysis()
        print("💤 10분 대기 후 다음 배치 시작...")
        time.sleep(600)