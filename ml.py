import os
import traceback
import torch
import numpy as np
import pandas as pd
import torch.nn.functional as F
import re
import json
import time
import asyncio
from utils import extract_keywords
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from elasticsearch import Elasticsearch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# [내 모듈 임포트]
from config import Config
from db import get_db
from logger import get_logger
import utils

# 로거 객체 생성
logger = get_logger(__name__)

# ==========================================
# 1. bert 모델 설정 및 es 연결
# ==========================================
MODEL_PATH = "./final_finance_model_v2"  # 학습시킨 모델 경로

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
# 기사 라벨링 4 - [감성 라벨링]: 문맥으로 기사 라벨링
# 라벨링 학습한 bert가 긍정/부정 판단 - (위에서 tokenizer 가져온 이후 점수 매김)
def get_bert_score(analysis_text):
    """문맥 파악 후 -1.0 ~ 1.0 사이 점수 산출
        새로운 라벨 시스템 적용:
    - LABEL_0: 긍정+중립 (점수: +1.0)
    - LABEL_1: 부정 1단계 (점수: -0.5)
    - LABEL_2: 부정 2단계 (점수: -1.0)
    """
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
        # 모델이 문맥(벡터화)을 보고 판단
        with torch.no_grad():
            outputs = bert_model(**inputs)
        # 모델의 예측값을 확률(0~1 사이)로 변환
        probs = F.softmax(outputs.logits, dim=-1)
        # 학습 라벨 순서에 맞춰 언패킹 (0:긍정/중립, 1:부정1, 2:부정2)
        pos_neu, neg_l1, neg_l2 = probs[0].tolist() # train_model.py에서 이 순서로 매김
        # 점수 산출 로직:
        # 긍정/중립은 가중치 1.0, 부정 1단계는 -0.5, 부정 2단계는 -1.0
        sentiment_score = (pos_neu * 1.0) + (neg_l1 * -0.5) + (neg_l2 * -1.0)
        return round(sentiment_score, 4)
    except Exception as e:
        print(f"BERT 오류: {e}")
        return 0.0

# 기사 라벨링 3 - [리스크 사전 라벨링]: 가중치 스코어링
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
async def get_ai_prediction_report(risk_level, title, keywords, scores):
    """Gemini AI 활용 리포트 생성"""
    # [Step 1] 가장 중요한 상위 2개 키워드 추출
    # 이미 extract_keywords에서 중요도 순으로 정렬되어 오므로 앞의 2개를 가져옵니다.
    if keywords and len(keywords) >= 2:
        target_kw = f"'{keywords[0]}'와(과) '{keywords[1]}'"
    elif keywords:
        target_kw = f"'{keywords[0]}'"
    else:
        target_kw = "주요 경제 지표"

    # [분기 1] 안정 단계: 확신과 안심 위주
    if risk_level == "안정":
        return {
            "prediction": f"✅ {target_kw} 중심의 시장 흐름이 매우 견조합니다.",
            "reason": (
                f"현재 {target_kw} 데이터를 정밀 분석한 결과, 변동성이 낮고 정상 범위 내에서 건강하게 움직이고 있습니다.\n\n"
                f"💡 시장의 신뢰도가 높아 돌발 변수에도 충분한 방어력이 확인되네요.\n"
                f"지금은 큰 걱정 없이 리더님의 기존 계획에 속도를 내셔도 좋은 시기입니다."
            )
        }

    # [분기 2] 주의 단계: 경계와 관찰 위주
    elif risk_level == "주의":
        return {
            "prediction": f"⚠️ {target_kw} 관련 지표에서 미묘한 변동성이 감지되었습니다.",
            "reason": (
                f"최근 {target_kw} 소식들을 종합하면, 당장 큰 충격이 올 확률은 낮지만 시장의 눈치싸움이 치열해진 상태입니다.\n\n"
                f"👀 작은 소식에도 민감하게 반응할 수 있는 구간이니 안심하기엔 이른 시점입니다.\n"
                f"무리한 판단보다는 지표를 꾸준히 모니터링하며 호흡을 길게 가져가는 전략을 추천합니다."
            )
        }

    prompt = f"""
        [Role] 
        데이터에 근거하여 경제 위기 상황을 냉철하게 분석하고, 
        사용자가 취해야 할 행동을 따뜻하게 조언해주는 경제 전략가

        [Input Data]
        - 기사 제목: {title}
        - 핵심 키워드: {keywords}
        - 위험 지수 및 지표: {scores}

        [작성 가이드라인 - 필수 준수]
        1. '비유(예: 황금알을 낳는 거위, 폭풍우 속의 배 등)'는 절대 사용하지 마십시오.
        2. 기사 제목과 키워드를 바탕으로 '누가(주체)', '무엇을(사건)', '얼마나(규모/수치)'가 포함되도록 사실 위주로 요약하십시오.
        3. 예측 근거는 논리적 인과관계(A로 인해 B가 발생하고, 결과적으로 C가 우려됨)로 작성하십시오.
        4. 이 사건 이후 발생할 2차 파장이나 예상되는 변화를 예측하여 작성하십시오(Short-term/Mid-term).
        5. 말투는 정중하고 둥근 대화체를 사용하되, 마지막에는 반드시 사용자가 참고할 만한 실질적인 대응 방안이나 관전 포인트를 추천하십시오.

        [출력 형식 (JSON)]
        {{
          "prediction": "🚨 [기사 내 핵심 사건과 그로 인한 직접적인 리스크를 한 줄로 요약]",
          "reason": "1. [사건의 핵심 내용]: 기사 속 주체와 행동, 구체적 수치를 바탕으로 현재 상황을 요약해 주세요.\\n2. [향후 변화 및 파급 효과]: 이 사건이 앞으로 시장이나 기업 가치에 미치는 구체적 영향을 예측해 주세요.\\n3. [행동 조언 및 대응]: 사용자가 앞으로 주의 깊게 살펴야 할 지표나 권장하는 대응 방안을 구체적으로 제안해 주세요."
        }}
        """
    for attempt in range(len(Config.GEMINI_API_KEYS)):
        try:
            # [해결책 1] 요청을 보내기 전에 2~3초간 잠시 쉽니다.
            # 이렇게 해야 무료 티어의 분당 호출 제한(RPM)을 피할 수 있어요.
            await asyncio.sleep(4)
            client = Config.get_next_client()
            response = client.models.generate_content(model=Config.GEMINI_MODEL_ID, contents=prompt)
            res_text = response.text.strip()
            json_match = re.search(r'\{.*\}', res_text, re.DOTALL)
            return json.loads(json_match.group()) if json_match else json.loads(res_text)
        except Exception as e:
            print(f"Gemini 키 교체 시도... ({e})")
            # [해결책 2] 키를 교체할 때도 잠시 텀을 줍니다.
            await asyncio.sleep(2)
            continue

    return {
        "prediction": "🚨 시장 리스크가 감지되었습니다. 실시간 분석이 지연되고 있습니다.",
        "reason": "1. 현재 분석 요청이 일시적으로 폭주하고 있습니다.\\n2. 로컬 모델 점수상으로는 '심각' 단계이니 주의가 필요합니다.\\n3. 잠시 후 대시보드를 새로고침하여 상세 AI 리포트를 확인해주세요."
    }


# ==========================================
# 3. 지표 분석 로직 (Z-Score 산출)
# ==========================================
# 환율/원자재 라벨링 기준
def calculate_indicator_score(today_return, return_history_30d):
    if not return_history_30d: return 0.5
    try:
        today_return = float(today_return)
        history_floats = [float(p) for p in return_history_30d]

        mean_val = np.mean(history_floats)
        std_val = np.std(history_floats)

        if std_val == 0: return 0.5

        z_score = (today_return - mean_val) / std_val

        # 이제 z_score가 float이므로 0.5(float)와 계산이 가능합니다!
        if z_score > 0:
            score = 1.0 - (z_score * 0.5)
            return round(max(0.0, score), 4)
        else:
            score = 1.0

        return round(score, 4)

    except Exception as e:
        logger.error(f"⚠️ 지표 점수 계산 중 타입 오류 발생: {e}")
        return 0.5


# (환율/원자재)그룹 점수를 각각 낼 때 사용
def aggregate_indicator(scores):
    valid = [max(0.0, float(s)) for s in scores if s is not None]
    if not valid: return 1.0
    # 평균값을 반환하여 전체적인 리스크 강도를 유지 (0.0 ~ 1.0 사이 값)
    avg_score = sum(valid) / len(valid)
    return round(max(0.0, avg_score), 4)


# ==========================================
# 4. 메인 분석 실행 함수 (run_analysis)
# ==========================================
# 기사 라벨링 5 [최종 통합 라벨링(벡엔드 저장)]: 30일치 지표(Z-Score) + BERT 감성 점수=>risk_lv
# 그 다음은 main.py
async def run_analysis():
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
            indicator_stats[i] = 0.5

    # [STEP 2] ES에서 미처리 뉴스 가져오기
    search_query = {"query": {"term": {"is_processed": False}}, "size": 50}
    raw_news = es.search(index="news_origin", body=search_query)
    docs = raw_news['hits']['hits']
    logger.info(f"📰 [ES] 분석 대기 중인 신규 기사: {len(docs)}건 발견")

    if not docs:
        logger.info("✅ 처리할 새 뉴스 없음")
        return

    for doc in docs:
        _id = doc['_id']
        data = doc['_source']

        refined_keywords = utils.extract_keywords(data['title'], data['content'])
        refined_country = utils.find_target_country(data['title'], data['content'])

        # [1] 키워드 점수 계산 및 본문 핵심 문장 추출 (이미 내부에서 [SEP] 처리됨)
        keyword_score, target_text = get_weighted_keyword_score(data['title'], data['content'])

        # [2] [핵심 수정] 제목과 추출된 문장을 [SEP]로 결합하여 모델 학습 환경과 일치시킴
        # 모델은 "제목 [SEP] 본문" 구조에서 가장 높은 성능을 냅니다.
        final_bert_input = f"{data['title']} [SEP] {target_text}"

        # [3] 최종 결합된 텍스트로 BERT 분석 수행
        ai_score = get_bert_score(final_bert_input)

        # [4] 최종 점수 합산 및 리스크 등급 판정
        final_sent_score = round((ai_score * 0.7) + (keyword_score * 0.3), 4)
        # [5] AI 감성 점수를 0~1 범위로 먼저 변환
        normalized_ai_score = (final_sent_score + 1) / 2
        # [6] 지표 점수 (Z-Score 활용)
        ex_score = aggregate_indicator([indicator_stats.get(i) for i in range(1, 5)])  # 환율
        ma_score = aggregate_indicator([indicator_stats.get(i) for i in range(5, 12)])  # 원자재

        # [5] 최종 가중치 합산 (0.5 : 0.35 : 0.15), 합산 결과도 무조건 0~1 사이가 됨
        total = (normalized_ai_score * 0.5) + (ex_score * 0.35) + (ma_score * 0.15)

        if total <= 0.4:
            risk_lv = "심각"
        elif total <= 0.7:
            risk_lv = "주의"
        else:
            risk_lv = "안정"

        # [STEP 4] Gemini 리포트
        ai_rep = await get_ai_prediction_report(
            risk_lv, data['title'], refined_keywords,
            {"sent": final_sent_score, "ex": ex_score, "ma": ma_score})

        # 한국 표준시(KST)로 정확하게 설정
        kst = timezone(timedelta(hours=9))  # 한국은 UTC보다 9시간 빠름
        now_kst = datetime.now(kst)

        # [STEP 5] 결과 데이터 구성
        labelled_doc = {
            "analyzed_at": now_kst.isoformat(),
            "title": data['title'],
            "keywords": refined_keywords,
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
                    "gold": float(indicator_stats.get(5, 0.5)),
                    "silver": float(indicator_stats.get(6, 0.5)),
                    "copper": float(indicator_stats.get(7, 0.5)),
                    "wti_oil": float(indicator_stats.get(8, 0.5)),
                    "bc_oil": float(indicator_stats.get(9, 0.5)),
                    "dc_oil": float(indicator_stats.get(10, 0.5)),
                    "ng": float(indicator_stats.get(11, 0.5))
                }
            },
            "published_date": data.get('published_date'),
            "country_name": refined_country
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
                f"   - 환율 지표 점수(EX): {ex_score}\n"
                f"   - 원자재 지표 점수(MA): {ma_score}\n"
                f"   - 최종 통합 점수: {round(total, 4)}\n"
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
        hits = res['hits']['hits']

        if not hits:
            logger.warning(f"⚠️ news_labeling 인덱스에 데이터가 하나도 없습니다.")
            return []
        # 데이터 가공 및 반환
        signals = [hit["_source"] for hit in hits]
        logger.info(f"✅ {len(signals)}건의 시그널 데이터를 성공적으로 불러왔습니다.")
        return signals

    except Exception as e:
        logger.error(f"❌ ES 데이터 조회 중 오류 발생: {e}")
        return []



if __name__ == "__main__":
    async def main_loop():
        while True:
            # await를 붙여서 비동기 함수를 실행합니다.
            await run_analysis()
            logger.info("💤 10분 대기 후 다음 배치 시작...")
            await asyncio.sleep(600)  # 10분 대기


    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("🛑 사용자에 의해 분석 엔진이 중단되었습니다.")