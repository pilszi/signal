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
import ollama
import config
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
MODEL_PATH = "./final_finance_model_v3"  # 학습시킨 모델 경로

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

async def check_yesterday_existence(keywords):
    """어제 날짜에 같은 키워드 조합의 기사가 있었는지 ES 조회"""
    try:
        from datetime import datetime, timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # 상위 키워드 2~3개만 사용하여 쿼리 (너무 많으면 안 잡힐 수 있음)
        search_keywords = keywords[:2]

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title": k}} for k in search_keywords
                    ],
                    "filter": [
                        {"range": {"analyzed_at": {"gte": yesterday + "T00:00:00"}}}
                    ]
                }
            }
        }
        res = es.search(index="news_labeling", body=query, size=1)
        return res['hits']['total']['value'] > 0
    except Exception as e:
        logger.error(f"Error checking yesterday news: {e}")
        return False

def clean_text(text):
    """분석에 방해되는 광고 및 안내 문구 제거"""
    # 1. 만화, 운세, 눈TV 등 불필요한 문구 제거

    for pattern in Config.junk_patterns:
        text = re.sub(pattern, "", text)
    return text.strip()


def clip_junk_after(text):
    # 1. 텍스트가 없으면 그대로 반환 (에러 방지)
    if not text: return ""
    for d in Config.delimiters:
        if d in text:
            # 3. 기준 단어가 나오면 그 앞부분만 취함
            text = text.split(d)[0]
    return text.strip()

def get_balanced_text(text, max_len=512):
    """긴 본문에서 앞, 중간, 뒤를 골고루 추출해 512자 내외로 만듦"""
    if not text or len(text) <= max_len:
        return text

    # 각 부분당 가져올 길이 (약 170자씩)
    part_len = max_len // 3

    first = text[:part_len]  # 기사 도입부
    middle = text[len(text) // 2: len(text) // 2 + part_len]  # 기사 중간 (본론)
    last = text[-part_len:]  # 기사 끝 (결론)

    return first + " " + middle + " " + last

# ==========================================
# 2 분석 핵심 로직 (BERT, Z-Score, 제미나이)
# ==========================================
# 기사 라벨링 4 - [감성 라벨링]: 문맥으로 기사 라벨링
# 라벨링 학습한 bert가 긍정/부정 판단 - (위에서 tokenizer 가져온 이후 점수 매김)
def get_bert_score(analysis_text):
    """모델이 조금이라도 한쪽으로 기울면 점수를 확실히 밀어주는 버전"""
    try:
        inputs = tokenizer(
            analysis_text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512
        ).to(device)

        with torch.no_grad():
            outputs = bert_model(**inputs)

        probs = F.softmax(outputs.logits, dim=-1)
        # 학습 라벨 순서: 0:중립, 1:긍정, 2:부정
        neu, pos, neg = probs[0].tolist()

        has_danger_word = False
        is_safe_in_bert = False

        # 리스크 단어 존재 여부 확인
        for word, score in Config.DANGER_DICTIONARY.items():
            if word in analysis_text:
                if score < 0:  # 음수 단어가 하나라도 있으면 위험 감지
                    has_danger_word = True
                elif score > 0:  # 양수 단어가 하나라도 있으면 안전(방어) 감지
                    is_safe_in_bert = True

        # [수정 로직] 가장 높은 확률을 가진 라벨로 점수 확정
        # 1. 긍정(pos)이 가장 높을 때 -> 확실한 플러스(+)
                # 모델의 1차 판단
        if pos > neu and pos > neg:
            val = 0.85 if pos > 0.6 else 0.6

            # [핵심 로직] 위험 단어가 있는데 "완화/지원" 같은 방어 단어가 없다면 긍정 차단
            if has_danger_word and not is_safe_in_bert:
                return -0.3
            return val

            # 2. 부정(neg)이 가장 높을 때
        elif neg > neu and neg > pos:
            return -0.85 if neg > 0.6 else -0.6

            # 3. 중립이거나 혼전일 때
        else:
            val = (pos * 1.0) + (neg * -1.0)
            # 중립 구간에서도 위험 단어만 있고 방어 단어가 없으면 점수 하향
            if has_danger_word and not is_safe_in_bert and val > 0:
                return -0.3
            return val

    except Exception as e:
        logger(f"BERT 오류: {e}")
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
            # 리스크 단어(score < 0)가 제목에 있으면 더 강력하게 감점
            title_weight = 2.0 if score < 0 else 1.2
            dict_score += (score * title_weight)

        # [2] 본문 등장 횟수 반영 및 주변 문장 추출
        if word in content:
            count = content.count(word)
            # 본문에 너무 많이 나와도 최대 감점폭을 제한 (보수적 운영)
            dict_score += (score * min(count, 3))

            # [3] 키워드가 포함된 문장과 그 전후 문장 추출
            for i, sentence in enumerate(sentences):
                if word in sentence:
                    relevant_sentences.add(sentence)  # 해당 문장
                    if i > 0: relevant_sentences.add(sentences[i - 1])  # 앞 문장
                    if i < len(sentences) - 1: relevant_sentences.add(sentences[i + 1])  # 뒤 문장

    # AI(BERT)에게 넘겨줄 요약본 생성 (추출된 문장이 없으면 본문 앞부분 사용)
    analysis_text = " ".join(list(relevant_sentences)) if relevant_sentences else content[:512]

    # 감성 점수 정규화 (-1.0 ~ 1.0)
    final_dict_score = max(-1.0, min(0.5, dict_score))

    return round(final_dict_score, 4), analysis_text


# 젬마 프롬프트
async def get_ai_prediction_report(risk_level, title, keywords, scores):
    """Ollama(Gemma2:2b) 모델을 활용한 리포트 생성"""
    # 1. 등급별 즉시 반환 로직 (기존 로직 유지)
    kw_str = ", ".join(keywords[:3]) if keywords else "주요 지표"
    subject = title[:20] + "..." if len(title) > 20 else title

    # 2. Gemma에게 보낼 프롬프트 구성
    prompt = f"""
        [Role]
        글로벌 거시경제와 공급망 분석에 능통한 '수석 경제 전략가'. 
        단순한 뉴스 전달자가 아닌, 데이터 이면의 리스크를 식별하고 시장의 연쇄 반응을 예측함.

        [Input Data]
        - 기사 제목: {title}
        - 핵심 키워드: {keywords}
        - 위험 지수 및 지표: {scores}

        [작성 가이드라인 - 필수 준수]
        1. 전문 용어 사용: '기회비용', '유동성 리스크', '헷징(Hedging)', '스프레드 확대', '신용 등급 하향' 등 금융 전문 용어를 활용하십시오.
        2. '비유' 금지: 비유 대신 '기회비용', '유동성 리스크', '헷징(Hedging)', '수익성 악화' 등 전문 용어를 사용하십시오.
        3. 인과관계 명확화: 사건(A) -> 시장 변수 변화(B) -> 산업별 손익(C)의 흐름으로 서술하십시오.
        4. 일반론 배제: '주의가 필요하다' 같은 도덕책 문구는 금지하며, 실제 자금 흐름에 기반한 조언을 하십시오.
        5. 규모 예측: 가능하다면 수치적 영향(예: 가계 부채 부담 증가, 생산 단가 상승 등)을 추론하여 포함하십시오.
        
        [Task: JSON으로만 답변]
        1. "prediction": [현상 요약] 이후 [예상되는 연쇄 반응 및 최종 결과]가 나타날 전망 (~할 가능성, ~로 이어질 전망 등 미래 시제 사용).
        2. "reason": 아래 구조를 반드시 지키되, 뉴스 내용과 상관없는 산업은 언급하지 마십시오.
           - [거시 흐름]: 국가 신용, 금리, 환율 등에 미칠 장기적 영향.
           - [섹터 변화]: 해당 이슈와 직접 관련된 산업군의 실질적 손익 및 비용 구조 변화.
           - [리스크 관리]: 분석가로서 제안하는 구체적 자산 방어 전략 및 헷징 방안.
           
        [출력 형식]
        {{
          "prediction": "🚨 전망 문구",
          "reason": "[거시 흐름]: ...\\n[섹터 변화]: ...\\n[리스크 관리]: ..."
        }}
        
        [중요 지시 - 위반 시 답변 재구성]
        1. 출력 형식 엄수: 서론, 결론, 주석, 마크다운 기호(
    ```json)를 절대 포함하지 마십시오. 오직 {{ }}로 시작하고 끝나는 순수 JSON 데이터만 출력하십시오.
        2. 전문적 통찰: 사용자가 즉각적으로 납득할 수 있는 금융/경제적 통찰을 담되, 일반적인 도덕책 같은 뻔한 조언이나 원론적인 이야기는 철저히 배제하십시오.
        3. 도메인 일관성: 뉴스 내용과 무관한 '반도체, IT, 에너지' 등의 단어를 기계적으로 반복하는 것을 금지합니다. 오직 기사 맥락과 핵심 키워드에 부합하는 산업군만 논리적으로 분석하십시오.
        4. 언어: 모든 답변은 한국어로 작성하십시오.
        """

    try:
        # 3. Ollama 호출 (로컬 실행이므로 API 키 순회 불필요)
        client = ollama.AsyncClient()
        response = await client.chat(
            model='gemma2:2b',
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.3}  # 분석 보고서이므로 일관성을 위해 낮게 설정
        )

        # json 형식만 추출
        res_text = response['message']['content'].strip()
        res_text = res_text.replace('\xa0', ' ').replace('\u200b', '')

        print("\n--- [젬마 원본 응답 시작] ---")
        print(res_text)
        print("--- [젬마 원본 응답 끝] ---\n")

        json_match = re.search(r'\{.*\}', res_text, re.DOTALL)
        if not json_match:
            raise ValueError("응답에서 JSON 구조를 찾을 수 없습니다.")

        clean_json = json_match.group()

        # [강력 정제 시작]
        # 1. 따옴표 내부의 실제 줄바꿈을 문자열 \n으로 치환
        def fix_newlines(match):
            return match.group(0).replace('\n', '\\n').replace('\r', '')

        # 큰따옴표로 둘러싸인 값 안의 줄바꿈만 찾아 바꿉니다.
        clean_json = re.sub(r'":\s*".*?"', fix_newlines, clean_json, flags=re.DOTALL)

        # 2. 마지막 쉼표 제거 (Trailing comma)
        clean_json = re.sub(r',\s*\}', '}', clean_json)

        # 3. 제어 문자 제거 (단, 줄바꿈 관련은 이미 \n으로 바꿨으므로 안전)
        clean_json = "".join(ch for ch in clean_json if ord(ch) >= 32 or ch in ['\n', '\r'])

        # --- 파싱 로직 시작 ---
        result = None
        try:
            result = json.loads(clean_json)
        except json.JSONDecodeError:
            # 마지막 수단: 더 공격적인 정제 (줄바꿈 강제 제거)
            try:
                very_clean = clean_json.replace('\n', ' ').replace('\r', ' ')
                result = json.loads(very_clean)
            except Exception as e:
                raise ValueError(f"JSON 파싱 실패: {str(e)}")

        # [성공 로직]
        if isinstance(result, dict):
            # 1. 필수 키(prediction, reason) 누락 시 방어 로직
            if 'prediction' not in result:
                # 데이터가 아예 없으면 기본값, 있으면 첫 번째 값 사용
                result['prediction'] = next(iter(result.values())) if result else "분석 결과 요약 없음"

            if 'reason' not in result:
                # prediction을 제외한 나머지 모든 키의 내용을 합쳐서 reason으로 생성
                all_text = " ".join([str(v) for k, v in result.items() if k != 'prediction'])
                result['reason'] = all_text if all_text else "상세 분석 근거 없음"

            # 2. 젬마가 "reason" 키 외에 "2", "3" 등으로 쪼개놨을 경우 합치기 (기존 로직 보강)
            final_reason = str(result.get('reason', ''))
            extra_parts = []
            for k, v in result.items():
                if k not in ['prediction', 'reason']:
                    extra_parts.append(f"{k}: {v}")

            if extra_parts:
                final_reason += "\\n" + "\\n".join(extra_parts)

            result['reason'] = final_reason
            # 3. 데이터 타입 정제 (리스트로 왔을 경우 문자열로 합침)
            if isinstance(result.get('reason'), list):
                result['reason'] = "\\n".join(map(str, result['reason']))

            # 4. 터미널 출력 로직 (심각/주의 등급)
            if risk_level in ["심각", "주의"]:
                print("\n" + "=" * 50)
                print(f"📡 [AI 분석 모니터링 - 등급: {risk_level}]")
                print(f"📌 제목: {title}")
                print(f"🤖 AI 예측: {result.get('prediction')}")

                # 출력용 줄바꿈 처리
                reason_display = str(result.get('reason', ''))
                print(f"💡 AI 근거: \n{reason_display.replace('\\n', '\n')}")
                print("=" * 50 + "\n")

            return result  # 최종 정제된 딕셔너리 반환
        else:
            raise ValueError("결과가 사전(dict) 형식이 아닙니다.")

    except Exception as e:
        # 4. 에러 발생 시 기본값 반환 (기존 시스템 안정성 유지)
        print(f"⚠️ Gemma 로컬 분석 실패: {str(e)}")

        # 위험 등급에 따른 기본 멘트 리턴
        if risk_level == "주의":
            return {
                "prediction": f"⚠️ {subject} 이슈로 인한 시장 변동성 확대 예상",
                "reason": f"현재 [{kw_str}] 등 리스크 요인이 관찰됩니다. 관련 동향을 예의주시하며 의사결정에 주의하시기 바랍니다."
            }
        elif risk_level == "안정":
            return {
                "prediction": f"✅ {subject} 상황에도 불구하고 지표 안정세 전망",
                "reason": f"분석 결과 [{kw_str}]를 포함한 전반적인 지표가 안정적입니다. 당분간 완만한 상태가 유지될 것으로 보입니다."
            }
        else:
            return {
                "prediction": f"분석 리포트 생성 지연 ({title[:15]}...)",
                "reason": "로컬 AI 모델이 현재 응답할 수 없습니다. 데이터 시각화 지표를 먼저 참고해 주세요."
            }

# ==========================================
# 3. 지표 분석 로직 (Z-Score 산출)
# ==========================================
# 환율/원자재 라벨링 기준
def calculate_indicator_score(today_return, return_history_30d):
    if not return_history_30d: return 1.0
    try:
        today_return = float(today_return)
        history_floats = [float(p) for p in return_history_30d]

        mean_val = np.mean(history_floats)
        std_val = np.std(history_floats)

        if std_val == 0: return 1.0

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
        return 1.0


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
            "SELECT indicator_no, price FROM indicator_data WHERE gathering_time >= DATE_SUB(NOW(), INTERVAL 14 DAY)")
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
    search_query = {"query": {"term": {"is_processed": False}}, "size": 20}
    raw_news = es.search(index="news_origin", body=search_query)
    docs = raw_news['hits']['hits']
    logger.info(f"📰 [ES] 분석 대기 중인 신규 기사: {len(docs)}건 발견")
    if not docs:
        logger.info("✅ 처리할 새 뉴스 없음")
        return

    # 분석 결과를 담을 리스트 생성
    processed_results = []
    for doc in docs:
        _id = doc['_id']
        data = doc['_source']

        title = data.get('title', '')
        url = data.get('url', '')

        is_sports = any(kw in title for kw in Config.SPORTS_KEYWORDS)
        is_economy_news = any(kw in title for kw in Config.economy_keywords) and not is_sports
        is_politics = "sid=100" in url or any(kw in title for kw in Config.politics_kws)

        # 🔥 [추가] 노이즈 초기 컷 (스포츠/연예/추천/잡뉴스)
        is_noise = (
                any(kw.lower() in title.lower() for kw in Config.SPORTS_KEYWORDS)
                or any(kw.lower() in title.lower() for kw in Config.ENTERTAINMENT_KEYWORDS)
                or any(kw.lower() in title.lower() for kw in Config.RECOMMENDATION_KEYWORDS)
                or any(kw.lower() in title.lower() for kw in Config.skip_keywords)
        )

        if is_noise:
            logger.info(f"⏩ [노이즈 컷] {title[:30]}")
            es.update(index="news_origin", id=_id, body={"doc": {"is_processed": True}})
            continue


        # ----------------------------------------------------------
        # 노이즈 기사 스킵 로직
        # ----------------------------------------------------------
        content = data.get('content', '')
        title = data.get('title', '')

        # 1. 본문이 너무 짧은 경우 (예: 100자 미만)
        # 2. 제목에 '아침 신문 보기', '뉴스 요약' 등 분석 가치 없는 단어가 포함된 경우

        if len(content) < 100 or any(kw in title for kw in Config.skip_keywords):
            logger.info(f"⏩ [노이즈 스킵] 분석 가치 부족으로 건너뜀: {title[:20]}...")

            # (중요) 스킵하더라도 처리는 완료된 것으로 표시해야 다음에 또 안 가져옵니다.
            try:
                es.update(index="news_origin", id=_id, body={"doc": {"is_processed": True}})
            except Exception as e:
                logger.error(f"❌ [스킵 처리 중 오류] ID {_id}: {e}")

            continue  # 다음 기사로 바로 넘어감

        refined_keywords = utils.extract_keywords(data['title'], data['content'])
        refined_country = utils.extract_country(data['title'], data['content'])

        # [1] 사전 가중 점수 계산
        keyword_score, target_text = get_weighted_keyword_score(data['title'], data['content'])

        # [2] 텍스트 정제
        cleaned_text = clean_text(target_text)
        cleaned_text = clip_junk_after(cleaned_text)

        # [강화된 스킵 로직] 내용이 너무 짧거나 노이즈인 경우 즉시 중단
        # ----------------------------------------------------------
        # 의미 있는 한글/영어 글자 수가 40자 미만이면 분석 가치 없음
        stripped_text = cleaned_text.strip()
        text_len = len(stripped_text)

        if len(cleaned_text.strip()) < 50:
            print(f"DEBUG: [길이 미달 스킵] {text_len}자 - 제목: {data.get('title')[:20]}...")
            es.update(index="news_origin", id=_id, body={"doc": {"is_processed": True}})
            continue

        # 2. 특정 언론사 노이즈 문구 포함 시 스킵
        noise_keywords = ["빅데이터 MSI", "헤럴드 리얼라이프", "주가시세표"]
        noise_found = next((noise for noise in noise_keywords if noise in cleaned_text), None)
        if noise_found:
            print(f"DEBUG: [노이즈 단어 발견 스킵] '{noise_found}' 포함 - 제목: {data.get('title')[:20]}...")
            # 여기도 처리 완료 표시 후 continue
            es.update(index="news_origin", id=_id, body={"doc": {"is_processed": True}})
            continue

        # ----------------------------------------------------------
        # 감점 폭주 방지 및 중복 제거 로직
        # ----------------------------------------------------------
        # 1. BERT 분석 먼저 실행 (기준점 확보)
        balanced_text = get_balanced_text(cleaned_text)
        ai_score = get_bert_score(balanced_text)

        # 변수 초기화
        penalty_score = 0
        body_penalty_sum = 0
        found_danger_title = False

        # 2. 사전(DANGER_DICTIONARY) 순회 - 단 한 번만 실행하여 중복 감점 방지
        for word, weight in Config.DANGER_DICTIONARY.items():
            if weight < 0:  # 리스크(음수) 단어만 감점 후보
                abs_w = abs(weight)

                if word in data['title']:
                    # 제목 감점: 가중치를 0.8배로 하향 (단어 중복 시 폭주 방지)
                    penalty_score += (abs_w * 0.25)
                    found_danger_title = True
                elif word in cleaned_text:
                    # 본문 감점: 가중치를 0.06배로 희석 (본문 노이즈 방지)
                    body_penalty_sum += (abs_w * 0.01)

        # 본문 페널티는 아무리 단어가 많아도 0.3점 이상 못 깎게 제한 (CAP 설정)
        body_penalty_sum = min(0.1, body_penalty_sum)

        # 4. 최종 합산 시 한 번 더 희석 (페널티의 영향력을 전체의 절반으로)
        total_penalty = (penalty_score + body_penalty_sum) * 0.3
        ai_score = ai_score - total_penalty

        # 부정 방어
        is_safe_news = any(
            word in data.get('title', '') for word, weight in Config.DANGER_DICTIONARY.items() if weight > 0)
        if is_safe_news:
            # 1. 지원책 기사라면 기존 페널티의 20%만 적용 (80% 삭감)
            ai_score = ai_score - (total_penalty * 0.2)

            # 2. 보정 후에도 점수가 너무 낮으면(부정적이면) '중립' 근처로 강제 견인
            # '고용위기' 단어 때문에 억울하게 깎인 점수를 복구해주는 단계입니다.
            if ai_score < -0.1:
                ai_score = -0.1
        else:
            # 일반 기사는 기존 방식 그대로 페널티 전체 적용
            ai_score = ai_score - total_penalty

        # -0.7 이하 하락 제동
        if ai_score < -0.6:
            excess_loss = ai_score - (-0.6)
            ai_score = -0.6 + (excess_loss * 0.25)

        # 5. 점수 가두기 (물리적 한계선)
        ai_score = max(-1.0, min(1.0, ai_score))

        # 제목이 정말 위험한데 AI가 너무 낙관적일 때만 '주의'급으로 조정
        if found_danger_title and ai_score > 0.5:
            ai_score = 0.3
        # ----------------------------------------------------------

        # [3] 최종 기사 점수 산출 (보정된 ai_score 사용)
        final_sent_score = round((ai_score * 0.7) + (keyword_score * 0.3), 4)
        # [4] AI 감성 점수를 0~1 범위로 먼저 변환
        normalized_ai_score = (final_sent_score + 1) / 2
        # [5] 지표 점수 (Z-Score 활용)
        ex_score = aggregate_indicator([indicator_stats.get(i) for i in range(1, 5)])  # 환율
        ma_score = aggregate_indicator([indicator_stats.get(i) for i in range(5, 12)])  # 원자재

        # 1. 지표 순수 점수 계산
        raw_indicator_score = (ex_score * 0.5) + (ma_score * 0.5)



        # [B] 점수 통합 (질문하신 0.9 / 0.7 로직이 여기 합쳐졌습니다)
        if is_sports:
            # 스포츠는 지표 무시하고 AI 점수 기반 하한선 보정
            total = max(0.65, (normalized_ai_score + 1) / 2)
        else:
            # 감성 점수가 극단적(0.25이하, 0.65이상)이면 뉴스 비중을 90%로 상향
            if normalized_ai_score <= 0.25 or normalized_ai_score >= 0.65:
                total = (normalized_ai_score * 0.9) + (raw_indicator_score * 0.1)
            else:
                total = (normalized_ai_score * 0.7) + (raw_indicator_score * 0.3)

        # [C] 경제 기사 페널티
        if is_economy_news and raw_indicator_score < 0.35:
            total -= 0.2

        # [D] 정치 및 반복 뉴스 보정
        if is_politics:
            total = min(1.0, total + 0.12)
            if total <= 0.63 and await check_yesterday_existence(refined_keywords):
                total += (0.65 - total) * 0.2

        is_stock_recommendation = any(
            kw.lower() in title.lower()
            for kw in Config.SAFE_FINANCE_PATTERNS
        )
        if is_stock_recommendation:
            boost = 0.06
            total = min(1.0, total * (1 + boost))

        is_corporate_news = any(
            kw.lower() in title.lower()
            for kw in Config.CORPORATE_NEWS_KEYWORDS
        )

        if is_corporate_news:
            floor = 0.52
            total = (total * 0.8) + (floor * 0.2)

        # [E] 최종 가두기 및 등급 판정
        total = max(0.0, min(1.0, total))
        risk_lv = "심각" if total <= 0.25 else "주의" if total <= 0.63 else "안정"
        # ----------------------------------------------------------
        # 제미나이 리포트
        try:
            ai_rep = await get_ai_prediction_report(
                risk_lv, data['title'], refined_keywords,
                {"sent": final_sent_score, "ex": ex_score, "ma": ma_score})

        except Exception as e:
            # 429 에러(할당량 초과)가 발생한 경우
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                logger.warning("🚦 [Gemini] 할당량 초과! 60초 대기 후 다음 기사로 넘어갑니다.")
                await asyncio.sleep(60)  # 비동기 대기 (서버 안 멈춤)

                # 대기 후 이번 기사는 기본값으로 채우고 넘어가기
                ai_rep = {'prediction': '분석 대기 중 (할당량 초과)', 'reason': 'API 할당량 초과로 인한 리포트 생성 지연'}
            else:
                # 다른 에러일 경우 처리
                logger.error(f"❌ [Gemini 에러] {e}")
                ai_rep = {'prediction': '분석 실패', 'reason': 'AI 서비스 일시적 오류'}

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
            "prediction_result": ai_rep.get('prediction', '분석 결과 생성 중입니다.'),
            "prediction_reason": ai_rep.get('reason', '세부 분석 내용을 생성할 수 없습니다.'),
            "risk_level": risk_lv,
            "debug_text": balanced_text, # 모델이 분석하는 본문
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
            "published_date": data.get('published_date'),
            "country_name": refined_country
        }

        # [STEP 6] ES 저장 및 상태 업데이트
        try:
            # 1. 분석 완료 데이터 저장 (ES_3)
            es.index(index="news_labeling", id=_id, body=labelled_doc)
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

            #  DB 저장을 위해 main.py로 보낼 배달 바구니에 담기
            processed_results.append({
                'level': risk_lv,
                'pred': labelled_doc.get('prediction', '분석 완료'),  # Gemini 연동 전이면 기본값
                'reason': labelled_doc.get('prediction_reason', '분석 근거 수집됨'),
                'doc_id': _id,  # <--- 바로 이 녀석이 MariaDB의 document_no
                'countries': [refined_country]
            })
        except Exception as e:
            logger.error(f"❌ [저장 에러] ID {_id} 처리 중 오류 발생: {e}")

    # 모든 루프 종료 후 요약 로그
    logger.info(f"✅ 이번 배치 분석 완료 (총 {len(docs)}건 처리)")
    logger.info("--------------------------------------------------")

    return processed_results # 루프가 다 끝나면 결과 리스트 반환


# 오늘의 뉴스
def get_latest_signals():
    """
    ES3 인덱스에서 라벨링이 완료된 모든 데이터를 가져와서 main으로 보내주는 함수
    """
    try:
        query = {
            "sort": [
                {"analyzed_at": {"order": "desc"}}
            ],
            "size": 10
        }
        res = es.search(index="news_labeling", body=query)
        hits = res['hits']['hits']
        print("ES 결과 개수 =", len(res['hits']['hits']))
        print(res['hits']['hits'][0])

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


# ml.py 안에 추가할 함수 예시
async def update_es_status(doc_id, status: bool):
    try:
        es.update(index="news_origin", id=doc_id, body={"doc": {"is_processed": status}})
    except Exception as e:
        logger.error(f"ES 상태 업데이트 실패 ({doc_id}): {e}")


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
        logger.info("🛑 사용자에 의해 분석 엔진이 중단")