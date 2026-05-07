import re
import html
from collections import Counter
import hashlib
from config import Config
from konlpy.tag import Okt
from config import Config



# 기사 고유 ID 생성 (제목 기반 중복 제거)
def generate_article_id(title):
    clean_title = re.sub(r'\s+', '', title)
    return hashlib.sha256(clean_title.encode('utf-8')).hexdigest()

# 기사 라벨링 1 -[라벨링 전처리]: 기사를 수집한 후 먼저 normalize, clean_html을 거침
# 1. 텍스트 정규화 (가장 많이 쓰임)
def normalize(text):
    if not text: return ""
    # 소문자 변환 및 모든 공백 제거
    return text.lower().replace(" ", "")

# 2. 태그 제거
def clean_html(text):
    if not text: return ""
    # &quot; -> " 형태로 변환 후 태그 제거
    decoded_text = html.unescape(text)
    clean = re.compile('<.*?>')
    return re.sub(clean, '', decoded_text).strip()

def is_hanja(char):
    """한자인지 확인하는 보조 함수"""
    return '\u4e00' <= char <= '\u9fff'

# 3. 국가 매칭 (G20_COUNTRY_MAP 활용)
def find_target_country(title, content):
    """
    국가 추출 로직 최종 고도화 버전
    1. 한국 우선 키워드 (강제 고정)
    2. 제목(Title) 매칭 (즉시 반환)
    3. 본문 가중치 매칭 (인트로 500자 중심)
    4. 도시명 매칭 (Fallback)
    """
    # [Step 1] 한국 우선 키워드 체크
    # 제목에 정부, 코스피 등이 있으면 본문 내용과 상관없이 Korea로 분류
    for k_word in Config.KOREA_PRIORITY_KEYWORDS:
        if k_word in title:
            return "Korea"

    # [Step 2] 전처리: 노이즈 제거 (언론사, 기관명 등)
    noise_pattern = "|".join(Config.COUNTRY_NOISE_INSTITUTIONS)
    clean_title = re.sub(noise_pattern, "", title)
    clean_content = re.sub(noise_pattern, "", content)

    # [Step 3] 제목(Title) 매칭 (핵심 키워드 우선)
    for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
        # 영문 국가명인 경우만 유효한 매칭으로 인정
        if not re.match(r'^[a-zA-Z\s]+$', str(en_name)):
            continue

        # 한자(美, 中 등)는 제목에 있으면 즉시 인정
        if is_hanja(kr_name) and kr_name in clean_title:
            return en_name

        # 한글 한 글자(한, 미 등)는 특수 패턴일 때만 인정
        if len(kr_name) == 1 and not is_hanja(kr_name):
            if re.search(rf'{kr_name}[\s\-·\.]', clean_title) or clean_title.startswith(kr_name):
                return en_name

        # 일반적인 두 글자 이상 단어는 제목에 있으면 즉시 인정
        elif kr_name in clean_title:
            return en_name

    # [Step 4] 본문 가중치 기반 최다 언급 국가 산출
    # 인트로(상단 500자)와 나머지 본문 분리
    intro = clean_content[:500]
    body = clean_content[500:]

    country_scores = {}

    for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
        if not re.match(r'^[a-zA-Z\s]+$', str(en_name)):
            continue

        # 한자는 1글자 이상, 한글은 2글자 이상일 때만 카운트
        if is_hanja(kr_name) or len(kr_name) >= 2:
            # 인트로 가중치 적용 (예: 3.0)
            intro_count = intro.count(kr_name)
            # 일반 본문 가중치 적용 (예: 1.0)
            body_count = body.count(kr_name)

            total_weight_score = (intro_count * 3.0) + (body_count * 1.0)

            if total_weight_score > 0:
                country_scores[en_name] = country_scores.get(en_name, 0) + total_weight_score

    # 가중치 합계가 가장 높은 국가 반환
    if country_scores:
        return max(country_scores, key=country_scores.get)

    # [Step 5] 도시명 매칭 (제목 기반 Fallback)
    for city_name, en_name in Config.CITY_TO_COUNTRY_MAP.items():
        if city_name in title:
            return en_name

    # [Step 6] 끝까지 없으면 Global 반환
    return "Global"

# 히트맵 데이터 가공
# EU, 동남아시아, 중동처럼 여러 나라들 포함됐을 때 히트맵으로 매핑
def prepare_heatmap_data(articles):
    """
    공식 적용: R = Sigma(abs(AI 감성 점수) * 언급 빈도)
    결과에 국가별 점수와 리스크 레벨(색상)을 함께 반환
    """
    country_risk_map = {}

    # 설정값 로드
    region_map = Config.REGION_TO_COUNTRIES
    composite_map = Config.COMPOSITE_COUNTRY_MAP

    for article in articles:
        # ES search 결과(hits)일 경우와 일반 dict일 경우를 모두 고려
        source = article.get('_source', article)

        country = source.get('country_name')
        title = source.get('title', "")
        content = source.get('content', "")
        # AI 분석 점수 가져오기 (없으면 0.0)
        sentiment = abs(source.get('sentiment_score', 0.0))

        # [Step 1] 'Global' 이거나 데이터가 없는 경우 시각화에서 제외
        if not country or country == "Global":
            continue

        # 2. 언급 빈도(Frequency) 계산
        # 본문에 국가명이 몇 번 등장하는지 측정 (최소 1회 보정)
        freq = content.count(country) if country != "Global" else 1
        if freq == 0: freq = 1

        # 3. 공식 적용: 이 기사가 기여하는 리스크 점수
        article_risk_score = sentiment * freq

        # 점수 누적 보조 함수
        def update_score(target_country):
            if target_country not in country_risk_map:
                country_risk_map[target_country] = 0.0
            country_risk_map[target_country] += article_risk_score

        # [Step A] 제목 내 복합 국가어(한미, 미중 등)가 있는지 먼저 확인
        found_composite = False
        for comp_word, countries in composite_map.items():
            if comp_word in title:
                for c in countries:
                    update_score(c)
                found_composite = True
                break  # 복합어 하나라도 찾으면 중단

        if found_composite:
            continue

        # [Step B] 지역명(Middle East, EU 등) 처리 및 단일 국가 처리
        if country in region_map:
            for sub_country in region_map[country]:
                update_score(sub_country)
        else:
            update_score(country)

    # 5. 최종 리스크 레벨(색상) 판정 로직
    final_heatmap_data = {}
    for country, total_score in country_risk_map.items():
        # 소수점 둘째자리 반올림
        total_score = round(total_score, 2)

        # Config에 설정된 임계값 기준 적용
        if total_score > 10.0:
            level = "Serious"  # 빨강
        elif total_score >= 3.0:
            level = "Caution"  # 노랑
        else:
            level = "Stable"  # 초록

        final_heatmap_data[country] = {
            "score": total_score,
            "level": level
        }

    return final_heatmap_data

# 상위 3개 국가 뽑는 함수 (signal station)
def get_top_risk_countries(heatmap_data):
    """
    합산된 리스크 점수를 바탕으로 상위 3개 국가를 추출
    """
    # 점수(value)를 기준으로 내림차순 정렬
    sorted_countries = sorted(heatmap_data.items(), key=lambda item: item[1]['score'], reverse=True)

    # 상위 3개만 슬라이싱
    top_3 = sorted_countries[:3]

    # Signal Station UI에 맞게 리스트 반환
    return [
        {
            "country": country,
            "score": info['score'],
            "level": info['level']
        }
        for country, info in top_3
    ]


def extract_country(title, content):
    """
    제목과 본문을 분석하여 가장 연관성 높은 국가 1개를 추출합니다.
    1. 한국 관련 키워드 가중치 부여 (Korea 우선)
    2. 제목 가중치 부여
    3. G20 국가 맵 매핑
    """
    try:
        text = (title + " ") + content
        # 모든 국가를 0점으로 초기화
        # Config.COUNTRIES가 리스트 형태여야 합니다.
        unique_countries = set(Config.G20_COUNTRY_MAP.values())
        country_scores = {country: 0 for country in unique_countries}


        # [1] 한국 보너스 강화: '한국', 'K-' 등이 보이면 무조건 점수 대폭 추가
        # 리더님, 여기에 '한국', '대한민국', 'K-'를 꼭 넣어주세요!

        for kw in Config.KOREA_PRIORITY_KEYWORDS:
            if kw in text:
                country_scores["Korea"] += 15  # 보너스를 15점으로 상향

        # [2] 한국 주요 기업명 보너스 (매우 중요!)
        # 본문에 한국 기업이 나오면 그건 한국 산업 뉴스
        for firm in Config.KOREA_FIRM_KEYWORDS:
            if firm in text:
                country_scores["Korea"] += 10  # 기업당 10점씩 추가

        # [3] 전체 텍스트에서 국가명 매핑 점수 계산
        for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
            if kr_name in text:
                # 제목에 있으면 5점, 본문에만 있으면 1점
                score = 5 if kr_name in title else 1
                country_scores[en_name] = country_scores.get(en_name, 0) + score

        # [3] 최고점 국가 찾기
        best_country = max(country_scores, key=country_scores.get)

        # 만약 최고점이 0점이라면(찾은 국가가 없다면) Global 반환
        if country_scores[best_country] == 0:
            return "Global"

        return best_country

    except Exception as e:
        print(f"⚠️ 국가 추출 오류: {e}")
        return "Global"

# 4. 키워드 필터링 도구 (STOPWORDS/NOISE_WORDS 활용)
# 지저분한 단어들 제외하고 핵심 단어들만 뽑기
def filter_keywords(keywords, filter_set):
    """
    불용어 제거 및 가독성 필터링
    """
    # 한글 외 영문(quot 등) 제거 및 불용어 제거, 2글자 이상만 허용
    filtered = [
        kw for kw in keywords
        if kw not in Config.STOPWORDS and len(kw) > 1 and not re.match(r'^[a-zA-Z]+$', kw)
    ]
    # 중복 제거 및 상위 10개 반환
    return list(dict.fromkeys(filtered))[:10]


# 5. 키워드 추출 함수
# 키워드 명사랑 수치만 정확하게 추출하는 함수
def extract_noun_number_pairs(text):
    """
    정규표현식 강화: [명사 + 숫자 + 단위] 추출
    예: "인수 금액 3조", "공급 계약 20억"
    """
    # 패턴 설명: (2글자 이상 명사) + (공백0개 이상) + (숫자) + (단위)
    pattern = r'([가-힣]{2,})\s*([\d\.,]+)\s*(조|억|만|%|달러|원|포인트|bp)'
    matches = re.findall(pattern, text)

    results = []
    for m in matches:
        noun = m[0]
        number = m[1]
        results.append(f"{noun} {number}")

    # "명사 숫자단위" 형태로 결합 (예: "인수 금액 3조")
    return [f"{m[0]} {m[1]}{m[2]}" for m in matches]


# 6. 깨진 키워드 복구 함수
def fix_broken_keywords(keywords):
    """
    '러시', '아산' 처럼 형태소 분석기에서 쪼개진 단어를 정상화합니다.
    """
    fixed = [Config.correction_map.get(kw, kw) for kw in keywords]
    return list(dict.fromkeys(fixed)) # 중복 제거



# Okt 객체는 함수 밖에서 한 번만 생성하여 메모리 효율을 높입니다.
okt = Okt()

# 기사 라벨링 2 -[속성 라벨링]: utils.py / extract_noun_number_pairs, filter_keywords
# 그 다음 ml.py로 넘어감
# 7. 키워드 추출 함수 - 위에 normalize와 clean_html을 거치면 본문에서 중요한 키워드만 추출
def extract_keywords(title, content, top_n=10):
    """
    KoNLPy와 가중치 로직을 결합한 최종 키워드 추출 함수
    1. 전처리 (기자명, 이메일 제거)
    2. 제목 가중치 부여 (제목 2회 반복)
    3. 명사 추출 (조사 제거: '제조업에' -> '제조업')
    4. 필터링 (불용어, 구어체, 1글자 제거)
    5. 단어 교정 및 빈도수 추출
    """
    try:
        # [1] 데이터 전처리: 사진 설명, 기자명, 이메일 등 뉴스 특유의 노이즈 제거
        # 본문 앞부분(800자)만 사용하여 분석 속도와 정확도 향상
        clean_content = re.sub(r'[가-힣]{2,4}\s?기자.*', '', content[:800])
        clean_content = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', clean_content)
        clean_content = re.sub(r'홈페이지\s?=\s?\S+', '', clean_content)

        # [2] 제목 가중치 부여: 제목에 나온 단어는 본문에 나온 것보다 중요하므로 제목을 2번 합칩니다.
        # 이렇게 하면 Counter가 제목 단어를 훨씬 높게 평가합니다.
        combined_text = (title + " ") * 2 + clean_content

        # [3] 명사 추출: Okt를 사용하여 '제조업에', '충격에'에서 조사('에')를 자동으로 떼어냅니다.
        raw_nouns = okt.nouns(combined_text)

        # [4] 필터링: 한 글자 단어 제외, Config에 설정한 불용어/구어체/노이즈 제거
        refined_nouns = [
            word for word in raw_nouns
            if len(word) > 1 and word not in Config.TOTAL_FILTERS and not any(char.isdigit() for char in word)
        ]

        # [5] 빈도수 계산
        counts = Counter(refined_nouns)

        # [6] 상위 키워드 추출 및 단어 교정 (러시 -> 러시아 등)
        final_keywords = []
        # 가장 많이 나온 순서대로 top_n 개를 가져옴
        for word, _ in counts.most_common(top_n * 2):  # 교정 후 중복 대비 2배수 추출
            corrected_word = Config.correction_map.get(word, word)

            # 교정 후 이미 목록에 있는 단어면 중복 추가 방지
            if corrected_word not in final_keywords:
                final_keywords.append(corrected_word)

            # 최종 10개가 채워지면 멈춤
            if len(final_keywords) >= top_n:
                break

        # [7] Fallback: 만약 명사가 너무 적게 추출되었다면 제목에서 직접 단어 추출
        if len(final_keywords) < 3:
            final_keywords = [w for w in title.split() if len(w) >= 2 and w not in Config.TOTAL_FILTERS][:5]

        return final_keywords

    except Exception as e:
        print(f"⚠️ 키워드 추출 오류 발생: {e}")
        return []