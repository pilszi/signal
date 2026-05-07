import re
import html
from collections import Counter
import hashlib
from config import Config
from konlpy.tag import Okt

okt = Okt()

def generate_article_id(url):
    return hashlib.sha256(url.strip().encode('utf-8')).hexdigest()

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


# 3. 국가 매칭 보조 로직 (G20_COUNTRY_MAP 활용)
def find_target_country(title, content):
    """
    국가 추출 로직 (화이트리스트 + 도시 매칭)
    '정부', '시장' 등 일반 명사 오인을 방지하고, 매핑되지 않으면 'Global'을 반환
    """
    # 분석할 전체 텍스트 (제목 + 본문)
    combined_text = f"{title} {content}"

    # 1순위: 국가명 매칭 (Config에 정의된 G20_COUNTRY_MAP 사용)
    for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
        if kr_name in combined_text:
            return en_name

    # 2순위: 도시명 매칭 (Config에 정의된 CITY_TO_COUNTRY_MAP 사용)
    for city_name, en_name in Config.CITY_TO_COUNTRY_MAP.items():
        if city_name in combined_text:
            return en_name

    # 3순위: 매핑되지 않은 경우 'Global'로 처리
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



# 기사 라벨링 2 -[속성 라벨링]: utils.py / extract_noun_number_pairs, filter_keywords
# 그 다음 ml.py로 넘어감
# 키워드 추출 함수 - 위에 normalize와 clean_html을 거치면 본문에서 중요한 키워드만 추출
def extract_keywords(title, content):
    """
    기사 제목과 본문에서 정제된 핵심 키워드 최대 10개 추출
    """
    try:
        from config import Config
        filters = Config.TOTAL_FILTERS
        okt = Okt()

        # [Step 1] 개체명 추출
        target_entities = [
            "미국","중국","일본","베트남","이란","러시아","우크라이나","EU","중동",
            "트럼프","바이든","푸틴","시진핑","파월",
            "삼성전자","SK하이닉스","TSMC","현대차","엔비디아","애플","ASML",
            "한은","기재부","금융위","산업부","국토부","백악관","IMF","FED","연준",
            "호르무즈","홍해","공급망","물류대란","수출규제","관세","보조금","반도체","이차전지",
            "환율","금리","유가","물가","인플레이션","추경","국가부채","적자","흑자","금리인상",
            "파업","셧다운","디폴트","스태그플레이션","희토류","이중용도"
        ]

        entities = [ent for ent in target_entities if ent in title or ent in content[:500]]

        # [Step 2] 수치 데이터
        value_pattern = r'[\$|₩]?\d+[\d,.]*\s?[%|배|조|억|만|포인트|p|달러|원|불]+'
        found_values = re.findall(value_pattern, title + " " + content[:500])

        # [Step 3] 텍스트 정제
        particles = re.compile(r'(으로|보다|에서|에대한|한다|했다|하며|하여|까지|부터|조차|이나|은|는|이|가|을|를|의|에|도)$')

        def clean_word(word):
            w = re.sub(r'[^가-힣a-zA-Z0-9%]', '', word)
            return particles.sub('', w)

        # [Step 4] 명사 빈도 기반
        clean_text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', title + " " + content[:1000])
        raw_words = clean_text.split()

        processed_words = []
        for w in raw_words:
            cleaned = clean_word(w)
            # filter_keywords 함수의 if 조건
            if len(cleaned) > 1 and cleaned not in filters and not cleaned.isdigit():
                processed_words.append(cleaned)

        top_nouns = [k for k, v in Counter(processed_words).most_common(15)]

        # [Step 5] 위에 있는 extract_noun_number_pairs 함수 -> 명사 + 수치 강제 추출
        pair_keywords = extract_noun_number_pairs(title + " " + content[:500])

        # [Step 6] 최종 병합
        all_candidates = (
            entities +
            pair_keywords +
            found_values +
            top_nouns
        )

        final_keywords = []

        for k in all_candidates:
            k_str = str(k).strip()

            if k_str not in final_keywords:
                if 2 <= len(k_str) <= 12:
                    if not k_str.isdigit():
                        final_keywords.append(k_str)

            if len(final_keywords) >= 10:
                break

        # [Step 7] fallback
        if not final_keywords:
            title_words = [clean_word(w) for w in title.split() if len(clean_word(w)) > 1]
            # 앞선 단계에서 키워드 안 뽑혔을 때도 filter_keywords 함수의 if 조건 활용
            final_keywords = [w for w in title_words if w not in filters][:5]

        return final_keywords

    except Exception as e:
        print(f"⚠️ 키워드 추출 오류: {e}")
        return []


# 네이버와 연합뉴스 중복 기사 제거
def generate_article_id(title):
    # 제목에서 공백과 특수문자를 제거해서 '순수 텍스트'만 추출
    clean_title = re.sub(r'\s+', '', title)
    return hashlib.sha256(clean_title.encode('utf-8')).hexdigest()