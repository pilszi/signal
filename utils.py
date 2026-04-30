import re
import html
from config import Config  # 클래스를 통째로 가져옵니다.
from collections import Counter
import hashlib

def generate_article_id(url):
    return hashlib.sha256(url.strip().encode('utf-8')).hexdigest()

# 1. 텍스트 정규화 (가장 많이 쓰임)
def normalize(text):
    if not text: return ""
    # 소문자 변환 및 모든 공백 제거
    return text.lower().replace(" ", "")


# 2. 뉴스 제목/본문 정제 도구
def clean_html(text):
    if not text: return ""
    # <b> 태그 등 제거 및 HTML 엔티티 복원
    clean_text = text.replace('<b>', '').replace('</b>', '')
    return html.unescape(clean_text)


# 3. 국가 매칭 보조 로직 (G20_COUNTRY_MAP 활용)
def find_target_country(title, content):
    # 1순위: 국가명 매칭
    for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
        if kr_name in title or kr_name in content:
            return en_name

    # 2순위: 도시명 매칭
    for city_name, en_name in Config.CITY_TO_COUNTRY_MAP.items():
        if city_name in title or city_name in content:
            return en_name

    return "Others"


# 4. 키워드 필터링 도구 (STOPWORDS/NOISE_WORDS 활용)
def filter_keywords(keywords, filter_set):
    return [
        k for k in keywords
        if k not in filter_set and not k.isdigit() and len(str(k)) > 1
    ]

# 5. 키워드 추출 함수
# 키워드 명사랑 수치만 정확하게 추출하는 함수
def extract_noun_number_pairs(text):
    """
    '명사 + 숫자%' 형태를 강제로 추출
    예: 수출 17.3%, 생산 10.5%
    """
    pattern = r'([가-힣A-Za-z]{2,10})[^0-9]{0,5}(\d+(?:\.\d+)?%)'
    matches = re.findall(pattern, text)

    results = []
    for m in matches:
        noun = m[0]
        number = m[1]
        results.append(f"{noun} {number}")

    return results


# 키워드 추출 함수
def extract_keywords(title, content):
    """
    기사 제목과 본문에서 정제된 핵심 키워드 최대 10개 추출 (최종 개선 버전)
    """
    try:
        from config import Config
        filters = Config.TOTAL_FILTERS

        # --- [Step 1] 개체명 추출 ---
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

        # --- [Step 2] 수치 데이터 ---
        value_pattern = r'[\$|₩]?\d+[\d,.]*\s?[%|배|조|억|만|포인트|p|달러|원|불]+'
        found_values = re.findall(value_pattern, title + " " + content[:500])

        # --- [Step 3] 텍스트 정제 ---
        particles = re.compile(r'(으로|보다|에서|에대한|한다|했다|하며|하여|까지|부터|조차|이나|은|는|이|가|을|를|의|에|도)$')

        def clean_word(word):
            w = re.sub(r'[^가-힣a-zA-Z0-9%]', '', word)
            return particles.sub('', w)

        # --- [Step 4] 명사 빈도 기반 ---
        clean_text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', title + " " + content[:1000])
        raw_words = clean_text.split()

        processed_words = []
        for w in raw_words:
            cleaned = clean_word(w)
            if len(cleaned) > 1 and cleaned not in filters and not cleaned.isdigit():
                processed_words.append(cleaned)

        top_nouns = [k for k, v in Counter(processed_words).most_common(15)]

        # 🔥 --- [Step 5] 핵심 추가: 명사 + 수치 강제 추출 ---
        pair_keywords = extract_noun_number_pairs(title + " " + content[:500])

        # 🔥 --- [Step 6] 최종 병합 ---
        all_candidates = (
            entities +
            pair_keywords +     # 🔥 핵심 추가
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

        # --- [Step 7] fallback ---
        if not final_keywords:
            title_words = [clean_word(w) for w in title.split() if len(clean_word(w)) > 1]
            final_keywords = [w for w in title_words if w not in filters][:5]

        return final_keywords

    except Exception as e:
        print(f"⚠️ 키워드 추출 오류: {e}")
        return []