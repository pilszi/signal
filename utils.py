import re
import html
from collections import Counter
import hashlib
import sqlalchemy
import logger
from sqlalchemy.orm import Session
from config import Config
from konlpy.tag import Okt

# okt 전역 객체 선언
okt = Okt()

# 기사 제목을 공백 제거 후 SHA-256으로 해싱해서 고유 ID 생성
def generate_article_id(title):
    clean_title = re.sub(r'\s+', '', title)
    return hashlib.sha256(clean_title.encode('utf-8')).hexdigest()

# 기사 라벨링 1 -[라벨링 전처리]: 기사를 수집한 후 먼저 normalize, clean_html을 거침
# 1. 텍스트 정규화
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

# 문자가 유니코드 한자 범위에 해당하는지 확인
# "美", "中" 같은 한자 국가명을 일반 한글 국가명보다 높은 가중치로 처리 -> find_target_country
def is_hanja(char):
    """문자열의 첫 글자가 한자 범위인지 확인하는 함수"""
    if not char:
        return False
    # Unicode 한자 범위: 4E00(一) ~ 9FFF(鿿)
    return 0x4E00 <= ord(char[0]) <= 0x9FFF


# 기사 라벨링 2 -[속성 라벨링]: 그 다음 ml.py로 넘어감
# 3. 키워드 추출 함수 - 위에 normalize와 clean_html을 거치면 본문에서 중요한 키워드만 추출
def extract_keywords(title, content, top_n=10):
    """
    KoNLPy와 가중치 로직을 결합한 최종 키워드 추출 함수
    1. 전처리 (기자명, 이메일 제거)
    2. 제목 가중치 부여 (제목 2회 반복)
    3. 명사 추출 (조사 제거: '제조업에' -> '제조업')
    4. 필터링 (불용어, 구어체, 1글자 제거)
    5. 단어 교정 및 빈도수 추출
    KoNLPy okt.nouns()로 명사를 추출하고, 제목을 2회 반복해서 제목 단어에 가중치를 주고, 불용어 필터링과 교정까지 수행
    """
    try:
        # [1] 데이터 전처리: 사진 설명, 기자명, 이메일 등 뉴스 특유의 노이즈 제거
        clean_content = re.sub(r'[가-힣]{2,4}\s?기자.*', '', content[:800])
        clean_content = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', clean_content)
        clean_content = re.sub(r'홈페이지\s?=\s?\S+', '', clean_content)

        # [2] 제목 가중치 부여: 제목에 나온 단어는 본문에 나온 것보다 중요하므로 제목을 2번 합칩니다.
        combined_text = (title + " ") * 2 + clean_content[:800]

        # [3] 명사(nouns) 추출
        raw_chunks = okt.nouns(combined_text)

        # [4] 필터링: 한 글자 단어 제외, Config에 설정한 불용어/구어체/노이즈 제거
        refined_keywords = [
            word for word in raw_chunks
            if len(word) > 1
               and word not in Config.TOTAL_FILTERS
               and not any(char.isdigit() for char in word)
        ]

        # [5] 빈도수 계산
        counts = Counter(refined_keywords)

        # [6] 상위 키워드 추출 및 찢어진 단어 교정 (러시 -> 러시아 등)
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
            fallback_phrases = okt.phrases(title)
            final_keywords = [w for w in fallback_phrases if len(w) >= 2 and w not in Config.TOTAL_FILTERS][:5]

        return final_keywords

    except Exception as e:
        print(f"⚠️ 키워드 추출 오류 발생: {e}")
        return []


# 3. 국가 매칭 (G20_COUNTRY_MAP 활용)
def find_target_country(title, content):
    """
    국가 추출 로직 최종 고도화 버전
    엔티티 매핑 → 지정학 지역 → 제목 점수화 → 한국 우선 -> 각 나라 도시 매핑 -> 본문 점수화 순
    ml.py의 run_analysis에서 refined_keywords와 refined_country를 만들 때 호출됨
    """
    # ---------------------------------------------------
    # [Step 1] 전처리
    # ---------------------------------------------------
    noise_pattern = "|".join(Config.COUNTRY_NOISE_INSTITUTIONS)
    clean_title = re.sub(noise_pattern, "", title)
    clean_content = re.sub(noise_pattern, "", content)

    # ---------------------------------------------------
    # [Step 2] 주요 엔티티 체크
    # ---------------------------------------------------
    for entity, country in Config.ENTITY_TO_COUNTRY_MAP.items():
        if entity in clean_title:
            return country

    # ---------------------------------------------------
    # [Step 3] 지정학 지역 체크
    # ---------------------------------------------------
    for region, country in Config.REGION_TO_COUNTRY_MAP.items():
        if region in clean_title:
            return country

    # ---------------------------------------------------
    # [Step 4] 제목 국가 점수화
    # ---------------------------------------------------
    title_scores = {}
    title_countries = set()
    for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
        score = 0
        # 한자 국가명
        if is_hanja(kr_name) and kr_name in clean_title:
            score += 5

        # 한 글자 국가명
        elif len(kr_name) == 1:
            if re.search(rf'{kr_name}[\s\-·\.]', clean_title):
                score += 4

        # 일반 국가명
        elif kr_name in clean_title:
            score += 3

        if score > 0:
            title_scores[en_name] = (
                title_scores.get(en_name, 0) + score
            )
            title_countries.add(en_name)

    # ---------------------------------------------------
    # [Step 4-1] 한국 영향 키워드 가산
    # ---------------------------------------------------
    korea_score = 0

    for impact_word in Config.KOREA_PRIORITY_KEYWORDS:
        if impact_word in clean_title:
            korea_score += 2

    if korea_score > 0:
        title_scores["Korea"] = (
            title_scores.get("Korea", 0) + korea_score
        )

    # ---------------------------------------------------
    # [Step 5] 도시명 보정
    # ---------------------------------------------------
    for city_name, en_name in Config.CITY_TO_COUNTRY_MAP.items():
        if city_name in clean_title:
            title_scores[en_name] = (
                title_scores.get(en_name, 0) + 2
            )

    # ---------------------------------------------------
    # [Step 6] 본문 스코어링
    # ---------------------------------------------------
    intro = clean_content[:500]
    body = clean_content[500:]

    country_scores = dict(title_scores)

    LOW_PRIORITY_COUNTRIES = {
        "North Korea",
        "China",
        "USA",
        "Russia"
    }

    for kr_name, en_name in Config.G20_COUNTRY_MAP.items():
        # 한자이거나 두 글자 이상만 허용
        if is_hanja(kr_name) or len(kr_name) >= 2:
            score = (
                    (intro.count(kr_name) * 3.0) +
                    (body.count(kr_name) * 1.0)
            )
            # 제목에 없는 강대국은 본문 점수 약화
            if (
                    en_name in LOW_PRIORITY_COUNTRIES
                    and en_name not in title_countries
            ):
                score *= 0.55

            if score > 0:
                country_scores[en_name] = (
                        country_scores.get(en_name, 0) + score
                )

    # ---------------------------------------------------
    # [Step 7] 최종 국가 판정
    # ---------------------------------------------------
    if country_scores:
        sorted_scores = sorted(
            country_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )
        top_country, top_score = sorted_scores[0]

        second_score = (
            sorted_scores[1][1]
            if len(sorted_scores) > 1 else 0
        )
        # 점수 차이 적으면 글로벌 기사 처리
        if top_score - second_score < 2:
            return "Global"

        return top_country

    # ---------------------------------------------------
    # [Step 8] 실패 시 Global
    # ---------------------------------------------------
    return "Global"


# 히트맵 데이터 가공
# EU, 동남아시아, 중동처럼 여러 나라들 포함됐을 때 히트맵으로 매핑
def prepare_heatmap_data(articles):
    """
    공식 적용: R = Sigma(abs(AI 감성 점수) * 언급 빈도)으로 리스크 점수를 누적
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
# prepare_heatmap_data의 출력을 받아서 점수 기준 상위 3개 국가만 추려서 반환
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


# 스케줄러 내부에 들어갈 저장 로직
def save_analysis_result(session: Session, analysis_data: dict):
    """
        분석 결과를 DB에 저장하는 함수
        :param cursor: DB 커서 객체
        :param connection: DB 연결 객체 (commit용)
        :param analysis_data: 분석 결과 딕셔너리
        ml.py에서 분석 완료된 결과를 MariaDB의 signal_message와 signal_country 테이블에 저장
        국가가 지역명이면 REGION_TO_COUNTRIES로 분해해서 개별 국가로 저장하고 생성된 signal_no를 반환
    """
    # 1. 시그널 본체 저장 (signal_message) (url 필드 제외)
    sql_msg = sqlalchemy.text("""
            INSERT INTO signal_message (risk_level, document_no, prediction, prediction_reason, url)
            VALUES (:level, :doc_no, :prediction, :reason, :url)
        """)

    result = session.execute(sql_msg, {
        'level': analysis_data.get('level'),
        'doc_no': analysis_data.get('doc_id') or analysis_data.get('id') or 'unknown_id',
        'prediction': analysis_data.get('prediction'),
        'reason': analysis_data.get('reason'),
        'url': analysis_data.get('url')  # ml.py에서 넘어온 url 저장
    })

    # 방금 생성된 PK(signal_no) 추출
    signal_no = result.lastrowid

    # 2. 국가 매핑 (기존과 동일)
    extracted_countries = analysis_data.get('countries', [])
    # 문자열 하나로 들어온 경우 보정
    if isinstance(extracted_countries, str):
        extracted_countries = [extracted_countries]

    for eng_name in extracted_countries:

        # 리스트 중첩 방어
        if isinstance(eng_name, list):
            for sub_name in eng_name:

                target_list = Config.REGION_TO_COUNTRIES.get(
                    sub_name,
                    [sub_name]
                )

                for country_name in target_list:

                    sql_c = sqlalchemy.text("""
                        SELECT country_no
                        FROM country
                        WHERE country_en_name = :c_name
                    """)

                    row = session.execute(
                        sql_c,
                        {"c_name": country_name}
                    ).fetchone()

                    if row:
                        country_no = row[0]

                        sql_map = sqlalchemy.text("""
                            INSERT INTO signal_country
                            (signal_no, country_no)
                            VALUES (:s_no, :c_no)
                        """)

                        session.execute(
                            sql_map,
                            {
                                "s_no": signal_no,
                                "c_no": country_no
                            }
                        )

        else:
            target_list = Config.REGION_TO_COUNTRIES.get(
                eng_name,
                [eng_name]
            )

            for country_name in target_list:

                sql_c = sqlalchemy.text("""
                    SELECT country_no
                    FROM country
                    WHERE country_en_name = :c_name
                """)

                row = session.execute(
                    sql_c,
                    {"c_name": country_name}
                ).fetchone()

                if row:
                    country_no = row[0]

                    sql_map = sqlalchemy.text("""
                        INSERT INTO signal_country
                        (signal_no, country_no)
                        VALUES (:s_no, :c_no)
                    """)

                    session.execute(
                        sql_map,
                        {
                            "s_no": signal_no,
                            "c_no": country_no
                        }
                    )
    # 3. 생성된 번호를 밖으로 던져줍니다!
    return signal_no


# 스포츠, 연예, 추천, 스킵 키워드가 제목에 있거나 본문이 100자 미만이면 True를 반환
# main.py
def is_noise_article(title, content, url, check_length=True):
    return (
        any(kw.lower() in title.lower() for kw in Config.SPORTS_KEYWORDS)
        or any(kw.lower() in title.lower() for kw in Config.ENTERTAINMENT_KEYWORDS)
        or any(kw.lower() in title.lower() for kw in Config.RECOMMENDATION_KEYWORDS)
        or any(kw.lower() in title.lower() for kw in Config.skip_keywords)
        or (check_length and len(content) < 100)
    )

def get_real_url(url):
    if not url: return ""

    # https:// 또는 http:// 가 중복되는지 체크
    for protocol in ["https://", "http://"]:
        if url.count(protocol) > 1:
            return protocol + url.split(protocol)[-1]

    return url