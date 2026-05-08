import os
from google import genai
from pathlib import Path
from dotenv import load_dotenv


# 현재 config.py 파일의 위치를 기준으로 프로젝트 루트 폴더를 찾기
base_dir = Path(__file__).resolve().parent
# 그 폴더 안에 있는 .env 파일을 가리킴
env_path = base_dir / '.env'
# [확인용 출력] 터미널에서 이 경로가 맞는지 눈으로 꼭 확인!
print(f"🔍 .env 경로 확인: {env_path}")

# 파일을 로드
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print("✅ .env 파일 로드 성공!")
else:
    print("❌ .env 파일을 찾을 수 없습니다. 경로를 다시 확인해주세요.")



def get_env(key: str, default=None, required=True):
    """환경변수 안전하게 가져오기"""
    value = os.getenv(key, default)
    if required and value is None:
        raise ValueError(f"❌ 환경변수 누락: {key}")
    return value



class Config:
    # --- 1. 시스템 설정 ---
    # KoNLPy/Java 관련 설정
    JAVA_HOME = get_env("JAVA_HOME", required=False)

    # --- 2. 네이버 API 설정 ---
    NAVER_CLIENT_ID = get_env("NAVER_CLIENT_ID")
    NAVER_CLIENT_SECRET = get_env("NAVER_CLIENT_SECRET")

    # --- 3. Gemini AI API 설정 (키 로테이션용 리스트) ---
    GEMINI_API_KEYS = [
        get_env("GEMINI_API_KEY_1"),
        get_env("GEMINI_API_KEY_2", required=False),
        get_env("GEMINI_API_KEY_3", required=False),
        get_env("GEMINI_API_KEY_4", required=False),
        get_env("GEMINI_API_KEY_5", required=False),
        get_env("GEMINI_API_KEY_6", required=False),
        get_env("GEMINI_API_KEY_7", required=False),
        get_env("GEMINI_API_KEY_8", required=False),
        get_env("GEMINI_API_KEY_9", required=False),
    ]
    GEMINI_API_KEYS = [k for k in GEMINI_API_KEYS if k]
    GEMINI_MODEL_ID = get_env("GEMINI_MODEL_ID", default="gemini-flash-latest", required=False)
    _current_key_index = 0 # 클래스 내부에서 인덱스를 관리합니다. (앞에 언더바 _를 붙여 내부용임을 표시)

    @classmethod
    def get_next_client(cls):
        if not cls.GEMINI_API_KEYS:
            raise ValueError("❌ GEMINI API 키 없음")

        api_key = cls.GEMINI_API_KEYS[cls._current_key_index]
        cls._current_key_index = (cls._current_key_index + 1) % len(cls.GEMINI_API_KEYS)

        return genai.Client(api_key=api_key)

    # --- 4. brevo api 설정 (이메일 발송 보안 설정) ---
    BREVO_API_KEY = get_env("BREVO_API_KEY")
    SENDER_EMAIL = get_env("SENDER_EMAIL")
    SENDER_NAME = get_env("SENDER_NAME", default="Signal", required=False)


    # --- 5. vapid 설정 (웹 푸시 보안 설정)
    VAPID_PRIVATE_KEY = get_env("VAPID_PRIVATE_KEY")
    VAPID_PUBLIC_KEY = get_env("VAPID_PUBLIC_KEY")
    ADMIN_EMAIL = get_env("ADMIN_EMAIL")


    # --- 4. 위안화 환율 API 설정 (ExchangeRate-API) ---
    CNY_API_KEYS = [
        get_env("CNY_API_KEY_1"),
        get_env("CNY_API_KEY_2", required=False),
    ]
    CNY_API_KEYS = [k for k in CNY_API_KEYS if k]




    # --- 5. 데이터베이스 및 저장소 설정 ---
    # 환경변수에서 'http://localhost:9200' 형태로 오든 'localhost'로 오든 대응 가능하게 설정
    _raw_es_host = get_env("ES_HOST", default="localhost", required=False)

    # http://가 포함되어 있지 않다면 붙여주기
    if not _raw_es_host.startswith("http"):
        ES_HOST = f"http://{_raw_es_host}"
    else:
        ES_HOST = _raw_es_host

    ES_PORT = get_env("ES_PORT", default="9200", required=False)

    # 최종적으로 ml.py에서 사용할 때 주소 형식을 안전하게 만듦
    @property
    def ES_URL(self):
        # 만약 ES_HOST에 이미 포트가 포함되어 있다면 그대로 반환, 없다면 포트 결합
        if f":{self.ES_PORT}" in self.ES_HOST:
            return self.ES_HOST
        return f"{self.ES_HOST}:{self.ES_PORT}"

    # 인증 정보 (환경변수에 있다면 가져오고 없으면 None)
    ES_USER = get_env("ES_USER", default=None, required=False)
    ES_PWD = get_env("ES_PWD", default=None, required=False)




    # --- 7. 스케줄러 설정 ---
    SCHEDULE_INTERVAL_MINUTES = 15

    # --- 8. G20기준 국가 및 도시 매핑 데이터 ---
    G20_COUNTRY_MAP = {
        # --- [한자 약어 대응] ---
        "韓": "Korea",
        "美": "United States",
        "中": "China",
        "日": "Japan",
        "英": "United Kingdom",
        "獨": "Germany",
        "佛": "France",
        "露": "Russia",
        "伊": "Italy",
        "印": "India",
        "越": "Vietnam",  # 베트남(越) 뉴스 대응
        "北": "North Korea",


        # --- [동아시아 및 주변국] ---
        "대한민국": "Korea", "한국": "Korea", "남한": "Korea", "우리나라": "Korea",
        "북한": "North Korea", "북측": "North Korea",
        "미국": "United States", "미": "United States", "미측": "United States",
        "중국": "China", "중": "China", "중측": "China",
        "일본": "Japan", "일": "Japan",
        "대만": "Taiwan", "타이완": "Taiwan",
        "홍콩": "China",

        # --- [중동 - 리스크 핵심 지역] ---
        "이스라엘": "Israel","이란": "Iran",
        "사우디아라비아": "Saudi Arabia", "사우디": "Saudi Arabia",
        "아랍에미리트": "UAE", "아랍에미레이트": "UAE", "두바이": "UAE",
        "카타르": "Qatar", "이라크": "Iraq", "쿠웨이트": "Kuwait",
        "이집트": "Egypt", "튀르키예": "Turkey", "터키": "Turkey", "중동": "Middle East",
        "시리아": "Syria","요르단": "Jordan","레바논": "Lebanon","오만": "Oman","예멘": "Yemen",

        # --- [유럽 - 경제 및 전쟁 리스크] ---
        "러시아": "Russia", "러": "Russia",
        "우크라이나": "Ukraine", "우크라": "Ukraine",
        "영국": "United Kingdom", "영": "United Kingdom",
        "프랑스": "France",
        "독일": "Germany", "독": "Germany",
        "이탈리아": "Italy", "이탈리": "Italy",
        "유럽연합": "EU", "EU": "EU",

        # --- [동남아/오세아니아 - 공급망 핵심] ---
        "베트남": "Vietnam",
        "인도네시아": "Indonesia", "인니": "Indonesia",
        "동남아": "ASEAN", "동남아시아": "ASEAN", "아세안": "ASEAN",
        "인도": "India",
        "싱가포르": "Singapore", "싱가폴": "Singapore",
        "태국": "Thailand",
        "필리핀": "Philippines",
        "호주": "Australia", "오스트레일리아": "Australia",

        # --- [아메리카/아프리카 - 자원 및 금융] ---
        "캐나다": "Canada",
        "멕시코": "Mexico",
        "브라질": "Brazil",
        "아르헨티나": "Argentina",
        "남아프리카공화국": "South Africa", "남아공": "South Africa",

        # 뉴스에 빈번하게 등장하는 국가/지역
        "필리핀": "Philippines", "파나마": "Panama",
        "파키스탄": "Pakistan", "베네수엘라": "Venezuela",
        "우즈베키스탄": "Uzbekistan", "우즈벡": "Uzbekistan",
        "아프가니스탄": "Afghanistan", "방글라데시": "Bangladesh", "스리랑카": "Sri Lanka",

        # 약어 대응 강화
        "우크라": "Ukraine", "불": "France", "독": "Germany", "영": "United Kingdom",
        "러": "Russia", "인니": "Indonesia", "남공": "South Africa"
    }

    # '중동' 키워드 발생 시 아래 국가들의 리스크 지수를 동시에 포함
    REGION_TO_COUNTRIES = {
        "Middle East": [
            "Israel", "Iran", "Saudi Arabia", "UAE", "Qatar", "Iraq",
            "Kuwait", "Egypt", "Turkey", "Syria", "Jordan", "Lebanon", "Oman", "Yemen", "Pakistan"
        ],
        "EU": [
            "France", "Germany", "Italy", "Spain", "Netherlands", "Belgium",
            "Poland", "Sweden", "Austria", "Greece"
        ],
        "ASEAN": [
            "Vietnam", "Indonesia", "Thailand", "Philippines", "Singapore",
            "Malaysia", "Myanmar", "Cambodia", "Laos", "Brunei"
        ]
    }

    # G20 주요 도시 매칭 맵
    CITY_TO_COUNTRY_MAP = {
        # --- [G7 & 주요 선진국] ---
        "뉴욕": "United States", "워싱턴": "United States", "시카고": "United States", "샌프란시스코": "United States",
        "LA": "United States",
        "도쿄": "Japan", "오사카": "Japan", "나고야": "Japan",
        "런던": "United Kingdom", "맨체스터": "United Kingdom",
        "파리": "France", "리옹": "France",
        "베를린": "Germany", "프랑크푸르트": "Germany", "뮌헨": "Germany",
        "로마": "Italy", "밀라노": "Italy",
        "토론토": "Canada", "오타와": "Canada",

        # --- [전략적 요충지: 중동] ---
        "테헤란": "Iran", "이스파한": "Iran",
        "예루살렘": "Israel", "텔아비브": "Israel",
        "리야드": "Saudi Arabia", "제다": "Saudi Arabia",
        "두바이": "UAE", "아부다비": "UAE",
        "도하": "Qatar",
        "바그다드": "Iraq",
        "카이로": "Egypt",

        # --- [신흥국 및 주요 경제권] ---
        "베이징": "China", "상하이": "China", "선전": "China", "광저우": "China", "홍콩": "China",
        "델리": "India", "뭄바이": "India", "뱅갈로르": "India",
        "모스크바": "Russia", "상트페테르부르크": "Russia",
        "상파울루": "Brazil", "리우데자네이루": "Brazil",
        "자카르타": "Indonesia",
        "싱가포르": "Singapore",
        "하노이": "Vietnam", "호치민": "Vietnam",
        "방콕": "Thailand",
        "멕시코시티": "Mexico",
        "시드니": "Australia", "캔버라": "Australia",
        "서울": "Korea",

        # 에너지/물류 핵심 요충지
        "얀부": "Saudi Arabia",  # 사우디 서부 우회 항구
        "푸자이라": "UAE",  # UAE 우회 석유 터미널
        "반다르아바스": "Iran",  # 이란 해군 기지 및 주요 항구
        "무스카트": "Oman",  # 호르무즈 인근 오만 수도
        "움알쿠와인": "UAE",  # 나무호 사고 인근 지역
        "아덴": "Yemen",  # 아덴만/홍해 길목
        "사마르칸트": "Uzbekistan",  # ADB 총회 개최지

        # 분쟁 지역 (남중국해)
        "샌디 케이": "Philippines",  # 혹은 China (분쟁 지역이므로 주된 리스크 국가로 매핑)
        "톄셴자오": "China",
        "파가사": "Philippines",

        # 지정학적 핵심 포인트
        "아덴만": "Yemen",  # 예멘 인근 해역
        "바브엘만데브": "Yemen",  # 홍해 입구 핵심 해협
        "티투 섬": "Philippines",  # 남중국해 분쟁 지역 (파가사 섬 인근)
        "예레반": "Armenia",  # 폰데어라이엔 위원장 방문지
        "상파울루": "Brazil"  # 커피/비료 뉴스 관련
    }

    # 사람 한자 이름을 한글로 바꿀 때 사용
    HANJA_TO_KR = {
        # 주요 성씨 (인물 지칭)
        "金": "김", "李": "이", "朴": "박", "崔": "최", "鄭": "정",
        "尹": "윤", "韓": "한", "安": "안", "洪": "홍", "曺": "조",

        # 직책 및 기관
        "總": "총리", "廳": "청", "院": "원", "軍": "군", "警": "경찰",
        "檢": "검찰", "法": "법원", "與": "여당", "野": "야당", "政": "정부",
        "代": "대표", "長": "장관", "委": "위원회", "室": "대통령실",

        # 주요 인물 성씨 및 국가 수장
        "王": "왕",  # 왕이 부장
        "習": "습",  # 시진핑 주석
        "拜": "배",  # 바이든 대통령
        "岸": "안",  # 기시다 총리
        "石": "석",  # 이시바 총리
    }

    # 국가 매칭 시 혼선을 주는 국내 기관 및 언론사 명칭
    COUNTRY_NOISE_INSTITUTIONS = [
        # 정부 부처 및 기관
        "한국은행", "한국투자증권", "국가데이터처", "재정경제부", "기획재정부",
        "산업통상자원부", "해양수산부", "외교부", "국방부", "전쟁부", "법무부",
        "대통령실", "청와대", "해양경찰청", "국립외교원", "한국거래소", "금융위원회",

        # 언론사 명칭 (매칭 방해 1순위)
        "연합뉴스", "서울신문", "중앙일보", "한국경제", "매일경제", "조선비즈",
        "JTBC", "YTN", "KBS", "SBS", "이데일리", "비즈워치", "문화일보",
        "아시아경제", "전자신문", "노컷뉴스", "지디넷코리아", "위키트리",
        "프레시안", "데일리안", "뉴시스", "헤럴드경제", "TV조선", "채널A",

        # 기타 국제 기구 (한국 지사가 언급될 경우 대비)
        "아시아개발은행", "국제통화기금", "세계은행", "OECD", "국제적십자사",

        # 기업 명칭 (본문에 있으면 무조건 Korea로 잡히는 노이즈)
        "삼성전자", "SK하이닉스", "LG에너지솔루션", "삼성SDI", "SK온", "HMM",
        "대한항공", "아시아나", "티웨이", "진에어", "에어부산", "에어서울", "제주항공", "에어프레미아",
        "SK이노베이션", "에쓰오일", "GS칼텍스", "HD현대오일뱅크", "KCC",

        # 증권사 및 금융 (시장 분석 기사 노이즈)
        "한국투자증권", "하나증권", "미래에셋증권", "KB증권", "신한투자증권", "신영증권", "대신증권", "iM증권",
        "금융투자협회", "한국거래소", "국제통화기금", "아시아개발은행",

        # 대학 명칭 (전문가 인터뷰 노이즈)
        "한국외대", "이화여대", "숙명여대", "연세대", "고려대", "세종대", "인하대",

        # 선박 및 해운 관련 노이즈 (중동 뉴스가 Korea로 잡히는 주범)
        "HMM", "나무호", "NAMU", "현대상선", "한국 선박", "우리 선박",
        "한국인 선원", "우리 선원", "한국 정부", "우리 정부", "정부 당국"
    ]

    # 수집할 데이터 검색어(네이버/연합뉴스 용)
    STRATEGIC_KEYWORDS = {
        "에너지/원자재": [
            "국제유가 급등 비상", "천연가스 공급 중단 리스크", "희토류 수출 통제 규제",
            "핵심광물 공급망 위기", "WTI 브렌트유 리스크", "중동 호르무즈 해협 봉쇄",
            "러시아 가스관 가동 중단"
        ],
        "핵심산업": [
            "반도체 수출 규제 제재", "이차전지 IRA 보조금 리스크", "자동차 관세 보복 조치",
            "HBM 반도체 수급 위기", "공급망 내재화 리스크", "대만 TSMC 가동 중단 위기",
            "AI 반도체 독점 규제"
        ],
        "금융/지표": [
            "원달러 환율 폭등 위기", "미국 연준 금리 인상 쇼크", "한국 무역수지 적자 원인",
            "스태그플레이션 경제 위기", "국가 신용등급 강등 리스크", "일본 엔저 경제 충격",
            "미국 국채 금리 급등"
        ],
        "지정학리스크": [
            "중동 분쟁 확산 경제", "미중 무역 전쟁 보복", "호르무즈 해협 마비",
            "러시아 우크라이나 전쟁 리스크", "대만 해협 지정학적 위기", "남중국해 영유권 분쟁",
            "이스라엘 이란 보복 공습"
        ],
        "글로벌정책": [
            "트럼프 보편적 관세 정책", "대중국 반도체 장비 수출제한", "미국 대선 경제 불확실성",
            "EU 탄소국경조정제도 규제", "보호무역주의 통상 리스크", "빅테크 반독점 규제 강화"
        ]
    }
    # 수집할 데이터 검색어(RSS 용)
    STRATEGIC_KEYWORDS_EN = {
        "Energy": [
            "Oil price", "Natural gas", "Rare earth",
            "Critical mineral", "Brent", "Hormuz"  # 'Strait of' 생략
        ],
        "Industry": [
            "Semiconductor", "IRA", "Tariff",  # 'Automotive tariff' -> 'Tariff'로 확장
            "HBM", "TSMC", "AI chip"
        ],
        "Finance": [
            "Exchange rate", "Fed", "Trade deficit",  # 'USD' 생략 (환율 관련 기사는 보통 단어가 포함됨)
            "Stagflation", "Credit rating", "Yen"
        ],
        "Geopolitics": [
            "Middle East", "Trade war", "Ukraine war",
            "Taiwan Strait", "South China Sea", "Israel", "Iran"
        ],
        "Policy": [
            "Trump", "China ban", "US election",
            "CBAM", "Protectionism", "Antitrust"
        ]
    }

    # 5. 감성 사전 및 불용어
    DANGER_DICTIONARY = {
        # --- [1. 파국/붕괴 - 최상급 리스크 (-1.0)] ---
        "위기": -1.0, "부도": -1.0, "디폴트": -1.0, "파산": -1.0, "폭락": -1.0,
        "전쟁": -1.0, "침공": -1.0, "붕괴": -1.0, "셧다운": -1.0, "스태그플레이션": -1.0,
        "공급중단": -1.0, "수출금지": -1.0, "적자전환": -1.0, "충격": -1.0,

        # --- [2. 경계/악화 - 고위험 리스크 (-0.8 ~ -0.9)] ---
        "폭등": -0.9, "상승세둔화": -0.8, "공급난": -0.9, "물류마비": -0.9,
        "관세폭탄": -0.9, "보복관세": -0.9, "무역분쟁": -0.8, "리스크": -0.8,
        "불안": -0.8, "침체": -0.9, "악재": -0.8, "적자": -1.0,

        # --- [3. 정책/규제 - 중위험 리스크 (-0.5 ~ -0.7)] ---
        "규제": -0.7, "제재": -0.7, "긴축": -0.6, "인상": -0.5, "금리인상": -0.6,
        "보조금제외": -0.7, "수사": -0.5, "조사": -0.5, "한계": -0.6, "부진": -0.6,

        # --- [4. 회복/성장 - 긍정 지표 (0.5 ~ 1.0)] ---
        "상승": 0.5, "회복": 0.7, "수주": 0.9, "흑자": 0.9, "돌파": 0.6,
        "반등": 0.7, "개선": 0.6, "완화": 0.8, "성장": 0.7, "협력": 0.5,
        "체결": 0.6, "투자확대": 0.8, "상생": 0.5
    }

    # 문장 내 기능적 불용어
    STOPWORDS = [
        # 문장 연결 및 구조 관련
        "이번", "지난", "통해", "대해", "관련", "위해", "위한", "때문", "경우",
        "사실", "이후", "직후", "현재", "모두", "다시", "결국", "일부", "가운데",
        "정도", "내외", "이상", "이하", "평균", "만큼", "대비", "가장", "매우",

        "지난달", "최근", "올해", "작년", "사흘", "차례", "일주일", "시간", "분기",

        # 뉴스 상투어 및 종결 어미
        "것으로", "밝혔습니다", "전했습니다", "말했다", "이날", "한때", "밝혔다",
        "전했다", "했다", "입니다", "한다", "있습니다", "따르면", "전날", "다음주",

        # 언론사 및 제보 노이즈 (청년, 중앙 추가)
        "기자", "뉴스", "기사", "네이버", "홈페이지", "보도", "제보", "말씀",
        "청년", "중앙", "전람회", "보고", "질문", "답변", "출연", "자료", "사진",
        "출처", "제공", "속보", "단독", "현지", "종합", "금지", "재판매", "코드",
        "앵커", "교수", "학부", "방송",

        # 의미 없는 일반 명사
        "확인", "설명", "지적", "분석", "결과", "진행", "예상", "전망", "내용",
        "정황", "상황", "이유", "모습", "역할", "발표", "예정", "정부", "시장", "일주일",
        "하나", "스프", "여백", "포인트", "상승", "오른", "전장", "시각", "기준",
        "종가", "최고", "왼쪽", "오른쪽", "자료사진", "공유", "살이", "더니",
        "받침", "오스", "면서", "위한", "포함", "치며", "통한", "지금", "먼저",
        "역시", "부담", "크게", "통해", "조용히", "지속", "직전", "마감",

        #
    ]

    # --- [중복 제거 및 보강된 NOISE_WORDS] ---
    NOISE_WORDS = [
        # 사회/생활/문화
        "애인", "아빠", "웹툰", "작가", "응원", "가족", "아버지", "어머니",
        "사랑", "행복", "육아", "드라마", "취미", "낚시", "야구", "축구", "배구",
        "웹소설", "연예인", "방송인", "결혼식", "장례식", "맛집", "레시피", "모색",
        "이동", "시사", "앞서",

        # 이벤트 및 교육
        "성료", "기념촬영", "바우처", "다문화", "캠프", "강좌", "수강생", "모집",

        # 스포츠 및 전시
        "리그", "시즌", "홈런", "골득실", "축제", "공연", "전시회", "전시",

        # 지명 및 시간 (오전, 오후는 여기서 관리)
        "서울", "오전", "오후", "개최", "무단", "배포",

        # 언론사 명칭
        "뉴시스", "연합뉴스", "SBS", "KBS", "YTN", "MBC", "TV조선", "채널A", "JTBC",
        "매일경제", "한국경제", "서울경제", "동아일보", "중앙일보", "문화일보", "조선비즈",
        "아이뉴스24", "디지털타임스", "노컷뉴스", "비즈워치", "프레시안", "데일리안",
        "위키트리", "지디넷코리아", "국제신문", "대전일보", "아시아투데이", "동행미디어", "머니투데이",

        # 사람 이름
        "이주희", "김열", "김혁", "젠슨", "마크"
    ]
    # 불용어, 노이즈 통합 관리
    TOTAL_FILTERS = set(STOPWORDS + NOISE_WORDS)


    # 핵심 개체명 가중치
    target_entities = [
        "미국", "중국", "일본", "베트남", "이란", "러시아", "우크라이나", "EU", "중동",
        "트럼프", "바이든", "푸틴", "시진핑", "파월",
        "삼성전자", "SK하이닉스", "TSMC", "현대차", "엔비디아", "애플", "ASML",
        "한은", "기재부", "금융위", "산업부", "국토부", "백악관", "IMF", "FED", "연준",
        "호르무즈", "홍해", "공급망", "물류대란", "수출규제", "관세", "보조금", "반도체", "이차전지",
        "환율", "금리", "유가", "물가", "인플레이션", "추경", "국가부채", "적자", "흑자", "금리인상",
        "파업", "셧다운", "디폴트", "스태그플레이션", "희토류", "이중용도"
    ]

    # 복합 국가 매핑 사전 정의 (히트맵)
    COMPOSITE_COUNTRY_MAP = {
        # 3개국 이상 (가장 먼저 체크)
        "한미일": ["Korea", "United States", "Japan"],
        "중동": ["Israel", "Iran", "Saudi Arabia", "UAE", "Qatar"],
        "유럽연합": ["Germany", "France", "Italy"],
        "EU": ["Germany", "France", "Italy"],

        # 2개국 (그다음 체크)
        "미·이란": ["United States", "Iran"],
        "미이란": ["United States", "Iran"],
        "미국과 이란": ["United States", "Iran"],
        "러우": ["Russia", "Ukraine"],
        "러·우": ["Russia", "Ukraine"],
        "미중": ["United States", "China"],
        "한미": ["Korea", "United States"],
        "한일": ["Korea", "Japan"],
        "한중": ["Korea", "China"],
        "미일": ["United States", "Japan"],
        "북미": ["North Korea", "United States"],
        "남북": ["Korea", "North Korea"],

        # 국가별 약어 (추가하면 성능 UP)
        "대중": ["China"],  # 예: 대중 수출 규제
        "대미": ["United States"],  # 예: 대미 통상 압박
        "대일": ["Japan"],
        "대러": ["Russia"]
    }

    # 잘린 단어 이어 붙여줌
    correction_map = {
        # [국가/지명 관련]
        "러시": "러시아", "아산": "러시아산", "이스라": "이스라엘",
        "필리": "필리핀", "우크라": "우크라이나", "베트": "베트남",
        "아프": "아프리카", "바트": "바트화", "게이": "니혼게이자이",
        "피":"코스피","장일":"연장일",

        # [기술/인프라 관련]
        "페트": "페트로라인", "라인": "페트로라인",
        "반도": "반도체", "이차": "이차전지", "전지": "이차전지",
        "포스": "트렌드포스", "데이터": "데이터센터", "센터": "데이터센터",


        # [추출 오류 교정]
        "대감": "기대감", "불기": "불기둥", "스물": "이스물라",
        "상의": "대한상의", "안전": "안전벨트", "지수": "코스피지수",
        "도널드": "도널드 트럼프", "트럼프": "도널드 트럼프", "중동": "중동전",
        "호르무즈": "호르무즈 해협", "해협": "호르무즈 해협", "양해": "양해각서",
        "각서": "양해각서", "다카": "다카이치","이치": "다카이치", "마중":"마중물",
        "사단":"82공수단",

        # [기업]
        "삼전": "삼성전자", "닉스": "SK하이닉스", "성전": "삼성전자",
        "안두": "안두릴","기판": "기판가격", "판값": "기판가격",
        "에이":"피에이치에이", "에이치":"피에이치에이","스택":"풀스택"
    }

    # 리스크 점수 가중치 공식용 설정 (히트맵)
    # 한국 관련 정책/경제 핵심 단어 (강제 고정용)
    KOREA_PRIORITY_KEYWORDS = [
        "정부", "관세청", "재경부", "기획재정부", "코스피", "삼성전자", "한은", "한국은행",
        "한국", "대한민국", "K-방산", "K-조선", "국내 기업", "부산", "서울",
        "기재부", "국내", "우리나라", "서산","여수", "대산", "평택", "국정원",
        "대한상의", "관세청"

    ]
    KOREA_FIRM_KEYWORDS = [
        "HD현대", "대한항공", "현대로템", "삼성전자", "SK하이닉스"
    ]

    # 리스크 점수 가중치 공식용 설정 (히트맵)
    # 미국 관련 정책/경제 핵심 단어 (강제 고정용)
    US_PRIORITY_KEYWORDS = [
        '뉴욕증시', '다우', '나스닥',"OpenAI"
    ]
