import os
from google import genai
from dotenv import load_dotenv


# .env 로드
load_dotenv()


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





    # --- 6. 분석 알고리즘 임계값 (Thresholds) ---
    # 코드 내의 수치들을 변수화하면 튜닝이 쉬워집니다.
    RISK_THRESHOLD_CRITICAL = 0.4
    RISK_THRESHOLD_WARNING = 0.6
    MATCHING_SCORE_THRESHOLD = 2.5

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

        # 주요 성씨 (인물 지칭)
        "金": "김", "李": "이", "朴": "박", "崔": "최", "鄭": "정",
        "尹": "윤", "韓": "한", "安": "안", "洪": "홍", "曺": "조",

        # 직책 및 기관
        "總": "총리", "廳": "청", "院": "원", "軍": "군", "警": "경찰",
        "檢": "검찰", "法": "법원", "與": "여당", "野": "야당", "政": "정부",
        "代": "대표", "長": "장관", "委": "위원회", "室": "대통령실",

        # --- [동아시아 및 주변국] ---
        "대한민국": "Korea", "한국": "Korea", "남한": "Korea", "우리나라": "Korea",
        "북한": "North Korea", "북측": "North Korea",
        "미국": "United States", "미": "United States", "미측": "United States",
        "중국": "China", "중": "China", "중측": "China",
        "일본": "Japan", "일": "Japan",
        "대만": "Taiwan", "타이완": "Taiwan",
        "홍콩": "China",

        # --- [중동 - 리스크 핵심 지역] ---
        "이스라엘": "Israel",
        "이란": "Iran",
        "사우디아라비아": "Saudi Arabia", "사우디": "Saudi Arabia",
        "아랍에미리트": "UAE", "아랍에미레이트": "UAE", "두바이": "UAE",
        "카타르": "Qatar",
        "이라크": "Iraq",
        "쿠웨이트": "Kuwait",
        "이집트": "Egypt",
        "튀르키예": "Turkey", "터키": "Turkey",

        # --- [유럽 - 경제 및 전쟁 리스크] ---
        "러시아": "Russia", "러": "Russia",
        "우크라이나": "Ukraine", "우크라": "Ukraine",
        "영국": "United Kingdom", "영": "United Kingdom",
        "프랑스": "France", "불": "France",
        "독일": "Germany", "독": "Germany",
        "이탈리아": "Italy", "이탈리": "Italy",
        "유럽연합": "France", "EU": "France",

        # --- [동남아/오세아니아 - 공급망 핵심] ---
        "베트남": "Vietnam",
        "인도네시아": "Indonesia", "인니": "Indonesia",
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
        "남아프리카공화국": "South Africa", "남아공": "South Africa"
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
        "서울": "Korea"
    }
    # 수집할 데이터 검색어
    STRATEGIC_KEYWORDS = {
        "에너지/원자재": [
            "국제유가 급등 비상", "천연가스 공급 중단 리스크", "희토류 수출 통제 규제",
            "핵심광물 공급망 위기", "WTI 브렌트유 리스크"
        ],
        "핵심산업": [
            "반도체 수출 규제 제재", "이차전지 IRA 보조금 리스크", "자동차 관세 보복 조치",
            "HBM 반도체 수급 위기", "공급망 내재화 리스크"
        ],
        "금융/지표": [
            "원달러 환율 폭등 위기", "미국 연준 금리 인상 쇼크", "한국 무역수지 적자 원인",
            "스태그플레이션 경제 위기", "국가 신용등급 강등 리스크"
        ],
        "지정학리스크": [
            "중동 분쟁 확산 경제", "미중 무역 전쟁 보복", "호르무즈 해협 마비",
            "러시아 우크라이나 전쟁 리스크", "대만 해협 지정학적 위기"
        ],
        "글로벌정책": [
            "트럼프 보편적 관세 정책", "대중국 반도체 장비 수출제한", "미국 대선 경제 불확실성",
            "EU 탄소국경조정제도 규제", "보호무역주의 통상 리스크"
        ]
    }

    # 5. 감성 사전 및 불용어 (Config에서 관리 권장하나 로직 유지를 위해 잔류)
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

    # 문장 내 기능적 불용어 (기사 문구 노이즈)
    STOPWORDS = [
        "기자", "뉴스", "네이버", "이번", "지난", "통해", "대해", "관련", "위해", "코드",
        "사진", "출처", "제공", "속보", "단독", "현지", "종합", "사실", "가장", "매우",
        "이후", "현재", "모두", "다시", "결국", "일부", "진행", "확인", "발표", "예정",

    ]

    # 주제와 상관없는 도메인 노이즈 (생활/사회/문화)
    NOISE_WORDS = [
        # 사회/생활/문화 (중의적이지 않은 것들)
        "애인", "아빠", "웹툰", "작가", "응원", "가족", "아버지", "어머니",
        "사랑", "행복", "육아", "드라마", "취미", "낚시", "야구", "축구", "배구",
        "웹소설", "연예인", "방송인", "결혼식", "장례식", "맛집", "레시피",

        # 확실한 이벤트성 (경제와 무관한 것)
        "성료", "기념촬영", "바우처", "다문화", "캠프", "강좌", "수강생", "모집",

        # '경기' 대신 확실하게 스포츠임을 나타내는 단어 사용
        "리그", "시즌", "홈런", "골득실", "축제", "공연", "전시회",  # '전시' 대신 '전시회'로 변경
    ]
    # 불용어, 노이즈 통합 관리
    TOTAL_FILTERS = set(STOPWORDS + NOISE_WORDS)



