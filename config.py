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
        "越": "Vietnam",
        "北": "North Korea",


        # --- [동아시아 및 주변국] ---
        "대한민국": "Korea", "한국": "Korea", "남한": "Korea", "우리나라": "Korea",
        "북한": "North Korea", "북측": "North Korea",
        "미국": "United States", "미": "United States", "미측": "United States",
        "중국": "China", "중": "China", "중측": "China",
        "일본": "Japan", "일": "Japan",
        "대만": "Taiwan", "타이완": "Taiwan",
        "홍콩": "China", "동아시아": "East Asia", "몽골": "Mongolia",

        # --- [중동 - 리스크 핵심 지역] ---
        "이스라엘": "Israel","이란": "Iran",
        "사우디아라비아": "Saudi Arabia", "사우디": "Saudi Arabia",
        "아랍에미리트": "United Arab Emirates", "아랍에미레이트": "United Arab Emirates", "두바이": "United Arab Emirates",
        "카타르": "Qatar", "이라크": "Iraq", "쿠웨이트": "Kuwait",
        "이집트": "Egypt", "튀르키예": "Turkey", "터키": "Turkey", "중동": "Middle East",
        "시리아": "Syria","요르단": "Jordan","레바논": "Lebanon","오만": "Oman","예멘": "Yemen",

        # --- [유럽 - 경제 및 전쟁 리스크] ---
        "러시아": "Russia", "러": "Russia",
        "우크라이나": "Ukraine", "우크라": "Ukraine",
        "영국": "United Kingdom", "영": "United Kingdom",
        "프랑스": "France", "불": "France",
        "독일": "Germany", "독": "Germany",
        "이탈리아": "Italy", "이탈리": "Italy", "이태리": "Italy",
        "유럽연합": "EU", "EU": "EU",
        "벨기에":"Belgium","헝가리":"Hungary",

        # --- [동남아/오세아니아 - 공급망 핵심] ---
        "베트남": "Vietnam",
        "인도네시아": "Indonesia", "인니": "Indonesia",
        "동남아": "ASEAN", "동남아시아": "ASEAN", "아세안": "ASEAN",
        "인도": "India", "인디아": "India",
        "싱가포르": "Singapore", "싱가폴": "Singapore",
        "태국": "Thailand",
        "필리핀": "Philippines",
        "호주": "Australia", "오스트레일리아": "Australia",


        # --- [아메리카/아프리카 - 자원 및 금융] ---
        "캐나다": "Canada",
        "멕시코": "Mexico",
        "브라질": "Brazil",
        "아르헨티나": "Argentina",
        "남아프리카공화국": "South Africa", "남아공": "South Africa", "남공": "South Africa",

        # 뉴스에 빈번하게 등장하는 국가/지역
        "파나마": "Panama",
        "파키스탄": "Pakistan", "베네수엘라": "Venezuela",
        "우즈베키스탄": "Uzbekistan", "우즈벡": "Uzbekistan",
        "아프가니스탄": "Afghanistan", "방글라데시": "Bangladesh", "스리랑카": "Sri Lanka",
        "브릭스": "BRICS", "BRICS": "BRICS",
        "G7": "G7", "주요7개국": "G7",
        "북미": "North America",
        "남미": "Latin America", "라틴아메리카": "Latin America",
        "오세아니아": "Oceania",
        "뉴질랜드": "New Zealand", "노르웨이": "Norway", "포르투갈": "Portugal",

        # 약어 대응 강화
        "남공": "South Africa", "우즈벡": "Uzbekistan",
    }

    # 지역 연합
    REGION_TO_COUNTRIES = {
        "East Asia": [
            "Korea", "North Korea", "China", "Japan", "Taiwan", "Hong Kong", "Mongolia", "Macau"
        ],
        "Middle East": [
            "Israel", "Iran", "Saudi Arabia", "United Arab Emirates", "Qatar", "Iraq",
            "Kuwait", "Egypt", "Turkey", "Syria", "Jordan", "Lebanon", "Oman", "Yemen", "Pakistan"
        ],
        "EU": [
            "France", "Germany", "Italy", "Spain", "Netherlands", "Belgium",
            "Poland", "Sweden", "Austria", "Greece"
        ],
        "ASEAN": [
            "Vietnam", "Indonesia", "Thailand", "Philippines", "Singapore",
            "Malaysia", "Myanmar", "Cambodia", "Laos", "Brunei"
        ],
        "Central Asia": [
            "Kazakhstan", "Uzbekistan", "Kyrgyzstan", "Tajikistan", "Turkmenistan"
        ],
        "BRICS": [
            "Brazil", "Russia", "India", "China", "Korea"
        ],
        "G7": [
            "United States", "United Kingdom", "France", "Germany", "Japan", "Italy", "Canada"
        ],
        "North America": [
            "United States", "Canada", "Mexico"
        ],
        "Latin America": [
            "Brazil", "Argentina", "Chile", "Colombia", "Mexico", "Venezuela"
        ],
        "Oceania": [
            "Australia", "New Zealand"
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
        "두바이": "United Arab Emirates", "아부다비": "United Arab Emirates",
        "도하": "Qatar",
        "바그다드": "Iraq",
        "카이로": "Egypt",

        # --- [신흥국 및 주요 경제권] ---
        "베이징": "China", "상하이": "China", "선전": "China", "광저우": "China", "홍콩": "China",
        "델리": "India", "뭄바이": "India", "뱅갈로르": "India",
        "모스크바": "Russia", "상트페테르부르크": "Russia",
        "리우데자네이루": "Brazil",
        "자카르타": "Indonesia",
        "싱가포르": "Singapore",
        "하노이": "Vietnam", "호치민": "Vietnam",
        "방콕": "Thailand",
        "멕시코시티": "Mexico",
        "시드니": "Australia", "캔버라": "Australia",
        "서울": "Korea", "부산": "Korea", "인천": "Korea",
        "평양": "North Korea",

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
            "Stagflation", "Credit rating", "Yen" ,"Forex", "Currency volatility"
        ],
        "Geopolitics": [
            "Middle East", "Trade war", "Ukraine war",
            "Taiwan Strait", "South China Sea", "Israel", "Iran"
        ],
        "Policy": [
            "Trump", "China ban", "US election", "Export control",
            "CBAM", "Protectionism", "Antitrust" , "Sanction"
        ]
    }

    BLACKLIST = [
        'buy now', 'stock to buy', 'should you buy', 'price target',
        'dividend', 'top picks', 'better buy', 'earnings result'
    ]

    # 5. 감성 사전 및 불용어
    DANGER_DICTIONARY = {
        # --- [1. 파국/붕괴 - 최상급 리스크 (-1.0)] ---
        "위기": -1.0, "부도": -1.0, "디폴트": -1.0, "파산": -1.0, "폭락": -1.0,
        "전쟁": -1.0, "침공": -1.0, "붕괴": -1.0, "셧다운": -1.0, "스태그플레이션": -1.0,
        "공급중단": -1.0, "수출금지": -1.0, "적자전환": -1.0, "충격": -1.0,
        "신용강등": -1.0, "등급강등": -1.0, "적자": -1.0, "추락": -1.0,
        "쓰나미": -1.0, "충돌": -1.0,

        # --- [2. 경계/악화 - 고위험 리스크 (-0.8 ~ -0.9)] ---
        "폭등": -0.9, "상승세둔화": -0.8, "공급난": -0.9, "물류마비": -0.9,
        "관세폭탄": -0.9, "보복관세": -0.9, "무역분쟁": -0.8, "리스크": -0.8,
        "불안": -0.8, "침체": -0.9, "악재": -0.8, "불매운동": -0.7,
        "전망하향": -0.9, "하향조정": -0.8, "순매도": -0.8, "자금이탈": -0.9,
        "경기둔화": -0.8, "수출규제": -0.9, "빈곤": -0.9, "부채급증": -0.9, "리스크 확대": -0.8,
        "하락": -0.5, "비상": -0.8, "성장 둔화": -0.8, "신용등급 하향": -0.9,
        "전망 빗나가": -0.8, "얼어붙": -0.8, "봉쇄" : -0.8,

        # --- [3. 정책/규제 - 중위험 리스크 (-0.5 ~ -0.7)] ---
        "규제": -0.7, "제재": -0.7, "긴축": -0.6, "인상": -0.5, "금리인상": -0.6, "위반" : -0.7,
        "보조금제외": -0.7, "수사": -0.5, "조사": -0.5, "한계": -0.6, "부진": -0.6, "경고" : -0.6,
        "부정적": -0.6, "불확실성": -0.6, "고령화": -0.7, "저출산": -0.7, "생산가능인구 감소": -0.8,
        "가계대출": -0.5, "미흡": -0.4, "난제": -0.5, "발목": -0.6, "불확실": -0.6, "부채 비율": -0.5,
        "부담": -0.5, "거절": -0.5, "성과 없는": -0.5, "수모": -0.4, "복수": -0.4, "파고": -0.5,

        # --- [4. 회복/성장 - 긍정 지표 (0.5 ~ 1.0)] ---
        "상승": 0.5, "회복": 0.7, "수주": 0.9, "흑자": 0.9, "돌파": 0.6,
        "반등": 0.7, "개선": 0.6, "완화": 0.8, "성장": 0.7,
        "체결": 0.6, "투자확대": 0.8, "상생": 0.5, "등급상향": 0.9, "상회": 0.7,
        "전망상향": 0.8, "순매수": 0.8, "사상최고": 0.9, "회복세": 0.7,
        "지원": 0.7, "공급": 0.5 , "혜택" : 0.5, "선점": 0.8, "활성화": 0.5,
        "어닝서프라이즈": 1.0, "불확실성해소": 0.8, "초격차": 0.9,
        "양산": 0.7, "규제완화": 0.8, "리스크감소": 0.7, "안정세": 0.6,
        "연착륙": 0.7, "독주": 0.9, "신기록": 0.8, "수익성개선": 0.7,
        "낙관": 0.6, "훈풍": 0.6, "잭팟": 0.9, "청신호": 0.7, "순항": 0.6,
        "활기": 0.5, "재개": 0.5, "수출확대": 0.8, "세제혜택": 0.7, "정상화": 0.7,
        "도전": 0.3, "열정": 0.4, "출격": 0.3, "개막": 0.4, "승리": 0.5,
        "4.5일제": 0.2, "노동시간": 0.1, "복귀": 0.3, "홈런": 0.5,
        "협력": 0.8, "강화": 0.6, "방문": 0.5, "MOU": 0.9, "우주": 0.4
    }

    # 불필요한 문구 제거
    junk_patterns = [
        r"최신 만화 보기", r"운세 보기", r"눈TV", r"바로가기",
        r"무단전재 및 재배포 금지", r"여러분의 제보를 기다립니다",
        r"구독", r"저작권자.*", r"\S+@\S+", r"전문\s*:\s*http\S+",
        r"ⓒ", r"Copyrights", r"홈페이지", r"핫뉴스"
    ]

    # 잘라낼 기준점들 (설정한 앞부분만 분석)
    delimiters = [
        "▶", "ⓒ", "저작권자", "기자 =", "기자=", "☞",
        "한편,", "관련 기사", "재배포 금지", "기재부 제공"
    ]

    # 노이즈 기사 스킵
    skip_keywords = [
        "아침 신문 보기", "뉴스투데이", "오늘의 날씨", "스포츠 뉴스", "뉴스클립", "뉴스 요약",
        "헤드라인", "주요뉴스", "뉴스전망대", "부고", "인사", "게시판", "오늘의 운세", "금통위 의사록",
        "프로야구", "하이라이트", "시청률", "개봉예정", "단독포착", "연예", "페낭", "묘지 투어","개척자", "식민지",
        "탄소국경조정제도", "출마 선언", "불출마 선언", "사퇴 요구", "막말 논란", "SNS 논란", "설전", "공방",
        "기싸움", "장외전", "단식", "삭발", "지지 호소", "팬덤", "악수 거부", "사진 논란", "상원의원 사퇴",
        "하원의원 사퇴", "불출마 선언",
    ]

    industry_kws = [
        "청와대", "남북", "국회", "여당", "야당", "정상회담", "검찰",
        "비핵화", "대북제재", "외교부", "공동선언", "특검", "조사",
        "선거", "공천", "개헌", "당대표", "지지도", "내각", "공방", "비판", "촉구"
    ]

    # 정치 뉴스를 가져왔을때 risk 점수 완화
    politics_kws = [
        "청와대", "남북", "국회", "여당", "야당", "정상회담", "검찰",
        "비핵화", "대북제재", "외교부", "공동선언", "특검", "조사",
        "선거", "공천", "개헌", "당대표", "지지도", "내각", "공방", "비판", "촉구"
    ]
    # 스포츠 기사 키워드
    SPORTS_KEYWORDS = [
        '야구', '축구', '농구', '배구', '골프', '축구', '테니스',
        '빙상', '피겨', '수영', '양궁',
        'MLB', '메이저리그', 'KBO', 'LIV', 'NBA', 'EPL', 'Lck',
        '선수', '투수', '타자', '홈런', '완봉', '리그', '구단',
        '복귀', '영입', '방출', '입단', '이적', 'FA', '선발', '출격',
        '우승', '승리', '패전', '경기', '대회', '챔피언십', '투어',
        '평가전', '전지훈련', '개막전', '포스트시즌',
    ]

    # 연예 기사 키워드
    ENTERTAINMENT_KEYWORDS = [
        "연예", "아이돌", "가수", "배우", "드라마",
        "영화", "예능", "방송", "MC", "시즌",
        "넷플릭스", "디즈니", "OTT",
        "컴백", "신곡", "음원", "차트",
        "아이유", "BTS", "블랙핑크",
        "시청률", "화제", "논란"
    ]

    # 추천 / 순위 / TOP / 키워드
    RECOMMENDATION_KEYWORDS = [
        "추천", "BEST", "베스트", "TOP", "순위",
        "1위", "2위", "3위", "랭킹",
        "총정리", "모음", "리스트",
        "가장", "최고", "핫한", "인기",
        "알아두면 좋은", "필수", "꿀팁",
        "해야 할", "가지 방법", "방법",
        "사야", "매수", "매도", "투자", "추천",
        "좋은 주식", "지금 살까", "지금 구매",
        "buy", "sell", "good stock", "is it good",
        "target price", "목표가", "상승 여력",
        "bullish", "bearish"
    ]


    # 경제 기사 키워드
    economy_keywords = [
        '반도체', '수출', '금리', '환율', '무역', '기업', '산업', '증시', '채권', '금융', '통제',
        '유가', '물가', '인플레', '실적', '실업', '유동성', '부도', '부양', '긴축', '공급망', '부채'
    ]

    # 주식 추천기사 제외 키워드
    SAFE_FINANCE_PATTERNS = [
        "추천", "수익", "매수", "투자 전략", "방어주", "배당", "유망",
        "버틸", "기회", "상승 여력", "장기 투자", "포트폴리오", "추천 종목", "수혜주",
        "관련주", "매수", "포트폴리오", "ETF", "배당주", "종목", "주식은", "수익을 낼",
        "투자 전략", "살 만한", "유망주", "Top Pick", "pick", "주식", "비트코인 전망",
        "암호화폐 전망", "채굴",
        "halving", "ETF 승인", "price prediction",
        "analysis", "forecast", "bull run",
        "인가요", "좋은가요", "할까요", "해야 할까",
        "should I", "is it good", "worth it",
        "지금", "현재", "buy now"
    ]

    # 기업경영 뉴스 완충
    CORPORATE_NEWS_KEYWORDS = [
        "합병", "인수", "CEO", "실적", "노조", "구조조정",
        "경영", "배당", "자사주",
    ]



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
        "다이브", "딥다이브", "면서", "기고", "줌인", "스타트", "뉴스UP", "속보", "칼럼",
        "이정환", "송세영", "최홍섭", "박현", "김혁", "일상", "이유", "의미",
        "역시", "여러", "모자라", "통해", "대신", "다시", "더욱", "다음", "올해",
        "작년", "올해", "작년", "내년", "내달", "매달", "현안", "시간", '재판매',
        '무단', '배포', '금지', '저작권', '기자', '뉴스', '연합뉴스', '로이터', '전재',
        "학습", "추측", "난무", '리그', '트윈스', '타자', '투수', '홈런', '안타', '승리', '패배',
        '메이저리그', 'KBO', 'MLB', '선수', '구단', '대표팀'


    ]

    # --- [중복 제거 및 보강된 NOISE_WORDS] ---
    NOISE_WORDS = [
        # 사회/생활/문화
        "애인", "아빠", "웹툰", "작가", "응원", "가족", "아버지", "어머니",
        "사랑", "행복", "육아", "드라마", "취미", "낚시", "야구", "축구", "배구",
        "웹소설", "연예인", "방송인", "결혼식", "장례식", "맛집", "레시피", "체험", "행사",
        "참여", "개막", "폐막", "방문", "관람", "축하", "출연", "팬미팅", "사인회", "콘서트",
        "뮤지컬", "영화", "예능", "촬영", "관련", "통해", "대해", "경우", "진행", "이후",
        "당시", "이번", "관계자", "소식통", "당국자", "설명", "강조", "언급", "발표",
        "밝혔다", "전했다", "말했다",

        # 이벤트 및 교육
        "성료", "기념촬영", "바우처", "다문화", "캠프", "강좌", "수강생", "모집",
        "아이돌", "배우", "가수", "유튜버", "치어리더", "팬덤", "앨범",

        # 날씨
        "기온", "폭염", "한파", "강풍", "호우", "미세먼지",
        "날씨", "강수량", "적설량",

        # 스포츠 및 전시
        "리그", "시즌", "홈런", "골득실", "축제", "공연", "전시회", "전시", "감독",
        "선수", "득점", "승리", "패배", "우승", "MVP", "투수", "타자", "경기", "구단", "프로야구",
        "K리그", "프리미어리그", "챔피언스리그", "NBA", "라리가",

        # 지명 및 시간 (오전, 오후는 여기서 관리)
        "오전", "오후", "개최", "무단", "배포",

        # 언론사 관련
        "뉴시스", "연합뉴스", "SBS", "KBS", "YTN", "MBC", "TV조선", "채널A", "JTBC",
        "매일경제", "한국경제", "서울경제", "동아일보", "중앙일보", "문화일보", "조선비즈",
        "아이뉴스24", "디지털타임스", "노컷뉴스", "비즈워치", "프레시안", "데일리안",
        "위키트리", "지디넷코리아", "국제신문", "대전일보", "아시아투데이", "동행미디어", "머니투데이",
        "기자", "특파원", "보도", "기사", "사진", "제공", "자료", "이미지", "캡처", "연합인포맥스",
        "뉴스1", "파이낸셜뉴스", "헤럴드경제", "전자신문", "이데일리", "아시아경제", "뉴스핌", "쿠키뉴스",
        "오마이뉴스", "속보", "단독", "인터뷰", "브리핑", "현장", "취재", "영상", "생중계", "홈페이지"

        # 사람 이름
        "이주희", "김열", "김혁", "이영재"
        
        # 깨진 키워드
        "석비", "서관"
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

    # 사건/지명 → 국가 리스트
    COMPOSITE_COUNTRY_MAP = {
        # 🌍 해협/지정학 포인트
        "Strait of Hormuz": ["Iran", "United Arab Emirates", "Oman", "Saudi Arabia"],
        "Red Sea": ["Egypt", "Saudi Arabia", "Yemen", "Sudan", "Eritrea"],
        "South China Sea": ["China", "Philippines", "Vietnam", "Malaysia", "Brunei", "Taiwan"],
        "Taiwan Strait": ["China", "Taiwan"],
        "Bab el-Mandeb": ["Yemen", "Djibouti", "Eritrea"],
        "Panama Canal": ["United States", "Panama"],
        "Suez Canal": ["Egypt"],

        # 🌍 지정학/분쟁
        "Ukraine War": ["Ukraine", "Russia"],
        "Middle East Conflict": ["Israel", "Iran", "Saudi Arabia", "United Arab Emirates"],

        # 🌍 경제 이슈
        "Semiconductor Supply Chain": ["Taiwan", "Korea", "United States", "China"],
        "Trade War": ["United States", "China"],
        "Energy Crisis": ["Russia", "EU", "Middle East"],
        "Financial Crisis": ["United States", "EU", "Japan", "China"],

        # 🌍 블록
        "G7": ["United States", "United Kingdom", "France", "Germany", "Japan", "Italy", "Canada"],
        "BRICS": ["Brazil", "Russia", "India", "China", "South Africa"],
        "EU": ["Germany", "France", "Italy", "Spain", "Netherlands", "Belgium"],
        "ASEAN": ["Vietnam", "Indonesia", "Thailand", "Philippines", "Singapore", "Malaysia"],
        "Central Asia": ["Kazakhstan", "Uzbekistan", "Kyrgyzstan", "Tajikistan", "Turkmenistan"],
        "East Asia": ["China", "Japan", "Korea", "Taiwan"],
        "North America": ["United States", "Canada", "Mexico"],
        "Latin America": ["Brazil", "Argentina", "Chile", "Colombia"],
        "Oceania": ["Australia", "New Zealand"],
        "Middle East": ["Israel", "Iran", "Saudi Arabia", "United Arab Emirates", "Qatar", "Iraq", "Egypt"],
    }


    # 별칭 테이블 (한글 → 영어 매핑)
    COMPOSITE_ALIAS_MAP = {
        # 🌍 지명 / 해협 / 운하
        "호르무즈": "Strait of Hormuz",
        "Strait of Hormuz": "Strait of Hormuz",

        "홍해": "Red Sea",
        "Red Sea": "Red Sea",

        "남중국해": "South China Sea",
        "South China Sea": "South China Sea",

        "대만해협": "Taiwan Strait",
        "Taiwan Strait": "Taiwan Strait",

        "페르시아만": "Persian Gulf",
        "Persian Gulf": "Persian Gulf",

        "바브엘만데브": "Bab el-Mandeb",
        "밥엘만데브": "Bab el-Mandeb",
        "Bab el-Mandeb": "Bab el-Mandeb",

        "파나마 운하": "Panama Canal",
        "Panama Canal": "Panama Canal",

        "수에즈 운하": "Suez Canal",
        "Suez Canal": "Suez Canal",

        # 🌍 전쟁/이슈
        "우크라이나 전쟁": "Ukraine War",
        "Ukraine War": "Ukraine War",

        "중동 분쟁": "Middle East Conflict",
        "Middle East Conflict": "Middle East Conflict",

        "반도체 공급망": "Semiconductor Supply Chain",
        "Semiconductor Supply Chain": "Semiconductor Supply Chain",

        "관세 전쟁": "Trade War",
        "Trade War": "Trade War",

        "에너지 위기": "Energy Crisis",
        "Energy Crisis": "Energy Crisis",

        "금융위기": "Financial Crisis",
        "Financial Crisis": "Financial Crisis",

        # 🌍 지역
        "중동": "Middle East",
        "Middle East": "Middle East",

        "유럽연합": "EU",
        "EU": "EU",
        "European Union": "EU",

        "아세안": "ASEAN",
        "동남아": "ASEAN",
        "ASEAN": "ASEAN",

        "브릭스": "BRICS",
        "BRICS": "BRICS",

        "G7": "G7",
        "주요7개국": "G7",

        "동아시아": "East Asia",
        "East Asia": "East Asia",

        "중앙아시아": "Central Asia",
        "Central Asia": "Central Asia",

        "남미": "Latin America",
        "라틴아메리카": "Latin America",
        "Latin America": "Latin America",

        "북미": "North America",
        "North America": "North America",

        "오세아니아": "Oceania",
        "Oceania": "Oceania",

        "서방": "G7",
        "인도태평양": "Global",
    }

    # 고정된 지역 단위 -> 국가명 리스트
    REGION_TO_COUNTRY_MAP = {
    "G7": ["United States", "United Kingdom", "France", "Germany", "Japan", "Italy", "Canada"],
    "BRICS": ["Brazil", "Russia", "India", "China", "South Africa"],
    "EU": ["Germany", "France", "Italy", "Spain", "Netherlands", "Belgium"],
    "ASEAN": ["Vietnam", "Indonesia", "Thailand", "Philippines", "Singapore", "Malaysia"],
    "Middle East": ["Israel", "Iran", "Saudi Arabia", "United Arab Emirates", "Qatar", "Iraq", "Egypt"],
    "East Asia": ["China", "Japan", "Korea", "Taiwan"],
}

    # 잘린 단어 이어 붙여줌
    correction_map = {
        # [국가/지명 관련]
        "러시": "러시아", "아산": "러시아산", "이스라": "이스라엘", "호르": "호르무즈 해협", "무즈": "호르무즈 해협",
        "필리": "필리핀", "우크라": "우크라이나", "베트": "베트남", "호르무즈": "호르무즈 해협",
        "아프": "아프리카", "바트": "바트화", "게이": "니혼게이자이", "닛케": "니혼게이자이",
        "코피":"코스피","장일":"연장일", "가자": "가자지구", "헤즈": "헤즈볼라", "팔레스": "팔레스타인",
        "사우디아라": "사우디아라비아", "아랍에미리": "아랍에미리트", "유럽연": "유럽연합", "하마": "하마스",

        # [기술/인프라 관련]
        "페트": "페트로라인",
        "반도": "반도체", "이차": "이차전지", "전지": "이차전지",
        "포스": "트렌드포스", "HBM": "고대역폭메모리", "고대": "고대역폭메모리",
        "파운드": "파운드리", "TSMC": "TSMC", "전고": "전고체배터리", "챗지": "챗GPT",

        # [추출 오류 교정]
        "대감": "기대감", "불기": "불기둥", "스물": "이스물라",
        "상의": "대한상의", "도널드": "도널드 트럼프",
        "트럼프": "도널드 트럼프", "중동": "중동전",
        "양해": "양해각서", "황이": "젠슨 황", "국제통화기": "국제통화기금", "세계무역기": "세계무역기구",
        "각서": "양해각서", "다카": "다카이치","이치": "다카이치", "마중":"마중물",
        "트럼": "도널드 트럼프", "럼프": "도널드 트럼프", "시진": "시진핑", "진핑": "시진핑",
        "푸틴": "블라디미르 푸틴", "양해각": "양해각서", "생산시설투": "생산시설투자", "시설투": "시설투자",
        "대규모투": "대규모투자", "기술협": "기술협력", "전략적협": "전략적협력",
        "생산설": "생산설비", "시설확": "시설확장", "기술이": "기술이전", "지분투": "지분투자", "공급계": "공급계약",


        # [기업, 경제]
        "삼전": "삼성전자", "닉스": "SK하이닉스", "성전": "삼성전자", "엔비": "엔비디아", "엔비디": "엔비디아",
        "안두": "안두릴", "기판": "기판가격", "판값": "기판가격", "디아": "엔비디아", "삼성전": "삼성전자",
        "에이치": "피에이치에이", "스택": "풀스택", "테슬": "테슬라", "슬라": "테슬라",
        "엘앤": "엘앤에프", "미투": "대미투자법", "삼파": "삼성파운드리",
        "자법": "대미투자법","클리": "위클리", "일리": "데일리",
        "나노": "8나노", "나스": "나스닥", "코스" : "코스닥", "닥지": "나스닥지수",
        "국장": "국내증시", "미장": "미국증시", "현중": "HD현대중공업", "삼바": "삼성바이오로직스",
        "셀트": "셀트리온", "카카": "카카오", "네이": "네이버", "하닉": "SK하이닉스", "마이크로소프": "마이크로소프트",
        "넷플릭": "넷플릭스", "현대차그": "현대차그룹", "삼성바이오로": "삼성바이오로직스",
        "코스피지": "코스피지수", "나스닥지": "나스닥지수", "다우존": "다우존스", "원달러환": "원달러환율",
    }


    # 리스크 점수 가중치 공식용 설정 (히트맵)
    # 1. 한국 영향권 (이게 제목에 있으면 무조건 Korea)
    KOREA_PRIORITY_KEYWORDS = [
        "정부", "관세청", "재경부", "기획재정부", "코스피", "삼성전자", "한은", "한국은행",
        "한국", "대한민국", "K-방산", "K-조선", "국내 기업", "부산", "서울",
        "기재부", "국내", "우리나라", "서산", "여수", "대산", "평택", "국정원",
        "대한상의", "SKT", "SK텔레콤", "현대차", "국회", "내수", "코스닥",
        "원화", "환율", "주유소", "오피넷", "장바구니", "물가",

    ]
    # 2. 주요 엔티티별 국가 매핑 (기업/기관명)
    ENTITY_TO_COUNTRY_MAP = {
        "삼성": "Korea", "삼성전자": "Korea", "SK하이닉스": "Korea", "현대차": "Korea",
        "HMM": "Korea", "에쓰오일": "Korea", "제주항공": "Korea", "대한항공": "Korea",
        "LG엔솔": "Korea", "한국은행": "Korea", "한은": "Korea", "공정위": "Korea",
        "엔비디아": "United States", "애플": "United States", "인텔": "United States",
        "TSMC": "Taiwan", "이란": "Iran", "이스라엘": "Israel", "HD현대": "Korea",
        "현대로템": "Korea", "개발": "Korea","SKT": "Korea", "경영": "Korea",
        '뉴욕증시': "United States", '다우': "United States", '나스닥': "United States",
        "OpenAI": "United States", "연준": "United States"
    }


