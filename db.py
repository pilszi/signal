from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import get_env

# db 연결 설정
# (한글/이모지 지원을 위한 charset 추가)
id = get_env("DB_ID")
pw = get_env("DB_PW")
host = get_env("DB_HOST", default="localhost", required=False)
port = get_env("DB_PORT", default="3306", required=False)
database = get_env("DB_NAME")
url = f"mysql+pymysql://{id}:{pw}@{host}:{port}/{database}?charset=utf8mb4"

# 엔진 생성 (pool_recycle 추가: 오래된 연결 자동 갱신)
# 스케줄러 돌리면 echo= False로 바꿔주기
engine = create_engine(
    url=url,
    echo=False,
    pool_size=5,
    pool_recycle=3600, # 1시간마다 연결 재생성 (연결 끊김 방지)
    pool_pre_ping=True  # 연결이 살아있는지 체크 후 사용
)

# 세션 생성
Session = sessionmaker(bind=engine)

# db 통로 함수
@contextmanager
def get_db():
    db_session = Session()
    try:
        yield db_session # 사용하는 곳에서 세션을 빌려줌
        db_session.commit() # 성공 시 자동 커밋
    except Exception:
        db_session.rollback() # 에러 발생 시 롤백
        raise
    finally:
        db_session.close() # 작업 끝나면 무조건 연결 닫기