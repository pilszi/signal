import sys
import os
import subprocess
import asyncio
from typing import Dict, Any
import traceback
import sqlalchemy
from fastapi import FastAPI, Body, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager
from sse_starlette.sse import EventSourceResponse

# --- 이 코드를 반드시 추가하세요 ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
# -------------------------------

# [내부 모듈 임포트]
from logger import get_logger
from dataReqType.regist import RegistModel
from db import get_db, engine
from hash import hash_password, verify_password

# 파일 연동 (수집 및 분석 모듈)
import naver
import yna
import RSS
import indicator
import translator_worker
import ml

# 로거 및 학습 완료 증명서 설정
logger = get_logger(__name__)
TRAIN_LOCK_FILE = "train_complete.lock" # 모델 자신이 학습했는지 아닌지를 확인하는 체크포인트

# 전역 스케줄러 객체 (함수 외부에서 접근 가능하도록 설정)
global_scheduler = AsyncIOScheduler()

# ==========================================
# 0. 핵심 파이프라인 제어 (학습 및 분석 통합)
# ==========================================
async def manage_ml_pipeline(scheduler: AsyncIOScheduler):
    """
    BERT 모델 학습 여부를 체크하고, 완료되었다면 ml.run_analysis를 주기적으로 실행함
    """
    if not os.path.exists(TRAIN_LOCK_FILE):
        logger.info("📡 [Pipeline] 첫 실행: 기존 CSV 데이터를 이용한 모델 학습을 시작합니다.")
        try:
            logger.info("🧠 [Pipeline] BERT 모델 파인튜닝 가동 (subprocess)...")
            # 가상환경 파이썬으로 train_model.py 실행
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                os.path.join(os.getcwd(), "train_model.py"),  # 절대 경로로 변경
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info("🎊 [Pipeline] 모델 학습 완료! 분석 모드로 전환합니다.")
                with open(TRAIN_LOCK_FILE, "w") as f:
                    f.write(f"Finished at {os.path.getmtime('train_model.py')}")

                # BERT 분석 작업 등록 (10분)
                if not scheduler.get_job('ml_analysis'):
                    scheduler.add_job(ml.run_analysis, 'interval', minutes=10, id='ml_analysis')
            else:
                logger.error(f"❌ [Pipeline] 학습 도중 에러 발생: {stderr.decode()}")
        except Exception as e:
            logger.error(f"❌ [Pipeline] 파이프라인 실행 중 예외 발생: {str(e)}")
            logger.error(traceback.format_exc())
    else:
        # 이미 학습이 완료된 경우 바로 분석 작업 등록
        if not scheduler.get_job('ml_analysis'):
            logger.info("🚀 [Pipeline] 기존 학습 모델 확인됨. 실시간 분석 모드로 가동합니다.")
            scheduler.add_job(ml.run_analysis, 'interval', minutes=10, id='ml_analysis')


# ==========================================
# 1. 서버 생애주기(Lifespan) 설정
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 각종 수집 작업 등록 (5~10분 간격인데 나중에 운영할 때는 1시간으로 늘리기)
    global_scheduler.add_job(naver.run_naver_collect, 'interval', minutes=10, id='nc')
    global_scheduler.add_job(yna.run_yna_collect, 'interval', minutes=10, id='yc')
    global_scheduler.add_job(RSS.run_reuters_collect, 'interval', minutes=10, id='rc')
    global_scheduler.add_job(translator_worker.process_translation, 'interval', minutes=2, id='tw')
    global_scheduler.add_job(indicator.collect_market_data_job, 'interval', minutes=5, id='ic')
    # 학습/분석 파이프라인 관리 (5분마다 체크)
    global_scheduler.add_job(manage_ml_pipeline, 'interval', minutes=5, args=[global_scheduler], id='ml_pipeline')

    # 서버 시작과 동시에 즉시 실행 (백그라운드 태스크)
    asyncio.create_task(manage_ml_pipeline(global_scheduler))

    global_scheduler.start()
    logger.info("🚀 리스크 관제 시스템 통합 스케줄러 가동")
    yield
    global_scheduler.shutdown()


# FastAPI 앱 초기화
app = FastAPI(lifespan=lifespan)
# 세션 유지 시간 30분으로 연장
app.add_middleware(SessionMiddleware, secret_key="secret", max_age=1800)
app.mount("/view", StaticFiles(directory="view"), name="view")


# ==========================================
# 2. API 엔드포인트
# ==========================================
@app.get('/')
def main():
    return RedirectResponse("/view/main.html")


@app.get('/overlay')
def overlay(id: str):
    """아이디 중복 체크"""
    sql = sqlalchemy.text("SELECT EXISTS (SELECT 1 FROM member_info WHERE id = :id) as is_taken")
    with get_db() as db:
        result = db.execute(sql, {"id": id}).mappings().fetchone()
        return {"msg": bool(result["is_taken"])}


@app.post("/regist")
def regist(info: RegistModel):
    """회원가입 및 키워드 저장"""
    pw = hash_password(info.pw)
    with get_db() as db:
        sql = sqlalchemy.text("""INSERT INTO member_info (id, pw, user_name, phone_number, email)
                                 VALUES (:id, :pw, :user_name, :phone_number, :email)""")
        res = db.execute(sql, {"id": info.id, "pw": pw, "user_name": info.user_name, "phone_number": info.phone_number,
                               "email": info.email})
        member_no = res.lastrowid

        child_sql = sqlalchemy.text("INSERT INTO member_keyword (member_no, keyword) VALUES (:member_no, :keyword)")
        for key in info.keyword:
            db.execute(child_sql, {"member_no": member_no, "keyword": key})
    return {"msg": "regist OK!"}


@app.post('/login')
def login(info: Dict[str, str], req: Request):
    """로그인 처리 및 로그 기록"""
    sql = sqlalchemy.text("SELECT member_no, pw, user_name FROM member_info WHERE id = :id")
    with get_db() as db:
        result = db.execute(sql, {"id": info["id"]}).mappings().fetchone()
        if result and verify_password(info["input_pw"], result.pw):
            client_ip = req.client.host
            log_sql = sqlalchemy.text(
                "INSERT INTO member_login_log (member_no, login_ip, status) VALUES(:member_no, :login_ip, 1)")
            res = db.execute(log_sql, {"member_no": result.member_no, "login_ip": client_ip})

            # 세션에 중요 정보 기록
            req.session['login_id'] = info["id"]
            req.session['user_name'] = result.user_name
            req.session["current_log_no"] = res.lastrowid
            return {"msg": True}
    return {"msg": False}


@app.get("/logout")
def logout(req: Request):
    """로그아웃 및 로그 엔드타임 갱신"""
    log_no = req.session.get("current_log_no")
    if log_no:
        with get_db() as db:
            db.execute(
                sqlalchemy.text("UPDATE member_login_log SET logout_time = NOW(), status = 0 WHERE log_no = :log_no"),
                {"log_no": log_no})
    req.session.clear()
    return {"msg": "logout OK!"}


@app.get("/profile")
def get_profile(id: str):
    """사용자의 프로필 정보와 관심 키워드를 가져옴"""
    with get_db() as db:
        # 1. 기본 정보 조회
        user_sql = sqlalchemy.text("SELECT user_name, email, phone_number, member_no FROM member_info WHERE id = :id")
        user = db.execute(user_sql, {"id": id}).mappings().fetchone()
        if not user:
            return {"msg": "User not found"}

        # 2. 키워드 조회
        kw_sql = sqlalchemy.text("SELECT keyword FROM member_keyword WHERE member_no = :member_no")
        keywords = db.execute(kw_sql, {"member_no": user["member_no"]}).scalars().all()

        return {
            "user_name": user["user_name"],
            "email": user["email"],
            "phone_number": user["phone_number"],
            "keyword": list(keywords)
        }


@app.post("/update_profile")
def update_profile(info: Dict[str, Any]):
    """회원 정보 수정 (키워드 포함)"""
    with get_db() as db:
        # 1. 회원 번호 확인
        id_sql = sqlalchemy.text("SELECT member_no FROM member_info WHERE id = :id")
        member_no = db.execute(id_sql, {"id": info["id"]}).mappings().fetchone()["member_no"]

        # 2. 기본 정보 및 비밀번호 수정
        if info.get("pw"):
            new_pw = hash_password(info["pw"])
            sql = sqlalchemy.text(
                "UPDATE member_info SET email=:email, phone_number=:phone_number, pw=:pw WHERE id=:id")
            db.execute(sql,
                       {"id": info["id"], "email": info["email"], "phone_number": info["phone_number"], "pw": new_pw})
        else:
            sql = sqlalchemy.text("UPDATE member_info SET email=:email, phone_number=:phone_number WHERE id=:id")
            db.execute(sql, {"id": info["id"], "email": info["email"], "phone_number": info["phone_number"]})

        # 키워드 갱신
        db.execute(sqlalchemy.text("DELETE FROM member_keyword WHERE member_no = :member_no"), {"member_no": member_no})
        key_insert = 0
        for key in info.get("keyword", []):
            ins_sql = sqlalchemy.text("INSERT INTO member_keyword (member_no, keyword) VALUES(:member_no, :keyword)")
            res = db.execute(ins_sql, {"member_no": member_no, "keyword": key})
            key_insert += res.rowcount

    return {"msg": "ok", "updated_keywords": key_insert}


@app.post("/delete_member")
def delete_member(info: Dict[str, str]):
    """비밀번호 확인 후 회원 탈퇴 처리 (관련 데이터 삭제)"""
    user_id = info.get("id")
    input_pw = info.get("input_pw")

    with get_db() as db:
        # 1. 사용자 비밀번호 확인
        sql = sqlalchemy.text("SELECT member_no, pw FROM member_info WHERE id = :id")
        result = db.execute(sql, {"id": user_id}).mappings().fetchone()

        if result and verify_password(input_pw, result.pw):
            m_no = result.member_no
            # 2. 연관 데이터 삭제 (외래키 제약조건 고려 순서)
            db.execute(sqlalchemy.text("DELETE FROM member_keyword WHERE member_no = :m_no"), {"m_no": m_no})
            db.execute(sqlalchemy.text("DELETE FROM member_login_log WHERE member_no = :m_no"), {"m_no": m_no})
            db.execute(sqlalchemy.text("DELETE FROM member_info WHERE member_no = :m_no"), {"m_no": m_no})
            return {"msg": True}
    return {"msg": False}

# ==========================================
# 3. 실시간 알림 API (ml.py 활용)
# ==========================================
# 기사 라벨링 6 -[최종 리스크 라벨링(브라우저용)]: 키워드 점수 + AI 점수 => 최종 등급(색상 부여)
# 그 다음으로 아래 get_risk_signals() 함수에서 브라우저로 보내는 작업 실행
def get_risk_status(score: float) -> str:
    """숫자로 된 리스크 점수를 설계서 기준 텍스트로 변환"""
    if score >= 70: return "위기"
    if score >= 40: return "주의"
    return "안정"


@app.get("/api/risk-signals")
async def get_risk_signals():
    """Elasticsearch 기반 데이터를 가져오되, 점수를 텍스트로 변환해서 전달"""
    try:
        # ml.py의 검색 함수(get_latest_signals) 호출
        results = ml.get_latest_signals(size=10)

        # 점수를 텍스트(안정/주의/위기)로 변환하는 매핑 작업
        for item in results:
            score = item.get("risk_score", 0)
            item["risk_status"] = get_risk_status(score)
            # HTML에서 쓸 색상 클래스도 미리 정의해주면 편함.
            item["risk_color"] = "red" if item["risk_status"] == "위기" else "orange" if item[
                                                                                           "risk_status"] == "주의" else "green"
        return {"status": "success", "data": results or []}
    except Exception as e:
        logger.error(f"Error in get_risk_signals: {str(e)}")
        return {"status": "error", "message": str(e)}

# 시그널 로그를 브라우저 알림에 뜨게 해주는 함수
@app.get("/api/stream-risk")
async def stream_risk():
    """브라우저에 알림을 쏴주는 SSE 통로"""
    async def event_generator():
        while True:
            # 1분마다 시스템이 살아있음을 알림 (실제 알림 로직은 고도화 가능)
            yield {"data": "🔔 실시간 분석 엔진 가동 중"}
            await asyncio.sleep(60)

    return EventSourceResponse(event_generator())


# ==========================================
# 4. 시장 지표(환율/원자재) 조회 API
# ==========================================
# indicator_map으로 환율/원자재 라벨링 2: 매핑해줌 -> 그 다음 main.html
@app.get("/api/market-indicators")
def get_market_indicators():
    """DB에서 각 지표별 최신 수치를 가져와 브라우저로 전달"""
    # indicator_no 매핑 (indicator.py와 동일하게 맞춤)
    indicator_map = {
        1: "usd", 2: "eur", 3: "jpy", 4: "cny",
        5: "gold", 6: "silver", 7: "copper",
        8: "wti", 9: "brent", 10: "gas", 11: "oil_mini"
    }

    with get_db() as db:
        # 각 지표(indicator_no)별로 가장 최신(MAX gathering_time) 데이터만 추출하는 SQL
        sql = sqlalchemy.text("""
            SELECT t1.indicator_no, t1.price, t1.gathering_time
            FROM indicator_data t1
            INNER JOIN (
                SELECT indicator_no, MAX(gathering_time) as max_time
                FROM indicator_data
                GROUP BY indicator_no
            ) t2 ON t1.indicator_no = t2.indicator_no AND t1.gathering_time = t2.max_time
        """)

        results = db.execute(sql).mappings().all()

        # 프론트엔드가 쓰기 편하게 JSON 형태로 변환
        # 예: {"usd": 1350.5, "cny": 192.4, ...}
        formatted_data = {
            indicator_map.get(row["indicator_no"], f"unknown_{row['indicator_no']}"): row["price"]
            for row in results
        }

        return {"status": "success", "data": formatted_data}



if __name__ == "__main__":
    import uvicorn
    # reload=True는 개발 중에만 사용하기
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)