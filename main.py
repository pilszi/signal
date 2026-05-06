from typing import Dict, Any

import sqlalchemy
from fastapi import FastAPI
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Body, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from dataReqType.regist import RegistModel
from db import get_db
from hash import hash_password, verify_password

app = FastAPI()
app.mount("/view", StaticFiles(directory="view"))
app.add_middleware(SessionMiddleware, secret_key="secret", max_age=600)

def chk_session(req:Request):
    return req.session.get('login_id', '')


# ==========================================
# 1. 서버 생애주기(Lifespan) 설정
# ==========================================
# 서버 시작 시 순차적으로 실행될 초기화 함수 정의
executor = ThreadPoolExecutor(max_workers=5)
async def run_initial_batch(scheduler):
    loop = asyncio.get_event_loop()
    try:
        logger.info("🎬 [초기화 시퀀스] 시작")
        # 동기 수집 함수들을 스레드 풀에서 실행
        await loop.run_in_executor(executor, naver.run_naver_collect)
        await loop.run_in_executor(executor, yna.run_yna_collect)
        await loop.run_in_executor(executor, RSS.run_reuters_collect)

        # 지표 수집 (동기 함수라면 executor 사용)
        await loop.run_in_executor(executor, indicator.collect_market_data_job)

        logger.info("🎬 [초기화 시퀀스] 2단계: 번역 작업 수행 (news_origin 생성)")
        # 수집된 데이터를 번역해서 news_origin으로 넘김
        await loop.run_in_executor(executor, translator_worker.process_translation)

        logger.info("🎬 [초기화 시퀀스] 3단계: AI 분석 파이프라인 가동")
        await manage_ml_pipeline(scheduler)

        logger.info("✅ [초기화 시퀀스] 모든 초기 배치 작업 완료")
    except Exception as e:
        logger.error(f"❌ 초기화 시퀀스 중 오류 발생: {e}")


# 실행시킬 스케줄러 함수
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 각종 수집 작업 등록 (5~10분 간격인데 나중에 운영할 때는 1시간으로 늘리기)
    global_scheduler.add_job(naver.run_naver_collect, 'interval', minutes=10, id='nc')
    global_scheduler.add_job(yna.run_yna_collect, 'interval', minutes=10, id='yc')
    global_scheduler.add_job(RSS.run_reuters_collect, 'interval', minutes=10, id='rc')
    global_scheduler.add_job(translator_worker.process_translation, 'interval', minutes=2, id='tw')
    global_scheduler.add_job(indicator.collect_market_data_job, 'interval', minutes=30, id='ic')
    # 학습/분석 파이프라인 관리 (5분마다 체크)
    global_scheduler.add_job(manage_ml_pipeline, 'interval', minutes=5, args=[global_scheduler], id='ml_pipeline')

    # 서버 시작과 동시에 즉시 실행 (백그라운드 태스크)
    asyncio.create_task(run_initial_batch(global_scheduler))

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


# 회원가입 요청 -> db member_info 와 member_keyword 에 저장
@app.post("/regist")
def regist(info: RegistModel):
    # print(f'info = {info}')
    pw = hash_password(info.pw)
    with get_db() as engine:
        parent_sql = sqlalchemy.text("""INSERT INTO member_info (id, pw, user_name, phone_number, email)
                            VALUES (:id, :pw, :user_name, :phone_number, :email)""")
        parent_res = engine.execute(parent_sql, {
            "id": info.id,
            "pw": pw,
            "user_name": info.user_name,
            "phone_number": info.phone_number,
            "email": info.email
        })
        parent_suc = parent_res.rowcount
        member_no = parent_res.lastrowid

        child_sql = sqlalchemy.text("""INSERT INTO member_keyword (member_no, keyword)
                                    VALUES (:member_no, :keyword)""")
        child_suc = 0
        for key in info.keyword:
            child_res = engine.execute(child_sql, {
                "member_no": member_no,
                "keyword": key
            })
            child_suc += child_res.rowcount
        print(f'회원 정보 저장 완료 member_info : {parent_suc} / member_keyword : {child_suc}')
    return {"msg": "regist OK!"}

# id 중복체크 요청
@app.get('/overlay')
def overlay(id:str):
    # print(f'중복체크 요청 id = {id}')
    success = False
    sql = sqlalchemy.text("""SELECT EXISTS (SELECT 1 FROM member_info WHERE id = :id) as is_taken""")
    with get_db() as engine:
        result = engine.execute(sql, {"id": id}).mappings().fetchone()
        # print(f'중복된 id 조회 결과 = {result["is_taken"]}')
        if result["is_taken"] == 1:
            success = True
    return {"msg": success}


# 로그인 요청
@app.post('/login')
def login(info:Dict[str, str], req:Request):
    success = False
    # print(f'info = {info}')
    sql = sqlalchemy.text("""SELECT member_no ,pw FROM member_info WHERE id = :id""")
    with get_db() as engine:
        result = engine.execute(sql,{"id":info["id"]}).mappings().fetchone()
        # print(f'result = {result}')
        success = verify_password(info["input_pw"], result.pw)
        # print(f'로그인 결과 = {success}')
        # 로그인 성공시 id, ip, log_no 세션에 저장
        try:
            if success:
                client_ip = req.client.host
                # print(f'접속한 ip = {client_ip}')
                login_sql = sqlalchemy.text("""INSERT INTO member_login_log (member_no, login_ip, status)
                                        VALUES(:member_no, :login_ip, 1)""")
                res = engine.execute(login_sql, {"member_no": result.member_no, "login_ip": client_ip})
                suc = res.rowcount
                # print(f' 로그인 로그 db 저장 완료 갯수 = {suc}')
                log_no = res.lastrowid
                req.session['login_id'] = info["id"]
                req.session["current_log_no"] = log_no
                # print(f'현재 저장된 세션 = {req.session}')
        except Exception as e:
            print(e)
    return {"msg": success}

# session 만료 계정 자동 로그아웃 : member_login_log 테이블 업데이트 - logout_time, status
@app.get('/session_out')
def session_out():
    count = 0
    with get_db() as engine:
        logout_sql = sqlalchemy.text("""UPDATE member_login_log SET logout_time = NOW(), status = 0
                                    WHERE status = 1 AND login_time <= NOW() - INTERVAL 60 MINUTE""")
        result = engine.execute(logout_sql)
        count = result.rowcount

    print(f'1시간이 지나 로그아웃 된 계정 갯수 = {count}')
    return {"msg": "session 만료 계정 로그아웃"}

# 로그아웃 버튼으로 로그아웃 요청 - DB 로그아웃 시간, status 업데이트, session 삭제
@app.get("/logout")
def logout(req:Request):
    log_no = req.session.get("current_log_no", "값이 없음")
    print(f'log_no = {log_no}')
    with get_db() as engine:
        logout_sql = sqlalchemy.text("""UPDATE member_login_log SET logout_time = NOW(), status = 0 
                                WHERE log_no = :log_no""")
        result = engine.execute(logout_sql, {"log_no": log_no})
        success = result.rowcount
        print(f'로그아웃 완료 계정 = {success}개')
    req.session.clear()
    return {"msg": "logout OK!"}


# 회원 탈퇴 요청
@app.post("/delete_member")
def delete_member(info:Dict[str, str]):
    # print(f'탈퇴 요청 정보 = {info}')
    success = False
    delete_cnt = 0
    # 계정 id로 DB에서 조회한 비밀번호와 사이트에서 입력한 비밀번호 확인 값 비교하여 같으면 계정 삭제
    sql = sqlalchemy.text("""SELECT pw FROM member_info WHERE id = :id""")
    with get_db() as engine:
        result = engine.execute(sql, {"id": info["id"]}).mappings().fetchone()
        success = verify_password(info["input_pw"], result.pw)
        if success:
            sql = sqlalchemy.text("""DELETE FROM member_info WHERE id = :id""")
            result = engine.execute(sql, {"id": info["id"]})
            delete_cnt = result.rowcount
    print(f'삭제 처리 된 계정 갯수 = {delete_cnt}')

    return {"msg": success}

# 개인 profile 페이지 요청
@app.get('/profile')
def profile(id:str):
    # print(f'프로필 페이지 요청 id = {id}')
    with get_db() as engine:
        # 1. profile 요청 id 로 member_no, user_name, email, phone_number 조회
        sql = sqlalchemy.text("""SELECT member_no, user_name, email, phone_number FROM member_info WHERE id = :id""")
        id_result = engine.execute(sql, {"id": id}).mappings().fetchone()
        # print(f'member_no = {result}')

        # 2. member_no 로 관심키워드 조회
        sql = sqlalchemy.text("""SELECT keyword FROM member_keyword WHERE member_no = :member_no""")
        no_result = engine.execute(sql, {"member_no": id_result["member_no"]}).mappings().fetchall()
        # print(f'member_no 에 해당하는 keyword = {no_result}')
        kewords = []
        for key in no_result:
            # print(key)
            kewords.append(key["keyword"])
        # print(f'keyword = {kewords}')
    return {"user_name": id_result.user_name, "email": id_result.email, "phone_number": id_result.phone_number, "keyword": kewords}


# 개인정보 수정 요청
@app.post("/update_profile")
def update_profile(info:Dict[str, Any]):
    print(f'수정 요청 = {info}')
    with get_db() as engine:
        sql = sqlalchemy.text("""SELECT member_no FROM member_info WHERE id = :id""")
        id_result = engine.execute(sql, {"id": info["id"]}).mappings().fetchone()
        print(id_result)
        # 패스워드 수정 여부에 따른 조건문
        if "pw" in info and info["pw"]:
            pw = hash_password(info["pw"])
            sql = sqlalchemy.text("""UPDATE member_info SET 
                            email = :email, phone_number = :phone_number, pw = :pw WHERE id = :id""")
            update_res = engine.execute(sql, {"id": info["id"],"email": info["email"], "phone_number": info["phone_number"], "pw": pw})
        else:
            sql = sqlalchemy.text("""UPDATE member_info SET email = :email, phone_number = :phone_number WHERE id = :id""")
            update_res = engine.execute(sql, {"id": info["id"], "email": info["email"], "phone_number": info["phone_number"]})

        # 키워드 수정
        del_sql = sqlalchemy.text("""DELETE FROM member_keyword WHERE member_no = :member_no""")
        del_res = engine.execute(del_sql, {"member_no": id_result.member_no})
        print({f"키워드 삭제 = {del_res.rowcount}"})
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
        for key in info["keyword"]:
            insert_sql = sqlalchemy.text("""INSERT INTO member_keyword (member_no, keyword) VALUES(:member_no, :keyword)""")
            insert_res = engine.execute(insert_sql, {"member_no": id_result["member_no"], "keyword": key})
            key_insert += insert_res.rowcount
        print(f'키워드 수정 = {key_insert}')
    return {"msg": "ok"}