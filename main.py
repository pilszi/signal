import json
import sys
import os
import subprocess
import math
import random
import asyncio
from typing import Dict, Any
import traceback
import sqlalchemy
import notifier
from db import SessionLocal
from fastapi import Depends
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from fastapi import FastAPI, Request, Query
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
from db import get_db
from hash import hash_password, verify_password
from config import Config
from utils import prepare_heatmap_data, save_analysis_result


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

# es 설정
es = Elasticsearch(["http://100.123.232.79:9200"])
# es = Elasticsearch(['http://localhost:9200'])



# ==========================================
# 0. 핵심 파이프라인 제어 (학습 및 분석 통합)
# ==========================================
is_processing = False

# 분석 실행 후 결과를 DB에 저장하는 함수
async def run_analysis_and_save():
    global is_processing

    # 이미 실행 중이면 중복 실행 차단
    if is_processing:
        logger.warning("⚠️ 이미 분석 작업 실행 중 - 이번 주기 스킵")
        return

    is_processing = True
    session = None

    try:
        logger.info("🧠 [Analysis] AI 분석 및 DB 저장 시퀀스 시작")

        results = await ml.run_analysis()

        if not results:
            logger.info("❌ [Analysis] 분석할 기사가 없습니다.")
            return

        processed_ids = []

        # DB 세션 생성
        session = SessionLocal()
        for res in results:
            try:
                # ==========================================
                # [STEP 1] MariaDB 저장
                # ==========================================
                new_signal_no = save_analysis_result(session, res)

                if not new_signal_no:
                    logger.error(f"❌ 저장 실패: {res.get('title')}")
                    session.rollback()
                    continue

                processed_ids.append(res.get('doc_id'))

                logger.info(
                    f"✅ [DB Success] signal_no: {new_signal_no}"
                )

                # ==========================================
                # [STEP 2] 키워드 매칭
                # ==========================================
                news_keywords = res.get('keywords', [])
                if news_keywords:

                    match_sql = sqlalchemy.text("""
                                    SELECT DISTINCT m.member_no, m.email
                                    FROM member_keyword mk
                                    JOIN member_info m
                                      ON mk.member_no = m.member_no
                                    WHERE :prediction LIKE CONCAT('%', mk.keyword, '%')
                                       OR :reason LIKE CONCAT('%', mk.keyword, '%')
                                """)

                    matched_users = session.execute(
                        match_sql,
                        {
                            "prediction": res.get('prediction', ''),
                            "reason": res.get('reason', '')
                        }
                    ).fetchall()

                    # 사용자별 처리
                    for user in matched_users:
                        logger.info(
                            f"🎯 매칭 성공! 사용자: {user.member_no}"
                        )

                        # ----------------------------------
                        # A. alarm_log 저장
                        # ----------------------------------
                        alarm_sql = sqlalchemy.text("""
                                        INSERT INTO alarm_log
                                        (member_no, signal_no, alarm_time, alarm_view)
                                        VALUES (:m_no, :s_no, NOW(), 0)
                                        ON DUPLICATE KEY UPDATE
                                            alarm_time = NOW()
                                    """)

                        session.execute(
                            alarm_sql,
                            {
                                "m_no": user.member_no,
                                "s_no": new_signal_no
                            }
                        )

                        # ----------------------------------
                        # B. 심각 단계 이메일 발송
                        # ----------------------------------
                        if res.get('level') == '심각':
                            try:
                                notifier.send_emergency_email(
                                    to_email=user.email,
                                    ai_report={
                                        'prediction': res.get('prediction'),
                                        'reason': res.get('reason')
                                    },
                                    news_url=res.get('url'),
                                    risk_level='심각',
                                    keywords_str=news_keywords,
                                    title=res.get('title','리스크 감지 알림')
                                )

                            except Exception as mail_err:
                                logger.error(
                                    f"📧 이메일 발송 실패 "
                                    f"({user.email}): {mail_err}"
                                )

                # ==========================================
                # [STEP 3] 기사 단위 commit
                # ==========================================
                session.commit()

            # ==========================================
            # 기사 단위 rollback
            # ==========================================
            except Exception:
                session.rollback()
                logger.error(traceback.format_exc())
                continue

            # ==========================================
            # [STEP 4] ES 상태 업데이트
            # ==========================================
        if processed_ids:

            for doc_id in processed_ids:
                await ml.update_es_status(
                    doc_id,
                    True,
                    refresh=True
                )

            logger.info(
                f"🚀 ES 업데이트 완료: {len(processed_ids)}건"
            )

    # ==========================================
    # 전체 배치 예외 처리
    # ==========================================
    except Exception:

        if session:
            session.rollback()

        logger.error(traceback.format_exc())

    # ==========================================
    # 세션 종료 및 잠금 해제
    # ==========================================
    finally:

        if session:
            session.close()

        is_processing = False
# ==========================================
# 1. 서버 생애주기(Lifespan) 설정
# ==========================================
# 서버 시작 시 순차적으로 실행될 초기화 함수 정의
executor = ThreadPoolExecutor(max_workers=5)
async def run_initial_batch(scheduler):
    loop = asyncio.get_event_loop()
    try:
        logger.info("🎬 [초기화 시퀀스] 1단계: 뉴스 수집 시작")

        # 동기 수집 함수들을 스레드 풀에서 실행
        await loop.run_in_executor(executor, naver.run_naver_collect)
        await loop.run_in_executor(executor, yna.run_yna_collect)
        await loop.run_in_executor(executor, RSS.run_reuters_collect)
        await loop.run_in_executor(executor, indicator.collect_market_data_job)

        logger.info("🎬 [초기화 시퀀스] 2단계: 번역 작업 수행")
        # 수집된 데이터를 번역해서 news_origin으로 넘김
        await loop.run_in_executor(executor, translator_worker.process_translation)

        logger.info("🎬 [초기화 시퀀스] 3단계: 분석 및 저장 가동")
        await run_analysis_and_save()

        logger.info("✅ [초기화 시퀀스] 모든 공정(수집-번역-분석) 완료!")
    except Exception as e:
        logger.error(f"❌ 초기화 시퀀스 중 오류 발생: {e}")


# 실행시킬 스케줄러 함수
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 각종 수집 작업 등록 (5~10분 간격인데 나중에 운영할 때는 1시간으로 늘리기)
    global_scheduler.add_job(naver.run_naver_collect, 'interval', minutes=30, id='nc')
    global_scheduler.add_job(yna.run_yna_collect, 'interval', minutes=30, id='yc')
    global_scheduler.add_job(RSS.run_reuters_collect, 'interval', minutes=30, id='rc')
    global_scheduler.add_job(translator_worker.process_translation, 'interval', minutes=10, id='tw',max_instances=1)
    global_scheduler.add_job(indicator.collect_market_data_job, 'interval', minutes=30, id='ic')
    global_scheduler.add_job(run_analysis_and_save,'interval',minutes=10,id='ml_analysis',max_instances=1,replace_existing=True)
    # 서버를 먼저 열고, 수집은 백그라운드에서 비동기로 실행
    asyncio.create_task(run_initial_batch(global_scheduler))

    global_scheduler.start()
    logger.info("🚀 리스크 관제 시스템 통합 스케줄러 가동")

    yield

    # --- 서버 종료 시 정리 로직 ---
    logger.info("🛑 서버 종료 중: 실행 중인 작업들을 정리합니다.")
    global_scheduler.shutdown(wait=False)  # 스케줄러 즉시 정지
    executor.shutdown(wait=False, cancel_futures=True)  # 스레드 풀 강제 종료


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
def overlay(id: str, db: Session = Depends(get_db)):
    """아이디 중복 체크"""
    sql = sqlalchemy.text("SELECT EXISTS (SELECT 1 FROM member_info WHERE id = :id) as is_taken")
    result = db.execute(sql, {"id": id}).mappings().fetchone()
    return {"msg": bool(result["is_taken"])}


@app.post("/regist")
def regist(info: RegistModel, req: Request, db: Session = Depends(get_db)):
    """회원가입 및 키워드 저장"""
    # [백엔드 이중 보안] 진짜 이메일 인증 단계를 통과했는지 세션 최종 검증
    session_data = req.session.get("regist_auth")
    if not session_data or session_data["email"] != info.email or not session_data["verified"]:
        return {"msg": "이메일 인증이 완료되지 않았거나 세션이 만료되었습니다."}

    pw = hash_password(info.pw)

    sql = sqlalchemy.text("""INSERT INTO member_info (id, pw, user_name, phone_number, email)
                             VALUES (:id, :pw, :user_name, :phone_number, :email)""")
    res = db.execute(sql, {"id": info.id, "pw": pw, "user_name": info.user_name, "phone_number": info.phone_number,
                           "email": info.email})
    member_no = res.lastrowid

    child_sql = sqlalchemy.text("INSERT INTO member_keyword (member_no, keyword) VALUES (:member_no, :keyword)")
    for key in info.keyword:
        db.execute(child_sql, {"member_no": member_no, "keyword": key})
    # 가입 성공 시 인증 관련 세션 파기하여 깔끔하게 정리
    req.session.pop("regist_auth", None)
    db.commit()
    return


# 회원가입 인증번호 요청
@app.post("/request_auth")
def request_auth(email: str):
    import random
    # 1. 6자리 랜덤 번호 생성
    auth_code = str(random.randint(100000, 999999))

    # 여기에 인증번호 호출 코드를 넣기
    notifier.send_emergency_email(
        to_email=email,
        ai_report={'prediction': auth_code},
        news_url=None,
        risk_level="AUTH",
        keywords_str=None,
        title="회원가입 인증번호"
    )

    # 이후 생성된 코드를 DB나 세션에 저장하여 검증
    return {"msg": "인증번호가 발송되었습니다."}




# 회원가입 1단계: 이메일 발송 요청
@app.post("/regist/request_code")
def regist_request_code(info: Dict[str, str], req: Request):
    email = info.get("email")
    if not email:
        return {"res": False, "msg": "이메일 주소를 입력해주세요."}

    # 6자리 랜덤 번호 생성
    auth_code = str(random.randint(100000, 999999))

    # 세션에 이메일과 매핑하여 인증코드 임시 보관
    req.session["regist_auth"] = {"email": email, "code": auth_code, "verified": False}

    # 만능 알림 함수 호출 (AUTH 모드 가동)
    success = notifier.send_emergency_email(
        to_email=email,
        ai_report={'prediction': auth_code},
        news_url=None,
        risk_level="AUTH",
        keywords_str=None,
        title="회원가입 이메일 인증번호"
    )
    return {"res": success, "msg": "인증코드가 발송되었습니다." if success else "메일 발송에 실패했습니다."}


# 회원가입 2단계: 인증코드 검증
@app.post("/regist/verify_code")
def regist_verify_code(info: Dict[str, str], req: Request):
    input_code = info.get("code")
    session_data = req.session.get("regist_auth")

    if not session_data or session_data["code"] != input_code:
        return {"res": False, "msg": "인증코드가 일치하지 않습니다."}

    # 검증 성공 상태를 세션에 업데이트
    session_data["verified"] = True
    req.session["regist_auth"] = session_data
    return {"res": True}



@app.post('/login')
def login(info: Dict[str, str], req: Request, db: Session = Depends(get_db)):
    """로그인 처리 및 로그 기록"""
    sql = sqlalchemy.text("SELECT member_no, pw, user_name FROM member_info WHERE id = :id")

    result = db.execute(sql, {"id": info["id"]}).mappings().fetchone()
    if result and verify_password(info["input_pw"], result.pw):
        client_ip = req.client.host
        log_sql = sqlalchemy.text(
            "INSERT INTO member_login_log (member_no, login_ip, status) VALUES(:member_no, :login_ip, 1)")
        db.execute(log_sql, {
            "member_no": result.member_no,
            "login_ip": client_ip
        })

        # 세션 저장
        req.session['login_id'] = info["id"]
        req.session['user_name'] = result.user_name
        req.session["member_no"] = result.member_no
        db.commit()
        return {"msg": True}
    else:
        return {"msg": False}


@app.get("/logout")
def logout(req: Request, db: Session = Depends(get_db)):
    """로그아웃 및 로그 엔드타임 갱신"""
    member_no = req.session.get("member_no")
    if member_no:
        db.execute(
            sqlalchemy.text("""
                    UPDATE member_login_log
                    SET logout_time = NOW(),
                        status = 0
                    WHERE member_no = :member_no
                    AND status = 1
                """),
            {"member_no": member_no}
        )

    req.session.clear()
    return

# session 만료 계정 자동 로그아웃 : member_login_log 테이블 업데이트 - logout_time, status
@app.get('/session_out')
def session_out(db: Session = Depends(get_db)):
    count = 0

    logout_sql = sqlalchemy.text("""UPDATE member_login_log SET logout_time = NOW(), status = 0
                                WHERE status = 1 AND login_time <= NOW() - INTERVAL 60 MINUTE""")
    result = db.execute(logout_sql)
    count = result.rowcount

    logger.info(f'1시간이 지나 로그아웃 된 계정 갯수 = {count}')
    return

@app.get("/profile")
def get_profile(id: str, db: Session = Depends(get_db)):
    """사용자의 프로필 정보와 관심 키워드를 가져옴"""

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
def update_profile(info: Dict[str, Any], db: Session = Depends(get_db)):
    """회원 정보 수정 (키워드 포함)"""

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
    bef_key = db.execute(sqlalchemy.text("""SELECT keyword FROM member_keyword WHERE member_no = :member_no"""), {"member_no": member_no}).mappings().fetchall()
    bef_keyword = []
    for k in bef_key:
        bef_keyword.append(k["keyword"])
    db.execute(sqlalchemy.text("DELETE FROM member_keyword WHERE member_no = :member_no"), {"member_no": member_no})
    key_insert = 0
    for key in info.get("keyword", []):
        ins_sql = sqlalchemy.text("INSERT INTO member_keyword (member_no, keyword) VALUES(:member_no, :keyword)")
        res = db.execute(ins_sql, {"member_no": member_no, "keyword": key})
        key_insert += res.rowcount
    db.commit()
    return {"updated_keywords": key_insert}


@app.post("/delete_member")
def delete_member(info: Dict[str, str], db: Session = Depends(get_db)):
    """비밀번호 확인 후 회원 탈퇴 처리 (관련 데이터 삭제)"""
    user_id = info.get("id")
    input_pw = info.get("input_pw")

    # 1. 사용자 비밀번호 확인
    sql = sqlalchemy.text("SELECT member_no, pw FROM member_info WHERE id = :id")
    result = db.execute(sql, {"id": user_id}).mappings().fetchone()

    if result and verify_password(input_pw, result.pw):
        m_no = result.member_no
        # 2. 연관 데이터 삭제 (외래키 제약조건 고려 순서)
        db.execute(sqlalchemy.text("DELETE FROM member_keyword WHERE member_no = :m_no"), {"m_no": m_no})
        db.execute(sqlalchemy.text("DELETE FROM member_login_log WHERE member_no = :m_no"), {"m_no": m_no})
        db.execute(sqlalchemy.text("DELETE FROM member_info WHERE member_no = :m_no"), {"m_no": m_no})
        save_admin_log(db=db, log_type='delete', title='회원탈퇴', target_id=user_id, content=f'{user_id} 님의 탈퇴요청',
                   before_data={"member_no": m_no})
        db.commit() # 3. 여기서 최종 확정!
        return {"msg": True}
    else:
        return {"msg": False}


# main 페이지 오늘의 뉴스 조회
@app.get("/public_signals")
def public_signals():
    """ 메인 페이지 기사 요청 및 필터링 """
    # [1] 검색 조건 설정 (날짜가 있으면 해당 날짜, 없으면 최근 7일치)
    date_filter = {
        "range": {
            "published_date": {
                "gte": "now-7d",
                "lte": "now",
                "time_zone": "+09:00"  # 한국 시간 기준
            }
        }
    }
    #
    #     date_filter = {
    #         "range": {
    #             "published_date": {
    #                 "gte": "now-15d",
    #                 "lte": "now",
    #                 "time_zone": "+09:00"
    #             }
    #         }
    #     }

    # [2] 최종 Elasticsearch 쿼리 바디
    search_body = {
        "query": {
            "bool": {
                "filter": [date_filter]
            }
        },
        "sort": [{"published_date": "desc"}],
        "size": 100
    }
    try:
        res = es.search(index="news_labeling", body=search_body)
        logger.info(f"👉가져온 기사 갯수 = {res['hits']['total']['value']}")

        public_news = []
        seen = set()

        for news in res['hits']['hits']:
            source = news.get("_source", {})
            # print(f'👉: source.keys()')
            # print(source)

            title = source.get("title", "").strip()
            url = source.get("url", "").strip()

            # URL 있으면 URL 기준, 없으면 제목 기준
            key = url if url else title

            # 중복 제거
            if key in seen:
                continue
            seen.add(key)

            # [3] 모든 필드에 .get()을 사용하여 방어적으로 데이터 추출
            _news = {
                'title': title,
                'url': url,
                'main_image': source.get("main_image") or "https://via.placeholder.com/400x300?text=No+Image",
                'published_date': source.get("published_date"),
                'press_name': source.get("press_name", "알 수 없음"),
                'risk_level': source.get("risk_level", "안정"),
                'risk_score': source.get("final_total_score", {}).get("total", 0),
                'country_name': source.get("country_name"),
                'news_origin': source.get("news_origin"),
                'news_labeling': source.get("news_labeling")
            }
            public_news.append(_news)

        return {"msg": public_news}

    except Exception as e:
        logger.info(f"❌ 검색 중 오류 발생: {e}")
        return {"msg": [], "error": str(e)}



@app.get("/main/country")
def country(db: Session = Depends(get_db)):
    """ 히트맵 데이터 요청 """
    sql = sqlalchemy.text("""
            SELECT
                t3.country_en_name as en_name,
                COUNT(CASE WHEN t2.risk_level = '안정' THEN 1 END) AS 안정_count,
                COUNT(CASE WHEN t2.risk_level = '주의' THEN 1 END) AS 주의_count,
                COUNT(CASE WHEN t2.risk_level = '심각' THEN 1 END) AS 심각_count
            FROM signal_country t1
            JOIN signal_message t2 ON t1.signal_no = t2.signal_no
            JOIN country t3 ON t3.country_no = t1.country_no
            WHERE 
                t2.signal_time >= CASE 
                    WHEN HOUR(NOW()) >= 24 THEN CURDATE()                       -- 오늘 오후라면 오늘 자정부터
                    ELSE CURDATE() - INTERVAL 24 HOUR                           -- 오늘 오전이라면 어제 정오부터
                END
                AND 
                t2.signal_time < CASE 
                    WHEN HOUR(NOW()) >= 24 THEN CURDATE() + INTERVAL 24 HOUR    -- 오늘 오후라면 오늘 정오까지
                    ELSE CURDATE()                                              -- 오늘 오전이라면 오늘 자정까지
                END
            GROUP BY t1.country_no, t3.country_en_name
        """)

    country_all = db.execute(sql).mappings().fetchall()
    logger.info(f'🔍 country_all = {country_all}')

    signal_country = []
    for c in country_all:
        severe = c["심각_count"]
        caution = c["주의_count"]
        stable = c["안정_count"]

        # -----------------------------------------------------------------
        # [STEP 1] 최고 위험 단계를 기준으로 기본 베이스라인 점수 획득 (보안 구멍 차단)
        # -----------------------------------------------------------------
        if severe >= 3:
            base_score = 300  # 심각 기사가 3개 이상 쌓이면 무조건 '심각(Red)' 등급 진입
        elif severe > 0:
            base_score = 200  # 🚨 단 1개의 심각 기사만 있어도 무조건 최소 '주의(Yellow)' 등급 보장!
        elif caution >= 5:
            base_score = 200  # 심각은 없지만 주의 단계 기사가 5개 이상 뭉치면 경계 등급 격상
        elif caution > 0:
            base_score = 100  # 주의 기사가 소수 존재할 때
        else:
            base_score = 50  # 오직 안정 기사만 존재할 때

        # -----------------------------------------------------------------
        # [STEP 2] 개수(Volume) 증가에 따른 로그 감쇠 가중치 합산 (점수 독점 방지)
        # 💡 math.log1p(x)는 ln(x+1)을 의미하며, 기사 수가 무한히 늘어나도 점수 상승폭이 완만해집니다.
        # -----------------------------------------------------------------
        severe_bonus = math.log1p(severe) * 25
        caution_bonus = math.log1p(caution) * 12
        stable_bonus = math.log1p(stable) * 2

        # 최종 가중치 스코어 합산 후 정밀 정수형 변환 (프론트엔드 호환용)
        final_score = int(base_score + severe_bonus + caution_bonus + stable_bonus)

        signal_country.append({
            "en_name": c["en_name"],
            "total_score": final_score
        })
    logger.info(f'📊 [정규화 완료] 최적화된 signal_country 점수: {signal_country}')
    return {"country_signal": signal_country}


# 기사 열람 기록 저장
@app.post("/news_view")
def news_view(info:Dict[str, str], db: Session = Depends(get_db)):
    print(f'{info["id"]} 가 열람한 기사 url = {info["url"]}')

    sql = sqlalchemy.text("""SELECT member_no FROM member_info WHERE id = :id""")
    user = db.execute(sql, {"id": info["id"]}).mappings().fetchone()
    if user:
        m_no = user["member_no"]
        sql = sqlalchemy.text("""SELECT count(news_url) as cnt FROM news_view WHERE member_no = :member_no AND news_url = :news_url""")
        chk_res = db.execute(sql, {"member_no": m_no, "news_url": info["url"]}).mappings().fetchone()

        if chk_res["cnt"] == 0:
            sql = sqlalchemy.text("""INSERT INTO news_view (member_no, news_url)VALUES(:member_no, :news_url)""")
            res = db.execute(sql, {"member_no": m_no, "news_url": info["url"]})
            print(f'기사열람 DB 삽입 성공 {res.rowcount}개')
    db.commit()
    return


# 맞춤형뉴스 요청
# [main.py 내부 - custom_news 함수 완전히 덮어씌우기]
@app.get("/custom_news")
def custom_news(id: str, db: Session = Depends(get_db)):
    """ 맞춤형 뉴스 데이터 요청 """
    logger.info(f'📡 맞춤형 요청 id = {id}')

    # 1. 사용자의 관심 키워드 조회
    kw_sql = sqlalchemy.text("""
        SELECT mk.keyword 
        FROM member_keyword mk 
        JOIN member_info m ON mk.member_no = m.member_no 
        WHERE m.id = :id
    """)
    user_keywords = db.execute(kw_sql, {"id": id}).scalars().all()

    # 2. 사용자가 읽은 기사 URL 목록 조회
    view_sql = sqlalchemy.text("""
        SELECT nv.news_url 
        FROM news_view nv 
        JOIN member_info m ON nv.member_no = m.member_no 
        WHERE m.id = :id
    """)
    read_urls = db.execute(view_sql, {"id": id}).scalars().all()

    # 키워드가 없으면 바로 빈 결과 반환
    if not user_keywords:
        return {"keyword": [], "total_val": 0, "news": []}

    # 공백 제거 및 중복 제거
    clean_keywords = list(set(k.strip() for k in user_keywords if k.strip()))

    # 3. Elasticsearch 쿼리 구성 (💡 term 쿼리를 대소문자/공백 방어형 match 쿼리로 업그레이드)
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                # match 쿼리로 바꾸어도 _name 속성을 통해 matched_queries 추출이 가능합니다!
                                {"match": {"keywords": {"query": kw, "_name": kw}}}
                                for kw in clean_keywords
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "published_date": {
                                "gte": "now-30d",  # 57건을 보려면 범위를 넉넉히 잡으세요
                                "lte": "now"
                            }
                        }
                    }
                ]

            }
        },
        "sort": [{"published_date": "desc"}],
        "size": 100
    }

    # 4. ES 검색 실행
    res = es.search(index="news_labeling", body=body)
    logger.info(f'es에서 가져왔는지 확인: {res['hits']['total']['value']}')

    custom_news_list = []
    for hit in res['hits']['hits']:
        source = hit["_source"]
        # 매칭된 쿼리 배열 추출
        display_keyword = hit.get('matched_queries', [])
        logger.info(f"🎯 매칭 키워드 = {display_keyword}")

        custom_news_list.append({
            'title': source.get("title"),
            'url': source.get("url"),
            'main_image': source.get("main_image") or "https://via.placeholder.com/400x300",
            'published_date': source.get("published_date"),
            'press_name': source.get("press_name", "알 수 없음"),
            'keyword': display_keyword, # 배열 구조 유지
            'risk_score': source.get("final_total_score", {}).get("total", 0),
            'risk_level': source.get("risk_level", "안정"),
            'is_read': source.get("url") in read_urls
        })

    logger.info(f'✅ {id}님의 맞춤형 뉴스 {len(custom_news_list)}개 추출 완료')
    return {
        "keyword": clean_keywords,
        "total_val": len(custom_news_list),
        "news": custom_news_list
    }

# 시그널로그 페이지
@app.get("/signal_log")
def signal_log(id: str, db: Session = Depends(get_db)):
    """ 로그인한 사용자의 관심 키워드 기반 시그널만 조회
    (ES 검색 대신, 분석 시점에 미리 매칭된 alarm_log 테이블 활용)
    """
    logger.info(f"📋 {id}님의 시그널 로그 조회 요청")


    # alarm_log와 signal_message를 조인하여 사용자의 개인화된 리스트 추출
    sql = sqlalchemy.text("""
        SELECT 
            sm.signal_no AS id,
            sm.risk_level, 
            sm.signal_time, 
            sm.prediction, 
            sm.prediction_reason,
            sm.url,
            al.alarm_view,
            GROUP_CONCAT(DISTINCT mk.keyword SEPARATOR ', ') as matched_keywords
        FROM alarm_log al
        JOIN signal_message sm ON al.signal_no = sm.signal_no
        JOIN member_info m ON al.member_no = m.member_no
        LEFT JOIN member_keyword mk ON m.member_no = mk.member_no 
            AND (sm.prediction LIKE CONCAT('%', mk.keyword, '%') 
                 OR sm.prediction_reason LIKE CONCAT('%', mk.keyword, '%'))
        WHERE m.id = :user_id
        GROUP BY sm.signal_no, al.alarm_view, sm.risk_level, sm.signal_time, sm.prediction, sm.prediction_reason, sm.url
        ORDER BY al.alarm_time DESC
        LIMIT 50
    """)

    # 쿼리 실행 및 맵핑 결과 획득
    rows = db.execute(sql, {"user_id": id}).mappings().fetchall()
    if not rows:
        logger.warning(f"⚠️ {id}님의 알림 로그가 비어있습니다. (alarm_log 확인 필요)")

    # 텅 비어있던 doc_no를 데이터로 채워주기
    doc_no = []
    for r in rows:
        kw_list = r["matched_keywords"].split(',') if r["matched_keywords"] else []
        doc_no.append({
            "id": r["id"],
            "risk_level": r["risk_level"],
            "signal_time": r["signal_time"].strftime("%Y-%m-%d %H:%M") if r["signal_time"] else "시간 미상",
            "prediction": r["prediction"],
            "prediction_reason": r["prediction_reason"],
            "url": r["url"] or "#",
            "is_read": r["alarm_view"],
            "match_keyword": kw_list
        })

    logger.info(f"✅ {id}님의 signal {len(doc_no)}건 반환 완료")
    return {"data": doc_no}



# signal 알림 토글 요청
@app.get("/noti/signal")
def noti_signal(id:str, db: Session = Depends(get_db)):
    """ 페이지 상단 네비게이션바 통합 알림 요청 """
    try:
        logger.info(f"📋 {id}님의 시그널 로그 알림 요청")

        sql = sqlalchemy.text("""
            SELECT
                t1.signal_no,
                t1.risk_level,
                t1.prediction AS message,
                t1.signal_time AS time,
                t2.alarm_view AS is_read
            FROM signal_message t1
            JOIN alarm_log t2 ON t1.signal_no = t2.signal_no
            JOIN member_info t3 ON t2.member_no = t3.member_no
            WHERE t3.id = :id
            AND t2.alarm_view = 0 
            AND t1.risk_level = '심각'
            ORDER BY t2.alarm_time DESC 
            """)
        res = db.execute(sql, {"id": id}).mappings().fetchall()
        # logger.info(f'에러나는지 확인 : {res}')

        notis = []
        for n in res:
            # DB의 datetime 객체를 프론트엔드가 다루기 좋게 문자열로 포맷팅
            time_str = n["time"].strftime("%Y-%m-%d %H:%M") if n.get("time") else ""
            notis.append({
                "id": n["signal_no"], # 구형 프론트엔드 호환용
                "signal_no": n["signal_no"], # 신형 스크립트 매핑용 (moveToSignalLog 사용)
                "type": "emergency",
                "risk_level": n.get("risk_level", "심각"),
                "title": f"{n.get('risk_level', '심각')} 위험 시그널",
                "message": n.get("message", "리스크 감지"),
                "is_read": n.get("is_read", 0),
                "time": time_str
            })
        return {"noti": notis}
    except Exception as e:
        # 💡 터미널에 찍히는 진짜 에러 내용을 여기서 확실히 보여줍니다.
        logger.error(f"❌ 알림 API 오류 발생: {traceback.format_exc()}")
        return {"noti": [], "error": str(e)}


# 알림토글 읽음 요청
@app.post("/noti/read")
def noti_read(info: Dict[str, Any], db: Session = Depends(get_db)):
    """ 알림 클릭 시 해당 알림 확인 상태 변경 """
    sql = sqlalchemy.text("""
            UPDATE alarm_log t1 JOIN member_info t2 
                ON t1.member_no = t2.member_no 
                    SET t1.alarm_view = 1 
                        WHERE t1.signal_no = :signal_no AND t2.id = :id
        """)
    res = db.execute(sql, {"signal_no": info["id"], "id": info["user_id"]})
    logger.info(f'👉 알림 확인 업데이트 완료 = {res.rowcount}개')
    db.commit()
    return {"res": True}


# X 버튼 클릭 시: 알림판에서만 제외 (DB 데이터는 보존)
@app.post("/noti/delete")
def noti_delete(info: Dict[str, Any], db: Session = Depends(get_db)):
    """ 사용자가 X 버튼을 눌러 알림판에서 숨김 처리 (Hard Delete ➔ Soft Hide) """

    # alarm_view 상태만 1로 바꿉니다.
    sql = sqlalchemy.text("""
        UPDATE alarm_log t1 
        JOIN member_info t2 ON t1.member_no = t2.member_no
        SET t1.alarm_view = 1 
        WHERE t1.signal_no = :signal_no AND t2.id = :id
    """)
    res = db.execute(sql, {"signal_no": info.get("id"), "id": info.get("user_id")})
    logger.info(f"👀 알림 숨김(읽음) 처리 완료 = {res.rowcount}개")
    db.commit()
    return {"res": True}


# 모든 알림토글 읽음 요청
@app.post("/noti/read_all")
def noti_raed_all(info: Dict[str, Any], db: Session = Depends(get_db)):
    """ 하단 '모두 읽음으로 표시' 클릭 시 드롭다운 일괄 드롭 """
    user_id = info.get("user_id")

    sql = sqlalchemy.text("""
            UPDATE alarm_log t1 
            JOIN member_info t2 ON t1.member_no = t2.member_no
            SET t1.alarm_view = 1 
            WHERE t1.alarm_view = 0 AND t2.id = :id
        """)
    res = db.execute(sql, {"id": user_id})
    logger.info(f"🧹 {user_id}의 모든 알림 읽음 처리 완료 = {res.rowcount}개")
    db.commit()
    return {"res": True}


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

# 현재 발생한 전체 시그널을 보여주는 조회용 API
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
def get_market_indicators(db: Session = Depends(get_db)):
    """DB에서 각 지표별 최신 수치를 가져와 브라우저로 전달"""
    # indicator_no 매핑 (indicator.py와 동일하게 맞춤)
    indicator_map = {
        1: "usd", 2: "eur", 3: "jpy", 4: "cny",
        5: "gold", 6: "silver", 7: "copper",
        8: "wti", 9: "brent", 10: "gas", 11: "oil_mini"
    }


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



# DB에 저장된 국가명 테이블 데이터 불러오기
@app.get("/main/countryMap")
def country_map(db: Session = Depends(get_db)):
    logger.info(f'----- 국가명 데이터 가져오기 -----')

    sql = sqlalchemy.text("""SELECT country_kr_name, country_en_name FROM country""")
    res = db.execute(sql).mappings().fetchall()
    # logger.info(f'가져온 국가명 데이터 {res}')

    return {"res": res}



# ===============================
#        find ID / PW
# ===============================
# 아이디 찾기 1단계: 인증코드 요청
@app.post("/find_id/request")
def find_id_request(info: Dict[str, str], req: Request, db: Session = Depends(get_db)):
    name = info.get("name")
    email = info.get("email")

    # 가입된 유저가 있는지 먼저 확인
    sql = sqlalchemy.text("SELECT 1 FROM member_info WHERE user_name = :name AND email = :email")
    user_exists = db.execute(sql, {"name": name, "email": email}).scalar()

    if not user_exists:
        return {"res": False, "msg": "일치하는 회원 정보가 없습니다."}

    # 정보가 일치하여 이메일로 인증번호를 쏘기 직전에 요청 기록 적재
    save_admin_log(
        db=db,
        log_type='find',
        title='아이디 찾기 요청',
        content=f'아이디 찾기 인증코드 요청 : {name}'
    )
    # 6자리 인증코드 생성 및 세션 저장
    auth_code = str(random.randint(100000, 999999))
    req.session["find_id_auth"] = {"email": email, "name": name, "code": auth_code}

    # 만능 알림 함수 호출 (AUTH 모드)
    notifier.send_emergency_email(
        to_email=email,
        ai_report={'prediction': auth_code},
        news_url=None,
        risk_level="AUTH",
        keywords_str=None,
        title="아이디 찾기 인증번호"
    )
    return {"res": True}


# 아이디 찾기 2단계: 코드 검증 및 아이디 반환
@app.post("/find_id/verify")
def find_id_verify(info: Dict[str, str], req: Request, db: Session = Depends(get_db)):
    input_code = info.get("code")
    session_data = req.session.get("find_id_auth")

    if not session_data or session_data["code"] != input_code:
        return {"res": False, "msg": "인증코드가 일치하지 않습니다."}

    # 인증 성공 시 아이디 조회 후 반환
    sql = sqlalchemy.text("SELECT id, create_at FROM member_info WHERE user_name = :name AND email = :email")
    res = db.execute(sql, {"name": session_data["name"], "email": session_data["email"]}).mappings().fetchall()
    # 🎯 [버그 수정 완료] info["name"] 대신 안전한 session_data["name"]을 사용하여 최종 성공 로그 적재
    save_admin_log(
        db=db,
        log_type='find',
        title='아이디 찾기 완료',
        content=f'아이디 찾기 최종 성공 : {session_data["name"]}'
    )
    # 사용 완료된 세션 파기
    req.session.pop("find_id_auth", None)

    # 데이터 가공하여 전달
    result_list = [{"id": r["id"], "create_at": r["create_at"].strftime("%Y-%m-%d")} for r in res]
    return {"res": True, "data": result_list}


# 비밀번호 찾기 1단계: 인증코드 요청
@app.post("/find_pw/request")
async def find_pw_request(info: Dict[str, str], req: Request, db: Session = Depends(get_db)):
    user_id = info.get("userId")
    email = info.get("email")

    sql = sqlalchemy.text("SELECT 1 FROM member_info WHERE id = :id AND email = :email")
    user_exists = db.execute(sql, {"id": user_id, "email": email}).scalar()

    if not user_exists:
        return {"res": False}

    # 회원 정보가 일치하여 '인증코드 요청'이 정상 수락되었을 때 로그 기록
    save_admin_log(
        db=db,
        log_type='find',
        title='비밀번호 요청',
        content=f'비밀번호 찾기 인증코드 요청 : {user_id}',
        target_id=user_id
    )

    # 6자리 인증코드 생성 및 세션 저장
    pwd_code = str(random.randint(100000, 999999))
    req.session["find_pw_auth"] = {"email": email, "userId": user_id, "code": pwd_code}

    # 만능 알림 함수 호출 (PWD 모드)
    success = notifier.send_emergency_email(
        to_email=email,
        ai_report={'prediction': pwd_code},  # 진짜 코드를 넣어서 쏘세요!
        news_url=None,
        risk_level="PWD",
        keywords_str=None,
        title="비밀번호 재설정 인증번호"
    )
    return {"res": success}


# 비밀번호 찾기 2단계: 코드 검증
@app.post("/find_pw/verify")
async def find_pw_verify(info: Dict[str, str], req: Request):
    input_code = info.get("code")
    session_data = req.session.get("find_pw_auth")

    logger.info("========================================")
    logger.info(f"💾 세션 전체 데이터: {session_data}")
    logger.info(f"📥 사용자가 친 코드: '{input_code}'")
    if session_data:
        logger.info(f"🎯 세션 안의 진짜 코드: '{str(session_data.get('code'))}'")
    logger.info("========================================")

    # 세션 자체가 유실되었다면 에러 메시지를 명확하게 분리해서 반환
    if not session_data:
        return {"res": False, "msg": "서버 세션이 만료되었거나 인증번호가 생성되지 않았습니다. 코드를 다시 받아주세요."}

    if str(session_data.get("code")).strip() != input_code:
        return {"res": False, "msg": "인증코드가 일치하지 않습니다."}

    return {"res": True}


# 3단계: 최종 비밀번호 변경
@app.post("/find_pw/reset")
async def find_pw_reset(info:Dict[str, str], req: Request, db: Session = Depends(get_db)):
    logger.info(f'비밀번호 변경 info = {info}')
    user_id = info.get("userId")
    new_pw = hash_password(info.get("password"))
    session_data = req.session.get("find_pw_auth")

    # 세션 검증 보안 장치 (인증코드, 코드검증 단계를 정상적으로 거쳤는지 확인)
    if not session_data or session_data["userId"] != user_id:
        return {"res": 0, "msg": "잘못된 접근입니다."}

    sql = sqlalchemy.text("UPDATE member_info SET pw = :pw WHERE id = :id")
    res = db.execute(sql, {"id": user_id, "pw": new_pw})

    if res.rowcount > 0:
        # 단순히 요청한 것뿐만 아니라, 진짜로 '비밀번호 변경이 최종 성공 완료'된 시점도 기록
        save_admin_log(
            db=db,
            log_type='find',
            title='비밀번호 변경 완료',
            content=f'비밀번호 재설정 최종 성공 : {user_id}',
            target_id=user_id
        )

    # 재설정 완료 시 세션 파기
    req.session.pop("find_pw_auth", None)
    db.commit()
    return {"res": res.rowcount}


# ======================
#     관리자 페이지
# ======================

# 관리자 페이지 요청
@app.get("/admin/user_list")
def user_list(db: Session = Depends(get_db)):
    logger.info("------관리자 페이지------")
    user_list = []


    sql = sqlalchemy.text("""SELECT
                                mi.id, mi.email, mi.create_at, mi.phone_number, ml.status, mk.keywords, mi.user_name
                            FROM member_info mi
                            LEFT JOIN (
                                SELECT t1.member_no, t1.status
                                FROM member_login_log t1
                                INNER JOIN (
                                    SELECT member_no, MAX(login_time) AS max_login_time
                                    FROM member_login_log
                                    GROUP BY member_no
                                ) t2
                                    ON t1.member_no = t2.member_no
                                   AND t1.login_time = t2.max_login_time
                            ) ml
                                ON mi.member_no = ml.member_no
                            LEFT JOIN (
                                SELECT
                                    member_no,
                                    GROUP_CONCAT(keyword SEPARATOR ', ') AS keywords
                                FROM member_keyword
                                GROUP BY member_no
                            ) mk
                                ON mi.member_no = mk.member_no
                                    WHERE id != 'admin'
                                    ORDER BY create_at DESC""")
    res = db.execute(sql).mappings().fetchall()
    logger.info(res)
    for user in res:
        user_list.append(user)
    return {"user": user_list}

# 유저 로그인로그 요청
@app.get("/admin/login_log")
def login_log(id:str, db: Session = Depends(get_db)):
    # logger.info(f'로그인로그 요청 id = {id}')

    sql = sqlalchemy.text("""SELECT
                                t2.login_time ,t2.logout_time, t2.login_ip
                            FROM member_info t1 
                                JOIN member_login_log t2 ON t1.member_no = t2.member_no
                                    WHERE id = :id""")
    res = db.execute(sql, {"id": id}).mappings().fetchall()
    # logger.info(res)
    result = []
    for r in res:
        row = dict(r)
        if row["login_time"]:
            row["login_time"] = row["login_time"].strftime("%Y-%m-%d %H:%M:%S")

        # logout_time 처리 (null 체크)
        if row["logout_time"]:
            row["logout_time"] = row["logout_time"].strftime("%Y-%m-%d %H:%M:%S")
        else:
            # null인 경우 빈 문자열("") 또는 특정 텍스트("-" 등)를 보냅니다.
            row["logout_time"] = ""
        result.append(row)
    return {"res": result}


@app.get("/admin/admin_log")
def admin_log(db: Session = Depends(get_db)):

    sql = sqlalchemy.text("""SELECT * FROM admin_logs WHERE created_at >= NOW() - INTERVAL 30 DAY ORDER BY created_at DESC""")
    res = db.execute(sql).mappings().fetchall()
    logger.info(res)
    return {"res": res}


@app.get("/admin/alarm_log")
def alarm_log(db: Session = Depends(get_db)):

    sql = sqlalchemy.text("""SELECT t1.signal_no, t1.risk_level, t1.prediction, t1.news_url,
                                MAX(t2.alarm_time) AS alarm_time,
                                GROUP_CONCAT(t2.member_no SEPARATOR ',') AS member_list
                            FROM signal_message t1
                            JOIN alarm_log t2
                                ON t1.signal_no = t2.signal_no
                            WHERE t2.alarm_time >= NOW() - INTERVAL 60 MINUTE
                              AND t1.risk_level = '심각'
                            GROUP BY t1.signal_no
                            ORDER BY MAX(t2.alarm_time) DESC""")
    res = db.execute(sql).mappings().fetchall()
    logger.info(res)

    return {"res": res}

# 관리자 열람 로그 함수
def save_admin_log(
    db: Session,
    log_type: str,
    title: str,
    target_id: str = None,
    content: str = None,
    before_data: dict = None,
    after_data: dict = None,
):
    """
       admin_logs 테이블에 로그 저장

       Parameters
       ----------
       db : MariaDB connection
       log_type : 로그 타입 (create/update/delete/login ...)
       title : 로그 제목
       target_id : 변경 대상 user id
       content : 추가 설명
       before_data : 변경 전 데이터(dict)
       after_data : 변경 후 데이터(dict)
       """

    sql = sqlalchemy.text("""
        INSERT INTO admin_logs (log_type, title, content, target_id, before_data, after_data, created_at)
        VALUES (:log_type, :title, :content, :target_id, :before_data, :after_data, NOW())
    """)

    # 쿼리 실행
    try:
        res = db.execute(sql, {
            "log_type": log_type,
            "title": title,
            "content": content,
            "before_data": (
                json.dumps(before_data, ensure_ascii=False)
                if before_data else None
            ),
            "after_data": (
                json.dumps(after_data, ensure_ascii=False)
                if after_data else None
            ),
            "target_id": target_id,
        })

        if res.rowcount and res.rowcount > 0:
            logger.info(f"{target_id} 의 {log_type} admin_log 저장 완료")
    except Exception as e:
        logger.info(f'{e} 발생으로 admin_log 저장 실패')


if __name__ == "__main__":
    import uvicorn
    import sys

    # 정책 설정은 임포트 직후 최상단에 있는 것도 좋지만, 실행 직전에도 한 번 더 확인
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # uvicorn 실행 시 루프 설정을 명시하거나,
    # reload=True 환경에서는 정책 선언이 잘 먹히지 않을 수 있으므로 주의가 필요합니다.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False, loop="asyncio")