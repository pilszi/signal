import sys
import os
import subprocess
import asyncio
from typing import Dict, Any
import traceback
import sqlalchemy
import notifier
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
from db import get_db, engine
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
es = Elasticsearch(["http://localhost:9200"])


# ==========================================
# 0. 핵심 파이프라인 제어 (학습 및 분석 통합)
# ==========================================
# 분석 실행 후 결과를 DB에 저장하는 함수
async def run_analysis_and_save():
    logger.info("🧠 [Analysis] AI 분석 및 DB 저장 시퀀스 시작")

    # 1. AI 분석 실행
    results = await ml.run_analysis() # 분석 결과 (기사제목, 키워드, 점수, 레벨 등)
    if not results:
        logger.info("❌ [Analysis] 분석할 기사 리스트가 비어있습니다 (results=None)")
        return
    logger.info(f"🔥 [Analysis] 분석 대상 {len(results)}건 발견! DB 저장 시작")

    processed_ids = []  # 성공한 문서 ID를 담을 바구니

    # 2. DB 연결 및 개인화 처리
    try:
        with get_db() as session:
            for res in results:
                # [STEP 1] 공통 시그널 DB 저장
                new_signal_no = save_analysis_result(session, res)

                if new_signal_no:
                    processed_ids.append(res.get('doc_id'))  # 성공 리스트에 추가
                    logger.info(f"✅ [DB Success] 신규 시그널 저장 완료 (signal_no: {new_signal_no})")
                else:
                    # 만약 이 로그가 찍힌다면 utils.py의 return값이 문제인 겁니다!
                    logger.error("❌ [DB Error] signal_no를 받지 못했습니다. utils.py를 확인하세요.")
                    continue  # 번호가 없으면 다음 루프로 넘어가서 에러 방지

                # [STEP 2] 키워드 매칭 (이 기사의 키워드를 등록한 사용자 찾기)
                news_keywords = res.get('keywords', [])
                if news_keywords:
                    # 💡키워드가 리스트이므로 공백을 제거한 깔끔한 리스트로 변환
                    clean_kw_list = [k.strip() for k in news_keywords if k.strip()]
                    if clean_kw_list:
                        # 사용자가 등록한 키워드와 기사 키워드가 정확히 일치하는지 조회
                        match_sql = sqlalchemy.text("""
                                SELECT DISTINCT mk.member_no 
                                FROM member_keyword mk
                                WHERE :prediction LIKE CONCAT('%', mk.keyword, '%')
                                   OR :reason LIKE CONCAT('%', mk.keyword, '%')
                            """)
                        matched_users = session.execute(match_sql, {
                            "prediction": res.get('pred', ''),
                            "reason": res.get('reason', '')
                        }).fetchall()

                        for user in matched_users:
                            logger.info(f"🎯 매칭 성공! 사용자 번호: {user.member_no}, 키워드: {clean_kw_list}")
                            # A. 사용자의 alarm_log에 저장 (모든 매칭 시그널은 signal.html에 띄움)
                            alarm_sql = sqlalchemy.text("""
                                                        INSERT INTO alarm_log (member_no, signal_no, alarm_time, alarm_view)
                                                        VALUES (:m_no, :s_no, NOW(), 0)
                                                        ON DUPLICATE KEY UPDATE alarm_time = NOW()
                                                    """)
                            session.execute(alarm_sql, {"m_no": user.member_no, "s_no": new_signal_no})

                        # B. '심각' 단계일 때만 비상 이메일 발송
                        if res.get('level') == '심각' and hasattr(user, 'email'):
                            try:
                                notifier.send_emergency_email(
                                    to_email=user.email,
                                    ai_report={
                                        'prediction': res.get('pred'),
                                        'reason': res.get('reason')
                                    },
                                    news_url=res.get('url'),
                                    risk_level='심각',
                                    keywords_str=", ".join(news_keywords),
                                    title=res.get('title', '리스크 감지 알림')
                                )
                            except Exception as mail_err:
                                # 이메일 발송 실패가 전체 흐름을 방해하지 않도록 개별 예외 처리
                                logger.error(f"📧 이메일 발송 실패 ({user.email}): {mail_err}")

            # 3. 모든 루프가 정상적으로 끝나면 세션 확정
            session.commit()
            logger.info(f"✅ [Analysis] {len(results)}건 분석 및 개인화 알림 처리 완료")

        # [STEP 4] 보완: DB 저장이 성공한 것들만 골라서 ES 상태를 업데이트!
        if processed_ids:
            for doc_id in processed_ids:
                # ml.py에 작성하신 업데이트 함수를 호출합니다.
                await ml.update_es_status(doc_id, True)
            logger.info(f"🚀 [Sync] {len(processed_ids)}건 ES 상태 업데이트(is_processed=True) 완료")

    except Exception as e:
        logger.error(f"❌ [Analysis] 분석 결과 처리 중 오류 발생: {e}")



async def manage_ml_pipeline(scheduler: AsyncIOScheduler):
    """
    BERT 모델 학습 여부를 체크하고, 완료되었다면 utils.run_analysis_and_save 주기적으로 실행함
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

            #  학습이 갓 끝난 직후 등록할 때
            if process.returncode == 0:
                logger.info("🎊 [Pipeline] 모델 학습 완료! 분석 모드로 전환합니다.")
                # 학습이 끝났다는 '락 파일'을 생성
                with open(TRAIN_LOCK_FILE, "w") as f:
                    f.write(f"Finished at {os.path.getmtime('train_model.py')}")
                # BERT 분석 작업 등록 (10분 주기로 실전 투입)
                if not scheduler.get_job('ml_analysis'):
                    scheduler.add_job(
                        run_analysis_and_save,
                        'interval',
                        minutes=10,
                        id='ml_analysis',
                        max_instances=1,  # 작업 겹침 방지
                        replace_existing=True  # 기존 작업 교체
                    )

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
            # next_run_time=datetime.now()를 추가하여 즉시 1회 실행 후 10분 주기 시작
            from datetime import datetime
            scheduler.add_job(
                run_analysis_and_save,
                'interval',
                minutes=10,
                id='ml_analysis',
                max_instances=1,  # 추가
                replace_existing=True,  # 추가

            )


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
        await manage_ml_pipeline(scheduler) # 그 다음 앞으로 10분마다 돌 수 있게 스케줄러에 등록

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
    # 학습/분석 파이프라인 관리 (5분마다 체크)
    global_scheduler.add_job(manage_ml_pipeline, 'interval', minutes=10, args=[global_scheduler], id='ml_pipeline',)

    # 서버 시작과 동시에 즉시 실행 (백그라운드 태스크)
    asyncio.create_task(run_initial_batch(global_scheduler))

    global_scheduler.start()
    logger.info("🚀 리스크 관제 시스템 통합 스케줄러 가동")

    yield

    # --- 서버 종료 시 정리 로직 ---
    logger.info("🛑 서버 종료 중: 실행 중인 작업들을 정리합니다.")        # 서버가 돌아가는 지점

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
    return


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
            db.execute(log_sql, {
                "member_no": result.member_no,
                "login_ip": client_ip
            })

            # 세션 저장
            req.session['login_id'] = info["id"]
            req.session['user_name'] = result.user_name
            req.session["member_no"] = result.member_no
            return {"msg": True}
        else:
            return {"msg": False}


@app.get("/logout")
def logout(req: Request):
    """로그아웃 및 로그 엔드타임 갱신"""
    member_no = req.session.get("member_no")
    if member_no:
        with get_db() as db:
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
def session_out():
    count = 0
    with get_db() as db:
        logout_sql = sqlalchemy.text("""UPDATE member_login_log SET logout_time = NOW(), status = 0
                                    WHERE status = 1 AND login_time <= NOW() - INTERVAL 60 MINUTE""")
        result = db.execute(logout_sql)
        count = result.rowcount

    print(f'1시간이 지나 로그아웃 된 계정 갯수 = {count}')
    return

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

        # A. signal_message 테이블에 '안내' 메시지 등록
        # (risk_level을 '안내'로 주면, 기존 API에서 'system' 타입으로 분류됩니다)
        msg_sql = sqlalchemy.text("""
                INSERT INTO signal_message (risk_level, prediction, prediction_reason)
                VALUES ('안내', '개인정보 수정 완료', '프로필 정보가 성공적으로 업데이트되었습니다.')
            """)
        res_msg = db.execute(msg_sql)
        new_signal_no = res_msg.lastrowid  # 방금 생성된 시그널 번호 가져오기

        # B. alarm_log 테이블에 해당 사용자와 연결
        alarm_sql = sqlalchemy.text("""
                INSERT INTO alarm_log (member_no, signal_no, alarm_time, alarm_view)
                VALUES (:member_no, :signal_no, NOW(), 0)
            """)
        db.execute(alarm_sql, {"member_no": member_no, "signal_no": new_signal_no})
        # ==========================================

        db.commit()  # 최종 확정

    return {"msg": "success", "updated_keywords": key_insert}


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
        else:
            return {"msg": False}


# main 페이지 오늘의 뉴스 조회
@app.get("/public_signals")
def public_signals(date: str = Query(None)):
    """ 메인 페이지 기사 요청 및 필터링 """
    # [1] 검색 조건 설정 (날짜가 있으면 해당 날짜, 없으면 최근 12시간)
    if date:
        date_filter = {
            "range": {
                "published_date": {
                    "gte": f"{date}T00:00:00",
                    "lte": f"{date}T23:59:59"
                }
            }
        }
    else:
        date_filter = {
            "range": {
                "published_date": {
                    "gte": "now-30d",
                    "lte": "now"
                }
            }
        }

    # [2] 최종 Elasticsearch 쿼리 바디
    search_body = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "published_date": {
                                "gte": "now-30d/d",
                                "lte": "now",
                                "time_zone": "+09:00"
                            }
                        }
                    }
                ]
            }
        },
        "sort": [{"published_date": "desc"}],
        "size": 100
    }
    try:

        res = es.search(index="news_labeling", body=search_body)

        print(f"가져온 기사 갯수 = {res['hits']['total']['value']}")

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
        print(f"❌ 검색 중 오류 발생: {e}")
        return {"msg": [], "error": str(e)}

@app.get("/main/country")
def country():
    """ 히트맵 데이터 요청 """
    with (get_db() as db):
        sql = sqlalchemy.text("""
                SELECT
                    t3.country_en_name as en_name,
                    COUNT(CASE WHEN t2.risk_level = '안정' THEN 1 END) AS 안정_count,
                    COUNT(CASE WHEN t2.risk_level = '주의' THEN 1 END) AS 주의_count,
                    COUNT(CASE WHEN t2.risk_level = '위기' THEN 1 END) AS 위기_count,
                    -- 바로 점수 계산까지!
                    SUM(CASE
                        WHEN t2.risk_level = '안정' THEN 10
                        WHEN t2.risk_level = '주의' THEN 30
                        WHEN t2.risk_level = '위기' THEN 100
                        ELSE 0
                    END) AS total_score
                FROM signal_country t1
                    JOIN signal_message t2 ON t1.signal_no = t2.signal_no
                        JOIN country t3 ON t3.country_no = t1.country_no
                        WHERE t2.signal_time >= (NOW() - INTERVAL 12 HOUR)
                            GROUP BY t1.country_no;
            """)
        country_all = db.execute(sql).mappings().fetchall()
        signal_country = []
        for country in country_all:
            con = {
                "en_name": country["en_name"],
                "total_score": country["total_score"]
            }
            signal_country.append(con)
    return {"country_signal": signal_country}


# 기사 열람 기록 저장
@app.post("/news_view")
def news_view(info:Dict[str, str]):
    print(f'{info["id"]} 가 열람한 기사 url = {info["url"]}')
    with get_db() as db:
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
    return


# 맞춤형뉴스 요청
@app.get("/custom_news")
def custom_news(id: str):
    """ 맞춤형 뉴스 데이터 요청 """
    logger.info(f'📡 맞춤형 요청 id = {id}')

    with get_db() as db:
        # 1. 사용자의 관심 키워드 조회 (단순하고 확실하게)
        kw_sql = sqlalchemy.text("""
            SELECT mk.keyword 
            FROM member_keyword mk 
            JOIN member_info m ON mk.member_no = m.member_no 
            WHERE m.id = :id
        """)
        user_keywords = db.execute(kw_sql, {"id": id}).scalars().all()

        # 2. 사용자가 읽은 기사 URL 목록 조회 (중복 제거를 위해)
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

        # 3. Elasticsearch 쿼리 구성
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    # term 쿼리는 keyword 타입 필드에 가장 정확합니다.
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

        custom_news_list = []
        for hit in res['hits']['hits']:
            source = hit["_source"]
            # hit 객체에서 바로 matched_queries를 가져옵니다.
            display_keyword = hit.get('matched_queries', [])
            logger.info(f"🎯 매칭 키워드 = {display_keyword}")

            custom_news_list.append({
                'title': source.get("title"),
                'url': source.get("url"),
                'main_image': source.get("main_image") or "https://via.placeholder.com/400x300",
                'published_date': source.get("published_date"),
                'press_name': source.get("press_name", "알 수 없음"),
                'keyword': display_keyword,
                'risk_score': source.get("final_total_score", {}).get("total", 0),
                'risk_level': source.get("risk_level", "안정"),
                # 여기서 열람 여부를 체크합니다.
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
def signal_log(id:str, db: Session = Depends(get_db)):
    """ 로그인한 사용자의 관심 키워드 기반 시그널만 조회
    (ES 검색 대신, 분석 시점에 미리 매칭된 alarm_log 테이블 활용)
    """
    # logger.info(f'==={id}===')
    logger.info(f"📋 {id}님의 시그널 로그 조회 요청")

    with get_db() as db:
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

        # 프론트엔드(signal.html)가 기대하는 데이터 형식으로 변환
        doc_no = []
        for r in rows:
            # 1. 문자열로 온 키워드들을 리스트(배열)로 변환
            kw_list = r["matched_keywords"].split(',') if r["matched_keywords"] else []
            # 2. 결과 리스트에 딕셔너리 추가
            doc_no.append({
                "id": r["id"],
                "risk_level": r["risk_level"],
                "signal_time": r["signal_time"].strftime("%Y-%m-%d %H:%M") if r["signal_time"] else "시간 미상",
                "prediction": r["prediction"],
                "prediction_reason": r["prediction_reason"],
                "url": r["url"] if r["url"] else "#",  # URL 부재 시 방어 코드
                "is_read": r["alarm_view"],
                "match_keyword": kw_list  # 👈 이제 배지가 정상적으로 뜹니다!
            })

        logger.info(f"✅ {id} 리더님 매칭 문서 {len(doc_no)}건 반환 완료")

    return {"data": doc_no}



# 네이게이션바 signal 알림 토글 요청
@app.get("/noti/signal")
def noti_signal(id:str):
    """ 페이지 상단 네비게이션바 통합 알림 요청 """
    # logger.info(f'----{id}----')
    with get_db() as db:
        sql = sqlalchemy.text("""
            SELECT
                t1.signal_no AS id
                ,t1.risk_level
                ,t1.prediction AS message
                ,t1.signal_time AS time
                ,t2.alarm_view AS is_read
                -- 리스크 레벨에 따라 유형을 강제로 부여 (사진 디자인 매칭용)
                ,CASE 
                    WHEN t1.risk_level = '심각' THEN 'emergency'
                    WHEN t1.risk_level = '주의' THEN 'keyword'
                    ELSE 'system'
                 END AS type
            FROM signal_message t1 
            JOIN alarm_log t2 ON t1.signal_no = t2.signal_no
            JOIN member_info t3 ON t2.member_no = t3.member_no
            WHERE t3.id = :id 
            -- AND t2.alarm_view = 0  <-- 읽은 알림도 목록에는 나와야 하므로 이 조건은 프론트에서 처리하거나 제거
            ORDER BY t2.alarm_time DESC LIMIT 15
        """)
        res = db.execute(sql, {"id": id}).mappings().fetchall()
        notis = []
        for n in res:
            notis.append({
                "id": n["id"],
                "type": n["type"],
                "risk_level": n["risk_level"],
                "title": f"{n['risk_level']} 위험 시그널" if n['type'] == 'emergency' else "키워드 알림",
                "message": n["message"],
                "is_read": n["is_read"],
                "time": n["time"]
            })
    return {"noti": notis}



# 알림토글 읽음 요청
@app.post("/noti/read")
def noti_read(info: Dict[str, Any]):
    with get_db() as db:
        sql = sqlalchemy.text("""
                UPDATE alarm_log t1 JOIN member_info t2 
                    ON t1.member_no = t2.member_no 
	                    SET t1.alarm_view = 1 
	                        WHERE t1.signal_no = :signal_no AND t2.id = :id
            """)
        res = db.execute(sql, {"signal_no": info["id"], "id": info["user_id"]})
        logger.info(f'알림 확인 업데이트 = {res.rowcount}개')
    return



# 모든 알림트글 읽음 요청
@app.get("/noti/read_all")
def noti_raed_all(id:str):
    """ 네비게이션바 시그널알림 확인 요청 """
    with get_db() as db:
        sql = sqlalchemy.text("""
            UPDATE alarm_log t1 JOIN member_info t2 
                ON t1.member_no = t2.member_no
	                SET t1.alarm_view = 1 
	                    WHERE t1.alarm_view = 0 AND t2.id = :id
        """)
        res = db.execute(sql, {"id": id})
        logger.info(f'{id} 의 모든 알림 읽음 업데이트 = {res.rowcount}개')
    return

# 알림 삭제 요청
@app.post("/noti/delete")
def noti_delete(info: Dict[str, Any]):
    """ 알림 개별 삭제 (X 버튼 클릭 시) """
    with get_db() as db:
        sql = sqlalchemy.text("""
            DELETE t1 FROM alarm_log t1
            JOIN member_info t2 ON t1.member_no = t2.member_no
            WHERE t1.signal_no = :signal_no AND t2.id = :id
        """)
        res = db.execute(sql, {"signal_no": info["id"], "id": info["user_id"]})
        return {"msg": "success", "count": res.rowcount}

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


# ==========================================
# 5. 히트맵
# ==========================================
@app.get("/stats/heatmap")
async def get_heatmap_stats():
    """
    프론트엔드 히트맵 지도를 위한 리스크 통계 데이터 반환
    """
    try:
        # 1. ES에서 분석 완료된 최신 데이터 100건 가져오기
        query = {
            "query": {"match_all": {}},
            "sort": [{"analyzed_at": "desc"}],
            "size": 100
        }
        res = es.search(index="news_labeling", body=query)
        docs = [hit['_source'] for hit in res['hits']['hits']]

        # 2. 모든 국가를 기본 '안정(Stable)'으로 초기화
        # Config.G20_COUNTRY_MAP.values()에서 유니크한 영어 국가명들을 가져옵니다.
        unique_countries = set(Config.G20_COUNTRY_MAP.values())
        stats = {country: {"score": 0, "level": "Stable"} for country in unique_countries}

        # 3. 데이터를 순회하며 국가별 최신 상태 업데이트
        for doc in docs:
            c_name = doc.get('country_name')

            # 'Middle East' 같은 지역명이 오면 해당 지역 국가 전체에 점수 전파
            target_countries = [c_name]
            if c_name in Config.REGION_TO_COUNTRIES:
                target_countries = Config.REGION_TO_COUNTRIES[c_name]

            for country in target_countries:
                if country in stats:
                    # 이미 데이터가 들어있다면(최신순 정렬이므로) 건너뜁니다.
                    if stats[country]["score"] != 0: continue

                    # ml.py에서 저장한 점수와 등급 가져오기
                    raw_score = doc.get('final_total_score', {}).get('total', 0)
                    # RISK 점수로 변환 (0~100 사이)
                    risk_score = round(abs(raw_score * 100), 1)

                    stats[country]["score"] = risk_score
                    stats[country]["level"] = doc.get('risk_level', '안정')

        return stats  # FastAPI는 jsonify 없이 그냥 리턴!
    except Exception as e:
        print(f"❌ 히트맵 통계 에러: {e}")
        return {}


if __name__ == "__main__":
    import uvicorn
    import sys

    # 정책 설정은 임포트 직후 최상단에 있는 것도 좋지만, 실행 직전에도 한 번 더 확인
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # uvicorn 실행 시 루프 설정을 명시하거나,
    # reload=True 환경에서는 정책 선언이 잘 먹히지 않을 수 있으므로 주의가 필요합니다.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False, loop="asyncio")