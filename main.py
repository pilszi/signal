import sys
import os
import subprocess
import asyncio
from typing import Dict, Any
import traceback

import jsonify
import sqlalchemy
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor

from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from elasticsearch import Elasticsearch
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
from config import Config
from utils import prepare_heatmap_data

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

            #  학습이 갓 끝난 직후 등록할 때
            if process.returncode == 0:
                logger.info("🎊 [Pipeline] 모델 학습 완료! 분석 모드로 전환합니다.")
                # 학습이 끝났다는 '락 파일'을 생성
                with open(TRAIN_LOCK_FILE, "w") as f:
                    f.write(f"Finished at {os.path.getmtime('train_model.py')}")
                # BERT 분석 작업 등록 (10분 주기로 실전 투입)
                if not scheduler.get_job('ml_analysis'):
                    scheduler.add_job(
                        ml.run_analysis,
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
                ml.run_analysis,
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

        logger.info("🎬 [초기화 시퀀스] 3단계: 분석 파이프라인 즉시 가동")
        await ml.run_analysis()
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
            res = db.execute(log_sql, {"member_no": result.member_no, "login_ip": client_ip})

            # 세션에 중요 정보 기록
            req.session['login_id'] = info["id"]
            req.session['user_name'] = result.user_name
            req.session["current_log_no"] = res.lastrowid
            return {"msg": True}
        else:
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

    return {"updated_keywords": key_insert}


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
def public_signals():
    """ 메인 페이지 기사 요청 """
    body = {"query": {"bool": {"filter": [{"range": {
                            "published_date": {
                                "gte": "now-12h",     # 현재 시각 기준 12시간 전부터
                                "lte": "now",         # 현재 시각까지
                            }}}]
            }
        },
        "sort": [{ "published_date": "desc" }]        # 최신 기사가 먼저 나오도록 정렬
        ,"size": 100
    }
    res = es.search(index= "news_labeling", body={"size":100})
    print(f"가져온 기사 갯수 = {res['hits']['total']['value']}")
    # print(res['hits']['hits'])
    public_news = []
    for news in res['hits']['hits']:
        # print(news["_source"])
        _news = {
            'title' : news["_source"]["title"],
            'url' : news["_source"]["url"],
            'main_image' : news["_source"]["main_image"],
            'published_date' : news["_source"]["published_date"],
            'press_name' : news["_source"]["press_name"],
            'risk_level' : news["_source"]["risk_level"],
            'risk_score' : news["_source"]["final_total_score"]["total"]
        }
        public_news.append(_news)
    # print(public_news)
    return {"msg": public_news}

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
def custom_news(id:str):
    """ 맞춤형 뉴스 데이터 요청 """
    print(f'맞춤형 요청 id = {id}')
    with get_db() as db:
        sql = sqlalchemy.text("""
                    SELECT 
                        t1.member_no
                        ,t2.keyword
                        ,t3.news_url
                    FROM member_info t1 JOIN member_keyword t2 ON t1.member_no = t2.member_no
                        JOIN news_view t3 ON t1.member_no = t3.member_no
                            WHERE t1.id = :id
            """)
        res = db.execute(sql, {"id": id}).mappings().fetchall()
        keyword = []
        read_url = []
        for key in res:
            # print(keywords['keyword'])
            clean_key = key["keyword"].strip()
            if clean_key:
                keyword.append(clean_key)
            read_url.append(key["news_url"])

        keywords = list(set(keyword))
        print(f'관심 키워드 = {keywords} / 열람 url = {read_url}')

        # 키워드가 들어간 뉴스기사 조회
        """ 추후 es_3 에서 데이터 가져올 때 사용할 쿼리"""
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "extracted_keywords": keyword  # 특정 키워드 조건
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "analyzed_at": {
                                    "gte": "now-24h",  # 현재(now) 기준 24시간 전(24h) 이상(gte)
                                    "lt": "now"  # 현재 미만(lt)
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {"published_date": "desc"} # 최신 기사가 먼저 나오도록 정렬
            ],
            "size": 100
        }
        res = es.search(index="news_labeling", body=body)
        print(f"검색 된 기사 갯수 = {res['hits']['total']['value']}")
        custom_news = []
        for news in res['hits']['hits']:
            # print(news["_source"])
            source_keywords = news["_source"].get("extracted_keyword", [])
            matched_list = list(set(source_keywords) & set(keyword))
            display_keyword = ''
            if matched_list:
                display_keyword = matched_list[0]
            else:
                display_keyword = '리스크'
            _news = {
                'title': news["_source"]["title"],
                'url': news["_source"]["url"],
                'main_image': news["_source"]["main_image"],
                'published_date': news["_source"]["published_date"],
                'press_name': news["_source"]["press_name"],
                'keyword': display_keyword,
                'is_read': news["_source"]["url"] in read_url
            }
            custom_news.append(_news)
        print(f'custom_news 갯수 = {len(custom_news)}')
    return {"keyword": keywords, "total_val": len(custom_news), "news": custom_news}


# # ==========================================
# # 3. 실시간 알림 API (ml.py 활용)
# # ==========================================
# @app.get("/api/signals")
# async def get_risk_signals():
#     """Elasticsearch 기반 최신 리스크 뉴스 데이터 제공"""
#     try:
#         # ml.py에 만든 get_latest_signals 함수를 호출
#         results = ml.get_latest_signals(size=20)
#         return {"status": "success", "data": results or []}
#     except Exception as e:
#         return {"status": "error", "message": str(e)}

# 시그널로그 페이지
@app.get("/signal_log")
def signal_log(id:str):
    """ 시그널로그 페이지 요청 """
    logger.info(f'==={id}===')

    with get_db() as db:
        # 1. id에 맞는 관심키워드 조회(db 에서 키워드 조회)
        sql = sqlalchemy.text("""
                SELECT
                    t1.keyword
                FROM member_keyword t1 JOIN member_info t2 
                    ON t1.member_no = t2.member_no
                        WHERE t2.id = :id
        """)
        key_res = db.execute(sql, {"id": id}).mappings().fetchall()
        keywords = []
        for key in key_res:
            clean_key = key["keyword"].strip()
            if clean_key:
                keywords.append(clean_key)

        logger.info(f'{id} 의 관심키워드 = {keywords}')
        # 2. 키워드에 맞는 기사 조회(es 에서 _id 조회)
        should_key = []
        for key in keywords:
            should_key.append(
                {
                    "match": {
                        "extracted_keywords": {
                            "query": key,
                            "_name": key
                        }
                    }
                }
            )

        body = {
            "query": {
                "bool": {
                    "should": should_key,
                    "minimum_should_match": 1
                }
            }
        }
        es_res = es.search(index="news_labeling", body=body)
        logger.info(f'관심키워드에 맞는 뉴스기사 = {len(es_res["hits"]["hits"])}')
        unique_ids = set()
        doc_no = []

        for i in es_res['hits']['hits']:
            doc_id = i["_id"]
            if doc_id not in unique_ids:
                unique_ids.add(doc_id)
                doc = {
                    "id": i["_id"],
                    "url": i["_source"]["url"],
                    "match_keyword": i.get("matched_queries", []),
                    "risk_level": i["_source"]["risk_level"],
                    "signal_time": i["_source"]["analyzed_at"],
                    "prediction": i["_source"]["prediction"],
                    "prediction_reason": i["_source"]["prediction_reason"],
                }
                doc_no.append(doc)

        logger.info(f'키워드에 해당하는 문서 = {len(doc_no)}')

        # 3. _id 에 해당하는 signal_no 조회(DB)
        sql = sqlalchemy.text("""
        #         SELECT
        #             risk_level
        #             ,signal_time
        #             ,prediction
        #             ,prediction_reason
        #         FROM signal_message
        #             WHERE document_no = :doc_no
        # """)

        """DB에서 데이터를 가져올 경우 사용"""
        # sig_doc = []
        # for doc in doc_no:
        #     db_res = db.execute(sql, {"doc_no": doc["id"]}).mappings().fetchone()
        #     logger.info(f'--------')
        #     if db_res:
        #         d = {
        #             "risk_level": db_res["risk_level"],
        #             "signal_time": db_res["signal_time"],
        #             "prediction": db_res["prediction"],
        #             "prediction_reason": db_res["prediction_reason"],
        #             "url": doc["url"],
        #             "match_keyword": doc["match_keyword"]
        #         }
        #         sig_doc.append(d)
        # logger.info(f'맞춤 signal_log = {len(sig_doc)} 개')
    return {"data": doc_no}


# 네이게이션바 signal 알림 토글 요청
@app.get("/noti/signal")
def noti_signal(id:str):
    """ 페이지 상단 네비게이션바 요청 """
    # logger.info(f'----{id}----')
    with get_db() as db:
        sql = sqlalchemy.text("""
            SELECT
                t1.signal_no
                ,t1.risk_level
                ,t1.signal_time
                ,t1.prediction
                ,t2.alarm_view
            FROM signal_message t1 JOIN alarm_log t2 ON t1.signal_no = t2.signal_no
                JOIN member_info t3 ON t2.member_no = t3.member_no
                    WHERE t3.id = :id and t2.alarm_view = 0 ORDER BY t2.alarm_time DESC LIMIT 10
        """)
        res = db.execute(sql, {"id": id}).mappings().fetchall()
        notis = []
        for n in res:
            noti = {
                "signal_no": n["signal_no"],
                "risk_level": n["risk_level"],
                "prediction": n["prediction"],
                "is_read": n["alarm_view"],
                "signal_time": n["signal_time"]
            }
            notis.append(noti)
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

            # [핵심] 'Middle East' 같은 지역명이 오면 해당 지역 국가 전체에 점수 전파
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
    # reload=True는 개발 중에만 사용하기
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)