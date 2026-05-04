from typing import Dict, Any

import sqlalchemy
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from dataReqType.regist import RegistModel
from db import get_db
from hash import hash_password, verify_password
from elasticsearch import Elasticsearch

app = FastAPI()
app.mount("/view", StaticFiles(directory="view"))
app.add_middleware(SessionMiddleware, secret_key="secret", max_age=600)

def chk_session(req:Request):
    return req.session.get('login_id', '')

es = Elasticsearch(
    ["http://localhost:9200"],
    request_timeout=30
)
es_index = "news_origin_es2"

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
        for key in info["keyword"]:
            insert_sql = sqlalchemy.text("""INSERT INTO member_keyword (member_no, keyword) VALUES(:member_no, :keyword)""")
            insert_res = engine.execute(insert_sql, {"member_no": id_result["member_no"], "keyword": key})
            key_insert += insert_res.rowcount
        print(f'키워드 수정 = {key_insert}')
    return {"msg": "ok"}


# main 페이지 오늘의 뉴스 조회
@app.get("/public_signals")
def public_signals():
    # print('뉴스 요청')
    body = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "published_date": {
                                "gte": "now-12h", # 현재 시각 기준 12시간 전부터
                                "lte": "now",      # 현재 시각까지
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            { "published_date": { "order": "desc" } } # 최신 기사가 먼저 나오도록 정렬
        ],
        "size": 100
    }
    res = es.search(index= es_index, body= body)
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
            'press_name' : news["_source"]["press_name"]
        }
        public_news.append(_news)
    # print(public_news)
    return {"msg": public_news}


# 기사 열람 기록 저장
@app.post("/news_view")
def news_view(info:Dict[str, str]):
    print(f'{info["id"]} 가 열람한 기사 url = {info["url"]}')
    with get_db() as engine:
        sql = sqlalchemy.text("""SELECT member_no FROM member_info WHERE id = :id""")
        res = engine.execute(sql, {"id": info["id"]}).mappings().fetchone()
        print(f'member_no = {res}')
        sql = sqlalchemy.text("""SELECT count(news_url) as cnt FROM news_view WHERE member_no = :member_no AND news_url = :news_url""")
        view_res = engine.execute(sql, {"member_no": res["member_no"], "news_url": info["url"]}).mappings().fetchone()
        if view_res["cnt"] == 0:
            sql = sqlalchemy.text("""INSERT INTO news_view (member_no, news_url)VALUES(:member_no, :news_url)""")
            res = engine.execute(sql, {"member_no": res["member_no"], "news_url": info["url"]})
            print(f'기사열람 DB 삽입 성공 {res.rowcount}개')
    return {"msg": "ok"}


# 맞춤형뉴스 요청
@app.get("/custom_news")
def custom_news(id:str):
    print(f'맞춤형 요청 id = {id}')
    with get_db() as engine:
        sql = sqlalchemy.text("""SELECT member_no FROM member_info WHERE id = :id""")
        res = engine.execute(sql, {"id": id}).mappings().fetchone()
        # print(res)

        # 회원별 관심 키워드 조회
        sql = sqlalchemy.text("""SELECT keyword FROM member_keyword WHERE member_no = :member_no""")
        keys_res = engine.execute(sql, {"member_no": res["member_no"]}).mappings().fetchall()
        # print(keys_res)
        keyword = []
        for key in keys_res:
            # print(keywords['keyword'])
            keyword.append(key["keyword"])
            # print(f'관심 키워드 = {key["keyword"]}')
        print(f'관심 키워드 = {keyword}')
        # 기존 열람 기사 url 조회
        sql = sqlalchemy.text("""SELECT news_url FROM news_view WHERE member_no = :member_no""")
        read_res = engine.execute(sql, {"member_no": res["member_no"]}).mappings().fetchall()
        read_url = []
        for url in read_res:
            # print(url["news_url"])
            read_url.append(url["news_url"])
        # 키워드가 들어간 뉴스기사 조회
        """ test 쿼리 """
        body = {
            "query": {
                "terms": {
                    "extracted_keyword": keyword    # 리스트 전달
                }
            },
            "sort": [
                {"published_date": "desc"} # 최신 기사가 먼저 나오도록 정렬
            ],
            "size": 100
        }
        """ 추후 es_3 에서 데이터 가져올 때 사용할 쿼리"""
        # body = {
        #     "query": {
        #         "bool": {
        #             "must": [
        #                 {
        #                     "terms": {
        #                         "extracted_keyword": keyword  # 특정 키워드 조건
        #                     }
        #                 }
        #             ],
        #             "filter": [
        #                 {
        #                     "range": {
        #                         "analyzed_at": {
        #                             "gte": "now-24h",  # 현재(now) 기준 24시간 전(24h) 이상(gte)
        #                             "lt": "now"  # 현재 미만(lt)
        #                         }
        #                     }
        #                 }
        #             ]
        #         }
        #     },
        #     "sort": [
        #         {"published_date": "desc"}
        #     ],
        #     "size": 100
        # }
        res = es.search(index= es_index, body= body)
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
    return {"keyword": keyword, "total_val": len(custom_news), "news": custom_news}