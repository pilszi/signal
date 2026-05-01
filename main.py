from typing import Dict

import sqlalchemy
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from dataReqType.regist import RegistModel
from db import get_db

app = FastAPI()
app.mount("/view", StaticFiles(directory="view"))
app.add_middleware(SessionMiddleware, secret_key="secret", max_age=600)

def chk_session(req:Request):
    return req.session.get('login_id', '')

@app.get('/')
def main():
    return RedirectResponse("/view/main.html")


# 회원가입 요청 -> db member_info 와 member_keyword 에 저장
@app.post("/regist")
def regist(info: RegistModel):
    # print(f'info = {info}')

    with get_db() as engine:
        parent_sql = sqlalchemy.text("""INSERT INTO member_info (id, pw, user_name, phone_number, email)
                            VALUES (:id, :pw, :user_name, :phone_number, :email)""")
        parent_res = engine.execute(parent_sql, {
            "id": info.id,
            "pw": info.pw,
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


# 로그인 요청
@app.post('/login')
def login(info:Dict[str, str], req:Request):
    success = 0
    # print(f'info = {info}')
    sql = sqlalchemy.text("""SELECT member_no ,count(id) as cnt FROM member_info WHERE id = :id AND pw = :pw""")
    with get_db() as engine:
        result = engine.execute(sql,{"id":info["id"], "pw":info["pw"]}).mappings().fetchone()
        # print(f'result = {result}')
        success = result.cnt
        # 로그인 성공시 id, ip, log_no 세션에 저장
        try:
            if success > 0:
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
    return {"msg": success, "login_id": req.session["login_id"]}

# session 만료 계정 자동 로그아웃 : member_login_log 테이블 업데이트 - logout_time, status
@app.get('/log_time')
def log_time():
    count = 0
    with get_db() as engine:
        # log_time_sql = sqlalchemy.text("""SELECT log_no FROM member_login_log
        #                                             WHERE login_time <= NOW() - INTERVAL 1 MINUTE""")
        # log_res = engine.execute(log_time_sql).mappings().fetchall()
        # for log in log_res:
        #     print(f'로그인 한지 1분이 지난 계정 = {log}')
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
    return {}