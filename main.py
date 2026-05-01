import sqlalchemy
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from dataReqType.regist import RegistModel
from db import get_db

app = FastAPI()
app.mount("/view", StaticFiles(directory="view"))


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