import hashlib
import os

# url hash값 변경 함수
def generate_article_id(url):
    # 1. URL 양 끝의 공백을 제거하고 인코딩합니다.
    encoded_url = url.strip().encode('utf-8')

    # 2. SHA-256 해시 객체를 생성합니다.
    hash_obj = hashlib.sha256(encoded_url)

    # 3. 16진수 문자열로 변환하여 반환합니다. (64자)
    return hash_obj.hexdigest()


# 사용 예시
# article_url = "https://news.example.com/2026/04/22/market_report"
# article_id = generate_article_id(article_url)
#
# print(f"Generated ID: {article_id}")

# 비밀번호 hash값으로 암호화 함수
def hash_password(password):
    salt = os.urandom(32)

    pw_hash = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt,
        100000
    )
    combine = salt + pw_hash
    pw = combine.hex()
    return pw

# 입력한 비밀번호 확인 함수
def verify_password(input_pw, db_pw):
    byte_pw = bytes.fromhex(db_pw)
    salt_db_pw = byte_pw[:32]
    hash_db_pw = byte_pw[32:]

    input_pw_hash = hashlib.pbkdf2_hmac(
        'sha256',
        input_pw.encode('utf-8'),
        salt_db_pw,
        100000
    )

    return input_pw_hash == hash_db_pw