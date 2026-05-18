import requests

from config import Config
from logger import get_logger

logger = get_logger(__name__)


def send_emergency_email(to_email, ai_report, news_url, risk_level, keywords_str, title):
    """
    [슈퍼 통합본]
    이 함수 하나로 '리스크 알림'과 '인증코드 발송'을 모두 처리합니다.
    - risk_level 이 'AUTH' 또는 'PWD' 면 인증번호 모드

    - 그 외에는 기존 리스크 알림 모드
    1. 제목 30자 요약
    2. 키워드 문자열 분절 현상(I, r, a, n) 방지
    3. 본문 텍스트 앞뒤 공백 및 불필요한 들여쓰기 제거
    4. HTML 줄바꿈 처리 및 발송
    """
    # --- [공통] 기본 변수 정제 ---
    prediction = ai_report.get('prediction', '').strip()
    raw_reason = ai_report.get('reason', '').strip()

    # --- [분기점] 인증코드 모드 (회원가입 또는 비밀번호 찾기) ---
    if risk_level in ['AUTH', 'PWD']:
        # 인증코드 모드일 때 ai_report['prediction']에 번호가 들어온다고 가정
        auth_code = prediction
        subject = f"📩 [Signal] {title} 안내"
        color = "#0275d8" if risk_level == 'AUTH' else "#d9534f"

        # 인증 전용 템플릿 (함수 안에 직접 내장)
        html_content = f"""
            <div style="max-width: 500px; margin: 0 auto; background: #fff; padding: 30px; border: 1px solid #eee; border-radius: 10px; font-family: 'Malgun Gothic', sans-serif;">
                <h2 style="color: {color}; margin-top: 0;">{title}</h2>
                <p style="color: #666;">안녕하세요! 요청하신 인증번호를 확인해 주세요.</p>
                <div style="background: #f8f9fa; border: 1px dashed {color}; padding: 20px; font-size: 32px; font-weight: bold; text-align: center; color: {color}; margin: 25px 0; letter-spacing: 5px;">
                    {auth_code}
                </div>
                <p style="font-size: 13px; color: #999;">인증번호는 3분간 유효합니다. 본인이 요청하지 않았다면 무시하세요.</p>
            </div>
            """

    # --- [분기점] 기존 리스크 알림 모드 ---
    else:
        # 1. 제목 요약
        short_title = title[:30] + "..." if len(title) > 30 else title
        subject = f"🚨 [긴급 리스크 알림] {short_title}"

        # 2. 키워드 정제 (I, r, a, n 현상 방지)
        if isinstance(keywords_str, list):
            clean_keywords = ", ".join(keywords_str)
        elif isinstance(keywords_str, str):
            clean_keywords = keywords_str.strip()
        else:
            clean_keywords = str(keywords_str)

        # 3. 상세 근거 들여쓰기 문제 해결
        reason_lines = [line.strip() for line in raw_reason.split('\n') if line.strip()]
        formatted_reason = "<br>".join(reason_lines)

        # 4. 리스크 알림 전용 템플릿 (기존 _create_html_template 호출 또는 내장)
        html_content = _create_html_template(
            title=title,
            risk_level=risk_level,
            prediction=prediction,
            reason=formatted_reason,
            keywords=clean_keywords,
            news_url=news_url
        )

    # --- [공통] 실제 발송 실행 ---
    return _execute_send(to_email, subject, html_content)



def _create_html_template(title,risk_level,prediction,reason,keywords,news_url):
    """
    이메일 HTML 템플릿 생성
    """
    return f"""
    <html>
    <body style="font-family: 'Malgun Gothic', sans-serif; line-height: 1.6; color: #333;">
        <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; padding: 25px; background-color: #ffffff;">
            <h2 style="color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px; margin-top: 0;">
                🚨 위기 감지 리포트 ({risk_level})
            </h2>
            
            <div style="margin-bottom: 20px;">
                <p style="margin: 5px 0;"><strong>기사 제목:</strong> {title}</p>
                <p style="margin: 5px 0;"><strong>매칭 키워드:</strong> <span style="color: #0275d8; font-weight: bold;">{keywords}</span></p>
            </div>
            
            <div style="background-color: #f9f9f9; border-left: 5px solid #d9534f; padding: 15px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #333; font-size: 16px;">🤖 AI 예측 전망</h3>
                <p style="font-size: 15px; font-weight: bold; margin-bottom: 0; color: #d9534f;">{prediction}</p>
            </div>
            
            <h3 style="font-size: 16px; color: #333; margin-bottom: 10px;">💡 상세 분석 근거</h3>
            <div style="font-size: 14px; text-align: justify; color: #444; line-height: 1.8;">
                {reason}
            </div>
            
            <div style="margin-top: 30px; text-align: center;">
                <a href="{news_url}" style="background-color: #333; color: #ffffff; padding: 12px 25px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block;">
                    원본 기사 확인하기
                </a>
            </div>
            <footer style="
                margin-top: 40px;
                font-size: 0.8em;
                color: #888;
                border-top: 1px solid #eee;
                padding-top: 10px;
            ">
                본 메일은 시스템에 의해 자동으로 발송되었습니다.
                설정하신 관심 키워드 기반 리스크 알림입니다.
            </footer>

        </div>
    </body>
    </html>
    """


def _execute_send(
        to_email,
        subject,
        body
):
    """
    Brevo API 기반 이메일 발송
    """
    try:
        headers = {
            "accept": "application/json",
            "api-key": Config.BREVO_API_KEY,
            "content-type": "application/json"
        }
        data = {
            "sender": {
                "name": Config.SENDER_NAME,
                "email": Config.SENDER_EMAIL
            },
            "to": [
                {
                    "email": to_email
                }
            ],
            "subject": subject,
            "htmlContent": body
        }

        response = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            json=data,
            headers=headers
        )
        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text)

        # 성공
        if response.status_code in [200, 201, 202]:
            logger.info(
                f"📧 이메일 발송 성공: {to_email}"
            )
            return True

        # 실패
        else:
            logger.error(
                f"❌ 이메일 발송 실패: {response.text}"
            )
            return False

    except Exception as e:
        logger.error(
            f"❌ 이메일 발송 예외: {str(e)}"
        )
        return False