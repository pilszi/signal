import requests

from config import Config
from logger import get_logger

logger = get_logger(__name__)


def send_emergency_email(
        to_email,
        ai_report,
        news_url,
        risk_level,
        keywords_str,
        title
):
    """
    심각 단계 리스크 감지 시 이메일 발송
    """

    short_title = title[:30] + "..." if len(title) > 30 else title
    subject = f"🚨 [긴급 리스크 알림] {short_title}"

    # AI 보고서 내용 추출
    prediction = ai_report.get(
        'prediction',
        '분석 내용 없음'
    )

    reason = (
        ai_report.get("reason", "상세 근거 없음")
        .replace("\n", "<br>")
    )

    # HTML 생성
    html_content = _create_html_template(
        title=title,
        risk_level=risk_level,
        prediction=prediction,
        reason=reason,
        keywords=keywords_str,
        news_url=news_url
    )

    return _execute_send(
        to_email,
        subject,
        html_content
    )


def _create_html_template(title,risk_level,prediction,reason,keywords,news_url):
    """
    이메일 HTML 템플릿 생성
    """
    return f"""
    <html>
    <body style="font-family: 'Malgun Gothic', sans-serif; line-height: 1.6; color: #333;">
        <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; padding: 20px;">
            <h2 style="color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px;">
                🚨 위기 감지 리포트 ({risk_level})
            </h2>
            <p>
                <strong>기사 제목:</strong>
                {title}
            </p>
            <p>
                <strong>매칭 키워드:</strong>
                <span style="color: #0275d8;">
                    {keywords}
                </span>
            </p>
            <div style="
                background-color: #f9f9f9;
                border-left: 5px solid #d9534f;
                padding: 15px;
                margin: 20px 0;
            ">
                <h3 style="margin-top: 0; color: #333;">
                    🤖 AI 예측 전망
                </h3>
                <p style="
                    font-size: 1.1em;
                    font-weight: bold;
                ">
                    {prediction}
                </p>
            </div>
            <h3>💡 상세 분석 근거</h3>
            <p style="white-space: pre-wrap;">
                {reason}
            </p>
            <div style="
                margin-top: 30px;
                text-align: center;
            ">
                <a
                    href="{news_url}"
                    style="
                        background-color: #333;
                        color: #fff;
                        padding: 10px 20px;
                        text-decoration: none;
                        border-radius: 5px;
                    "
                >
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