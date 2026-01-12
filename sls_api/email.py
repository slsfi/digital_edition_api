from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import io
import logging
import os
from ruamel.yaml import YAML
import smtplib
from sls_api.endpoints.generics import FRONTEND_EXTERNAL_URL

logger = logging.getLogger("sls_api.email")

email_config_path = os.path.join("sls_api", "configs", "email.yml")

if os.path.exists(email_config_path):
    with io.open(email_config_path, encoding="UTF-8") as config:
        yaml = YAML(typ="safe")
        email_config = yaml.load(config)
else:
    email_config = None
    logger.error("Email configuration missing!")


def send_address_verification_email(to_address: str, access_token: str) -> bool:
    """
    Send an address verification email to the given address using the configured email server.
    Returns True on success, False on failure.
    """
    if not email_config:
        logger.error("Email configuration missing!")
        return False
    verification_link = f"{FRONTEND_EXTERNAL_URL}/verify_email?jwt={access_token}"
    email_subject = "Email verification for SLS Digital Editions"
    email_body = f"Your email address has been registered on {FRONTEND_EXTERNAL_URL}.\n\nIn order to log in, please first verify your email address by clicking on this link (expires in 8 hours): {verification_link}"
    success = send_email(to_address, email_subject, email_body)
    return success


def send_password_reset_email(to_address: str, access_token: str) -> bool:
    """
    Send a password reset email to the given address using the configured email server.
    Returns True on success, False on failure
    """
    if not email_config:
        logger.error("Email configuration missing!")
        return False
    reset_link = f"{FRONTEND_EXTERNAL_URL}/reset_password?jwt={access_token}"
    email_subject = "Password reset link for SLS Digital Editions"
    email_body = f"A password reset has been requested for your account on {FRONTEND_EXTERNAL_URL}.\n\nTo begin this process, please click this link (expires in 30 minutes): {reset_link}\n\nIf you did not request this password reset, you may ignore this email."
    success = send_email(to_address, email_subject, email_body)
    return success


def send_email(to_address: str, subject: str, body: str) -> bool:
    """
    Send an email with the given subject and body to the given address using the configured email server.
    Returns True on success, False on failure.
    """
    if not email_config:
        logger.error("Email configuration missing!")
        return False
    try:
        msg = MIMEMultipart()
        msg["From"] = email_config["SMTP_FROM_ADDRESS"]
        msg["To"] = to_address
        msg["Subject"] = subject
        msg.attach(MIMEText(body))

        mailserver = smtplib.SMTP(host=email_config["SMTP_RELAY_ADDRESS"], port=email_config["SMTP_RELAY_PORT"])
        mailserver.ehlo()
        if email_config["SMTP_STARTTLS"]:
            mailserver.starttls()
        mailserver.sendmail(from_addr=email_config["SMTP_FROM_ADDRESS"], to_addrs=to_address, msg=msg.as_string())
        mailserver.quit()
        return True
    except Exception:
        logger.exception("Exception during email send!")
        return False
