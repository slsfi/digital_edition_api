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


def send_address_verification_email(to_address: str, access_token: str, user_language="en") -> bool:
    """
    Send an address verification email to the given address using the configured email server.
    Returns True on success, False on failure.
    """
    if not email_config:
        logger.error("Email configuration missing!")
        return False
    verification_link = f"{FRONTEND_EXTERNAL_URL}/{user_language}/verify-email#jwt={access_token}"
    if user_language == "en":
        email_subject = f"Verify your email address (SLS digital editions, {FRONTEND_EXTERNAL_URL}/)"
        email_body = f"You have created a user account for SLS digital editions with this email address.\n\nThe account was created on the website:\n{FRONTEND_EXTERNAL_URL}/\n\nTo be able to log in, you must verify your email address by following the link below. The link is valid for 8 hours:\n\n{verification_link}\n\nIf you do not verify your email address within 8 hours, the link will expire. You will then need to create the account again on:\n{FRONTEND_EXTERNAL_URL}/{user_language}/register\n\nIf you did not create this user account yourself, you may disregard this email.\n\nIf you have any questions, please contact info@sls.fi.\n\nThis email was sent automatically by the Society of Swedish Literature in Finland (SLS). Please do not reply to this message."
    elif user_language == "sv":
        email_subject = f"Verifiera din e-postadress (SLS digitala utgåvor, {FRONTEND_EXTERNAL_URL}/)"
        email_body = f"Du har skapat ett användarkonto för SLS digitala utgåvor med den här e-postadressen.\n\nKontot skapades på webbplatsen:\n{FRONTEND_EXTERNAL_URL}/\n\nFör att kunna logga in måste du verifiera din e-postadress genom att följa länken nedan. Länken är giltig i 8 timmar:\n\n{verification_link}\n\nOm du inte verifierar din e-postadress inom 8 timmar upphör länken att gälla. Då måste du skapa kontot på nytt på:\n{FRONTEND_EXTERNAL_URL}/{user_language}/register\n\nOm du inte själv har skapat detta användarkonto kan du bortse från det här mejlet.\n\nVid frågor, kontakta info@sls.fi.\n\nDetta mejl har skickats automatiskt av Svenska litteratursällskapet i Finland (SLS). Vänligen svara inte på meddelandet."
    elif user_language == "fi":
        email_subject = f"Vahvista sähköpostiosoitteesi (SLS:n digitaaliset editiot, {FRONTEND_EXTERNAL_URL}/)"
        email_body = f"Olet luonut käyttäjätilin SLS digitaalisia editioita varten tällä sähköpostiosoitteella.\n\nTili luotiin sivustolla:\n{FRONTEND_EXTERNAL_URL}/\n\nJotta voit kirjautua sisään, sinun täytyy vahvistaa sähköpostiosoitteesi alla olevan linkin kautta. Linkki on voimassa 8 tuntia:\n\n{verification_link}\n\nJos et vahvista sähköpostiosoitettasi 8 tunnin kuluessa, linkki vanhenee. Tämän jälkeen sinun täytyy luoda tili uudelleen osoitteessa:\n{FRONTEND_EXTERNAL_URL}/{user_language}/register\n\nJos et itse ole luonut tätä käyttäjätiliä, voit jättää tämän viestin huomiotta.\n\nJos sinulla on kysyttävää, ota yhteyttä osoitteeseen info@sls.fi.\n\nTämä on Svenska litteratursällskapet i Finlandin (SLS) automaattisesti lähettämä viesti. Älä vastaa tähän viestiin."
    success = send_email(to_address, email_subject, email_body)
    return success


def send_password_reset_email(to_address: str, access_token: str, user_language="en") -> bool:
    """
    Send a password reset email to the given address using the configured email server.
    Returns True on success, False on failure
    """
    if not email_config:
        logger.error("Email configuration missing!")
        return False
    reset_link = f"{FRONTEND_EXTERNAL_URL}/{user_language}/reset-password#jwt={access_token}"
    if user_language == "en":
        email_subject = f"Change password (SLS digital editions, {FRONTEND_EXTERNAL_URL}/)"
        email_body = f"You have requested to change the password for your user account for SLS digital editions.\n\nThe request was made on the website:\n{FRONTEND_EXTERNAL_URL}/\n\nTo choose a new password, follow the link below. The link is valid for 30 minutes:\n\n{reset_link}\n\nIf you did not request this change yourself, you may disregard this email. No change will be made until you follow the link and complete the process.\n\nIf you have any questions, please contact info@sls.fi.\n\nThis email was sent automatically by the Society of Swedish Literature in Finland (SLS). Please do not reply to this message."
    elif user_language == "sv":
        email_subject = f"Ändra lösenord (SLS digitala utgåvor, {FRONTEND_EXTERNAL_URL}/)"
        email_body = f"Du har begärt att ändra lösenordet för ditt användarkonto för SLS digitala utgåvor.\n\nBegäran gjordes på webbplatsen:\n{FRONTEND_EXTERNAL_URL}/\n\nFör att välja ett nytt lösenord, följ länken nedan. Länken är giltig i 30 minuter:\n\n{reset_link}\n\nOm du inte själv har begärt denna ändring kan du bortse från det här mejlet. Ingen ändring görs förrän du följer länken och slutför processen.\n\nVid frågor, kontakta info@sls.fi.\n\nDetta mejl har skickats automatiskt av Svenska litteratursällskapet i Finland (SLS). Vänligen svara inte på meddelandet."
    elif user_language == "fi":
        email_subject = f"Vaihda salasana (SLS:n digitaaliset editiot, {FRONTEND_EXTERNAL_URL}/)"
        email_body = f"Olet pyytänyt vaihtamaan SLS:n digitaalisten editioiden käyttäjätilisi salasanan.\n\nPyyntö tehtiin sivustolla:\n{FRONTEND_EXTERNAL_URL}/\n\nValitse uusi salasana alla olevan linkin kautta. Linkki on voimassa 30 minuuttia:\n\n{reset_link}\n\nJos et ole itse pyytänyt tätä muutosta, voit jättää tämän viestin huomiotta. Salasanaa ei muuteta, ennen kuin avaat linkin ja viet prosessin loppuun.\n\nJos sinulla on kysyttävää, ota yhteyttä osoitteeseen info@sls.fi.\n\nTämä on Svenska litteratursällskapet i Finlandin (SLS) automaattisesti lähettämä viesti. Älä vastaa tähän viestiin."
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
