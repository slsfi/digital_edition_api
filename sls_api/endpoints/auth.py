import datetime
from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token, create_refresh_token, get_jwt_identity, jwt_required
import logging
from sls_api import jwt_redis_blocklist, rate_limiter
from sls_api.email import send_address_verification_email, send_password_reset_email
from sls_api.models import User

# minimum password length for users
MINIMUM_PASSWORD_LENGTH = 12

auth = Blueprint('auth', __name__)

logger = logging.getLogger("sls_api.auth")

"""
JWT-based Authorization

Routes in the API protected by @jwt_required() can only be accessed with a proper JWT token in the header
Routes in the API protected by @reader_auth_required() be only be accessed by logged-in users with email_verified=True, if reader_auth_required=True in the API config
Routes in the API protected by @cms_required() can only be accessed by logged-in CMS users
Routes in the API protected by @cms_required(edit=True) can only be accessed by logged-in CMS users with edit rights for the project
JWT Header format is "Authorization: Bearer <JWT_TOKEN>"
"""


@auth.route("/register", methods=["POST"])
@rate_limiter.limit("30/minute")
def register_user():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No JSON in payload."}), 400

    email = data.get("email", None)
    password = data.get("password", None)
    user_language = data.get("language", "en")      # ISO 639-1 language code
    if user_language not in ["en", "sv", "fi"]:
        logger.warning(f"User supplied invalid language {user_language} on register, defaulting to 'en'")
        user_language = "en"

    if not email or not password:
        logger.error("Invalid request to register user - no credentials provided")
        return jsonify({"msg": "email or password not in JSON payload.", "err": "NO_CREDENTIALS"}), 400
    # verify password meets requirements
    if len(password) < MINIMUM_PASSWORD_LENGTH:
        logger.error("Invalid request to register user - password too short")
        return jsonify({"msg": f"Password is too short, minimum length is {MINIMUM_PASSWORD_LENGTH}", "err": "PASSWORD_TOO_SHORT"}), 400
    # check for existing user account with this email
    existing_user = User.find_by_email(data["email"])
    if existing_user:
        if existing_user.email_is_verified:
            logger.error("Invalid request to register user - verified account already exists")
            return jsonify({"msg": "User {!r} already exists.".format(data["email"]), "err": "USER_ALREADY_EXISTS"}), 400
        else:
            # delete existing un-verified user, so a new user object can be created and a new verification link can be sent
            User.delete_user(data["email"])

    try:
        new_user = User.create_new_user(email, password)
        # create temporary access token for email verification
        verification_token = create_access_token(identity=new_user.email, expires_delta=datetime.timedelta(hours=8), fresh=True)
        # send token to user by email
        send_address_verification_email(to_address=new_user.email, access_token=verification_token, language=user_language)
        return jsonify(
            {
                "msg": "User {!r} was created. Please check email inbox to verify email before login.".format(data["email"])
            }
        ), 201
    except Exception:
        logger.exception("Error in user registration")
        return jsonify({"msg": "Error in user registration"}), 500


@auth.route("/login", methods=["POST"])
def login_user():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No credentials provided.", "err": "NO_CREDENTIALS"}), 400

    email = data.get("email", None)
    password = data.get("password", None)
    current_user = User.find_by_email(email)
    try:
        if not current_user.email_is_verified:
            return jsonify({"msg": "Email address has not been verified. Check your email inbox for a verification link.", "err": "EMAIL_NOT_VERIFIED"}, 403)
        success = current_user.check_password(password)
    except Exception:
        # user not found
        return jsonify({"msg": "Incorrect email or password.", "err": "INCORRECT_CREDENTIALS"}), 401
    if not success:
        # password mismatch
        return jsonify({"msg": "Incorrect email or password.", "err": "INCORRECT_CREDENTIALS"}), 401
    else:
        # update last_login_timestamp for user
        User.update_login_timestamp(email)

    projects = current_user.get_projects()  # get current projects for user to add as additional claims

    return jsonify(
        {
            "msg": "Logged in as {!r}".format(data["email"]),
            "access_token": create_access_token(identity=current_user.email, additional_claims={"projects": projects}),
            "refresh_token": create_refresh_token(identity=current_user.email, additional_claims={"projects": projects}),
            "user_projects": projects
        }
    ), 200


@auth.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh_token():
    identity = get_jwt_identity()
    user = User.find_by_email(identity)
    projects = user.get_projects()
    # update last_login_timestamp, a token refresh is equivalent to a login
    User.update_login_timestamp(identity)
    return jsonify(
        {
            "msg": "Logged in as {!r}".format(identity),
            "access_token": create_access_token(identity=identity, additional_claims={"projects": projects}),
            "user_projects": projects
        }
    ), 200


@auth.route("/verify_email", methods=["POST"])
@jwt_required(fresh=True)
def verify_email():
    identity = get_jwt_identity()
    user = User.find_by_email(identity)
    if user:
        User.mark_email_verified(identity)
        return jsonify({"msg": f"Email address {identity} verified. You may now log in."}, 200)
    else:
        return jsonify({"msg": f"Email address {identity} not a valid user in the system.", "err": "INCORRECT_CREDENTIALS"}, 400)


@auth.route("/forgot_password", methods=["POST"])
@rate_limiter.limit("5/minute")
def start_password_reset():
    """
    Begin password reset flow - take in email from JSON, send reset email to user if exists
    """
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No email provided.", "err": "NO_CREDENTIALS"}), 400
    email = data.get("email", None)
    if not email:
        return jsonify({"msg": "No email provided.", "err": "NO_CREDENTIALS"}), 400
    user = User.find_by_email(email)
    if not user:
        return jsonify({"msg": "If an account exists for this email address, a password reset link has been sent."}), 200
    user_language = data.get("language", "en")      # ISO 639-1 language code
    if user_language not in ["en", "sv", "fi"]:
        logger.warning(f"User supplied invalid language {user_language} with password reset request, defaulting to 'en'")
        user_language = "en"
    access_token = create_access_token(identity=user.email, expires_delta=datetime.timedelta(minutes=30), fresh=True)
    success = send_password_reset_email(to_address=user.email, access_token=access_token, language=user_language)
    if success:
        return jsonify({"msg": "If an account exists for this email address, a password reset link has been sent."}), 200
    else:
        return jsonify({"msg": "Internal Server Error"}), 500


@auth.route("/reset_password", methods=["POST"])
@jwt_required(fresh=True)
def finish_password_reset():
    """
    Finish password reset flow - verify temporary JWT and reset password to one given in JSON
    """
    identity = get_jwt_identity()
    user = User.find_by_email(identity)
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No password provided.", "err": "NO_CREDENTIALS"}), 400
    password = data.get("password", None)
    if not password:
        return jsonify({"msg": "No password provided.", "err": "NO_CREDENTIALS"}), 400
    if len(password) < MINIMUM_PASSWORD_LENGTH:
        return jsonify({"msg": f"Password is too short, minimum length is {MINIMUM_PASSWORD_LENGTH}", "err": "PASSWORD_TOO_SHORT"}), 400
    password_set = User.reset_password(user.email, password)
    if password_set:
        # revoke reset token
        jwt_id = get_jwt()["jti"]
        jwt_redis_blocklist.set(jwt_id, "", ex=datetime.timedelta(minutes=30))
        return jsonify({"msg": f"New password set for {user.email}"}), 200
    else:
        return jsonify({"msg": f"Failed to set password for {user.email}"}), 500


@auth.route("/logout", methods=["DELETE"])
@jwt_required(verify_type=False)
def logout():
    """
    Take in a valid access or refresh token and revoke it, effectively logging out.
    Can also be used to get rid of valid refresh tokens, for example on a password reset.
    """
    token = get_jwt()
    token_id = token["jti"]
    token_type = token["type"]
    if token_type == "access":
        jwt_redis_blocklist.set(token_id, "", ex=datetime.timedelta(minutes=30))
        return jsonify({"msg": "User logged out"}), 200
    elif token_type == "refresh":
        jwt_redis_blocklist.set(token_id, "", ex=datetime.timedelta(days=30))
        return jsonify({"msg": "User logged out"}), 200


@auth.route("/test", methods=["POST"])
@jwt_required()
def test_authentication():
    return jsonify(get_jwt_identity())
