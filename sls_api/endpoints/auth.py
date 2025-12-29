import datetime
from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token, create_refresh_token, get_jwt_identity, jwt_required
from sls_api.email import send_address_verification_email, send_password_reset_email
from sls_api.models import User

auth = Blueprint('auth', __name__)

"""
JWT-based Authorization

Routes in the API protected by @jwt_required() can only be accessed with a proper JWT token in the header
Routes in the API protected by @reader_auth_required() be only be accessed by logged-in users with email_verified=True, if reader_auth_required=True in the API config
Routes in the API protected by @cms_required() can only be accessed by logged-in CMS users
Routes in the API protected by @cms_required(edit=True) can only be accessed by logged-in CMS users with edit rights for the project
JWT Header format is "Authorization: Bearer <JWT_TOKEN>"
"""


@auth.route("/register", methods=["POST"])
def register_user():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No JSON in payload."}), 400

    email = data.get("email", None)
    password = data.get("password", None)

    if not email or not password:
        return jsonify({"msg": "email or password not in JSON payload."}), 400
    existing_user = User.find_by_email(data["email"])
    if existing_user:
        if existing_user.email_is_verified:
            return jsonify({"msg": "User {!r} already exists.".format(data["email"])}), 400
        else:
            # delete existing un-verified user, so a new user object can be created and a new verification link can be sent
            User.delete_user(data["email"])

    try:
        new_user = User.create_new_user(email, password)
        # create temporary access token for email verification
        verification_token = create_access_token(identity=new_user.email, expires_delta=datetime.timedelta(hours=8), fresh=True)
        send_address_verification_email(to_address=new_user.email, access_token=verification_token)
        return jsonify(
            {
                "msg": "User {!r} was created. Please check email inbox to verify email before login.".format(data["email"])
            }
        ), 201
    except Exception:
        return jsonify({"msg": "Error in user registration"}), 500


@auth.route("/login", methods=["POST"])
def login_user():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No credentials provided."}), 400

    email = data.get("email", None)
    password = data.get("password", None)
    current_user = User.find_by_email(email)
    if not current_user.email_is_verified:
        return jsonify({"msg": "Email address has not been verified. Check your email inbox for a verification link."}, 400)
    else:
        try:
            success = current_user.check_password(password)
        except Exception:
            return jsonify({"msg": "Incorrect email or password."}), 400
        if not success:
            return jsonify({"msg": "Incorrect email or password."}), 400
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
@jwt_required(locations=["query_string"], fresh=True)
def verify_email():
    identity = get_jwt_identity()
    user = User.find_by_email(identity)
    if user:
        User.mark_email_verified(identity)
        return jsonify({"msg": f"Email address {identity} verified. You may now log in."}, 200)
    else:
        return jsonify({"msg": f"Email address {identity} not a valid user in the system."}, 400)


@auth.route("/forgot_password", methods=["POST"])
def start_password_reset():
    """
    Begin password reset flow - take in email from JSON, send reset email to user if exists
    """
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No email provided."}), 400
    email = data.get("email", None)
    if not email:
        return jsonify({"msg": "No email provided."}), 400
    user = User.find_by_email(email)
    if not user:
        return jsonify({"msg": "User not found"}), 400
    access_token = create_access_token(identity=user.email, expires_delta=datetime.timedelta(minutes=30), fresh=True)
    success = send_password_reset_email(to_address=user.email, access_token=access_token)
    if success:
        return jsonify({"msg": "Password reset email sent"}), 200
    else:
        return jsonify({"msg": "Internal Server Error"}), 500


@auth.route("/reset_password", methods=["POST"])
@jwt_required(locations=["query_string"], fresh=True)
def finish_password_reset():
    """
    Finish password reset flow - verify temporary JWT and reset password to one given in JSON
    """
    identity = get_jwt_identity()
    user = User.find_by_email(identity)
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No password provided."}), 400
    password = data.get("password", None)
    if not password:
        return jsonify({"msg": "No password provided."}), 400
    password_set = User.reset_password(user.email, password)
    if password_set:
        return jsonify({"msg": f"New password set for {user.email}"}), 200
    else:
        return jsonify({"msg": f"Failed to set password for {user.email}"}), 500


@auth.route("/test", methods=["POST"])
@jwt_required()
def test_authentication():
    return jsonify(get_jwt_identity())
