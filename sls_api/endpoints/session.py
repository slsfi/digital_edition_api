from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt, get_jwt_identity
from sls_api import rate_limiter
from sls_api.models import User

"""
Session probe endpoints for frontend authentication checks.

These routes are intentionally outside `/auth/*` so frontend request
interceptors can attach access tokens and run refresh logic uniformly.
"""

session = Blueprint('session', __name__, url_prefix="/session")


def _invalid_credentials_response():
    response = jsonify({"msg": "Invalid credentials", "err": "INCORRECT_CREDENTIALS"})
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response, 401


@session.route("/validate", methods=["GET"])
@rate_limiter.limit("60/minute")
@jwt_required()
def validate_session():
    """
    Validate current access-token session.

    Returns 200 only when the token is valid, not invalidated, and the
    user's email is verified. Returns 401 for all invalid states.
    """

    claims = get_jwt()
    identity = get_jwt_identity()
    jwt_issued_at = claims.get("iat")

    user = User.find_by_email(identity)
    if not user:
        return _invalid_credentials_response()

    if not jwt_issued_at:
        return _invalid_credentials_response()

    # Reject invalidated tokens
    if not User.check_token_validity(identity, jwt_issued_at):
        return _invalid_credentials_response()

    # Unverified users are treated as unauthorized for app session
    if not user.email_is_verified():
        return _invalid_credentials_response()

    response = jsonify({"authenticated": True})
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response, 200
