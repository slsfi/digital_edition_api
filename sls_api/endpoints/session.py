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


def _authenticated_response():
    response = jsonify({"authenticated": True})
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response, 200


def _has_valid_session(require_cms: bool = False) -> bool:
    claims = get_jwt()
    identity = get_jwt_identity()
    jwt_issued_at = claims.get("iat")

    user = User.find_by_email(identity)
    if not user:
        return False

    if not jwt_issued_at:
        return False

    # Reject invalidated tokens
    if not User.check_token_validity(identity, jwt_issued_at):
        return False

    # Unverified users are treated as unauthorized for app session
    if not user.email_is_verified():
        return False

    # Reject non-CMS users if CMS user required
    if require_cms and not user.cms_user:
        return False

    return True


@session.route("/validate", methods=["GET"])
@rate_limiter.limit("60/minute")
@jwt_required()
def validate_session():
    """
    Validate current access-token session.

    Returns 200 only when the token is valid, not invalidated, and the
    user's email is verified. Returns 401 for all invalid states.
    """

    if not _has_valid_session():
        return _invalid_credentials_response()

    return _authenticated_response()


@session.route("/validate_cms", methods=["GET"])
@rate_limiter.limit("60/minute")
@jwt_required()
def validate_cms_session():
    """
    Validate current access-token session for CMS access.

    Returns 200 only when the token is valid, not invalidated, the user's
    email is verified, and the user has CMS access. Returns 401 for all
    invalid states.
    """

    if not _has_valid_session(require_cms=True):
        return _invalid_credentials_response()

    return _authenticated_response()
