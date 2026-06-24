from collections import OrderedDict
import logging
import os
from typing import Any, List, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger("sls_api.security_headers")


def normalize_csp_frame_ancestor_source(source: str) -> str:
    """
    Normalize absolute URL sources to origins and leave other CSP sources intact.
    """
    source = os.path.expandvars(source.strip())
    parsed_url = urlsplit(source)
    if parsed_url.scheme and parsed_url.netloc:
        return urlunsplit((
            parsed_url.scheme.lower(),
            parsed_url.netloc.lower(),
            '',
            '',
            ''
        ))

    return source


def get_frontend_external_origin(frontend_external_url: str) -> Optional[str]:
    """
    Return the origin portion of the frontend external URL for CSP source lists.
    """
    frontend_url = frontend_external_url.strip()
    if not frontend_url:
        return None

    parsed_url = urlsplit(frontend_url)
    if not parsed_url.scheme or not parsed_url.netloc:
        logger.warning(
            "Skipping FRONTEND_URL when building CSP frame-ancestors header "
            "because it is not an absolute URL: %s",
            frontend_url
        )
        return None

    return normalize_csp_frame_ancestor_source(frontend_url)


def get_configured_csp_frame_ancestors(config_value: Any) -> List[str]:
    """
    Return configured CSP frame-ancestor sources as a list.
    """
    if config_value is None:
        return []

    if isinstance(config_value, str):
        # Keep compatibility with the earlier space-separated config format.
        return [
            normalize_csp_frame_ancestor_source(source)
            for source in config_value.split()
        ]

    if isinstance(config_value, list):
        return [
            normalize_csp_frame_ancestor_source(source)
            for source in config_value
            if isinstance(source, str) and source.strip()
        ]

    logger.warning(
        "Ignoring allowed_csp_frame_ancestors because it must be a list "
        "or a space-separated string, got %s",
        type(config_value).__name__
    )
    return []


def get_allowed_csp_frame_ancestors(
        project_config: Optional[Mapping[str, Any]],
        frontend_external_url: str
) -> Optional[str]:
    """
    Return the Content-Security-Policy frame-ancestors value for a project.

    The frontend external URL is included automatically when it is an absolute
    URL. If the returned header value is not None, 'self' is included as the
    first frame-ancestors source.
    """
    if not project_config:
        return None

    configured_sources = get_configured_csp_frame_ancestors(
        project_config.get('allowed_csp_frame_ancestors')
    )
    frontend_origin = get_frontend_external_origin(frontend_external_url)
    if frontend_origin:
        configured_sources.append(frontend_origin)

    allowed_sources = [
        source
        for source in OrderedDict.fromkeys(configured_sources)
        if source != "'self'"
    ]
    if not allowed_sources:
        return None

    frame_ancestor_sources = ["'self'"] + allowed_sources
    return f"frame-ancestors {' '.join(frame_ancestor_sources)}"
