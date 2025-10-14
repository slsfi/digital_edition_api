import calendar
from collections import OrderedDict
from datetime import datetime
from flask import jsonify, Response
from flask_jwt_extended import get_jwt, get_jwt_identity, verify_jwt_in_request
from functools import wraps
import glob
import hashlib
import io
import logging
from lxml import etree
from saxonche import PySaxonProcessor, PyXslt30Processor, PyXsltExecutable
import os
import re
from ruamel.yaml import YAML
from sls_api.models import User
from sqlalchemy import create_engine, Connection, MetaData, RowMapping, Table
from sqlalchemy.sql import and_, select, text
from sqlalchemy.sql.selectable import Select
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple
from werkzeug.security import safe_join

from sls_api.scripts.saxon_xml_document import SaxonXMLDocument

ALLOWED_EXTENSIONS_FOR_FACSIMILE_UPLOAD = ["tif", "tiff", "png", "jpg", "jpeg"]

# temporary folder uploaded facsimiles are stored in before being resized and stored properly in the project files
FACSIMILE_UPLOAD_FOLDER = "/tmp/uploads"

# these are the max resolutions for each zoom level of facsimile, used for resizing uploaded TIF files.
# imagemagick retains aspect ratio by default, so resizing a 730x1200 image to "600x600" would result in a 365x600 file
FACSIMILE_IMAGE_SIZES = {
    1: "600x600",
    2: "1200x1200",
    3: "2000x2000",
    4: "4000x4000"
}

# Default PostgreSQL collation for ordering
DEFAULT_COLLATION = "sv-x-icu"  # Generic Swedish Unicode collation

# Folder path from the project root to the folder where prerendered
# HTML output of collection texts are located. The original XML files
# are located in the "documents" folder and the generated web XML files
# in the "xml" folder. Hence "html/documents" (we might also have other
# HTML than prerendered HTML from the XML files).
PRERENDERED_HTML_PATH_IN_PROJECT_ROOT = "html/documents"

# Map of paths to XSLT stylesheets for HTML transformations for different
# text types. The paths to the XSLT stylesheets are relative to the
# project root.
XSL_PATH_MAP_FOR_HTML_TRANSFORMATIONS = {
    "com": "xslt/com.xsl",
    "est": "xslt/est.xsl",
    "fore": "xslt/foreword.xsl",
    "inl": "xslt/introduction.xsl",
    "ms_changes": "xslt/ms_changes.xsl",
    "ms_normalized": "xslt/ms_normalized.xsl",
    "tit": "xslt/title.xsl",
    "var_base": "xslt/poem_variants_est.xsl",
    "var_other": "xslt/poem_variants_other.xsl"
}

metadata = MetaData()

logger = logging.getLogger("sls_api.generics")

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with io.open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as config:
    yaml = YAML(typ="safe")
    config = yaml.load(config)

    # handle environment variables in the configuration file
    for setting, value in config.items():
        if isinstance(value, str):
            # handle strings that are or contain environment variables
            config[setting] = os.path.expandvars(value)
        elif isinstance(value, dict):
            # handle project settings that are or contain environment variables
            for project_setting, project_value in value.items():
                if isinstance(project_value, str):
                    value[project_setting] = os.path.expandvars(project_value)

    # connection pool settings - keep a pool of up to 30 connections, but allow spillover to up to 60 if needed.
    # after a connection has been idle for 5 minutes, invalidate it so it's recycled on the next database call
    db_engine = create_engine(config["engine"], pool_size=30, max_overflow=30, pool_recycle=300)
    elastic_config = config["elasticsearch_connection"]

    # reflect all tables from database so we know what they look like
    metadata.reflect(bind=db_engine)

# Initialise a Saxon processor and Saxon XSLT 3.0 processor so they can
# be used for all Saxon XSLT 3.0 transformations and don't need to be
# initialised separately for each transformation.
# Documentation for SaxonC's Python API:
# https://www.saxonica.com/saxon-c/doc12/html/saxonc.html
saxon_proc: PySaxonProcessor = PySaxonProcessor(license=False)
saxon_xslt_proc: PyXslt30Processor = saxon_proc.new_xslt30_processor()


def allowed_facsimile(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_FOR_FACSIMILE_UPLOAD


def get_project_config(project_name):
    if project_name in config:
        return config[project_name]
    return None


def get_project_collation(project_name: str) -> str:
    project_config = get_project_config(project_name)

    if project_config is None or "collation" not in project_config:
        return DEFAULT_COLLATION
    else:
        return project_config["collation"]


def int_or_none(var):
    try:
        return int(var)
    except Exception:
        return None


def calculate_checksum(full_file_path) -> str:
    """
    Read 'full_file_path' in chunks and generate an MD5 checksum for the file, returning as string
    """
    hash_md5 = hashlib.md5()
    with open(full_file_path, "rb") as f:
        logger.debug(f"Calculating MD5 checksum for {full_file_path}...")
        # read in chunks to prevent having to load entire file into memory at once
        for chunk in iter(lambda: f.read(8 * hash_md5.block_size), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def safe_checksum(path: str) -> str | None:
    """
    Return the MD5 checksum of a file if it exists, otherwise return None.

    This function is a "safe" wrapper around `calculate_checksum()` that
    avoids raising errors for missing files. It checks whether the given
    path exists first; if so, it calculates and returns the checksum,
    otherwise it returns `None`.

    Args:
        path (str): Path to the file.

    Returns:
        str | None: The MD5 checksum string if the file exists, otherwise None.
    """
    return calculate_checksum(path) if os.path.isfile(path) else None


def file_fingerprint(path: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Return a snapshot of a file as (size, md5).

    Args:
        path (str): Filesystem path to the file.

    Returns:
        tuple[Optional[int], Optional[str]]:
            - (None, None) if the file does not exist.
            - (size_in_bytes, md5_hex) if the file exists.

    Notes:
        - Always computes the checksum if the file exists. This ensures that
          later comparison against a post-fingerprint can detect same-size
          modifications.
    """
    try:
        size = os.path.getsize(path)
    except OSError:
        return (None, None)

    return (size, safe_checksum(path))  # safe_checksum returns str | None


def changed_by_size_or_hash(pre: Tuple[Optional[int], Optional[str]], path: str) -> bool:
    """
    Compare a pre-fingerprint of a file with the current file state to detect changes.

    Args:
        pre (tuple[Optional[int], Optional[str]]): The file state before
            processing, as returned by `file_fingerprint()`.
            - First element: file size in bytes, or None if file didn’t exist.
            - Second element: md5 checksum, or None if file didn’t exist.
        path (str): Filesystem path to check against the current state.

    Returns:
        bool: True if the file was created, deleted, resized, or modified;
        False if unchanged.

    Logic:
        - If existence or size differs → considered changed.
        - If file never existed and still doesn’t → unchanged.
        - If sizes are equal and file existed before → compute post-hash and
          compare with pre-hash to detect same-size modifications.
    """
    pre_size = pre[0]
    pre_md5 = pre[1]

    try:
        post_size = os.path.getsize(path)
    except OSError:
        post_size = None

    if pre_size != post_size:
        return True  # created, deleted, or resized

    if pre_size is None:
        return False  # didn't exist before, still doesn't

    # Same size and existed before → confirm with post-hash
    post_md5 = safe_checksum(path)
    return pre_md5 != post_md5


def project_permission_required(fn):
    """
    Function decorator that checks for JWT authorization and that the user has edit rights for the project.
    The project the method concerns should be the first positional argument or a keyword argument.
    """
    @wraps(fn)
    def decorated_function(*args, **kwargs):
        verify_jwt_in_request()
        # get JWT identity
        identity = get_jwt_identity()
        # get JWT claims to check for claimed project access
        claims = get_jwt()
        if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and identity == "test@test.com":
            # If in FLASK_DEBUG mode, test@test.com user has access to all projects
            return fn(*args, **kwargs)
        else:
            # locate project arg in function arguments
            if len(args) > 0:
                project = args[0]
            elif "project" in kwargs:
                project = kwargs["project"]
            else:
                return jsonify({"msg": "No project identified."}), 500

            # check for permission
            if "projects" not in claims or not claims["projects"]:
                # according to JWT, no access to any projects
                return jsonify({"msg": "No access to this project."}), 403
            elif check_for_project_permission_in_database(identity, project):
                # only run function if database says user *actually* has permissions
                return fn(*args, **kwargs)
            else:
                return jsonify({"msg": "No access to this project."}), 403
    return decorated_function


def check_for_project_permission_in_database(user_email, project_name) -> bool:
    """
    Helper method to check in database for project permission.
    Returns true if user has permission for the project in question, otherwise false.
    """
    # make sure user actually has edit rights
    user = User.find_by_email(user_email)
    if user:
        return user.can_edit_project(project_name)
    else:
        # user not found in database
        logger.warning(f"Ostensibly logged in user {user_email} was not found in the database.")
        return False


def get_project_id_from_name(project):
    projects = Table('project', metadata, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select(projects.c.id).where(projects.c.name == project)
    project_id = connection.execute(statement).fetchone()
    connection.close()
    try:
        return int(project_id.id)
    except Exception:
        return None


def select_all_from_table(table_name):
    table = Table(table_name, metadata, autoload_with=db_engine)
    connection = db_engine.connect()
    rows = connection.execute(select(table)).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


def get_table(table_name):
    return Table(table_name, metadata, autoload_with=db_engine)


def slugify_route(path):
    path = path.replace(" - ", "")
    path = path.replace(" ", "-")
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub(r'[^a-zA-Z0-9\\\/-]|_', '', re.sub('.md', '', path))
    return path.lower()


def slugify_id(path: str, language: Optional[str] = None) -> str:
    """
    Generates a slug identifier from a file path by extracting numeric
    prefixes from path segments.

    This function scans each segment of the given file path and collects
    leading digit sequences (of any length) from segments that start with
    digits. These numeric parts are joined with hyphens to form the slug.
    If a non-empty `language` string is provided, it is prepended to
    the slug with a hyphen separator.

    Args:
        path (str): The full file path from which to generate the slug.
        language (str): An optional language code to prefix the slug. If
                        empty, no prefix is added.

    Returns:
        str: A hyphen-separated slug composed of leading numeric parts
             from the path, optionally prefixed by the language code.

    Example:
        >>> slugify_id("/path/to/04 - Articles/01 - Introduction.md", "en")
        'en-04-01'

        >>> slugify_id("/docs/202 - History/003 - Chapter.md", "")
        '202-003'
    """
    segments = path.split(os.sep)
    numbered_parts = []

    for segment in segments:
        segment = segment.lstrip()
        digits = []
        for ch in segment:  # collect digits at start of the segment
            if ch.isdigit():
                digits.append(ch)
            else:
                break
        if digits:
            numbered_parts.append(''.join(digits))

    slug = '-'.join(numbered_parts)
    return f"{language}-{slug}" if language else slug


def slugify_path(project, path):
    project_config = get_project_config(project)
    path = split_after(path, "/" + project_config["file_root"] + "/md/")
    return path.replace('.md', '')


def path_hierarchy(project, path, language):
    project_config = get_project_config(project)
    hierarchy = {
        'id': slugify_id(path, language),
        'title': filter_title(os.path.basename(path)),
        'basename': os.path.basename(path).replace('.md', ''),
        'path': slugify_path(project, path),
        'fullpath': path,
        'route': slugify_route(split_after(path, "/" + project_config["file_root"] + "/md/")),
        'type': 'folder',
        'children': [path_hierarchy(project, p, language) for p in sorted(glob.glob(os.path.join(path, '*')))]
    }

    if not hierarchy['children']:
        del hierarchy['children']
        hierarchy['type'] = 'file'

    return hierarchy


def filter_title(path):
    path = path.lstrip(' -0123456789')
    path = path.replace('.md', '')
    return path.strip()


def split_after(value, a):
    return value.lstrip(a)


def cache_is_recent(source_file, xsl_file, cache_file):
    """
    Returns False if the source or xsl file have been modified since the creation of the cache file
    Returns False if the cache is more than 'cache_lifetime_seconds' seconds old, as defined in config file
    Otherwise, returns True
    """
    try:
        source_file_mtime = os.path.getmtime(source_file)
        xsl_file_mtime = os.path.getmtime(xsl_file)
        cache_file_mtime = os.path.getmtime(cache_file)
    except OSError:
        return False
    if source_file_mtime > cache_file_mtime or xsl_file_mtime > cache_file_mtime:
        return False
    elif calendar.timegm(time.gmtime()) > (cache_file_mtime + config["cache_lifetime_seconds"]):
        return False
    return True


def get_published_status(
        project: str,
        collection_id: str,
        publication_id: Optional[str] = None
) -> Tuple[bool, str, Optional[str]]:
    """
    Returns info on if project, publication_collection, and optionally
    publication are all valid and published. Also validates that
    collection_id and publication_id (if not None) can be converted to
    integers.

    Returns three values:
        - a boolean which is True if the following conditions are met:
            - project name and config are valid
            - collection_id and optional publication_id are valid and
              can be converted to integers
            - the collection/publication can be shown
        - a message text why it can't be shown, or an empty string if
          it can be
        - legacy_id value of the collection

    Collections/publications can be shown if they're externally
    published (published==2), or if they're internally published
    (published==1) and show_internally_published is True
    """
    project_config = get_project_config(project)
    if project_config is None:
        return False, f"The project '{project}' does not exist.", None

    if project_config.get("file_root") is None:
        return False, f"File root missing from '{project}' project config.", None

    c_id = int_or_none(collection_id)
    if c_id is None or c_id < 1:
        return False, "Invalid collection_id.", None

    p_id = int_or_none(publication_id)
    if publication_id is not None and (p_id is None or p_id < 1):
        return False, "Invalid publication_id.", None

    try:
        project_table = get_table("project")
        collection_table = get_table("publication_collection")
        publication_table = get_table("publication")

        with db_engine.connect() as connection:
            cols = [
                project_table.c.published.label("proj_pub"),
                collection_table.c.published.label("col_pub"),
                collection_table.c.legacy_id.label("col_legacy_id"),
            ]

            from_clause = project_table.join(
                collection_table,
                collection_table.c.project_id == project_table.c.id,
            )

            wheres = [
                project_table.c.name == str(project),
                collection_table.c.id == c_id,
                project_table.c.deleted < 1,
                collection_table.c.deleted < 1
            ]

            if publication_id is not None:
                cols.append(publication_table.c.published.label("pub"))

                from_clause = from_clause.join(
                    publication_table,
                    publication_table.c.publication_collection_id == collection_table.c.id
                )

                wheres += [
                    publication_table.c.deleted < 1,
                    publication_table.c.id == p_id
                ]

            statement = select(*cols).select_from(from_clause).where(*wheres)
            row = connection.execute(statement).mappings().first()
    except Exception:
        message = "Unexpected error getting published status."
        logger.exception(message)
        return False, message, None

    show_internal = project_config["show_internally_published"]
    can_show = False
    message = ""
    col_legacy_id = None

    if row is None:
        message = "Content does not exist."
    else:
        col_legacy_id = row["col_legacy_id"]
        pub_values = [row["proj_pub"], row["col_pub"]]
        if publication_id is not None:
            pub_values.append(row["pub"])

        status = -1 if any(v is None for v in pub_values) else min(pub_values)

        if status < 1:
            message = "Content is not published."
        elif status == 1 and not show_internal:
            message = "Content is not publicly available."
        else:
            can_show = True

    return can_show, message, col_legacy_id


class FileResolver(etree.Resolver):
    def resolve(self, system_url, public_id, context):
        logger.debug("Resolving {}".format(system_url))
        return self.resolve_filename(system_url, context)


def transform_xml(
        xsl_file_path: Optional[str],
        xml_file_path: str,
        params: Optional[Dict[str, Any]] = None,
        use_saxon: bool = False,
        saxon_proc: Optional[PySaxonProcessor] = None,
        xslt_exec: Optional[PyXsltExecutable] = None
) -> str:
    """
    Transform an XML document using an XSLT stylesheet with optional parameters.
    The transformation can be performed either using the lxml XSLT 1.0
    processor (default) or the Saxon XSLT 3.0 processor.

    Parameters:
        xsl_file_path (str or None): File path to an XSLT stylesheet. If None,
            Saxon must be used and an compiled XSLT executable passed.
        xml_file_path (str): File path to the XML document which is to be
            transformed.
        params (dict or OrderedDict, optional): A dictionary with parameters
            for the XSLT stylesheet. Defaults to None.
        use_saxon (bool, optional): Whether to use the Saxon processor (instead
            of the lxml processor) or not. Defaults to False.
        passed_saxon_proc (PySaxonProcessor, optional): A Saxon processor that
            should be used instead of the global Saxon processor. Defaults to None.
        passed_xslt_exec (PyXsltExecutable, optional): A compiled Saxon XSLT
            executable that should be used instead of compiling `xsl_file_path`.
            Defaults to None.

    Returns:
        String representation of the result document.
    """
    logger.debug("Transforming %s using %s", xml_file_path, xsl_file_path)

    if params is not None:
        logger.debug("Parameters are %r", params)
        if not isinstance(params, dict) and not isinstance(params, OrderedDict):
            raise Exception(f"Invalid parameters for XSLT transformation, must be of type dict or OrderedDict, not {type(params)}")

        if not use_saxon:
            # lxml requires string parameters to be valid XPath string literals
            # strparam() ensures proper quoting and escaping
            params = {
                key: lxml_escape_quotes_if_string(val)
                for key, val in params.items()
            }

    if not os.path.isfile(xml_file_path):
        return f"XML file {xml_file_path!r} not found!"

    if use_saxon:
        # Use the Saxon XSLT 3.0 processor.
        if saxon_proc is None:
            return "Saxon XSLT processor not set!"

        if xslt_exec is None and xsl_file_path is not None:
            if not os.path.isfile(xsl_file_path):
                return f"XSL file {xsl_file_path!r} not found!"

            xslt_exec: PyXsltExecutable = saxon_xslt_proc.compile_stylesheet(
                    stylesheet_file=xsl_file_path,
                    encoding="utf-8"
            )
        elif xslt_exec is None:
            return "Neither XSL file nor Saxon XSLT executable passed to transformation!"

        xml_doc: SaxonXMLDocument = SaxonXMLDocument(saxon_proc, xml_file_path)
        return xml_doc.transform_to_string(xslt_exec, params, format_output=False)
    else:
        if not os.path.isfile(xsl_file_path) or xsl_file_path is None:
            return f"XSL file {xsl_file_path!r} not found!"

        # Use the lxml XSLT 1.0 processor.
        with open(xml_file_path, mode="rb") as xml_file:
            xml_contents = xml_file.read()
            xml_root = etree.fromstring(xml_contents)

        xsl_parser = etree.XMLParser()
        xsl_parser.resolvers.add(FileResolver())
        with open(xsl_file_path, encoding="utf-8") as xsl_file:
            xslt_root = etree.parse(xsl_file, parser=xsl_parser)
            xsl_transform = etree.XSLT(xslt_root)

        if params is None:
            result = xsl_transform(xml_root)
        else:
            result = xsl_transform(xml_root, **params)

        if len(xsl_transform.error_log) > 0:
            logger.error("XSL transform error: %s", xsl_transform.error_log)

        return str(result)


def get_transformed_xml_content_with_caching(
        project: str,
        base_text_type: str,
        xml_filename: str,
        xsl_path: str,
        xslt_parameters: Optional[Dict] = None
) -> str:
    """
    Transforms the given XML file with the given XSLT stylesheet and returns
    the result as a string. The result is cached.
    """
    project_config = get_project_config(project)
    if project_config is None:
        return "No such project."

    xml_file_path = safe_join(project_config["file_root"], "xml", base_text_type, xml_filename)
    xsl_file_path = safe_join(project_config["file_root"], xsl_path)
    cache_folder = os.path.join("/tmp", "api_cache", project, base_text_type)
    os.makedirs(cache_folder, exist_ok=True)

    if base_text_type == "ms" and "ms_changes" in xsl_path:
        cache_extension = "_changes.html"
    elif base_text_type == "ms" and "ms_normalized" in xsl_path:
        cache_extension = "_normalized.html"
    else:
        cache_extension = ".html"

    cache_filename_stem = xml_filename.split(".xml")[0]
    if xslt_parameters is not None:
        if 'noteId' in xslt_parameters:
            cache_filename_stem = f"{cache_filename_stem}_{xslt_parameters['noteId']}"
        if 'sectionId' in xslt_parameters:
            cache_filename_stem = f"{cache_filename_stem}_{xslt_parameters['sectionId']}"

    cache_file_path = os.path.join(cache_folder, f"{cache_filename_stem}{cache_extension}")
    logger.debug("Cache file path for %s is %s", xml_filename, cache_file_path)

    content = None

    if os.path.isfile(cache_file_path):
        if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
            try:
                with open(cache_file_path, encoding="utf-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                logger.exception("Error reading content from cache for %s", cache_file_path)
                # Ensure content is set to None so we try to get it by transforming
                content = None
                os.remove(cache_file_path)
            else:
                logger.debug("Content fetched from cache.")
        else:
            logger.debug("Cache file is old or invalid, deleting cache file...")
            os.remove(cache_file_path)

    if os.path.isfile(xml_file_path) and content is None:
        logger.debug("Transforming %s with %s", xml_file_path, xsl_file_path)
        try:
            use_saxon_xslt = project_config.get("use_saxon_xslt", False)
            content = transform_xml(
                    xsl_file_path,
                    xml_file_path,
                    params=xslt_parameters,
                    use_saxon=use_saxon_xslt,
                    saxon_proc=(saxon_proc if use_saxon_xslt else None)
            )

            if not use_saxon_xslt:
                # The legacy XSLT stylesheets output @id where @data-id is
                # required by the frontend, so replace them for applicable
                # text types.
                # TODO: fix this in all projects’ XSLT and then remove from here
                # TODO: and from publisher.py
                if base_text_type in ["est", "ms", "inl", "tit", "fore"]:
                    content = content.replace(" id=", " data-id=")

            try:
                with open(cache_file_path, mode="w", encoding="utf-8") as cache_file:
                    cache_file.write(content)
            except Exception:
                logger.exception("Could not create cachefile")
        except Exception as e:
            logger.exception("Error when parsing/transforming XML file")
            content = "Error parsing/transforming document\n"
            content += str(e)
    elif content is None:
        content = "File not found"

    return content


def get_prerendered_html_content(
        project_file_root: str,
        base_text_type: str,
        html_filename: str
) -> Optional[str]:
    """
    Returns the content of the given prerenderd HTML file, or None if
    an error occurs.
    """
    file_path = safe_join(project_file_root,
                          PRERENDERED_HTML_PATH_IN_PROJECT_ROOT,
                          base_text_type,
                          html_filename)

    if file_path is None:
        logger.error("safe_join returned None for path %r", html_filename)
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as html_file:
            return html_file.read()
    except UnicodeDecodeError as e:
        logger.exception("Decode error reading %s at pos %s", file_path, e.start)
    except OSError as e:
        logger.exception("OS error reading %s: %s", file_path, e)
    except Exception:
        logger.exception("Unexpected error when reading %s", file_path)

    return None


def get_prerendered_or_transformed_xml_content(
        text_type: str,
        filename_stem: str,
        project: str,
        project_config: Optional[Mapping] = None,
        xslt_parameters: Optional[Dict] = None
) -> Tuple[str, str]:
    """
    Return HTML for an XML-based text plus the source used. Tries to load
    prerendered HTML when `prerender_html` in the project config is
    truthy. If unavailable, falls back to transforming the corresponding
    XML via the XSLT mapped by `text_type`.

    Parameters:
        text_type (str): Key selecting the XSLT (e.g., "est", "inl", "ms_*").
        filename_stem (str): Base filename without extension.
        project (str): Project name.
        project_config (Mapping, optional): Optional project config;
            fetched based on project name `None`.
        xslt_parameters (dict, optional): Optional XSLT parameters.

    Returns:
        (content, source): HTML string and either "prerendered" or
        "transformed".
    """
    base_text_type = text_type.split("_")[0]
    content = None
    used_source = None

    if project_config is None:
        project_config = get_project_config(project)
    file_root = project_config.get("file_root", "")

    if project_config.get("prerender_html", False):
        # Get prerendered HTML
        html_filename = filename_stem
        if "sectionId" in xslt_parameters:
            html_filename = f"{filename_stem}_{xslt_parameters['sectionId']}"
        html_filename = f"{html_filename}.html"

        content = get_prerendered_html_content(
            project_file_root=file_root,
            base_text_type=base_text_type,
            html_filename=html_filename
        )
        if content is not None:
            used_source = "prerendered"

    if content is None:
        # No prerendered content -> transform XML to HTML
        xsl_path = XSL_PATH_MAP_FOR_HTML_TRANSFORMATIONS.get(text_type)
        xml_filename = (
            filename_stem.replace(f"_{text_type}_", f"_{base_text_type}_")
            if base_text_type == "ms" else filename_stem
        )
        xml_filename = f"{xml_filename}.xml"

        if xsl_path is not None:
            content = get_transformed_xml_content_with_caching(
                project=project,
                base_text_type=base_text_type,
                xml_filename=xml_filename,
                xsl_path=xsl_path,
                xslt_parameters=xslt_parameters
            )
        else:
            content = f"Could not find XSLT stylesheet for the text type '{text_type}'"
            logger.error("XSL map for text type '%s' returned `None`, unable to transform %s.", text_type, xml_filename)
        used_source = "transformed"

    return content, used_source


def get_frontmatter_page_content(
        text_type: str,
        collection_id: str,
        language: str,
        project: str
) -> Tuple[str, str]:
    project_config = get_project_config(project)
    show_internal = project_config.get("show_internally_published", False)
    version = "int" if show_internal else "ext"
    filename_stem = f"{collection_id}_{text_type}_{language}_{version}"

    return get_prerendered_or_transformed_xml_content(
        text_type=text_type,
        filename_stem=filename_stem,
        project=project,
        project_config=project_config
    )


def update_publication_related_table(
        connection: Connection,
        text_type: str,
        id: int,
        values: Dict[str, Any],
        return_all_columns: bool = False,
        exclude_deleted: bool = True
) -> Optional[List[Dict[str, Any]]]:
    """
    Helper function to update rows in the appropriate publication-related
    table based on the provided text type.

    This function updates records in one of the publication-related
    tables ('publication', 'publication_comment', 'publication_manuscript',
    'publication_version', 'publication_collection_introduction', or
    'publication_collection_title') based on the specified `text_type`. It
    dynamically constructs the update statement depending on the table and
    the ID column relevant to that table.

    Args:
        connection (Connection): An active database connection through
            SQLAlchemy.
        text_type (str): The type of text to update. Must be one of
            'publication', 'comment', 'manuscript', 'version',
            'collection_introduction', or 'collection_title'.
        id (int): The ID of the row to update. Refers to either the
            `id` column (for 'publication', 'comment',
            'collection_introduction' and 'collection_title') or the
            `publication_id` column (for 'manuscript' and 'version').
        values (Dict[str, Any]): A dictionary of column names and their
            new values to update.
        return_all_columns (bool): When set to `True`, the function
            returns all columns of updated rows, otherwise just the `id`
            column of updated rows. Defaults to `False`.
        exclude_deleted (bool): When set to `True`, the function updates
            only records that are non-deleted, otherwise no filtering
            is done base on deleted status. Defaults to `True`.

    Returns:
        A list of dictionaries with the updated rows. Returns None if no
        update is performed or an error occurs.

    Logs:
        Exception: Any exceptions encountered during the update operation
        are logged. The function returns None if an exception occurs.
    """
    try:
        if text_type not in ["publication",
                             "comment",
                             "manuscript",
                             "version",
                             "collection_introduction",
                             "collection_title"]:
            return None

        target_table = (get_table(f"publication_{text_type}")
                        if text_type != "publication"
                        else get_table("publication"))

        id_column = (target_table.c.publication_id
                     if text_type in ["manuscript", "version"]
                     else target_table.c.id)

        return_data = (target_table.c
                       if return_all_columns
                       else (target_table.c.id,))

        stmt = target_table.update().where(id_column == id)
        if exclude_deleted:
            stmt = stmt.where(target_table.c.deleted < 1)
        stmt = stmt.values(**values).returning(*return_data)

        updated_rows = connection.execute(stmt).fetchall()

        return [row._asdict() for row in updated_rows]

    except Exception:
        return None


def build_select_with_filters(table: Table, filters: dict, columns=None) -> Select:
    """
    Build a parameterized SQLAlchemy SELECT statement with dynamic WHERE
    conditions.

    This function constructs a SELECT statement against the given
    SQLAlchemy Table object, applying equality-based filtering conditions
    derived from the `filters` dictionary. Each key in the dictionary is
    expected to match a column name in the table. Only valid columns are
    included in the final WHERE clause.

    Args:
        table (sqlalchemy.Table): The SQLAlchemy Table object
            representing the database table to query.
        filters (dict): A dictionary where each key is a column name (as
            a string) and the corresponding value is the value to filter
            by. For example:
            {
                "publication_id": 1,
                "deleted": 0
            }
        columns (str | list[str] | None, optional): A single column name
            or list of column names to select. If None, all columns are
            selected.

    Returns:
        sqlalchemy.sql.selectable.Select: A SQLAlchemy SELECT object with
        a dynamically constructed WHERE clause.

    Raises:
        ValueError if none of the keys in the filter dictionary match
        any columns in the provided table, or if the dictionary is empty,
        or if no valid columns are provided.

    Example:

    >>> stmt = build_select_with_filters(my_table, {"id": 42, "type": "draft"}, columns=["id"])
    >>> result = connection.execute(stmt).fetchone()
    """
    # Validate and collect filter conditions
    conditions = [
        table.c[key] == value
        for key, value in filters.items()
        if key in table.c
    ]
    if not conditions:
        raise ValueError("No valid filters found")

    # Handle column selection
    if columns is None:
        stmt = select(table)
    else:
        if isinstance(columns, str):
            columns = [columns]
        selected_cols = [
            table.c[col]
            for col in columns
            if col in table.c
        ]
        if not selected_cols:
            raise ValueError("No valid columns found for selection")
        stmt = select(*selected_cols)

    return stmt.where(and_(*conditions))


def create_translation(neutral, connection=None):
    """
    Inserts a new translation record with the provided neutral text and
    returns the generated ID.

    If a connection is provided, it uses the existing connection. If no
    connection is provided, a new connection is created for the operation,
    and it will be closed automatically once the insertion is completed.

    Args:
        neutral (str): The neutral text to be inserted into the 'translation' table.
        connection (optional, sqlalchemy.engine.Connection): An existing database connection. If None, a new connection will be created.

    Returns:
        int/None: The ID of the newly created translation record, or None if no ID is returned.
    """
    # If no connection is provided, create a new one
    if connection is None:
        connection = db_engine.connect()
        new_connection = True
    else:
        new_connection = False

    try:
        # Use the provided or newly created connection
        stmt = """ INSERT INTO translation (neutral_text) VALUES(:neutral) RETURNING id """
        statement = text(stmt).bindparams(neutral=neutral)
        result = connection.execute(statement)
        row = result.fetchone()

        # Return the translation ID if available
        return row.id if row else None
    except Exception:
        return None
    finally:
        # Close the connection only if it was created inside this function
        if new_connection:
            connection.close()


# Create a stub for a translation text
def create_translation_text(translation_id, table_name):
    connection = db_engine.connect()
    if translation_id is not None:
        with connection.begin():
            stmt = """ INSERT INTO translation_text (translation_id, text, table_name, field_name, language) VALUES(:t_id, 'placeholder', :table_name, 'language', 'not set') RETURNING id """
            statement = text(stmt).bindparams(t_id=translation_id, table_name=table_name)
            connection.execute(statement)
    connection.close()


# Get a translation_text_id based on translation_id, table_name, field_name, language
def get_translation_text_id(translation_id, table_name, field_name, language):
    connection = db_engine.connect()
    if translation_id is not None:
        stmt = """
            SELECT id
            FROM translation_text
            WHERE
                (
                    translation_id = :t_id
                    AND (
                        language IS NULL
                        OR language = 'not set'
                    )
                    AND table_name = :table_name
                    AND field_name = :field_name
                    AND deleted = 0
                )
                OR
                (
                    translation_id = :t_id
                    AND language = :language
                    AND table_name = :table_name
                    AND field_name = :field_name
                    AND language != 'not set'
                    AND deleted = 0
                )
            LIMIT 1
        """
        statement = text(stmt).bindparams(t_id=translation_id, table_name=table_name, field_name=field_name, language=language)
        result = connection.execute(statement)
        row = result.fetchone()
        connection.close()
        if row is not None:
            return row.id
        else:
            return None
    else:
        return None


def get_xml_content(
        project: str,
        folder: str,
        xml_filename: str,
        xsl_filename: Optional[str],
        parameters
) -> str:
    """
    Transforms the given XML file with the given XSLT stylesheet and
    returns the result as a string. No caching of the result.

    If the XSLT filename is `None`, the content of the XML file is
    returned untransformed.
    """
    project_config = get_project_config(project)
    if project_config is None:
        return "No such project."
    xml_file_path = safe_join(project_config["file_root"], "xml", folder, xml_filename)
    if xsl_filename is not None:
        xsl_file_path = safe_join(project_config["file_root"], "xslt", xsl_filename)
    else:
        xsl_file_path = None

    if os.path.isfile(xml_file_path):
        logger.info("Transforming %s with %s", xml_file_path, xsl_filename)
        if xsl_file_path is not None:
            try:
                use_saxon_xslt = project_config.get("use_saxon_xslt", False)
                content = transform_xml(
                        xsl_file_path,
                        xml_file_path,
                        params=parameters,
                        use_saxon=use_saxon_xslt,
                        saxon_proc=(saxon_proc if use_saxon_xslt else None)
                )
            except Exception as e:
                logger.exception("Error when parsing/transforming XML file")
                content = "Error parsing/transforming document"
                content += str(e)
        else:
            try:
                with open(xml_file_path, encoding="utf-8-sig") as xml_file:
                    content = xml_file.read()
            except Exception as e:
                logger.exception("Error opening/reading XML file")
                content = "Error opening/reading XML file"
                content += str(e)
    else:
        content = "File not found"
    return content


def flatten_json(json, flattened):
    """
    Recursive function for flattening the given json, i.e. turning it into
    a one dimensional array, which is stored in "flattened".
    """
    if json is not None:
        if json.get('children') is not None:
            for i in range(len(json['children'])):
                if json['children'][i].get('itemId') is not None and json['children'][i].get('itemId') != '':
                    flattened.append(json['children'][i])
                flatten_json(json['children'][i], flattened)


def get_first_valid_item_from_toc(flattened_toc):
    """
    Searches the given array of toc items for the first one that has an
    `itemId` value and a type value other than `subtitle` and
    `section_title`.
    """
    for i in range(len(flattened_toc)):
        if (
            flattened_toc[i].get('itemId') is not None and
            flattened_toc[i].get('itemId') != '' and
            flattened_toc[i].get('type') is not None and
            flattened_toc[i].get('type') != 'subtitle' and
            flattened_toc[i].get('type') != 'section_title'
        ):
            return flattened_toc[i]
    return {}


def get_allowed_cors_origins(project: str) -> list:
    """
    Retrieve the allowed CORS origins for a specific project.

    Args:
        project (str): The name of the project to get allowed CORS origins for.

    Returns:
        list: A list of allowed CORS origins for the project, or an empty list if none are found.
    """
    project_config = get_project_config(project)
    if not project_config:
        return []
    return project_config.get("allowed_cors_origins", [])


def validate_project_name(name: str) -> Tuple[bool, Optional[str]]:
    """
    Validates the project name according to specified constraints.

    The project name must meet the following criteria:
    - Length no less than 3 and no more than 32 characters.
    - Contains only lowercase letters (a-z), digits (0-9) and underscores (_).

    Parameters:
    - name (str): The project name to validate.

    Returns:
    - Tuple[bool, Optional[str]]: A tuple where the first element is a
      boolean indicating if the validation passed (`True`) or failed
      (`False`), and the second element is an error message string if
      validation failed, or `None` if validation passed.
    """
    # Check length constraint
    if len(name) < 3 or len(name) > 32:
        return False, "'name' must be minimum 3 and maximum 32 characters in length."

    # Check allowed characters (lowercase letters a-z, digits 0-9 and underscores _)
    if not re.fullmatch(r'[a-z0-9_]+', name):
        return False, "'name' can only contain lowercase letters a-z and digits 0-9."

    return True, None


def validate_int(
        value: Any,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None
) -> bool:
    """
    Validates that 'value' is an integer and optionally within specified
    bounds.

    Parameters:
    - value (Any): The value to validate.
    - min_value (Optional[int]): Minimum allowed value (inclusive).
    - max_value (Optional[int]): Maximum allowed value (inclusive).

    Returns:
    - A boolean indicating if the validation passed (`True`) or failed
      (`False`).
    """
    if (
        not isinstance(value, int)
        or (min_value is not None and value < min_value)
        or (max_value is not None and value > max_value)
    ):
        return False
    return True


def create_success_response(
    message: str,
    data: Optional[Any] = None,
    status_code: int = 200
) -> Tuple[Response, int]:
    """
    Create a standardized JSON success response.

    Args:

        message (str): A message describing the success.
        data (Any, optional): The data to include in the response. Defaults to None.
        status_code (int, optional): The HTTP status code for the response. Defaults to 200.

    Returns:

        A tuple containing the Flask Response object with JSON data and the HTTP status code.
    """
    return jsonify({
        "success": True,
        "message": message,
        "data": data
    }), status_code


def create_error_response(
    message: str,
    status_code: int = 400,
    data: Optional[Any] = None
) -> Tuple[Response, int]:
    """
    Create a standardized JSON error response.

    Args:

        message (str): A message describing the error.
        status_code (int, optional): The HTTP status code for the response. Defaults to 400.
        data (Any, optional): The data to include in the response. Defaults to None.

    Returns:

        A tuple containing the Flask Response object with JSON data and the HTTP status code.
    """
    return jsonify({
        "success": False,
        "message": message,
        "data": data
    }), status_code


def handle_deleted_flag(values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adjusts the 'published' flag in the provided values dictionary if the
    'deleted' flag is set to a truthy value (like 1). This ensures that
    if a record is marked as deleted, it cannot remain published.

    Args:
        values (Dict[str, Any]): A dictionary containing the fields to
            update for a record. If the dictionary contains a 'deleted'
            key with a truthy value, the value of the 'published' key
            will be set to 0.

    Returns:
        The updated dictionary.
    """
    if values.get("deleted"):
        values["published"] = 0
    return values


def is_valid_year(year_string: str) -> bool:
    """
    Checks if a string can be parsed as a four-digit year between 1 and 9999.

    The function validates that the input string consists of only digits and
    represents a year between 1 and 9999. It handles both zero-padded and
    non-zero-padded formats for years less than 1000 (e.g., "0456" and "456"
    are both valid).

    Args:

        year_string (str): The input string representing a year.

    Returns:

        bool: True if the string represents a valid year between 1 and 9999,
        False otherwise.
    """
    if not year_string.isdigit():
        return False

    # Check if the integer value is between 1 and 9999
    year = int(year_string)
    return 1 <= year <= 9999


def is_valid_date(date_string: str) -> bool:
    """
    Validates if a given string conforms to the 'YYYY-MM-DD' date format
    and checks if it represents a sensible date.

    Parameters:

        date_string (str): The input string to be checked.

    Returns:

        bool: True if the string is a valid 'YYYY-MM-DD' date format and
        represents a logically correct date; False otherwise.

    Examples:

        >>> is_valid_date("2023-10-31")
        True
        >>> is_valid_date("2023-02-29")
        False  # 2023 is not a leap year
        >>> is_valid_date("2023-13-31")
        False  # Invalid month
        >>> is_valid_date("23-10-31")
        False  # Invalid format
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_valid_year_month(date_string: str) -> bool:
    """
    Validates if a given string conforms to the 'YYYY-MM' date format.
    If the YYYY part is a year before 1000, it must be zero-padded.

    Parameters:

        date_string (str): The input string to be checked.

    Returns:

        bool: True if the string is a valid 'YYYY-MM' format; False
        otherwise.

    Examples:

        >>> is_valid_year_month("2023-10")
        True
        >>> is_valid_year_month("2023-13")
        False  # Invalid month
    """
    try:
        datetime.strptime(date_string, "%Y-%m")
        return True
    except ValueError:
        return False


def is_any_valid_date_format(date_string: str) -> bool:
    """
    Validates if a given string conforms to any of the following date
    formats:

    - 'YYYY': four-digit year between 1 and 9999, also valid for
      non-zero-padded years before 1000.
    - 'YYYY-MM'
    - 'YYYY-MM-DD'

    Parameters:

        date_string (str): The input string to be checked.

    Returns:

        bool: True if the string is in any of the valid date formats;
        False otherwise.
    """
    if (
        is_valid_year(date_string)
        or is_valid_date(date_string)
        or is_valid_year_month(date_string)
    ):
        return True

    return False


def is_valid_language(language_tag: str) -> bool:
    """
    Validate the language tag against a list of base language tags
    (BCP 47 subset).

    Args:
        language_tag (str): The language tag to validate.

    Returns:
        bool: True if the language tag is valid, False otherwise.
    """
    valid_tags = {
        "ar",  # Arabic
        "cs",  # Czech
        "da",  # Danish
        "de",  # German
        "el",  # Greek
        "en",  # English
        "es",  # Spanish
        "fi",  # Finnish
        "fr",  # French
        "hu",  # Hungarian
        "is",  # Icelandic
        "it",  # Italian
        "la",  # Latin
        "nl",  # Dutch
        "no",  # Norwegian
        "pl",  # Polish
        "pt",  # Portuguese
        "ru",  # Russian
        "sv"   # Swedish
    }
    return language_tag in valid_tags


def lxml_escape_quotes_if_string(value: Any) -> Any:
    """
    Ensures safe escaping of string values when passing parameters to lxml
    XSLT transformations.

    If the value is a string, it will be wrapped and escaped using
    etree.XSLT.strparam() to make it a valid XPath string literal. This
    prevents issues with embedded quotes or special characters when used
    as XSLT parameters.

    Non-string values are returned unchanged.

    Args:
        value (Any): The value to be checked and potentially escaped.

    Returns:
        Any: The escaped string for lxml XSLT, or the original value if not
        a string.
    """
    return etree.XSLT.strparam(value) if isinstance(value, str) else value


def get_publication_language_row(publication_id: int) -> Optional[RowMapping]:
    """
    Return a RowMapping with key 'language' for the given publication,
    excluding rows with deleted >= 1. Returns None if no row exists.
    """
    publication_table = get_table("publication")

    with db_engine.connect() as connection:
        statement = (
            select(publication_table.c.language)
            .where(publication_table.c.id == publication_id)
            .where(publication_table.c.deleted < 1)
        )
        return connection.execute(statement).mappings().first()


def get_comment_published_row(publication_id: int) -> Optional[RowMapping]:
    """
    Return a RowMapping with key 'published' for the comment linked to the
    given publication, excluding rows with deleted >= 1. Returns None if
    no row exists.
    """
    comment_table = get_table("publication_comment")
    publication_table = get_table("publication")

    with db_engine.connect() as connection:
        statement = (
            select(comment_table.c.published)
            .select_from(
                comment_table.join(
                    publication_table,
                    comment_table.c.id == publication_table.c.publication_comment_id,
                )
            )
            .where(publication_table.c.id == publication_id)
            .where(publication_table.c.deleted < 1)
            .where(comment_table.c.deleted < 1)
        )
        return connection.execute(statement).mappings().first()
