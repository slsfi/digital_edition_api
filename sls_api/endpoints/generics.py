import calendar
from collections import OrderedDict
from flask import jsonify
from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request
from functools import wraps
import glob
import hashlib
import io
import logging
from lxml import etree
import os
import re
from ruamel.yaml import YAML
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select, text
import time
from typing import Any, Tuple, Optional
from werkzeug.security import safe_join

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


def allowed_facsimile(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_FOR_FACSIMILE_UPLOAD


def get_project_config(project_name):
    if project_name in config:
        return config[project_name]
    return None


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


def project_permission_required(fn):
    """
    Function decorator that checks for JWT authorization and that the user has edit rights for the project.
    The project the method concerns should be the first positional argument or a keyword argument.
    """
    @wraps(fn)
    def decorated_function(*args, **kwargs):
        verify_jwt_in_request()
        identity = get_jwt_identity()
        if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and identity["sub"] == "test@test.com":
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
            if "projects" not in identity or ["projects"] is None or not identity["projects"]:
                return jsonify({"msg": "No access to this project."}), 403
            elif project in identity["projects"]:
                return fn(*args, **kwargs)
            else:
                return jsonify({"msg": "No access to this project."}), 403
    return decorated_function


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


def get_collection_legacy_id(collection_id):
    publication_collection = Table('publication_collection', metadata, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select(publication_collection.c.legacy_id).where(publication_collection.c.id == collection_id)
    collection_legacy_id = connection.execute(statement).fetchone()
    connection.close()
    try:
        return int(collection_legacy_id.legacy_id)
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


def slugify_id(path, language):
    path = re.sub(r'[^0-9]', '', path)
    path = language + path
    path = '-'.join(path[i:i + 2] for i in range(0, len(path), 2))
    return path


def slugify_path(project, path):
    project_config = get_project_config(project)
    path = split_after(path, "/" + project_config["file_root"] + "/md/")
    return re.sub('.md', '', path)


def path_hierarchy(project, path, language):
    project_config = get_project_config(project)
    hierarchy = {'id': slugify_id(path, language), 'title': filter_title(os.path.basename(path)),
                 'basename': re.sub('.md', '', os.path.basename(path)), 'path': slugify_path(project, path),
                 'fullpath': path,
                 'route': slugify_route(split_after(path, "/" + project_config["file_root"] + "/md/")),
                 'type': 'folder',
                 'children': [path_hierarchy(project, p, language) for p in sorted(glob.glob(os.path.join(path, '*')))]}

    if not hierarchy['children']:
        del hierarchy['children']
        hierarchy['type'] = 'file'

    return hierarchy


def filter_title(path):
    path = path.lstrip(' -0123456789')
    path = path.replace('.md', '')
    return path.strip()


def split_after(value, a):
    pos_a = value.rfind(a)
    if pos_a == -1:
        return ""
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= len(value):
        return ""
    return value[adjusted_pos_a:]


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


def get_published_status(project, collection_id, publication_id):
    """
    Returns info on if project, publication_collection, and publication are all published
    Returns two values:
        - a boolean if the publication can be shown
        - a message text why it can't be shown, if that is the case

    Publications can be shown if they're externally published (published==2),
    or if they're internally published (published==1) and show_internally_published is True
    """
    project_config = get_project_config(project)
    if project_config is None:
        return False, "No such project."

    if publication_id is None or str(publication_id) == "undefined":
        return False, "No such publication_id."

    connection = db_engine.connect()
    stmt = """SELECT project.published AS proj_pub, publication_collection.published AS col_pub, publication.published as pub
    FROM project
    JOIN publication_collection ON publication_collection.project_id = project.id
    JOIN publication ON publication.publication_collection_id = publication_collection.id
    WHERE project.id = publication_collection.project_id
    AND publication.publication_collection_id = publication_collection.id
    AND project.name = :project AND publication_collection.id = :c_id AND (publication.id = :p_id OR split_part(publication.legacy_id, '_', 2) = :str_p_id)
    """
    statement = text(stmt).bindparams(project=project, c_id=collection_id, p_id=publication_id, str_p_id=str(publication_id))
    result = connection.execute(statement)
    show_internal = project_config["show_internally_published"]
    can_show = False
    message = ""
    row = result.fetchone()
    if row is None:
        message = "Content does not exist"
    else:
        if row.proj_pub is None or row.col_pub is None or row.pub is None:
            status = -1
        else:
            status = min(row.proj_pub, row.col_pub, row.pub)
        if status < 1:
            message = "Content is not published"
        elif status == 1 and not show_internal:
            message = "Content is not externally published"
        else:
            can_show = True
    connection.close()
    return can_show, message


def get_collection_published_status(project, collection_id):
    """
    Returns info on if project, publication_collection, and publication are all published
    Returns two values:
        - a boolean if the publication can be shown
        - a message text why it can't be shown, if that is the case

    Publications can be shown if they're externally published (published==2),
    or if they're internally published (published==1) and show_internally_published is True
    """
    project_config = get_project_config(project)
    if project_config is None:
        return False, "No such project."
    connection = db_engine.connect()

    project_id = get_project_id_from_name(project)

    stmt = """SELECT project.published AS proj_pub, publication_collection.published AS col_pub
    FROM project
    JOIN publication_collection ON publication_collection.project_id = project.id
    AND project.id = :project_id AND publication_collection.id = :c_id
    """
    statement = text(stmt).bindparams(project_id=project_id, c_id=collection_id)
    result = connection.execute(statement)
    show_internal = project_config["show_internally_published"]
    can_show = False
    message = ""
    row = result.fetchone()
    if row is None:
        message = "Content does not exist"
    else:
        status = min(row.proj_pub, row.col_pub)
        if status < 1:
            message = "Content is not published"
        elif status == 1 and not show_internal:
            message = "Content is not externally published"
        else:
            can_show = True
    connection.close()
    return can_show, message


class FileResolver(etree.Resolver):
    def resolve(self, system_url, public_id, context):
        logger.debug("Resolving {}".format(system_url))
        return self.resolve_filename(system_url, context)


def transform_xml(xsl_file_path, xml_file_path, replace_namespace=False, params=None):
    logger.debug("Transforming {} using {}".format(xml_file_path, xsl_file_path))
    if params is not None:
        logger.debug("Parameters are {}".format(params))
    if not os.path.exists(xsl_file_path):
        return "XSL file {!r} not found!".format(xsl_file_path)
    if not os.path.exists(xml_file_path):
        return "XML file {!r} not found!".format(xml_file_path)

    with io.open(xml_file_path, mode="rb") as xml_file:
        xml_contents = xml_file.read()
        if replace_namespace:
            xml_contents = xml_contents.replace(b'xmlns="http://www.sls.fi/tei"',
                                                b'xmlns="http://www.tei-c.org/ns/1.0"')

        xml_root = etree.fromstring(xml_contents)

    xsl_parser = etree.XMLParser()
    xsl_parser.resolvers.add(FileResolver())
    with io.open(xsl_file_path, encoding="UTF-8") as xsl_file:
        xslt_root = etree.parse(xsl_file, parser=xsl_parser)
        xsl_transform = etree.XSLT(xslt_root)

    if params is None:
        result = xsl_transform(xml_root)
    elif isinstance(params, dict) or isinstance(params, OrderedDict):
        result = xsl_transform(xml_root, **params)
    else:
        raise Exception(
            "Invalid parameters for XSLT transformation, must be of type dict or OrderedDict, not {}".format(
                type(params)))
    if len(xsl_transform.error_log) > 0:
        logging.debug(xsl_transform.error_log)
    return str(result)


def get_content(project, folder, xml_filename, xsl_filename, parameters):
    project_config = get_project_config(project)
    if project_config is None:
        return "No such project."
    xml_file_path = safe_join(project_config["file_root"], "xml", folder, xml_filename)
    xsl_file_path = safe_join(project_config["file_root"], "xslt", xsl_filename)
    cache_folder = os.path.join("/tmp", "api_cache", project, folder)
    os.makedirs(cache_folder, exist_ok=True)
    if "ms" in xsl_filename:
        # xsl_filename is 'ms_changes.xsl' or 'ms_normalized.xsl'
        # ensure that '_changes' or '_normalized' is appended to the cache filename accordingly
        cache_extension = "{}.html".format(xsl_filename.split("ms")[1].replace(".xsl", ""))
    else:
        cache_extension = ".html"
    cache_file_path = os.path.join(cache_folder, xml_filename.replace(".xml", cache_extension))

    content = None
    param_ext = ''
    if parameters is not None:
        if 'noteId' in parameters:
            param_ext += "_" + parameters["noteId"]
        if 'sectionId' in parameters:
            param_ext += "_" + parameters["sectionId"]
        # not needed for bookId
        param_file_name = xml_filename.split(".xml")[0] + param_ext
        cache_file_path = cache_file_path.replace(xml_filename.split(".xml")[0], param_file_name)
        cache_file_path = cache_file_path.replace('"', '')

    logger.debug("Cache file path for {} is {}".format(xml_filename, cache_file_path))

    if os.path.exists(cache_file_path):
        if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                logger.exception("Error reading content from cache for {}".format(cache_file_path))
                content = "Error reading content from cache."
            else:
                logger.info("Content fetched from cache.")
        else:
            logger.info("Cache file is old or invalid, deleting cache file...")
            os.remove(cache_file_path)
    if os.path.exists(xml_file_path) and content is None:
        logger.info("Getting contents from file and transforming...")
        try:
            content = transform_xml(xsl_file_path, xml_file_path, params=parameters).replace('\n', '').replace('\r', '')
            try:
                with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                    cache_file.write(content)
            except Exception:
                logger.exception("Could not create cachefile")
                content = "Successfully fetched content but could not generate cache for it."
        except Exception as e:
            logger.exception("Error when parsing XML file")
            content = "Error parsing document"
            content += str(e)
    elif content is None:
        content = "File not found"

    return content


# Create a stub for a translation
def create_translation(neutral):
    connection = db_engine.connect()
    with connection.begin():
        stmt = """ INSERT INTO translation (neutral_text) VALUES(:neutral) RETURNING id """
        statement = text(stmt).bindparams(neutral=neutral, )
        result = connection.execute(statement)
        row = result.fetchone()
    connection.close()
    if row is not None:
        return row.id
    else:
        return None


# Create a stub for a translation
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
        stmt = """ SELECT id FROM translation_text WHERE
                            (translation_id = :t_id AND (language is NULL OR language = 'not set') AND table_name = :table_name AND field_name = :field_name AND deleted = 0)
                            OR (translation_id = :t_id AND language = :language AND table_name = :table_name AND field_name = :field_name AND language != 'not set' AND deleted = 0)
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


def get_xml_content(project, folder, xml_filename, xsl_filename, parameters):
    project_config = get_project_config(project)
    if project_config is None:
        return "No such project."
    xml_file_path = safe_join(project_config["file_root"], "xml", folder, xml_filename)
    if xsl_filename is not None:
        xsl_file_path = safe_join(project_config["file_root"], "xslt", xsl_filename)
    else:
        xsl_file_path = None

    if os.path.exists(xml_file_path):
        logger.info("Getting contents from file ...")
        if xsl_file_path is not None:
            try:
                content = transform_xml(xsl_file_path, xml_file_path, params=parameters)
            except Exception as e:
                logger.exception("Error when parsing XML file")
                content = "Error parsing document"
                content += str(e)
        else:
            try:
                with io.open(xml_file_path, encoding="UTF-8") as xml_file:
                    content = xml_file.read()
            except Exception as e:
                logger.exception("Error opening/reading XML file")
                content = "Error opening/reading XML file"
                content += str(e)
    else:
        content = "File not found"
    return content


# Recursive function for flattening the given json, i.e. turning it into a one dimensional array, which is stored in "flattened"
def flatten_json(json, flattened):
    if json is not None:
        if json.get('children') is not None:
            for i in range(len(json['children'])):
                if json['children'][i].get('itemId') is not None and json['children'][i].get('itemId') != '':
                    flattened.append(json['children'][i])
                flatten_json(json['children'][i], flattened)


# Searches the given array of toc items for the first one that has an itemId value and a type value other than "subtitle" and "section_title"
def get_first_valid_item_from_toc(flattened_toc):
    for i in range(len(flattened_toc)):
        if flattened_toc[i].get('itemId') is not None and flattened_toc[i].get('itemId') != '' and flattened_toc[i].get('type') is not None and flattened_toc[i].get('type') != 'subtitle' and flattened_toc[i].get('type') != 'section_title':
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
        return False, "Project name must be no less than 3 and no more than 32 characters long."

    # Check allowed characters (lowercase letters a-z, digits 0-9 and underscores _)
    if not re.fullmatch(r'[a-z0-9_]+', name):
        return False, "Project name can only contain lowercase letters a-z and digits 0-9."

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
