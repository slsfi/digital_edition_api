import calendar
from collections import OrderedDict
from flask import abort, Blueprint, request, safe_join
from flask.json import jsonify
import io
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import os
from ruamel.yaml import YAML
from sqlalchemy import create_engine
import sqlalchemy.sql
import time
import re
import glob

digital_edition = Blueprint('digital_edition', __name__)

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with io.open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as digital_editions_config:
    yaml = YAML()
    project_config = yaml.load(digital_editions_config)

logger = logging.getLogger("sls_api.digital_edition")

file_handler = TimedRotatingFileHandler(filename=project_config["log_file"], when="midnight", backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(file_handler)

db_engine = create_engine(project_config["engine"], pool_pre_ping=True)


@digital_edition.after_request
def set_access_control_headers(response):
    if "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Accept, Origin, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET"

    if response.headers.get("Content-Type") == "application/json":
        response.headers["Content-Type"] = "application/json;charset=utf-8"

    return response


@digital_edition.route("/<project>/html/<filename>")
def get_html_contents_as_json(project, filename):
    logger.info("Getting static content from /{}/html/{}".format(project, filename))
    file_path = safe_join(project_config[project]["file_root"], "html", "{}.html".format(filename))
    if os.path.exists(file_path):
        with io.open(file_path, encoding="UTF-8") as html_file:
            contents = html_file.read()
        data = {
            "filename": filename,
            "content": contents
        }
        return jsonify(data), 200
    else:
        abort(404)


@digital_edition.route("/<project>/md/<fileid>")
def get_md_contents_as_json(project, fileid):
    # TODO safer handling of paths, glob.iglob is not secure with arbitrary user input to fileid

    path = "*/".join(fileid.split("-")) + "*"

    file_path_query = safe_join(project_config[project]["file_root"], "md", path)

    try:
        file_path = [f for f in glob.iglob(file_path_query)][0]
        print(file_path)
        if os.path.exists(file_path):
            with io.open(file_path, encoding="UTF-8") as md_file:
                contents = md_file.read()
            data = {
                "fileid": fileid,
                "content": contents
            }
            return jsonify(data), 200
        else:
            abort(404)
    except Exception:
        print(file_path_query)
        abort(404)


@digital_edition.route("/<project>/static-pages-toc/<language>")
def get_static_pages_as_json(project, language):
    logger.info("Getting static content from /{}/static-pages-toc/{}".format(project, language))
    folder_path = safe_join(project_config[project]["file_root"], "md", language)

    if os.path.exists(folder_path):
        data = path_hierarchy(folder_path, language)
        return jsonify(data), 200
    else:
        abort(404)


@digital_edition.route("/<project>/manuscript/<publication_id>")
def get_manuscripts(project, publication_id):
    logger.info("Getting manuscript /{}/manuscript/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publicationManuscript WHERE publication_id=:pub_id")
    statement = sql.bindparams(pub_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@digital_edition.route("/<project>/publication/<publication_id>")
def get_publication(project, publication_id):
    logger.info("Getting publication /{}/publication/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication WHERE id:=p_id ORDER BY name")
    statement = sql.bindparams(p_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/inl")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/inl/<lang>")
def get_introduction(project, collection_id, publication_id, lang="swe"):
    """
    Get introduction text for a given publiction
    """
    if is_published(project, collection_id, publication_id):
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        filename = "{}_{}_inl_{}.xml".format(collection_id, publication_id, lang)
        xsl_file = "est.xsl"
        content = get_content(project, "inl", filename, xsl_file, None)
        data = {
            "id": "{}_{}_inl".format(collection_id, publication_id),
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": "Content not published for external viewing."
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/tit")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/tit/<lang>")
def get_title(project, collection_id, publication_id, lang="swe"):
    """
    Get title page for a given publication
    """
    if is_published(project, collection_id, publication_id):
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        filename = "{}_{}_tit_{}.xml".format(collection_id, publication_id, lang)
        xsl_file = "title.xsl"
        content = get_content(project, "tit", filename, xsl_file, None)
        data = {
            "id": "{}_{}_tit".format(collection_id, publication_id),
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": "Content not published for external viewing."
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id):
    """
    Get reading text for a given publication
    """
    if is_published(project, collection_id, publication_id):
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        filename = "{}_{}_est.xml".format(collection_id, publication_id)
        xsl_file = "est.xsl"
        content = get_content(project, "est", filename, xsl_file, None)
        data = {
            "id": "{}_{}_est".format(collection_id, publication_id),
            "content": content.replace("id=", "data-id=")
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": "Content not published for external viewing."
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/com")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>")
def get_comments(project, collection_id, publication_id, note_id=None):
    """
    Get comments file text for a given publication
    """
    if is_published(project, collection_id, publication_id):
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        filename = "{}_{}_com.xml".format(collection_id, publication_id)
        params = {
            "estDocument": '"file://{}'.format(safe_join(project_config[project]["file_root"], "xml", "est", filename.replace("com", "est")))
        }
        if note_id is not None:
            params["noteId"] = '"{}"'.format(note_id)
            xsl_file = "notes.xsl"
        else:
            xsl_file = "com.xsl"

        content = get_content(project, "com", filename, xsl_file, params)
        data = {
            "id": "{}_{}_com".format(collection_id, publication_id),
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": "Content not published for external viewing."
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/ms/")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/ms/<manuscript_id>")
def get_manuscript(project, collection_id, publication_id, manuscript_id=None):
    """
    Get one or all manuscripts for a given publication
    """
    if is_published(project, collection_id, publication_id):
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        if manuscript_id is None:
            filename_search = "{}_{}_ms_%".format(collection_id, publication_id)
            select = "SELECT name, originalFilename, id FROM publicationManuscript WHERE originalFilename LIKE :query"
            statement = sqlalchemy.sql.text(select).bindparams(query=filename_search)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()
        else:
            select = "SELECT name, originalFilename, id FROM publicationManuscript WHERE id = :m_id"
            statement = sqlalchemy.sql.text(select).bindparams(m_id=manuscript_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()

        for index in range(len(manuscript_info)):
            manuscript = manuscript_info[index]
            params = {
                "bookId": collection_id
            }
            manuscript_info[index]["manuscript_changes"] = get_content(project, "ms", manuscript["originalFilename"], "ms_changes.xsl", params)
            manuscript_info[index]["manuscript_normalized"] = get_content(project, "ms", manuscript["originalFilename"], "ms_normalized.xsl", params)

        data = {
            "id": "{}_{}".format(collection_id, publication_id),
            "manuscripts": manuscript_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}_ms".format(collection_id, publication_id),
            "error": "Content not published for external viewing."
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/var/")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/var/<section_id>")
def get_variant(project, collection_id, publication_id, section_id=None):
    """
    Get all variants for a given publication, optionally specifying a section (chapter)
    """
    if is_published(project, collection_id, publication_id):
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        filename_search = "{}_{}_var_%".format(collection_id, publication_id)
        if section_id is not None:
            select = "SELECT title, type, originalFilename, id FROM publicationVersion WHERE originalFilename LIKE :f_name AND section_id = :s_id"
            statement = sqlalchemy.sql.text(select).bindparams(f_name=filename_search, s_id=section_id)
        else:
            select = "SELECT title, type, originalFilename, id FROM publicationVersion WHERE originalFilename LIKE :f_name"
            statement = sqlalchemy.sql.text(select).bindparams(f_name=filename_search)
        variation_info = []
        for row in connection.execute(statement).fetchall():
            variation_info.append(dict(row))
        connection.close()

        for index in range(len(variation_info)):
            variation = variation_info[index]
            params = {
                "bookId": collection_id
            }

            if variation["type"] == 1:
                xsl_file = "poem_variants_est.xsl"
            else:
                xsl_file = "poem_variants_other.xsl"

            if section_id is not None:
                params["sectionId"] = section_id

            variation_info[index]["content"] = get_content(project, "var", variation["originalFilename"], xsl_file, params)

        data = {
            "id": "{}_{}_var".format(collection_id, publication_id),
            "variations": variation_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": "Content not published for external viewing."
        }), 403


@digital_edition.route("/tooltips/subjects")
def subject_tooltips():
    """
    List all available subject tooltips as id and name
    """
    return jsonify(list_tooltips("subject"))


@digital_edition.route("/tooltips/tags")
def tag_tooltips():
    """
    List all available tag tooltips as id and name
    """
    return jsonify(list_tooltips("tag"))


@digital_edition.route("/tooltips/locations")
def location_tooltips():
    """
    List all available location tooltips as id and name
    """
    return jsonify(list_tooltips("location"))


@digital_edition.route("/tooltips/subject/<ident>")
def get_subject_tooltip(ident):
    """
    Get a specific subject tooltip by ID
    """
    return jsonify(get_tooltip("subject", ident))


@digital_edition.route("/tooltips/tag/<ident>")
def get_tag_tooltip(ident):
    """
    Get a specific tag tooltip by ID
    """
    return jsonify(get_tooltip("tag", ident))


@digital_edition.route("/tooltips/location/<ident>")
def get_location_tooltip(ident):
    """
    Get a specific location tooltip by ID
    """
    return jsonify(get_tooltip("location", ident))


def list_tooltips(table):
    """
    List available tooltips for subjects, tags, or locations
    table should be 'subject', 'tag', or 'location'
    """
    if table not in ["subject", "tag", "location"]:
        return ""
    connection = db_engine.connect()
    if table == "subject":
        sql = sqlalchemy.sql.text("SELECT id, fullName, project_id FROM subject")
    else:
        sql = sqlalchemy.sql.text("SELECT id, name, project_id FROM {}".format(table))
    results = []
    for row in connection.execute(sql).fetchall():
        results.append(dict(row))
    return results


def get_tooltip(table, row_id):
    """
    Get 'tooltip' style info for a single subject, tag, or location by its ID
    table should be 'subject', 'tag', or 'location'
    """
    connection = db_engine.connect()
    if table == "subject":
        sql = sqlalchemy.sql.text("SELECT fullName, description FROM subject WHERE id=:id")
    elif table == "tag":
        sql = sqlalchemy.sql.text("SELECT name, description FROM tag WHERE id=:id")
    else:
        sql = sqlalchemy.sql.text("SELECT name, description FROM location WHERE id=:id")
    statement = sql.bindparams(id=row_id)
    result = connection.execute(statement).fetchone()
    connection.close()
    return dict(result)


'''
    HELPER FUNCTIONS  
'''


def slugify_route(path):
    path = path.replace(" - ", "")
    path = path.replace(" ", "-")
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub('[^a-zA-Z0-9\\\/-]|_', '', re.sub('.md', '', path))
    return path.lower()


def slugify_id(path, language):
    path = re.sub('[^0-9]', '', path)
    path = language + path
    path = '-'.join(path[i:i+2] for i in range(0, len(path), 2))
    return path


def slugify_path(path):
    path = split_after(path, "/topelius_required/md/")
    return re.sub('.md', '', path)


def path_hierarchy(path, language):
    hierarchy = {'id': slugify_id(path, language), 'title': filter_title(os.path.basename(path)),
                 'basename': re.sub('.md', '', os.path.basename(path)), 'path': slugify_path(path), 'fullpath': path,
                 'route': slugify_route(split_after(path, "/topelius_required/md/")), 'type': 'folder',
                 'children': [path_hierarchy(p, language) for p in glob.glob(os.path.join(path, '*'))]}

    if not hierarchy['children']:
        del hierarchy['children']
        hierarchy['type'] = 'file'

    return dict(hierarchy)


def filter_title(path):
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub('-', '', path)
    path = re.sub('.md', '', path)
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
    elif calendar.timegm(time.gmtime()) > (cache_file_mtime + project_config["cache_lifetime_seconds"]):
        return False
    return True


def is_published(project, collection_id, publication_id):
    """
    Returns true only if project, publicationCollection, and publication are all published
    """
    connection = db_engine.connect()
    select = """SELECT project.published AS proj_pub, publicationCollection.published AS col_pub, publication.published as pub 
    FROM project JOIN publicationCollection JOIN publication 
    WHERE project.id = publicationCollection.project_id 
    AND publication.publicationCollection_id = publicationCollection.id 
    AND project.name = :project AND publicationCollection.id = :c_id AND publication.id = :p_id
    """
    statement = sqlalchemy.sql.text(select).bindparams(project=project, c_id=collection_id, p_id=publication_id)
    result = connection.execute(statement)

    published = True
    for row in result.fetchall():
        if row.proj_pub == 0 or row.col_pub == 0 or row.pub == 0:
            published = False

    return published


class FileResolver(etree.Resolver):
    def resolve(self, system_url, public_id, context):
        logger.debug("Resolving {}".format(system_url))
        return self.resolve_filename(system_url, context)


def xml_to_html(xsl_file_path, xml_file_path, replace_namespace=True, params=None):
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
            xml_contents = xml_contents.replace(b'xmlns="http://www.sls.fi/tei"', b'xmlns="http://www.tei-c.org/ns/1.0"')

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
        raise Exception("Invalid parameters for XSLT transformation, must be of type dict or OrderedDict, not {}".format(type(params)))
    if len(xsl_transform.error_log) > 0:
        logging.debug(xsl_transform.error_log)
    return str(result)


def get_content(project, folder, xml_filename, xsl_filename, parameters):
    xml_file_path = safe_join(project_config[project]["file_root"], "xml", folder, xml_filename)
    xsl_file_path = safe_join(project_config[project]["file_root"], "xslt", xsl_filename)
    cache_file_path = xml_file_path.replace("/xml/", "/cache/").replace(".xml", ".html")
    content = None

    if os.path.exists(cache_file_path):
        if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                content = "Error reading content from cache."
            else:
                logger.info("Content fetched from cache.")
        else:
            logger.info("Cache file is old or invalid, deleting cache file...")
            os.remove(cache_file_path)
    if os.path.exists(xml_file_path) and content is None:
        logger.info("Getting contents from file and transforming...")
        try:
            content = xml_to_html(xsl_file_path, xml_file_path, params=parameters).replace('\n', '').replace('\r', '')
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
