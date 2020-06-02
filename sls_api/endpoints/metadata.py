from flask import abort, Blueprint, request, Response, safe_join
from flask.json import jsonify
from flask_jwt_extended import jwt_optional, get_jwt_identity
import glob
import io
import json
import logging
import os
import sqlalchemy.sql
from urllib.parse import unquote

from sls_api.endpoints.generics import db_engine, get_project_config, get_project_id_from_name, path_hierarchy, select_all_from_table
from sls_api.endpoints.tools.files import git_commit_and_push_file

meta = Blueprint('metadata', __name__)

logger = logging.getLogger("sls_api.metadata")

# Metadata and JSON data functions


@meta.route("/projects/")
@jwt_optional
def get_projects():
    """
    List all GDE projects
    """
    jwt = get_jwt_identity()
    if jwt is None:
        return select_all_from_table("project")
    else:
        if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and jwt["sub"] == "test@test.com":
            # test user in DEBUG mode has access to all projects
            return select_all_from_table("project")
        else:
            return jsonify(jwt["projects"])


@meta.route("/<project>/html/<filename>")
def get_html_contents_as_json(project, filename):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting static content from /{}/html/{}".format(project, filename))
        file_path = safe_join(config["file_root"], "html", "{}.html".format(filename))
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


@meta.route("/<project>/md/<fileid>")
def get_md_contents_as_json(project, fileid):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        parts = fileid.split("-")
        pathTmp = fileid
        if len(parts) > 4:
            pathTmp = parts[0] + "-" + parts[1] + "-" + parts[2] + "-0" + parts[4]
        path = "*/".join(pathTmp.split("-")) + "*"

        file_path_query = safe_join(config["file_root"], "md", path)

        try:
            file_path_full = [f for f in glob.iglob(file_path_query)]
            if len(file_path_full) <= 0:
                abort(404)
            else:
                file_path = file_path_full[0]
                logger.info("Finding {} (md_contents fetch)".format(file_path))
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
            logger.exception("Error fetching: {}".format(file_path_query))
            abort(404)


@meta.route("/<project>/static-pages-toc/<language>/sort")
@meta.route("/<project>/static-pages-toc/<language>")
def get_static_pages_as_json(project, language):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting static content from /{}/static-pages-toc/{}".format(project, language))
        folder_path = safe_join(config["file_root"], "md", language)

        if os.path.exists(folder_path):
            data = path_hierarchy(project, folder_path, language)
            return jsonify(data), 200
        else:
            logger.info("did not find {}".format(folder_path))
            abort(404)


@meta.route("/<project>/manuscript/<publication_id>")
def get_manuscripts(project, publication_id):
    logger.info("Getting manuscript /{}/manuscript/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text('SELECT * FROM publication_manuscript WHERE publication_id=:pub_id')
    statement = sql.bindparams(pub_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@meta.route("/<project>/toc/<collection_id>", methods=["GET", "PUT"])
@jwt_optional
def handle_toc(project, collection_id):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        if request.method == "GET":
            logger.info(f"Getting table of contents for /{project}/toc/{collection_id}")
            file_path_query = safe_join(config["file_root"], "toc", f'{collection_id}.json')

            try:
                file_path = [f for f in glob.iglob(file_path_query)][0]
                logger.info(f"Finding {file_path} (toc collection fetch)")
                if os.path.exists(file_path):
                    with io.open(file_path, encoding="UTF-8") as json_file:
                        contents = json_file.read()
                    return contents, 200
                else:
                    abort(404)
            except IndexError:
                logger.warning(f"File {file_path_query} not found on disk.")
                abort(404)
            except Exception:
                logger.exception(f"Error fetching {file_path_query}")
                abort(404)
        elif request.method == "PUT":
            # uploading a new table of contents requires authorization and project permission
            identity = get_jwt_identity()
            if identity is None:
                return jsonify({"msg": "Missing Authorization Header"}), 403
            else:
                authorized = False
                # in debug mode, test user has access to every project
                if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and identity["sub"] == "test@test.com":
                    authorized = True
                elif project in identity["projects"]:
                    authorized = True

                if not authorized:
                    return jsonify({"msg": "No access to this project."}), 403
                else:
                    logger.info(f"Processing new table of contents for /{project}/toc/{collection_id}")
                    data = request.get_json()
                    if not data:
                        return jsonify({"msg": "No JSON in payload."}), 400
                    file_path = safe_join(config["file_root"], "toc", f"{collection_id}.json")
                    try:
                        # save new toc as file_path.new
                        with open(f"{file_path}.new", "w", encoding="utf-8") as outfile:
                            json.dump(data, outfile)
                    except Exception as ex:
                        # if we fail to save the file, make sure it doesn't exist before returning an error
                        try:
                            os.remove(f"{file_path}.new")
                        except FileNotFoundError:
                            pass
                        return jsonify({"msg": "Failed to save JSON data to disk.", "reason": ex}), 500
                    else:
                        # if we succeed, remove the old file and rename file_path.new to file_path
                        # (could be combined into just os.rename, but some OSes don't like that)
                        os.rename(f"{file_path}.new", file_path)

                        # get author and construct git commit message
                        author_email = get_jwt_identity()["sub"]
                        author = "{} <{}>".format(
                            author_email.split("@")[0],
                            author_email
                        )
                        message = "TOC update by {}".format(author_email)

                        # git commit (and possibly push) file
                        commit_result = git_commit_and_push_file(project, author, message, file_path)
                        if commit_result:
                            return jsonify({"msg": f"Saved new toc as {file_path}"})
                        else:
                            return jsonify({"msg": "git commit failed! Possible configuration fault or git conflict."}), 500


@meta.route("/<project>/collections")
def get_collections(project):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting collections /{}/collections".format(project))
        connection = db_engine.connect()
        status = 1 if config["show_internally_published"] else 2
        project_id = get_project_id_from_name(project)
        sql = sqlalchemy.sql.text(
            "SELECT id, name as title FROM publication_collection WHERE project_id = :p_id AND published>=:p_status ORDER BY name")
        statement = sql.bindparams(p_status=status, p_id=project_id)
        results = []
        for row in connection.execute(statement).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results)


@meta.route("/<project>/collection/<collection_id>")
def get_collection(project, collection_id):
    logger.info("Getting collection /{}/collection/{}".format(project, collection_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication_collection WHERE id=:c_id ORDER BY name")
    statement = sql.bindparams(c_id=collection_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@meta.route("/<project>/publication/<publication_id>")
def get_publication(project, publication_id):
    logger.info("Getting publication /{}/publication/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication WHERE id=:p_id ORDER BY name")
    statement = sql.bindparams(p_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@meta.route("/<project>/collection/<collection_id>/publications")
def get_collection_publications(project, collection_id):
    logger.info("Getting publication /{}/collections/{}/publications".format(project, collection_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication WHERE publication_collection_id=:c_id ORDER BY id")
    statement = sql.bindparams(c_id=collection_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


# Get the collection and publication id for a legacy id
@meta.route("/<project>/legacy/<legacy_id>")
def get_collection_publication_by_legacyid(project, legacy_id):
    logger.info("Getting /<project>/legacy/<legacy_id>")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text("SELECT p.id as pub_id, pc.id as coll_id FROM publication p \
                                JOIN publication_collection pc ON pc.id = p.publication_collection_id \
                                WHERE p.legacy_id = :l_id AND pc.project_id = :p_id ORDER BY pc.id")
    statement = sql.bindparams(l_id=legacy_id, p_id=project_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


# Get all subjects for a project
@meta.route("/<project>/subjects")
def get_project_subjects(project):
    logger.info("Getting /<project>/subjects")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text(""" SELECT * FROM subject WHERE project_id = :p_id """)
    statement = sql.bindparams(p_id=project_id, )
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


# Get all subjects for a project
@meta.route("/<project>/locations")
def get_project_locations(project):
    logger.info("Getting /<project>/locations")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text(""" SELECT * FROM location WHERE project_id = :p_id """)
    statement = sql.bindparams(p_id=project_id, )
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


# Get all subjects for a project
@meta.route("/<project>/tags")
def get_project_tags(project):
    logger.info("Getting /<project>/tags")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text(""" SELECT * FROM tag WHERE project_id = :p_id """)
    statement = sql.bindparams(p_id=project_id, )
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


# Get all subjects for a project
@meta.route("/<project>/works")
def get_project_works(project):
    logger.info("Getting /<project>/works")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text(""" SELECT * FROM work WHERE project_id = :p_id """)
    statement = sql.bindparams(p_id=project_id, )
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@meta.route("/tooltips/subjects")
def subject_tooltips():
    """
    List all available subject tooltips as id and name
    """
    return jsonify(list_tooltips("subject"))


@meta.route("/tooltips/tags")
def tag_tooltips():
    """
    List all available tag tooltips as id and name
    """
    return jsonify(list_tooltips("tag"))


@meta.route("/tooltips/locations")
def location_tooltips():
    """
    List all available location tooltips as id and name
    """
    return jsonify(list_tooltips("location"))


@meta.route("/tooltips/<object_type>/<ident>")
def get_tooltip_text(object_type, ident):
    """
    Get tooltip text for a specific subject, tag, or location
    object_type: one of "subject", "tag", "location"
    ident: legacy or numerical ID for desired object
    """
    if object_type not in ["subject", "tag", "location"]:
        abort(404)
    else:
        return jsonify(get_tooltip(object_type, ident))


@meta.route("/<project>/tooltips/<object_type>/<ident>/")
@meta.route("/<project>/tooltips/<object_type>/<ident>/<use_legacy>/")
def get_project_tooltip_text(project, object_type, ident, use_legacy=False):
    """
    Get tooltip text for a specific subject, tag, or location
    object_type: one of "subject", "tag", "location"
    ident: legacy or numerical ID for desired object
    """
    if object_type not in ["subject", "tag", "location"]:
        abort(404)
    else:
        return jsonify(get_tooltip(object_type, ident, project, use_legacy))


@meta.route("/<project>/subject/<subject_id>")
def get_subject(project, subject_id):
    logger.info("Getting subject /{}/subject/{}".format(project, subject_id))
    connection = db_engine.connect()
    subject_sql = "SELECT * FROM subject "

    # Check if subject_id is a number
    try:
        subject_id = int(subject_id)
        subject_sql = subject_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        subject_id = subject_id
        subject_sql = subject_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(subject_sql).bindparams(id=subject_id)
    return_data = connection.execute(statement).fetchone()

    if return_data is None:
        project_id = get_project_id_from_name(project)
        subject_sql = " SELECT * FROM subject WHERE legacy_id = :id AND deleted = 0 AND project_id = :p_id "
        statement = sqlalchemy.sql.text(subject_sql).bindparams(id=str(subject_id), p_id=project_id)
        return_data = connection.execute(statement).fetchone()
        connection.close()
        if return_data is None:
            return jsonify({"msg": "Desired subject not found in database."}), 404
        else:
            return jsonify(dict(return_data)), 200
    else:
        connection.close()
        return jsonify(dict(return_data)), 200


@meta.route("/<project>/tag/<tag_id>")
def get_tag(project, tag_id):
    logger.info("Getting tag /{}/tag/{}".format(project, tag_id))
    connection = db_engine.connect()
    tag_sql = "SELECT * FROM tag "

    # Check if tag_id is a number
    try:
        tag_id = int(tag_id)
        tag_sql = tag_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        tag_id = tag_id
        tag_sql = tag_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(tag_sql).bindparams(id=tag_id)
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        project_id = get_project_id_from_name(project)
        tag_sql = "SELECT * FROM tag WHERE legacy_id = :id AND deleted = 0 AND project_id = :p_id "
        statement = sqlalchemy.sql.text(tag_sql).bindparams(id=str(tag_id), p_id=project_id)
        return_data = connection.execute(statement).fetchone()
        connection.close()
        if return_data is None:
            return jsonify({"msg": "Desired tag not found in database."}), 404
        else:
            return jsonify(dict(return_data)), 200
    else:
        return jsonify(dict(return_data)), 200


@meta.route("/<project>/work/<work_id>")
def get_work(project, work_id):
    logger.info("Getting work /{}/work/{}".format(project, work_id))
    connection = db_engine.connect()
    work_sql = "SELECT * FROM work "

    # Check if work_id is a number
    try:
        work_id = int(work_id)
        work_sql = work_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        work_id = work_id
        work_sql = work_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(work_sql).bindparams(id=work_id)
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        return jsonify({"msg": "Desired work not found in database."}), 404
    else:
        return jsonify(dict(return_data)), 200


@meta.route("/<project>/location/<location_id>")
def get_location(project, location_id):
    logger.info("Getting location /{}/location/{}".format(project, location_id))
    connection = db_engine.connect()
    location_sql = "SELECT * FROM location "

    # Check if location_id is a number
    try:
        location_id = int(location_id)
        location_sql = location_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        location_id = location_id
        location_sql = location_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(location_sql).bindparams(id=location_id)
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        project_id = get_project_id_from_name(project)
        location_sql = "SELECT * FROM location WHERE legacy_id = :id AND deleted = 0 AND project_id = :p_id "
        statement = sqlalchemy.sql.text(location_sql).bindparams(id=str(location_id), p_id=project_id)
        return_data = connection.execute(statement).fetchone()
        connection.close()
        if return_data is None:
            return jsonify({"msg": "Desired location not found in database."}), 404
        else:
            return jsonify(dict(return_data)), 200
    else:
        return jsonify(dict(return_data)), 200


@meta.route("/<project>/files/<folder>/<file_name>/")
def get_json_file(project, folder, file_name):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        file_path = safe_join(config["file_root"], folder, "{}.json".format(str(file_name)))
        try:
            with open(file_path) as f:
                data = json.load(f)
            return jsonify(data), 200
        except Exception:
            logger.exception(f"Failed to read JSON file at {file_path}")
            return Response("File not found.", status=404, content_type="text/json")


@meta.route("/<project>/urn/<url>/")
@meta.route("/<project>/urn/<url>/<legacy_id>/")
def get_urn(project, url, legacy_id=None):
    url = unquote(unquote(url))
    logger.info("Getting urn /{}/urn/{}/{}/".format(project, url, legacy_id))
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    if legacy_id is not None:
        sql = sqlalchemy.sql.text("SELECT * FROM urn_lookup where legacy_id=:l_id  AND project_id=:p_id").bindparams(
            l_id=str(legacy_id), p_id=project_id)
    else:
        url_like_str = "%#{}".format(url)
        sql = sqlalchemy.sql.text("SELECT * FROM urn_lookup where url LIKE :url AND project_id=:p_id").bindparams(
            url=url_like_str, p_id=project_id)
    return_data = []
    for row in connection.execute(sql).fetchall():
        return_data.append(dict(row))
    connection.close()
    return jsonify(return_data), 200


def list_tooltips(table):
    """
    List available tooltips for subjects, tags, or locations
    table should be 'subject', 'tag', or 'location'
    """
    if table not in ["subject", "tag", "location"]:
        return ""
    connection = db_engine.connect()
    if table == "subject":
        sql = sqlalchemy.sql.text("SELECT id, full_name, project_id, legacy_id FROM subject")
    else:
        sql = sqlalchemy.sql.text("SELECT id, name, project_id, legacy_id FROM {}".format(table))
    results = []
    for row in connection.execute(sql).fetchall():
        results.append(dict(row))
    connection.close()
    return results


def get_tooltip(table, row_id, project=None, use_legacy=False):
    """
    Get 'tooltip' style info for a single subject, tag, or location by its ID
    table should be 'subject', 'tag', or 'location'
    """
    connection = db_engine.connect()
    try:
        ident = int(row_id)
        is_legacy_id = False
    except ValueError:
        ident = str(row_id)
        is_legacy_id = True

    if use_legacy:
        ident = str(row_id)
        is_legacy_id = True

    project_sql = " AND project_id = :project_id "
    if project is None:
        project_sql = ""

    if is_legacy_id:
        if table == "subject":
            sql = sqlalchemy.sql.text("SELECT id, legacy_id, full_name, description FROM subject WHERE legacy_id=:id" + project_sql)
        else:
            sql_query = "SELECT id, legacy_id, name, description FROM {} WHERE legacy_id=:id " + project_sql
            sql = sqlalchemy.sql.text(sql_query.format(table))
    else:
        if table == "subject":
            sql = sqlalchemy.sql.text("SELECT id, legacy_id, full_name, description FROM subject WHERE id=:id" + project_sql)
        else:
            sql_query = "SELECT id, legacy_id, name, description FROM {} WHERE id=:id" + project_sql
            sql = sqlalchemy.sql.text(sql_query.format(table))

    if project is None:
        statement = sql.bindparams(id=ident)
    else:
        project_id = get_project_id_from_name(project)
        statement = sql.bindparams(id=ident, project_id=project_id)

    result = connection.execute(statement).fetchone()
    connection.close()
    if result is None:
        return dict()
    else:
        return dict(result)
