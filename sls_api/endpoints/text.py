from flask import Blueprint, jsonify, request
import logging
import sqlalchemy
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, \
    get_collection_published_status, get_content, get_xml_content, \
    get_xml_content, get_prerendered_content, get_project_config, \
    get_published_status, get_collection_legacy_id

text = Blueprint('text', __name__)
logger = logging.getLogger("sls_api.text")

# Text functions


@text.route("/<project>/text/<text_type>/<text_id>")
def get_text_by_type(project, text_type, text_id):
    logger.info("Getting text by type /{}/text/{}/{}".format(project, text_type, text_id))

    text_table = ''
    if text_type == 'manuscript':
        text_table = 'publication_manuscript'
    elif text_type == 'variation':
        text_table = 'publication_version'
    elif text_type == 'commentary':
        text_table = 'publication_comment'
    elif text_type == 'facsimile':
        text_table = 'publication_facsimile'

    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM {} WHERE id=:t_id".format(text_table))
    statement = sql.bindparams(t_id=text_id)
    results = []
    for row in connection.execute(statement).fetchall():
        if row is not None:
            results.append(row._asdict())
    connection.close()
    return jsonify(results)


@text.route("/<project>/text/<collection_id>/<publication_id>/inl")
@text.route("/<project>/text/<collection_id>/<publication_id>/inl/<lang>")
def get_introduction(project, collection_id, publication_id, lang="swe"):
    """
    Get introduction text for a given collection.

    @TODO: remove publication_id, it is not needed or used.
    @TODO: get original_filename from publication_collection_introduction
           table? how handle language/version
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 404

    can_show, message = get_collection_published_status(project, collection_id)
    if not can_show:
        return jsonify({
            "id": f"{collection_id}_{publication_id}_inl",
            "error": message
        }), 403

    # logger.info("Getting XML for {} and transforming...".format(request.full_path))

    version = "int" if config.get("show_internally_published") else "ext"
    filename_stem = f"{collection_id}_inl_{lang}_{version}"
    content = None

    if config.get("prerender_xml", False):
        filename = f"{filename_stem}.html"
        content = get_prerendered_content(config, "inl", filename)

    if content is None:
        filename = f"{filename_stem}.xml"
        xsl_file = "introduction.xsl"
        content = get_content(project, "inl", filename, xsl_file, None)
        content = content.replace(" id=", " data-id=")

    data = {
        "id": f"{collection_id}_{publication_id}_inl",
        "content": content
    }
    return jsonify(data), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/tit")
@text.route("/<project>/text/<collection_id>/<publication_id>/tit/<lang>")
def get_title(project, collection_id, publication_id, lang="swe"):
    """
    Get title page for a given publication @TODO: remove publication_id, it is not needed?
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_collection_published_status(project, collection_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from publication_collection_title table? how handle language/version
            filename = "{}_tit_{}_{}.xml".format(collection_id, lang, version)
            xsl_file = "title.xsl"
            content = get_content(project, "tit", filename, xsl_file, None)
            data = {
                "id": "{}_{}_tit".format(collection_id, publication_id),
                "content": content.replace(" id=", " data-id=")
            }
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}_{}".format(collection_id, publication_id),
                "error": message
            }), 403


@text.route("/<project>/text/<collection_id>/fore")
@text.route("/<project>/text/<collection_id>/fore/<lang>")
def get_foreword(project, collection_id, lang="sv"):
    """
    Get foreword for a given collection
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_collection_published_status(project, collection_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from database table? how handle language/version
            filename = "{}_fore_{}_{}.xml".format(collection_id, lang, version)
            xsl_file = "foreword.xsl"
            content = get_content(project, "fore", filename, xsl_file, None)
            data = {
                "id": "{}_fore".format(collection_id),
                "content": content.replace(" id=", " data-id=")
            }
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}".format(collection_id),
                "error": message
            }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT legacy_id FROM publication WHERE id = :p_id AND original_filename IS NULL"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()
        if result is None or language is not None:
            filename = "{}_{}_est.xml".format(collection_id, publication_id)
            if language is not None:
                filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)
                logger.debug("Filename (est) for {} is {}".format(publication_id, filename))
        else:
            filename = "{}_est.xml".format(result.legacy_id)
        logger.debug("Filename (est) for {} is {}".format(publication_id, filename))
        xsl_file = "est.xsl"

        xslt_params = {
            "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
        }
        if section_id is not None:
            xslt_params["sectionId"] = str(section_id)

        content = get_content(project, "est", filename, xsl_file, xslt_params)

        select = "SELECT language FROM publication WHERE id = :p_id"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()

        text_language = ""
        if result is not None and result.language is not None:
            text_language = result.language

        data = {
            "id": "{}_{}_est".format(collection_id, publication_id),
            "content": content.replace(" id=", " data-id="),
            "language": text_language
        }
        connection.close()

        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/com")
@text.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>/<section_id>")
def get_comments(project, collection_id, publication_id, note_id=None, section_id=None):
    """
    Get comments file text for a given publication
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_published_status(project, collection_id, publication_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            connection = db_engine.connect()
            select = "SELECT legacy_id FROM publication_comment WHERE id IN (SELECT publication_comment_id FROM publication WHERE id = :p_id) \
                        AND legacy_id IS NOT NULL AND original_filename IS NULL"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            result = connection.execute(statement).fetchone()
            connection.close()

            if result is not None:
                filename = "{}_com.xml".format(result.legacy_id)
            else:
                filename = "{}_{}_com.xml".format(collection_id, publication_id)

            logger.debug("Filename (com) for {} is {}".format(publication_id, filename))

            xslt_params = {
                "estDocument": f'file://{safe_join(config["file_root"], "xml", "est", filename.replace("com", "est"))}',
                "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
            }

            if note_id is not None and section_id is None:
                xslt_params["noteId"] = str(note_id)
                xsl_file = "notes.xsl"
            else:
                xsl_file = "com.xsl"

            if section_id is not None:
                xslt_params["sectionId"] = str(section_id)

            content = get_content(project, "com", filename, xsl_file, xslt_params)

            data = {
                "id": "{}_{}_com".format(collection_id, publication_id),
                "content": content
            }
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}_{}".format(collection_id, publication_id),
                "error": message
            }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/list/ms")
@text.route("/<project>/text/<collection_id>/<publication_id>/list/ms/<section_id>")
def get_manuscript_list(project, collection_id, publication_id, section_id=None):
    """
    Get all manuscripts for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        connection = db_engine.connect()
        if section_id is not None:
            section_id = str(section_id).replace('ch', '')
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE publication_id = :p_id AND section_id = :section AND deleted != 1 ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id, section=section_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                if row is not None:
                    manuscript_info.append(row._asdict())
            connection.close()
        else:
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE publication_id = :p_id AND deleted != 1 ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                if row is not None:
                    manuscript_info.append(row._asdict())
            connection.close()

        data = {
            "id": "{}_{}".format(collection_id, publication_id),
            "manuscripts": manuscript_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}_ms".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/ms/")
@text.route("/<project>/text/<collection_id>/<publication_id>/ms/<manuscript_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/ms/<manuscript_id>/<section_id>")
def get_manuscript(project, collection_id, publication_id, manuscript_id=None, section_id=None):
    """
    Get one or all manuscripts for a given publication.

    If neither `manuscript_id` nor `section_id` are specified, returns all manuscripts
    for the publication without section processing.

    If only `manuscript_id` is specified and 'ch' is not in the ID, the manuscript with the
    ID is returned without section processing. If, however, 'ch' is in the `manuscript_id`,
    it's regarded as a section ID instead of a manuscript ID, and then all manuscripts
    are returned but only the specified section of them.

    If both `manuscript_id` and `section_id` are specified, and 'ch' is not in the
    `manuscript_id`, only the section with `section_id` of the manuscript with
    `manuscript_id` ID is returned. If, however, 'ch' is in the `manuscript_id`, then all
    manuscripts are returned but only the section of them marked with `section_id`
    (generally this case should not happen).
    """
    can_show, message = get_published_status(project, collection_id, publication_id)

    if not can_show:
        return jsonify({
            "id": f"{collection_id}_{publication_id}_ms",
            "error": message
        }), 403

    logger.info(f"Getting XML for {request.full_path} and transforming...")
    connection = db_engine.connect()

    # Get manuscripts (either by specific ID or all)
    if manuscript_id is not None and 'ch' not in str(manuscript_id):
        query = """
            SELECT sort_order, name, legacy_id, id, original_filename, language
            FROM publication_manuscript
            WHERE id = :m_id AND deleted != 1
            ORDER BY sort_order ASC
        """
        statement = sqlalchemy.sql.text(query).bindparams(m_id=manuscript_id)
    else:
        query = """
            SELECT sort_order, name, legacy_id, id, original_filename, language
            FROM publication_manuscript
            WHERE publication_id = :p_id AND deleted != 1
            ORDER BY sort_order ASC
        """
        statement = sqlalchemy.sql.text(query).bindparams(p_id=publication_id)

    manuscripts_list = [row._asdict() for row in connection.execute(statement).fetchall()]
    connection.close()

    # Initialise dict with XSLT parameters
    xslt_params = {
        "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
    }

    # Determine sectionId parameter for XSLT and append to xslt_params
    if section_id is not None:
        xslt_params['sectionId'] = str(section_id)
    elif manuscript_id is not None and 'ch' in str(manuscript_id):
        xslt_params['sectionId'] = str(manuscript_id)

    for manuscript in manuscripts_list:
        # Construct filename
        if manuscript["original_filename"] is None and manuscript["legacy_id"]:
            filename = f"{manuscript['legacy_id']}.xml"
        else:
            filename = f"{collection_id}_{publication_id}_ms_{manuscript['id']}.xml"

        # Apply transformations
        manuscript["manuscript_changes"] = get_content(
            project, "ms", filename, "ms_changes.xsl", xslt_params
        ).replace(" id=", " data-id=")

        manuscript["manuscript_normalized"] = get_content(
            project, "ms", filename, "ms_normalized.xsl", xslt_params
        ).replace(" id=", " data-id=")

    # Construct and return response
    return jsonify({
        "id": f"{collection_id}_{publication_id}",
        "manuscripts": manuscripts_list
    }), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/var/")
@text.route("/<project>/text/<collection_id>/<publication_id>/var/<section_id>")
def get_variant(project, collection_id, publication_id, section_id=None):
    """
    Get all variants for a given publication, optionally specifying a section (chapter)
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT sort_order, name, type, legacy_id, id, original_filename FROM publication_version WHERE publication_id = :p_id AND deleted != 1 ORDER BY type, sort_order ASC"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        variation_info = []
        for row in connection.execute(statement).fetchall():
            if row is not None:
                variation_info.append(row._asdict())
        connection.close()

        xslt_params = {
            "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
        }

        if section_id is not None:
            xslt_params["sectionId"] = str(section_id)

        for index in range(len(variation_info)):
            variation = variation_info[index]

            if variation["type"] == 1:
                xsl_file = "poem_variants_est.xsl"
            else:
                xsl_file = "poem_variants_other.xsl"

            if variation["original_filename"] is None and variation["legacy_id"] is not None:
                filename = "{}.xml".format(variation["legacy_id"])
            else:
                filename = "{}_{}_var_{}.xml".format(collection_id, publication_id, variation["id"])

            variation_info[index]["content"] = get_content(project, "var", filename, xsl_file, xslt_params)

        data = {
            "id": "{}_{}_var".format(collection_id, publication_id),
            "variations": variation_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/downloadable/<format>/<collection_id>/inl")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/inl/<lang>")
def get_introduction_downloadable_format(project, format, collection_id, lang="sv"):
    """
    Get introduction text in a downloadable format for a given collection
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_collection_published_status(project, collection_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from publication_collection_introduction table? how handle language/version
            filename = "{}_inl_{}_{}.xml".format(collection_id, lang, version)
            if format == "xml":
                xsl_file = None
                content = get_xml_content(project, "inl", filename, xsl_file, None)
                data = {
                    "id": "{}_inl".format(collection_id),
                    "content": content
                }
                return jsonify(data), 200
            else:
                return jsonify({
                    "id": "{}_inl".format(collection_id),
                    "error": f"Unknown file format {format}"
                })
        else:
            return jsonify({
                "id": "{}_inl".format(collection_id),
                "error": message
            }), 403


@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/est")
def get_reading_text_downloadable_format(project, format, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text in a downloadable format for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} ...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT legacy_id FROM publication WHERE id = :p_id AND original_filename IS NULL"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()
        if result is None or language is not None:
            filename = "{}_{}_est.xml".format(collection_id, publication_id)
            if language is not None:
                filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)
                logger.debug("Filename (est xml) for {} is {}".format(publication_id, filename))
        else:
            filename = "{}_est.xml".format(result.legacy_id)
        logger.debug("Filename (est xml) for {} is {}".format(publication_id, filename))

        if format == "xml":
            xsl_file = "est_downloadable_xml.xsl"
        elif format == "txt":
            xsl_file = "est_downloadable_txt.xsl"
        else:
            xsl_file = None

        xslt_params = {
            "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
        }
        if section_id is not None:
            xslt_params["sectionId"] = str(section_id)

        content = get_xml_content(project, "est", filename, xsl_file, xslt_params)

        select = "SELECT language FROM publication WHERE id = :p_id"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()

        text_language = ""
        if result is not None and result.language is not None:
            text_language = result.language

        data = {
            "id": "{}_{}_est".format(collection_id, publication_id),
            "content": content,
            "language": text_language
        }

        connection.close()

        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/com")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/com/<section_id>")
def get_comments_downloadable_format(project, format, collection_id, publication_id, section_id=None):
    """
    Get comments in a downloadable format for a given publication
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_published_status(project, collection_id, publication_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            connection = db_engine.connect()
            select = "SELECT legacy_id FROM publication_comment WHERE id IN (SELECT publication_comment_id FROM publication WHERE id = :p_id) \
                        AND legacy_id IS NOT NULL AND original_filename IS NULL"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            result = connection.execute(statement).fetchone()

            if result is not None:
                filename = "{}_com.xml".format(result.legacy_id)
                connection.close()
            else:
                filename = "{}_{}_com.xml".format(collection_id, publication_id)
                connection.close()
            logger.debug("Filename (com) for {} is {}".format(publication_id, filename))

            xslt_params = {
                "estDocument": f'file://{safe_join(config["file_root"], "xml", "est", filename.replace("com", "est"))}',
                "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
            }

            if format == "xml":
                xsl_file = "com_downloadable_xml.xsl"
            elif format == "txt":
                xsl_file = "com_downloadable_txt.xsl"
            else:
                xsl_file = None

            if section_id is not None:
                xslt_params["sectionId"] = str(section_id)

            content = get_xml_content(project, "com", filename, xsl_file, xslt_params)

            data = {
                "id": "{}_{}_com".format(collection_id, publication_id),
                "content": content
            }
            connection.close()
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}_{}".format(collection_id, publication_id),
                "error": message
            }), 403
