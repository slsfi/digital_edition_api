from flask import Blueprint, jsonify, request
import logging
import sqlalchemy
from sqlalchemy import select
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, \
    get_collection_legacy_id, get_collection_published_status, \
    get_project_config, get_published_status, get_table, \
    get_xml_content, get_transformed_xml_content_with_caching, \
    is_valid_language, get_frontmatter_page_content, \
    get_prerendered_or_transformed_xml_content, int_or_none

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
def get_introduction(project, collection_id, publication_id, lang="sv"):
    """
    Get introduction text for a given collection.

    @TODO: remove publication_id, it is not needed or used.
    @TODO: get original_filename from publication_collection_introduction
           table? how handle language/version
    """
    text_type_key = "inl"
    resp_id = f"{collection_id}_{text_type_key}"

    can_show, message = get_collection_published_status(project, collection_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    if not is_valid_language(lang):
        return jsonify({
            "id": resp_id,
            "error": "Invalid language parameter."
        }), 400

    content, used_source = get_frontmatter_page_content(
        text_type_key=text_type_key,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s introduction for %s", used_source, request.full_path)
    data = {
        "id": resp_id,
        "content": content
    }
    return jsonify(data), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/tit")
@text.route("/<project>/text/<collection_id>/<publication_id>/tit/<lang>")
def get_title(project, collection_id, publication_id, lang="sv"):
    """
    Get title page for a given collection.

    @TODO: remove publication_id, it is not needed or used.
    @TODO: get original_filename from publication_collection_title
           table? how handle language/version
    """
    text_type_key = "tit"
    resp_id = f"{collection_id}_{text_type_key}"

    can_show, message = get_collection_published_status(project, collection_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    if not is_valid_language(lang):
        return jsonify({
            "id": resp_id,
            "error": "Invalid language parameter."
        }), 400

    content, used_source = get_frontmatter_page_content(
        text_type_key=text_type_key,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s title page for %s", used_source, request.full_path)
    data = {
        "id": resp_id,
        "content": content
    }
    return jsonify(data), 200


@text.route("/<project>/text/<collection_id>/fore")
@text.route("/<project>/text/<collection_id>/fore/<lang>")
def get_foreword(project, collection_id, lang="sv"):
    """
    Get foreword for a given collection.
    """
    text_type_key = "fore"
    resp_id = f"{collection_id}_{text_type_key}"

    can_show, message = get_collection_published_status(project, collection_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    if not is_valid_language(lang):
        return jsonify({
            "id": resp_id,
            "error": "Invalid language parameter."
        }), 400

    content, used_source = get_frontmatter_page_content(
        text_type_key=text_type_key,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s foreword for %s", used_source, request.full_path)
    data = {
        "id": resp_id,
        "content": content
    }
    return jsonify(data), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text for a given publication.
    """
    text_type_key = "est"
    resp_id = f"{collection_id}_{publication_id}_{text_type_key}"

    can_show, message = get_published_status(project, collection_id, publication_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    if language is not None and not is_valid_language(language):
        return jsonify({
            "id": resp_id,
            "error": "Invalid language parameter."
        }), 400

    try:
        publication_table = get_table("publication")

        with db_engine.connect() as connection:
            statement = (
                select(
                    publication_table.c.legacy_id,
                    publication_table.c.original_filename,
                    publication_table.c.language
                )
                .where(publication_table.c.id == int(publication_id))
                .where(publication_table.c.deleted < 1)
            )
            row = connection.execute(statement).mappings().first()
    except Exception:
        logger.exception("Unexpected error getting publication data for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting publication data."
        }), 500

    if row is None:
        # get_published_status() already checks existence, but currently not if deleted.
        return jsonify({
            "id": resp_id,
            "error": "No such publication_id."
        }), 404

    # TODO: check projects if we really need to support this filename-by-legacy_id
    # thing.
    if (
        row["original_filename"] is not None or
        row["legacy_id"] is None or
        language is not None
    ):
        filename_stem = (f"{collection_id}_{publication_id}"
                         if language is None
                         else f"{collection_id}_{publication_id}_{language}")
    else:
        filename_stem = f"{row['legacy_id']}"
    filename_stem = f"{filename_stem}_{text_type_key}"
    logger.debug("Reading text filename stem for publication %s is %s",
                 publication_id, filename_stem)

    # TODO: this is a separate query, should be combined with the query above
    xslt_params = {
        "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
    }
    if section_id is not None:
        xslt_params["sectionId"] = str(section_id)

    content, used_source = get_prerendered_or_transformed_xml_content(
        text_type=text_type_key,
        filename_stem=filename_stem,
        project=project,
        config=None,
        xslt_parameters=xslt_params
    )
    logger.info("Served %s reading text for %s", used_source, request.full_path)
    data = {
        "id": resp_id,
        "content": content,
        "language": row["language"] or ""
    }
    return jsonify(data), 200   


@text.route("/<project>/text/<collection_id>/<publication_id>/com")
@text.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>/<section_id>")
def get_comments(project, collection_id, publication_id, note_id=None, section_id=None):
    """
    Get comments file text for a given publication
    """
    text_type_key = "com"
    resp_id = f"{collection_id}_{publication_id}_{text_type_key}"

    can_show, message = get_published_status(project, collection_id, publication_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    try:
        comment_table = get_table("publication_comment")
        publication_table = get_table("publication")
        p_id = int(publication_id)

        with db_engine.connect() as connection:
            statement = (
                select(
                    comment_table.c.legacy_id,
                    comment_table.c.original_filename
                )
                .select_from(
                    comment_table.join(
                        publication_table,
                        comment_table.c.id == publication_table.c.publication_comment_id,
                    )
                )
                .where(publication_table.c.id == p_id)
                .where(publication_table.c.deleted < 1)
                .where(comment_table.c.deleted < 1)
            )
            row = connection.execute(statement).mappings().first()
    except Exception:
        logger.exception("Unexpected error getting comment data from the database for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting comment data."
        }), 500

    if row is None:
        return jsonify({
            "id": resp_id,
            "error": "No such publication comment."
        }), 404

    if row["legacy_id"] is not None and row["original_filename"] is None:
        filename_stem = f"{row['legacy_id']}_{text_type_key}"
    else:
        filename_stem = f"{collection_id}_{publication_id}_{text_type_key}"
    logger.debug("Comment filename stem for publication %s is %s",
                 publication_id, filename_stem)

    config = get_project_config(project)
    xslt_params = {
        "estDocument": f'file://{safe_join(config["file_root"],
                                           "xml",
                                           "est",
                                           f"{collection_id}_{publication_id}_est.xml")}',
        "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
    }

    if note_id is not None and section_id is None:
        xslt_params["noteId"] = str(note_id)
        xsl_path = "xslt/notes.xsl"
        content = get_transformed_xml_content_with_caching(
            project=project,
            base_text_type=text_type_key,
            xml_filename=f"{filename_stem}.xml",
            xsl_path=xsl_path,
            xslt_parameters=xslt_params
        )
        logger.info("Served single note from comments for %s", used_source, request.full_path)
    else:
        if section_id is not None:
            xslt_params["sectionId"] = str(section_id)

        content, used_source = get_prerendered_or_transformed_xml_content(
            text_type=text_type_key,
            filename_stem=filename_stem,
            project=project,
            config=config,
            xslt_parameters=xslt_params
        )
        logger.info("Served %s comments for %s", used_source, request.full_path)

    data = {
        "id": resp_id,
        "content": content
    }
    return jsonify(data), 200


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

    If neither `manuscript_id` nor `section_id` are specified, returns all
    manuscripts for the publication without section processing.

    If only `manuscript_id` is specified and it consists only of digits,
    the manuscript with the ID is returned without section processing. If,
    however, `manuscript_id` can't be converted to an integer, it's
    regarded as a section ID instead of a manuscript ID, and then all
    manuscripts are returned but only the specified section of them.

    If both `manuscript_id` and `section_id` are specified, and 'ch' is not in the
    `manuscript_id`, only the section with `section_id` of the manuscript with
    `manuscript_id` ID is returned. If, however, 'ch' is in the `manuscript_id`, then all
    manuscripts are returned but only the section of them marked with `section_id`
    (generally this case should not happen).
    """
    text_type_key = "ms"
    resp_id = f"{collection_id}_{publication_id}_{text_type_key}"

    can_show, message = get_published_status(project, collection_id, publication_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    try:
        ms_table = get_table("publication_manuscript")
        p_id = int_or_none(publication_id)
        ms_id = int_or_none(manuscript_id)

        with db_engine.connect() as connection:
            statement = (
                select(
                    ms_table.c.id,
                    ms_table.c.name,
                    ms_table.c.sort_order,
                    ms_table.c.legacy_id,
                    ms_table.c.original_filename,
                    ms_table.c.language
                )
                .where(ms_table.c.publication_id == p_id)
                .where(ms_table.c.deleted < 1)
            )

            if ms_id is not None and ms_id > 0:
                # Get specific manuscript
                statement = statement.where(ms_table.c.id == ms_id)

            statement = statement.order_by(ms_table.c.sort_order)
            rows = connection.execute(statement).mappings().all()
            manuscripts_list = [dict(r) for r in rows]
    except Exception:
        logger.exception("Unexpected error getting manuscript data for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting manuscript data."
        }), 500

    xslt_params = {
        "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
    }

    if section_id is not None:
        xslt_params['sectionId'] = str(section_id)
    elif manuscript_id is not None and 'ch' in str(manuscript_id):
        xslt_params['sectionId'] = str(manuscript_id)

    for manuscript in manuscripts_list:
        ms_versions = ["changes", "normalized"]
        for ms_version in ms_versions:
            if manuscript["original_filename"] is None and manuscript["legacy_id"]:
                filename_stem = f"{manuscript['legacy_id']}"
            else:
                filename_stem = f"{collection_id}_{publication_id}_{text_type_key}_{ms_version}_{manuscript['id']}"

            content, used_source = get_prerendered_or_transformed_xml_content(
                text_type=f"{text_type_key}_{ms_version}",
                filename_stem=filename_stem,
                project=project,
                config=None,
                xslt_parameters=xslt_params
            )
            manuscript[f"manuscript_{ms_version}"] = content
            logger.debug("Fetched %s manuscript with id %s for %s",
                         used_source, manuscript["id"], request.full_path)

    logger.info("Served manuscripts for %s", request.full_path)
    return jsonify({ "id": resp_id, "manuscripts": manuscripts_list }), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/var/")
@text.route("/<project>/text/<collection_id>/<publication_id>/var/<section_id>")
def get_variant(project, collection_id, publication_id, section_id=None):
    """
    Get all variants for a given publication, optionally specifying a
    section (chapter).
    """
    text_type_key = "var"
    resp_id = f"{collection_id}_{publication_id}_{text_type_key}"

    can_show, message = get_published_status(project, collection_id, publication_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    try:
        var_table = get_table("publication_version")
        p_id = int_or_none(publication_id)

        with db_engine.connect() as connection:
            statement = (
                select(
                    var_table.c.id,
                    var_table.c.name,
                    var_table.c.sort_order,
                    var_table.c.type,
                    var_table.c.legacy_id,
                    var_table.c.original_filename
                )
                .where(var_table.c.publication_id == p_id)
                .where(var_table.c.deleted < 1)
                .order_by(var_table.c.type)
                .order_by(var_table.c.sort_order)
            )

            rows = connection.execute(statement).mappings().all()
            variants_list = [dict(r) for r in rows]
    except Exception:
        logger.exception("Unexpected error getting variant data for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting variant data."
        }), 500

    xslt_params = {
        "bookId": str(get_collection_legacy_id(collection_id) or collection_id)
    }

    if section_id is not None:
        xslt_params["sectionId"] = str(section_id)

    for variant in variants_list:
        var_version = "base" if variant["type"] == 1 else "other"

        if variant["original_filename"] is None and variant["legacy_id"] is not None:
            filename_stem = f"{variant["legacy_id"]}"
        else:
            filename_stem = f"{collection_id}_{publication_id}_{text_type_key}_{variant['id']}"

        content, used_source = get_prerendered_or_transformed_xml_content(
            text_type=f"{text_type_key}_{var_version}",
            filename_stem=filename_stem,
            project=project,
            config=None,
            xslt_parameters=xslt_params
        )
        variant["content"] = content
        logger.debug("Fetched %s variant with id %s for %s",
                        used_source, variant["id"], request.full_path)

    logger.info("Served variants for %s", request.full_path)
    return jsonify({ "id": resp_id, "variations": variants_list }), 200


@text.route("/<project>/text/downloadable/<format>/<collection_id>/inl")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/inl/<lang>")
def get_introduction_downloadable_format(project, format, collection_id, lang="sv"):
    """
    Get introduction text in a downloadable format for a given collection

    @TODO: get original_filename from publication_collection_introduction
    table? how handle language/version
    """
    text_type_key = "inl"
    resp_id = f"{collection_id}_{text_type_key}"

    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 404

    can_show, message = get_collection_published_status(project, collection_id)
    if not can_show:
        return jsonify({ "id": resp_id, "error": message }), 403

    logger.info("Getting XML for %s and transforming...", request.full_path)
    version = "int" if config["show_internally_published"] else "ext"
    filename = f"{collection_id}_inl_{lang}_{version}.xml"

    if format == "xml":
        xsl_file = None
        content = get_xml_content(project, "inl", filename, xsl_file, None)
        data = {
            "id": resp_id,
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": resp_id,
            "error": f"Unsupported file format: {format}"
        }), 400


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
