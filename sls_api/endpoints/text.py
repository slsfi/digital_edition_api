from flask import Blueprint, jsonify, request
import logging
from sqlalchemy import select
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, \
    get_project_config, get_published_status, get_table, \
    get_xml_content, get_transformed_xml_content_with_caching, \
    is_valid_language, get_frontmatter_page_content, \
    get_prerendered_or_transformed_xml_content, int_or_none

text = Blueprint('text', __name__)
logger = logging.getLogger("sls_api.text")

# Text functions


@text.route("/<project>/text/<text_type>/<text_id>")
def get_text_by_type(project, text_type, text_id):
    logger.info("Getting text by type /%s/text/%s/%s", project, text_type, text_id)

    table = None
    if text_type == 'manuscript':
        table = get_table("publication_manuscript")
    elif text_type == 'variation':
        table = get_table("publication_version")
    elif text_type == 'commentary':
        table = get_table("publication_comment")
    elif text_type == 'facsimile':
        table = get_table("publication_facsimile")
    else:
        return jsonify({"error": "Invalid text type."}), 400

    t_id = int_or_none(text_id)
    if t_id is None or t_id < 1:
        return jsonify({"error": "Invalid text id."}), 400

    try:
        with db_engine.connect() as connection:
            statement = (
                select(*table.c)
                .where(table.c.id == t_id)
                .order_by(table.c.id)
            )
            rows = connection.execute(statement).mappings().all()
            results = [dict(r) for r in rows]
    except Exception:
        logger.exception("Unexpected error getting text by type for %s",
                         request.full_path)
        return jsonify({"error": "Unexpected error getting text by type."}), 500

    return jsonify(results)


@text.route("/<project>/frontmatter/<collection_id>/<text_type>")
@text.route("/<project>/frontmatter/<collection_id>/<text_type>/<lang>")
def get_frontmatter(project, collection_id, text_type, lang="sv"):
    """
    Get the specified front matter text for a given collection.
    `text_type` must be one of:
    - "fore": foreword
    - "inl": introduction
    - "tit": title page

    This endpoint replaces the separate get_foreword(), get_introduction()
    and get_title() endpoints, which, however, need to be retained until
    the frontends of all projects are compatible with this endpoint.
    """
    valid_text_types = {
        "fore": "foreword",
        "inl": "introduction",
        "tit": "title page"
    }
    resp_id = f"{collection_id}_{text_type}"

    if text_type not in valid_text_types:
        return jsonify({"id": resp_id, "error": "Invalid text type."}), 400

    if not is_valid_language(lang):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, _ = get_published_status(project, collection_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    content, used_source = get_frontmatter_page_content(
        text_type=text_type,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s %s for %s",
                used_source, valid_text_types[text_type], request.full_path)
    return jsonify({"id": resp_id, "content": content}), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/inl")
@text.route("/<project>/text/<collection_id>/<publication_id>/inl/<lang>")
def get_introduction(project, collection_id, publication_id, lang="sv"):
    """
    Get introduction text for a given collection.

    @TODO: remove publication_id, it is not needed or used.
    @TODO: get original_filename from publication_collection_introduction
           table? how handle language/version
    """
    text_type = "inl"
    resp_id = f"{collection_id}_{text_type}"

    if not is_valid_language(lang):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, _ = get_published_status(project, collection_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    content, used_source = get_frontmatter_page_content(
        text_type=text_type,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s introduction for %s", used_source, request.full_path)
    return jsonify({"id": resp_id, "content": content}), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/tit")
@text.route("/<project>/text/<collection_id>/<publication_id>/tit/<lang>")
def get_title(project, collection_id, publication_id, lang="sv"):
    """
    Get title page for a given collection.

    @TODO: remove publication_id, it is not needed or used.
    @TODO: get original_filename from publication_collection_title
           table? how handle language/version
    """
    text_type = "tit"
    resp_id = f"{collection_id}_{text_type}"

    if not is_valid_language(lang):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, _ = get_published_status(project, collection_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    content, used_source = get_frontmatter_page_content(
        text_type=text_type,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s title page for %s", used_source, request.full_path)
    return jsonify({"id": resp_id, "content": content}), 200


@text.route("/<project>/text/<collection_id>/fore")
@text.route("/<project>/text/<collection_id>/fore/<lang>")
def get_foreword(project, collection_id, lang="sv"):
    """
    Get foreword for a given collection.
    """
    text_type = "fore"
    resp_id = f"{collection_id}_{text_type}"

    if not is_valid_language(lang):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, _ = get_published_status(project, collection_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    content, used_source = get_frontmatter_page_content(
        text_type=text_type,
        collection_id=collection_id,
        language=lang,
        project=project
    )
    logger.info("Served %s foreword for %s", used_source, request.full_path)
    return jsonify({"id": resp_id, "content": content}), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text for a given publication.
    """
    text_type = "est"
    resp_id = f"{collection_id}_{publication_id}_{text_type}"

    if language is not None and not is_valid_language(language):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, c_legacy_id = get_published_status(project,
                                                          collection_id,
                                                          publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    try:
        publication_table = get_table("publication")

        with db_engine.connect() as connection:
            statement = (
                select(publication_table.c.language)
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
        return jsonify({"id": resp_id, "error": "Content does not exist."}), 404

    filename_stem = (f"{collection_id}_{publication_id}_{text_type}"
                     if language is None
                     else f"{collection_id}_{publication_id}_{language}_{text_type}")
    logger.debug("Reading text filename stem for publication %s is %s",
                 publication_id, filename_stem)

    xslt_params = {"bookId": str(c_legacy_id or collection_id)}

    if section_id is not None:
        xslt_params["sectionId"] = str(section_id)

    content, used_source = get_prerendered_or_transformed_xml_content(
        text_type=text_type,
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
    Get comments text for a given publication.
    """
    text_type = "com"
    resp_id = f"{collection_id}_{publication_id}_{text_type}"

    can_show, message, c_legacy_id = get_published_status(project,
                                                          collection_id,
                                                          publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    try:
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
                .where(publication_table.c.id == int(publication_id))
                .where(publication_table.c.deleted < 1)
                .where(comment_table.c.deleted < 1)
            )
            row = connection.execute(statement).mappings().first()
    except Exception:
        logger.exception("Unexpected error getting comment data for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting comment data."
        }), 500

    if row is None:
        return jsonify({"id": resp_id, "error": "Content does not exist."}), 404
    # TODO: Ideally we would here check that the published value of the comment
    # record makes the comment showable. But we can't introduce a check like
    # that for the moment because all projects don't necessarily have correct
    # published values for their comments as it hasn't been enforced.

    filename_stem = f"{collection_id}_{publication_id}_{text_type}"
    logger.debug("Comment filename stem for publication %s is %s",
                 publication_id, filename_stem)

    config = get_project_config(project)
    xslt_params = {
        "estDocument": f'file://{safe_join(config["file_root"],
                                           "xml",
                                           "est",
                                           f"{collection_id}_{publication_id}_est.xml")}',
        "bookId": str(c_legacy_id or collection_id)
    }

    if note_id is not None and section_id is None:
        xslt_params["noteId"] = str(note_id)
        xsl_path = "xslt/notes.xsl"
        content = get_transformed_xml_content_with_caching(
            project=project,
            base_text_type=text_type,
            xml_filename=f"{filename_stem}.xml",
            xsl_path=xsl_path,
            xslt_parameters=xslt_params
        )
        logger.info("Served single note from comments for %s", request.full_path)
    else:
        if section_id is not None:
            xslt_params["sectionId"] = str(section_id)

        content, used_source = get_prerendered_or_transformed_xml_content(
            text_type=text_type,
            filename_stem=filename_stem,
            project=project,
            config=config,
            xslt_parameters=xslt_params
        )
        logger.info("Served %s comments for %s", used_source, request.full_path)

    return jsonify({"id": resp_id, "content": content}), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/list/ms")
@text.route("/<project>/text/<collection_id>/<publication_id>/list/ms/<section_id>")
def get_manuscript_list(project, collection_id, publication_id, section_id=None):
    """
    Get a list of metadata of all manuscripts for a given publication.
    """
    text_type = "ms"
    resp_id = f"{collection_id}_{publication_id}_{text_type}"

    can_show, message, _ = get_published_status(project,
                                                collection_id,
                                                publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    try:
        ms_table = get_table("publication_manuscript")

        with db_engine.connect() as connection:
            wheres = [
                ms_table.c.publication_id == int(publication_id),
                ms_table.c.deleted < 1
            ]

            if section_id is not None:
                s_id = str(section_id).replace("ch", "")
                wheres += [ms_table.c.section_id == s_id]

            statement = (
                select(
                    ms_table.c.id,
                    ms_table.c.name,
                    ms_table.c.sort_order,
                    ms_table.c.original_filename,
                    ms_table.c.legacy_id
                )
                .where(*wheres)
                .order_by(ms_table.c.sort_order)
            )
            rows = connection.execute(statement).mappings().all()
            manuscripts_list = [dict(r) for r in rows]
    except Exception:
        logger.exception("Unexpected error getting list of manuscripts for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting list of manuscripts."
        }), 500

    logger.info("Served manuscripts list for %s", request.full_path)
    data = {"id": resp_id, "manuscripts": manuscripts_list}
    return jsonify(data), 200


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
    text_type = "ms"
    resp_id = f"{collection_id}_{publication_id}_{text_type}"

    can_show, message, c_legacy_id = get_published_status(project,
                                                          collection_id,
                                                          publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    try:
        ms_table = get_table("publication_manuscript")
        ms_id = int_or_none(manuscript_id)

        with db_engine.connect() as connection:
            wheres = [
                ms_table.c.publication_id == int(publication_id),
                ms_table.c.deleted < 1
            ]
            if ms_id is not None and ms_id > 0:
                # Get specific manuscript
                wheres += [ms_table.c.id == ms_id]

            statement = (
                select(
                    ms_table.c.id,
                    ms_table.c.name,
                    ms_table.c.sort_order,
                    ms_table.c.legacy_id,
                    ms_table.c.original_filename,
                    ms_table.c.language
                )
                .where(*wheres)
                .order_by(ms_table.c.sort_order)
            )

            rows = connection.execute(statement).mappings().all()
            manuscripts_list = [dict(r) for r in rows]
    except Exception:
        logger.exception("Unexpected error getting manuscript data for %s",
                         request.full_path)
        return jsonify({
            "id": resp_id,
            "error": "Unexpected error getting manuscript data."
        }), 500

    # TODO: Ideally we would here check that the published values of the
    # manuscripts make them showable. But we can't introduce a check like
    # that for the moment because all projects don't necessarily have correct
    # published values for their manuscripts as it hasn't been enforced.

    xslt_params = {"bookId": str(c_legacy_id or collection_id)}

    if section_id is not None:
        xslt_params['sectionId'] = str(section_id)
    elif manuscript_id is not None and ms_id is None:
        xslt_params['sectionId'] = str(manuscript_id)

    for manuscript in manuscripts_list:
        ms_versions = ["changes", "normalized"]
        for ms_version in ms_versions:
            filename_stem = f"{collection_id}_{publication_id}_{text_type}_{ms_version}_{manuscript['id']}"

            content, used_source = get_prerendered_or_transformed_xml_content(
                text_type=f"{text_type}_{ms_version}",
                filename_stem=filename_stem,
                project=project,
                config=None,
                xslt_parameters=xslt_params
            )
            manuscript[f"manuscript_{ms_version}"] = content
            logger.debug("Fetched %s manuscript with id %s for %s",
                         used_source, manuscript["id"], request.full_path)

    logger.info("Served manuscripts for %s", request.full_path)
    return jsonify({"id": resp_id, "manuscripts": manuscripts_list}), 200


@text.route("/<project>/text/<collection_id>/<publication_id>/var/")
@text.route("/<project>/text/<collection_id>/<publication_id>/var/<section_id>")
def get_variant(project, collection_id, publication_id, section_id=None):
    """
    Get all variants for a given publication, optionally specifying a
    section (chapter).
    """
    text_type = "var"
    resp_id = f"{collection_id}_{publication_id}_{text_type}"

    can_show, message, c_legacy_id = get_published_status(project,
                                                          collection_id,
                                                          publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    try:
        var_table = get_table("publication_version")

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
                .where(var_table.c.publication_id == int(publication_id))
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

    # TODO: Ideally we would here check that the published values of the
    # variants make them showable. But we can't introduce a check like
    # that for the moment because all projects don't necessarily have correct
    # published values for their variants as it hasn't been enforced.

    xslt_params = {"bookId": str(c_legacy_id or collection_id)}

    if section_id is not None:
        xslt_params["sectionId"] = str(section_id)

    for variant in variants_list:
        var_version = "base" if variant["type"] == 1 else "other"
        filename_stem = f"{collection_id}_{publication_id}_{text_type}_{variant['id']}"

        content, used_source = get_prerendered_or_transformed_xml_content(
            text_type=f"{text_type}_{var_version}",
            filename_stem=filename_stem,
            project=project,
            config=None,
            xslt_parameters=xslt_params
        )
        variant["content"] = content
        logger.debug("Fetched %s variant with id %s for %s",
                     used_source, variant["id"], request.full_path)

    logger.info("Served variants for %s", request.full_path)
    return jsonify({"id": resp_id, "variations": variants_list}), 200


@text.route("/<project>/text/downloadable/<format>/<collection_id>/inl")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/inl/<lang>")
def get_introduction_downloadable_format(project, format, collection_id, lang="sv"):
    """
    Get introduction text in a downloadable format for a given collection

    @TODO: get original_filename from publication_collection_introduction
    table? how handle language/version
    """
    valid_formats = ["txt", "xml"]
    text_type = "inl"
    resp_id = f"{collection_id}_{text_type}"

    if format not in valid_formats:
        return jsonify({"id": resp_id, "error": "Unsupported format."}), 400

    if not is_valid_language(lang):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, _ = get_published_status(project, collection_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    config = get_project_config(project)
    version = "int" if config["show_internally_published"] else "ext"
    xml_filename = f"{collection_id}_{text_type}_{lang}_{version}.xml"
    xsl_filename = ("introduction_downloadable_txt.xsl"
                    if format == "txt" else None)

    content = get_xml_content(project,
                              text_type,
                              xml_filename,
                              xsl_filename,
                              None)
    logger.info("Served downloadable introduction in %s format for %s",
                format, request.full_path)
    return jsonify({"id": resp_id, "content": content}), 200


@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/est")
def get_reading_text_downloadable_format(project, format, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text in a downloadable format for a given publication.
    """
    valid_formats = ["txt", "xml"]
    text_type = "est"
    resp_id = f"{collection_id}_{publication_id}_{text_type}"

    if format not in valid_formats:
        return jsonify({"id": resp_id, "error": "Unsupported format."}), 400

    if language is not None and not is_valid_language(language):
        return jsonify({"id": resp_id, "error": "Invalid language."}), 400

    can_show, message, c_legacy_id = get_published_status(project,
                                                          collection_id,
                                                          publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    try:
        publication_table = get_table("publication")

        with db_engine.connect() as connection:
            statement = (
                select(publication_table.c.language)
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
        return jsonify({"id": resp_id, "error": "Content does not exist."}), 404

    filename_stem = (f"{collection_id}_{publication_id}_{text_type}"
                     if language is None
                     else f"{collection_id}_{publication_id}_{language}_{text_type}")
    xml_filename = f"{filename_stem}.xml"
    xsl_filename = f"est_downloadable_{format}.xsl"

    xslt_params = {"bookId": str(c_legacy_id or collection_id)}

    if section_id is not None:
        xslt_params["sectionId"] = str(section_id)

    content = get_xml_content(project,
                              text_type,
                              xml_filename,
                              xsl_filename,
                              xslt_params)
    logger.info("Served downloadable reading text in %s format for %s",
                format, request.full_path)
    data = {
        "id": resp_id,
        "content": content,
        "language": row["language"] or ""
    }
    return jsonify(data), 200


@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/com")
@text.route("/<project>/text/downloadable/<format>/<collection_id>/<publication_id>/com/<section_id>")
def get_comments_downloadable_format(project, format, collection_id, publication_id, section_id=None):
    """
    Get comments in a downloadable format for a given publication.
    """
    valid_formats = ["txt", "xml"]
    text_type_key = "com"
    resp_id = f"{collection_id}_{publication_id}_{text_type_key}"

    if format not in valid_formats:
        return jsonify({"id": resp_id, "error": "Unsupported format."}), 400

    can_show, message, c_legacy_id = get_published_status(project,
                                                          collection_id,
                                                          publication_id)
    if not can_show:
        return jsonify({"id": resp_id, "error": message}), 403

    xml_filename = f"{collection_id}_{publication_id}_{text_type_key}.xml"
    xsl_filename = f"com_downloadable_{format}.xsl"

    config = get_project_config(project)
    xslt_params = {
        "estDocument": f'file://{safe_join(config["file_root"],
                                           "xml",
                                           "est",
                                           f"{collection_id}_{publication_id}_est.xml")}',
        "bookId": str(c_legacy_id or collection_id)
    }

    if section_id is not None:
        xslt_params["sectionId"] = str(section_id)

    content = get_xml_content(project,
                              text_type_key,
                              xml_filename,
                              xsl_filename,
                              xslt_params)
    logger.info("Served downloadable comments in %s format for %s",
                format, request.full_path)
    return jsonify({"id": resp_id, "content": content}), 200
