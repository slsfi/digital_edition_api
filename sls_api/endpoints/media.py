from flask.typing import ResponseReturnValue
from flask import abort, Blueprint, jsonify, Response, send_file, make_response
import io
import logging
import sqlalchemy
from werkzeug.security import safe_join

from sls_api.endpoints.generics import (
    db_engine,
    FRONTEND_EXTERNAL_URL,
    get_project_config,
    get_project_id_from_name,
    reader_auth_required
)
from sls_api.security_headers import get_allowed_csp_frame_ancestors

media = Blueprint('media', __name__, url_prefix="/digitaledition")
logger = logging.getLogger("sls_api.media")

# Media and Gallery functions


@media.route("/<string:project>/media/data/<string:type>/<int:type_id>")
@reader_auth_required()
def get_media_data(project: str, type: str, type_id: int) -> ResponseReturnValue:
    logger.info("Getting media data...")
    media_column = "{}_id".format(type)
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.id, media.description FROM media \
            JOIN media_connection mc ON mc.media_id = media.id \
            WHERE mc.{}=:m_id AND media.type='image' ".format(media_column))
        statement = sql.bindparams(m_id=type_id)
        result = connection.execute(statement).fetchone()
        if result is not None:
            result = result._asdict()
            result["image_path"] = "/" + safe_join(project, "media", "image", str(result["id"]))
            connection.close()
            return jsonify(result), 200
        else:
            connection.close()
            logger.error(f"Failed to get media {type} {type_id} (database returned None).")
            return Response("Couldn't get media data.", status=404, content_type="text/json")
    except Exception:
        logger.exception("Failed to get media data.")
        return Response("Couldn't get media data.", status=404, content_type="text/json")


@media.route("/<string:project>/media/articles/<string:type>/<int:type_id>")
@reader_auth_required()
def get_media_article_data(project: str, type: str, type_id: int) -> ResponseReturnValue:
    logger.info("Getting media data...")
    media_column = "{}_id".format(type)
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.id, media.description FROM media \
            JOIN media_connection mc ON mc.media_id = media.id \
            WHERE mc.{}=:m_id AND media.type = 'pdf'".format(media_column))
        statement = sql.bindparams(m_id=type_id)
        return_data = []
        for row in connection.execute(statement).fetchall():
            if row is not None:
                row = row._asdict()
                row["pdf_path"] = "/" + safe_join(project, "media", "pdf", str(row["id"]))
                return_data.append(row)
        connection.close()
        return jsonify(return_data), 200
    except Exception:
        logger.exception("Failed to get article data.")
        return Response("Couldn't get article data.", status=404, content_type="text/json")


@media.route("/<string:project>/media/image/<int:image_id>")
@reader_auth_required()
def get_media_data_image(project: str, image_id: int) -> ResponseReturnValue:
    logger.info("Getting media image...")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.image FROM media WHERE id = :image_id ").bindparams(image_id=image_id)
        result = connection.execute(sql).fetchone()
        if result is not None:
            result = result._asdict()
            connection.close()
            return Response(io.BytesIO(result["image"]), status=200, content_type="image/jpeg")
        else:
            connection.close()
            logger.error(f"Failed to get media image {image_id} (database returned None)")
            return Response("Couldn't get media image.", status=404, content_type="text/json")
    except Exception:
        logger.exception("Failed to get media image from database.")
        return Response("Couldn't get media image.", status=404, content_type="text/json")


@media.route("/<string:project>/media/image/metadata/<int:media_id>/<string:lang>")
@reader_auth_required()
def get_media_image_metadata(project: str, media_id: int, lang: str) -> ResponseReturnValue:
    logger.info("Getting media metadata...")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("""SELECT
                                    (select text from translation_text where translation_id = m.description_translation_id and language = :lang) as media_description_translation,
                                    (select text from translation_text where translation_id = m.title_translation_id and language = :lang) as media_title_translation,
                                    (select text from translation_text where translation_id = mc.title_translation_id and language = :lang) as media_collection_title_translation,
                                    (select text from translation_text where translation_id = mc.description_translation_id and language = :lang) as media_collection_description_translation,
                                    (select text from translation_text where translation_id = l.name_translation_id and language = :lang) as location_name_translation,
                                    (select text from translation_text where translation_id = m.art_technique_translation_id and language = :lang) as media_art_technique_translation,
                                    m.*,
                                    mc.image_path,
                                    s.full_name, s.description as subject_description, s.date_born, s.id as subject_id, s.date_deceased,
                                    l.name as location_name, l.country as location_country, l.description as location_description
                                    FROM media m
                                    JOIN media_collection mc ON mc.id = m.media_collection_id
                                    JOIN media_connection mcon ON mcon.media_id = m.id
                                    LEFT JOIN location l ON l.id = mcon.location_id
                                    LEFT JOIN subject s ON s.id = mcon.subject_id
                                    WHERE m.id = :id or m.legacy_id = :id""").bindparams(id=media_id, lang=lang)
        result = connection.execute(sql).fetchone()
        if result is not None:
            result = result._asdict()
        connection.close()
        return jsonify(result), 200
    except Exception:
        logger.exception("Failed to get media metadata from database.")
        return Response("Couldn't get media metadata.", status=404, content_type="text/json")


@media.route("/<string:project>/media/connections/<string:connection_type>/<int:media_id>")
@reader_auth_required()
def get_media_connections(project: str, connection_type: str, media_id: int) -> ResponseReturnValue:
    logger.info("Getting media connection data...")
    if connection_type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get media connection data.", status=404, content_type="text/json")
    type_column = "{}_id".format(connection_type)
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()

        sql = sqlalchemy.sql.text(f"SELECT t.* FROM media_connection mcon \
            JOIN {connection_type} t ON t.id = mcon.{type_column} \
            JOIN media m ON m.id = mcon.media_id \
            JOIN media_collection mcol ON mcol.id = m.media_collection_id \
            WHERE m.id = :id \
            AND t.project_id = :p_id \
            AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1")
        statement = sql.bindparams(id=media_id, p_id=project_id)
        results = []
        for row in connection.execute(statement).fetchall():
            if row is not None:
                results.append(row._asdict())
        connection.close()
        return jsonify(results), 200
    except Exception:
        logger.exception("Failed to get media connection data.")
        return Response("Couldn't get media connection data.", status=404, content_type="text/json")


@media.route("/<string:project>/gallery/connections/<string:connection_type>")
@media.route("/<string:project>/gallery/connections/<string:connection_type>/<int:gallery_id>")
@reader_auth_required()
def get_gallery_connections(project: str, connection_type: str, gallery_id: int | None = None) -> ResponseReturnValue:
    logger.info("Getting gallery connection data...")
    if connection_type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get gallery connection data.", status=404, content_type="text/json")
    type_column = "{}_id".format(connection_type)
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()
        if gallery_id is not None:
            if connection_type in ['tag', 'location']:
                sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                                mcol.id as media_collection_id, mcol.image_path, t.* FROM media_connection mcon \
                                            JOIN {connection_type} t ON t.id = mcon.{type_column} \
                                            JOIN media m ON m.id = mcon.media_id \
                                            JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                            WHERE mcol.id = :id \
                                            AND t.project_id = :p_id \
                                            AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 ")
            else:
                sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                                mcol.id as media_collection_id, mcol.image_path, t.full_name as name, t.id FROM media_connection mcon \
                                            JOIN {connection_type} t ON t.id = mcon.{type_column} \
                                            JOIN media m ON m.id = mcon.media_id \
                                            JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                            WHERE mcol.id = :id \
                                            AND t.project_id = :p_id \
                                            AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 ")
            statement = sql.bindparams(id=gallery_id, p_id=project_id)
        else:
            if connection_type in ['tag', 'location']:
                sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                                mcol.id as media_collection_id, mcol.image_path, t.* FROM media_connection mcon \
                                            JOIN {connection_type} t ON t.id = mcon.{type_column} \
                                            JOIN media m ON m.id = mcon.media_id \
                                            JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                            WHERE t.project_id = :p_id \
                                            AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 ")
            else:
                sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                                mcol.id as media_collection_id, mcol.image_path, t.full_name as name, t.id FROM media_connection mcon \
                                            JOIN {connection_type} t ON t.id = mcon.{type_column} \
                                            JOIN media m ON m.id = mcon.media_id \
                                            JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                            WHERE t.project_id = :p_id \
                                            AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 ")
            statement = sql.bindparams(p_id=project_id)
        results = []
        for row in connection.execute(statement).fetchall():
            if row is not None:
                results.append(row._asdict())
        connection.close()
        return jsonify(results), 200
    except Exception as e:
        logger.debug(e)
        return Response("Couldn't get gallery connection data due to error.", status=404, content_type="text/json")


@media.route("/<string:project>/gallery/<string:connection_type>/connections/<int:type_id>")
@media.route("/<string:project>/gallery/<string:connection_type>/connections/<int:type_id>/<int:limit>")
@reader_auth_required()
def get_type_gallery_connections(project: str, connection_type: str, type_id: int, limit: int | None = None) -> ResponseReturnValue:
    logger.info("Getting type gallery connection data...")
    if connection_type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get gallery type connection data.", status=404, content_type="text/json")
    limit_sql = " LIMIT 1 " if limit is not None else ""
    type_column = "{}_id".format(connection_type)
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                        mcol.id as media_collection_id, mcol.image_path, t.* FROM media_connection mcon \
                                    JOIN {connection_type} t ON t.id = mcon.{type_column} \
                                    JOIN media m ON m.id = mcon.media_id \
                                    JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                    WHERE t.id = :id \
                                    AND t.project_id = :p_id \
                                    AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 {limit_sql}")
        statement = sql.bindparams(id=type_id, p_id=project_id)

        results = []
        for row in connection.execute(statement).fetchall():
            if row is not None:
                results.append(row._asdict())
        connection.close()
        return jsonify(results), 200
    except Exception:
        logger.exception("Failed to get type gallery connection data.")
        return Response("Couldn't get type gallery connection data.", status=404, content_type="text/json")


@media.route("/<string:project>/gallery/data/<int:gallery_id>/<string:lang>")
@reader_auth_required()
def get_gallery_data(project: str, gallery_id: int, lang: str | None = None) -> ResponseReturnValue:
    logger.info("Getting gallery image data")
    try:
        connection = db_engine.connect()
        project_id = get_project_id_from_name(project)
        sql = sqlalchemy.sql.text("""SELECT mc.id as collection_id, m.image_filename_front AS front, m.image_filename_back AS back,
                                    mc.image_path AS folder,
                                    (SELECT text FROM translation_text tt JOIN translation t ON t.id = tt.translation_id WHERE t.id = mc.title_translation_id AND tt.language = :lang) AS title,
                                    tt_desc.text AS description,
                                    (select text from translation_text where translation_id = m.title_translation_id and language = :lang) as media_title_translation, tt_desc.text AS description,
                                    (select full_name from subject where id in (select subject_id from media_connection where media_id = m.id )  limit 1) as subject_name
                                    FROM media m
                                    JOIN media_collection mc ON m.media_collection_id = mc.id
                                    JOIN translation t_desc ON t_desc.id = m.description_translation_id
                                    JOIN translation_text tt_desc ON tt_desc.translation_id = t_desc.id AND tt_desc.language=:lang
                                    WHERE mc.project_id = :p_id
                                    AND mc.id= :gallery_id
                                    AND m.type='image_ref' AND m.deleted != 1 """).bindparams(gallery_id=gallery_id, p_id=project_id, lang=lang)
        results = []
        for row in connection.execute(sql).fetchall():
            if row is not None:
                results.append(row._asdict())
        connection.close()
        return jsonify(results), 200
    except Exception:
        logger.exception("Failed to get gallery image data.")
        return Response("Couldn't get gallery image data.", status=404, content_type="text/json")


@media.route("/<string:project>/gallery/data/<string:lang>")
@reader_auth_required()
def get_galleries(project: str, lang: str | None = None) -> ResponseReturnValue:
    logger.info("Getting galleries")
    try:
        connection = db_engine.connect()
        project_id = get_project_id_from_name(project)
        sql = sqlalchemy.sql.text("SELECT mc.*, count(m.id) AS media_count, \
                                    (SELECT text \
                                    FROM translation_text tt \
                                    JOIN translation t ON t.id = tt.translation_id \
                                    WHERE t.id = mc.title_translation_id AND tt.language = :l_id) AS title \
                                    FROM media m\
                                    JOIN media_collection mc ON m.media_collection_id = mc.id\
                                    WHERE m.deleted != 1 AND mc.deleted != 1 AND mc.project_id = :p_id\
                                    GROUP BY mc.id ORDER BY mc.sort_order ASC ").bindparams(p_id=project_id, l_id=lang)
        results = []
        for row in connection.execute(sql).fetchall():
            if row is not None:
                results.append(row._asdict())
        connection.close()
        return jsonify(results), 200
    except Exception:
        logger.exception("Failed to get galleries data.")
        return Response("Couldn't get galleries.", status=404, content_type="text/json")


@media.route("/<string:project>/gallery/get/<int:collection_id>/<string:file_name>")
@reader_auth_required()
def get_gallery_image(project: str, collection_id: int, file_name: str) -> ResponseReturnValue:
    logger.info("Getting galleries")
    try:
        project_id = get_project_id_from_name(project)
        config = get_project_config(project)
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text(
            "SELECT image_path as image_path from media_collection WHERE project_id = :p_id AND id = :id ").bindparams(
            p_id=project_id, id=collection_id)
        result = connection.execute(sql).fetchone()
        if result is not None:
            result = result._asdict()
            connection.close()
        else:
            logger.error(f"Failed to get gallery image {file_name} (database returned None)")
            connection.close()
            return Response("Couldn't get gallery file.", status=404, content_type="text/json")
        file_path = safe_join(config["file_root"], "media", str(result['image_path']), "{}".format(str(file_name)))
        try:
            output = io.BytesIO()
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            logger.exception(f"Failed to read from image file at {file_path}")
            return Response("File not found: " + file_path, status=404, content_type="text/json")
    except Exception:
        logger.exception("Failed to parse gallery image request.")
        return Response("Couldn't get gallery file.", status=404, content_type="text/json")


@media.route("/<string:project>/gallery/thumb/<string:connection_type>/<int:connection_id>")
@reader_auth_required()
def get_type_gallery_image(project: str, connection_type: str, connection_id: int) -> ResponseReturnValue:
    logger.info("Getting gallery file")
    if connection_type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get media connection data.", status=404, content_type="text/json")
    type_column = "{}_id".format(connection_type)
    try:
        project_id = get_project_id_from_name(project)
        config = get_project_config(project)
        connection = db_engine.connect()
        sql = f"SELECT mcol.image_path, m.image_filename_front FROM media_connection mcon " \
              f"JOIN {connection_type} t ON t.id = mcon.{type_column} " \
              f"JOIN media m ON m.id = mcon.media_id " \
              f"JOIN media_collection mcol ON mcol.id = m.media_collection_id " \
              f"WHERE t.id = :id " \
              f"AND t.project_id = :p_id " \
              f"AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 LIMIT 1"
        sql = sqlalchemy.sql.text(sql).bindparams(p_id=project_id, id=connection_id)
        result = connection.execute(sql).fetchone()
        if result is not None:
            result = result._asdict()
            connection.close()
        else:
            connection.close()
            logger.error(f"Failed to get gallery file for {connection_type} {connection_id} (database returned None)")
            return Response("Couldn't get type file.", status=404, content_type="text/json")
        file_path = safe_join(config["file_root"], "media", str(result['image_path']),
                              str(result['image_filename_front']).replace(".jpg", "_thumb.jpg"))
        try:
            output = io.BytesIO()
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            logger.exception(f"Failed to read from image file at {file_path}")
            return Response("File not found: " + file_path, status=404, content_type="text/json")
    except Exception:
        logger.exception("Failed to parse gallery image request.")
        return Response("Couldn't get type file.", status=404, content_type="text/json")


# TODO: get subjects, locations and tags for gallery

@media.route("/<string:project>/media/pdf/<int:pdf_id>")
@reader_auth_required()
def get_media_data_pdf(project: str, pdf_id: int) -> ResponseReturnValue:
    logger.info("Getting media image...")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.pdf FROM media WHERE id = :pdf_id").bindparams(pdf_id=pdf_id)
        result = connection.execute(sql).fetchone()
        if result is not None:
            result = result._asdict()
            connection.close()
        else:
            logger.error(f"Failed to get media PDF {pdf_id} (database returned None)")
            connection.close()
            return Response("Couldn't get media image.", status=404, content_type="text/json")

        response = Response(io.BytesIO(result["pdf"]), status=200, content_type="application/pdf")
        frame_ancestors_value = get_allowed_csp_frame_ancestors(
            get_project_config(project),
            FRONTEND_EXTERNAL_URL
        )

        if frame_ancestors_value:
            response.headers["Content-Security-Policy"] = frame_ancestors_value

        return response
    except Exception:
        logger.exception("Failed to get PDF from database.")
        return Response("Couldn't get media image.", status=404, content_type="text/json")


@media.route("/<string:project>/galleries")
@reader_auth_required()
def get_project_galleries(project: str) -> ResponseReturnValue:
    logger.info("Getting project galleries...")
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT * FROM media_collection WHERE project_id = :p_id").bindparams(p_id=project_id)
        results = []
        for row in connection.execute(sql).fetchall():
            if row is not None:
                results.append(row._asdict())
        connection.close()
        return jsonify(results), 200
    except Exception:
        logger.exception("Failed to get galleries list from database.")
        return Response("Failed to get galleries list from database.", status=404, content_type="text/json")


@media.route("/<string:project>/files/<int:collection_id>/<string:file_type>/<string:download_name>/", defaults={'use_download_name': None})
@media.route("/<string:project>/files/<int:collection_id>/<string:file_type>/<string:download_name>/<string:use_download_name>")
@reader_auth_required()
def get_pdf_file(project: str, collection_id: int, file_type: str, download_name: str, use_download_name: str | None) -> ResponseReturnValue:
    """
    Serve a PDF or EPUB from the project's downloads directory.

    Reader authentication follows the API configuration. When embedding
    sources are available, CSP frame-ancestors permits 'self', the project's
    allowed_csp_frame_ancestors, and the FRONTEND_URL origin.
    """
    if use_download_name is not None and use_download_name.lower() not in {"true", "false", "1", "0", "yes", "no", "on", "off"}:
        abort(404)
    use_requested_filename = use_download_name is not None and use_download_name.lower() in {"true", "1", "yes", "on"}
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400

    connection = db_engine.connect()
    try:
        # Check that the collection exists
        statement = sqlalchemy.sql.text("SELECT * FROM publication_collection WHERE id=:coll_id").bindparams(
            coll_id=collection_id)
        row = connection.execute(statement).fetchone()
        if row is None:
            return jsonify({
                "msg": "Desired publication collection was not found in database!"
            }), 404

        file_path = ""
        mimetype = None

        if 'pdf' in str(file_type):
            if use_requested_filename:
                if '.pdf' in str(download_name):
                    direct_download_name = download_name.split('.pdf')[0]
                else:
                    direct_download_name = download_name
            else:
                direct_download_name = collection_id

            mimetype = "application/pdf"
            file_path = safe_join(config["file_root"],
                                  "downloads",
                                  str(collection_id),
                                  "{}.pdf".format(direct_download_name))
        elif 'epub' in str(file_type):
            mimetype = "application/epub+zip"
            file_path = safe_join(config["file_root"],
                                  "downloads",
                                  str(collection_id),
                                  "{}.epub".format(collection_id))
    finally:
        connection.close()

    try:
        response = make_response(
            send_file(file_path, mimetype=mimetype, download_name=download_name, conditional=True)
        )

        # Set Content-Security-Policy: frame-ancestors to allow embedding the file
        # on the configured hosts, FRONTEND_EXTERNAL_URL, and the origin from which
        # the file is being served (i.e. 'self'). In the SLS deployment, HAProxy
        # adds X-Frame-Options: SAMEORIGIN when this CSP header is not present.
        frame_ancestors_value = get_allowed_csp_frame_ancestors(
            config,
            FRONTEND_EXTERNAL_URL
        )

        if frame_ancestors_value:
            response.headers["Content-Security-Policy"] = frame_ancestors_value

        return response
    except Exception:
        logger.exception(f"Failed sending file from {file_path}")
        return Response("File not found.", status=404, content_type="text/json")
