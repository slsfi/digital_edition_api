from flask import Blueprint, jsonify, Response, send_file, make_response, request
import io
import logging
import sqlalchemy
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, get_project_config, get_project_id_from_name, get_allowed_cors_origins

media = Blueprint('media', __name__)
logger = logging.getLogger("sls_api.media")

# Media and Gallery functions


@media.route("/<project>/media/data/<type>/<type_id>")
@reader_auth_required()
def get_media_data(project, type, type_id):
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


@media.route("/<project>/media/articles/<type>/<type_id>")
@reader_auth_required()
def get_media_article_data(project, type, type_id):
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


@media.route("/<project>/media/image/<image_id>")
@reader_auth_required()
def get_media_data_image(project, image_id):
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


@media.route("/<project>/media/image/metadata/<media_id>/<lang>")
@reader_auth_required()
def get_media_image_metadata(project, media_id, lang):
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


@media.route("/<project>/media/connections/<connection_type>/<media_id>")
@reader_auth_required()
def get_media_connections(project, connection_type, media_id):
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


@media.route("/<project>/gallery/connections/<connection_type>")
@media.route("/<project>/gallery/connections/<connection_type>/<gallery_id>")
@reader_auth_required()
def get_gallery_connections(project, connection_type, gallery_id=None):
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


@media.route("/<project>/gallery/<connection_type>/connections/<type_id>")
@media.route("/<project>/gallery/<connection_type>/connections/<type_id>/<limit>")
@reader_auth_required()
def get_type_gallery_connections(project, connection_type, type_id, limit=None):
    logger.info("Getting type gallery connection data...")
    if connection_type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get gallery type connection data.", status=404, content_type="text/json")
    if limit is not None:
        limit = " LIMIT 1 "
    else:
        limit = ""
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
                                    AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 {limit}")
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


@media.route("/<project>/gallery/data/<gallery_id>/<lang>")
@reader_auth_required()
def get_gallery_data(project, gallery_id, lang=None):
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


@media.route("/<project>/gallery/data/<lang>")
@reader_auth_required()
def get_galleries(project, lang=None):
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


@media.route("/<project>/gallery/get/<collection_id>/<file_name>")
@reader_auth_required()
def get_gallery_image(project, collection_id, file_name):
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


@media.route("/<project>/gallery/thumb/<connection_type>/<connection_id>")
@reader_auth_required()
def get_type_gallery_image(project, connection_type, connection_id):
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

@media.route("/<project>/media/pdf/<pdf_id>")
@reader_auth_required()
def get_media_data_pdf(project, pdf_id):
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
        return Response(io.BytesIO(result["pdf"]), status=200, content_type="application/pdf")
    except Exception:
        logger.exception("Failed to get PDF from database.")
        return Response("Couldn't get media image.", status=404, content_type="text/json")


@media.route("/<project>/galleries")
@reader_auth_required()
def get_project_galleries(project):
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


@media.route("/<project>/files/<collection_id>/<file_type>/<download_name>/", defaults={'use_download_name': None})
@media.route("/<project>/files/<collection_id>/<file_type>/<download_name>/<use_download_name>")
@reader_auth_required()
def get_pdf_file(project, collection_id, file_type, download_name, use_download_name):
    """
    Retrieve a single file from project root
    Currently only PDF or ePub
    Only allow requests from *.sls.fi subdomains
    """
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
            if use_download_name:
                if '.pdf' in str(download_name):
                    direct_download_name = download_name.split('.pdf')[0]
                else:
                    direct_download_name = download_name
            else:
                direct_download_name = int(collection_id)

            mimetype = "application/pdf"
            file_path = safe_join(config["file_root"],
                                  "downloads",
                                  collection_id,
                                  "{}.pdf".format(direct_download_name))
        elif 'epub' in str(file_type):
            mimetype = "application/epub+zip"
            file_path = safe_join(config["file_root"],
                                  "downloads",
                                  collection_id,
                                  "{}.epub".format(int(collection_id)))
    finally:
        connection.close()

    try:
        response = make_response(
            send_file(file_path, mimetype=mimetype, download_name=download_name, conditional=True)
        )
        # Dynamically set the Access-Control-Allow-Origin header
        # to the request origin if the origin is defined in the list
        # of allowed CORS origins in the config.
        # This makes it possible to embed PDFs served by the API on
        # allowed sites.
        origin = request.headers.get("Origin")
        if origin and origin in get_allowed_cors_origins(project):
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Vary"] = "Origin"
        return response
    except Exception:
        logger.exception(f"Failed sending file from {file_path}")
        return Response("File not found.", status=404, content_type="text/json")
