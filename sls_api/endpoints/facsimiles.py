from flask.typing import ResponseReturnValue
from flask import Blueprint, jsonify, request, Response
import io
import logging
import sqlalchemy
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, get_project_config, get_project_id_from_name, int_or_none, reader_auth_required

facsimiles = Blueprint('facsimiles', __name__, url_prefix="/digitaledition")
logger = logging.getLogger("sls_api.facsimiles")

# Facsimile metadata and file functions


@facsimiles.route("/<string:project>/facsimiles/<int:publication_id>")
@facsimiles.route("/<string:project>/facsimiles/<int:publication_id>/<string:section_id>")
@reader_auth_required()
def get_facsimiles(project: str, publication_id: int, section_id: str | None = None) -> ResponseReturnValue:
    section_number = int_or_none(section_id.replace("ch", "")) if section_id is not None else None
    if section_id is not None and section_number is None:
        return jsonify({"msg": "Invalid section_id."}), 400

    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting facsimiles /{}/facsimiles/{}".format(project, publication_id))

        connection = db_engine.connect()

        sql = 'select *, f.id as publication_facsimile_id from publication_facsimile as f \
        left join publication_facsimile_collection as fc on fc.id=f.publication_facsimile_collection_id \
        left join publication p on p.id=f.publication_id \
        where f.deleted != 1 and fc.deleted != 1 and f.publication_id=:p_id \
        '

        if config["show_internally_published"]:
            sql = " ".join([sql, "and p.published>0"])
        elif config["show_unpublished"]:
            sql = " ".join([sql, "and p.published>2"])

        if section_id is not None:
            sql = " ".join([sql, "and f.section_id = :section"])

        sql = " ".join([sql, "ORDER BY f.priority"])

        if section_id is not None:
            statement = sqlalchemy.sql.text(sql).bindparams(p_id=publication_id, section=section_number)
        else:
            statement = sqlalchemy.sql.text(sql).bindparams(p_id=publication_id)

        result = []
        for row in connection.execute(statement).fetchall():
            if row is not None:
                facsimile = row._asdict()
            else:
                facsimile = {}
            if row.folder_path != '' and row.folder_path is not None:
                facsimile["start_url"] = row.folder_path
            else:
                facsimile["start_url"] = safe_join(
                    "digitaledition",
                    project,
                    "facsimile",
                    str(row.publication_facsimile_collection_id)
                )
            pre_pages = row.start_page_number or 0

            facsimile["first_page"] = pre_pages + row.page_nr

            sql2 = "SELECT * FROM publication_facsimile WHERE deleted != 1 AND publication_facsimile_collection_id=:fc_id AND page_nr>:page_nr ORDER BY page_nr ASC LIMIT 1"
            statement2 = sqlalchemy.sql.text(sql2).bindparams(fc_id=row.publication_facsimile_collection_id,
                                                              page_nr=row.page_nr)
            for row2 in connection.execute(statement2).fetchall():
                facsimile["last_page"] = pre_pages + row2.page_nr - 1

            if "last_page" not in facsimile.keys():
                facsimile["last_page"] = row.number_of_pages

            result.append(facsimile)
        connection.close()

        return_data = result
        return jsonify(return_data), 200


@facsimiles.route("/<string:project>/publication-facsimile-relations/")
@reader_auth_required()
def get_project_publication_facsimile_relations(project: str) -> ResponseReturnValue:
    logger.info("Getting publication relations for {}".format(project))
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text(
        "SELECT pc.id as pc_id, p.id as p_id, pf.id as pf_id,\
         pc.name as pc_name, p.name as p_name, pf.page_nr FROM publication_collection pc \
         JOIN publication p ON p.publication_collection_id=pc.id \
         JOIN publication_facsimile pf ON pf.publication_id = p.id \
         WHERE p.deleted != 1 AND pf.deleted != 1 AND pc.deleted != 1 AND project_id=:p_id ORDER BY pc.id")
    statement = sql.bindparams(p_id=project_id)
    results = []
    for row in connection.execute(statement).fetchall():
        if row is not None:
            results.append(row._asdict())
    connection.close()
    return jsonify(results)


@facsimiles.route("/<string:project>/facsimiles/collections/<string:facsimile_collection_ids>")
@reader_auth_required()
def get_facsimile_collections(project: str, facsimile_collection_ids: str) -> ResponseReturnValue:
    id_parts = facsimile_collection_ids.split(",")
    if not all(part.isascii() and part.isdecimal() for part in id_parts):
        return jsonify({"msg": "Invalid facsimile_collection_ids."}), 404
    collection_ids = tuple(int(part) for part in id_parts)

    logger.info("Getting facsimiles /{}/facsimiles/collections/{}".format(project, facsimile_collection_ids))
    connection = db_engine.connect()
    sql = """SELECT * FROM publication_facsimile_collection where deleted != 1 and id in :ids"""
    statement = sqlalchemy.sql.text(sql).bindparams(
        sqlalchemy.bindparam("ids", value=collection_ids, expanding=True, type_=sqlalchemy.BigInteger())
    )
    return_data = []
    for row in connection.execute(statement).fetchall():
        if row is not None:
            return_data.append(row._asdict())
    connection.close()
    return jsonify(return_data), 200


@facsimiles.route("/<string:project>/facsimiles/<int:collection_id>/<int:number>/<int:zoom_level>")
@reader_auth_required()
def get_facsimile_file(project: str, collection_id: int, number: int, zoom_level: int) -> ResponseReturnValue:
    """
    Retrieve a single facsimile image file from project root

    Facsimile files are stored as follows: root/facsimiles/<collection_id>/<zoom_level>/<page_number>.jpg
    The collection_id these are sorted by is the publication_facsimile_collection id, stored as publication_id in the old database structure?

    However, the first page of a publication is not necessarily 1.jpg, as facsimiles often contain title pages and blank pages
    Thus, calling for facsimiles/1/1/1 may require fetching a file from root/facsimiles/1/1/5.jpg
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        connection = db_engine.connect()
        check_statement = sqlalchemy.sql.text("SELECT published FROM publication WHERE deleted != 1 AND id = "
                                              "(SELECT publication_id FROM publication_facsimile WHERE deleted != 1 AND publication_facsimile_collection_id=:coll_id LIMIT 1)").bindparams(
            coll_id=collection_id)
        row = connection.execute(check_statement).fetchone()
        if row is None:
            return jsonify({
                "msg": "Desired facsimile file not found in database."
            }), 404
        else:
            try:
                status = int(row[0])
            except ValueError:
                logger.exception(f"Couldn't convert {row[0]} to integer.")
                return jsonify({
                    "msg": "Desired facsimile file not found in database."
                }), 404
            except Exception:
                logger.exception(f"Unknown exception handling {row} during facsimile file fetch.")
                return jsonify({
                    "msg": "Desired facsimile file not found in database."
                }), 404
            if status == 0:
                return jsonify({
                    "msg": "Desired facsimile file not found in database."
                }), 404
            elif status == 1:
                if not config["show_internally_published"]:
                    return jsonify({
                        "msg": "Desired facsimile file not found in database."
                    }), 404

        statement = sqlalchemy.sql.text("SELECT * FROM publication_facsimile_collection WHERE deleted != 1 AND id=:coll_id").bindparams(
            coll_id=collection_id)
        row = connection.execute(statement).fetchone()
        if row is None:
            return jsonify({
                "msg": "Desired facsimile collection was not found in database!"
            }), 404
        elif row.folder_path != '' and row.folder_path is not None:
            file_path = safe_join(row.folder_path, str(collection_id), str(zoom_level), "{}.jpg".format(number))
        else:
            file_path = safe_join(config["file_root"],
                                  "facsimiles",
                                  str(collection_id),
                                  str(zoom_level),
                                  "{}.jpg".format(number))
        connection.close()

        output = io.BytesIO()
        try:
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            logger.exception(f"Exception reading facsimile at {file_path}")
            return jsonify({
                "msg": "Desired facsimile file not found."
            }), 404


@facsimiles.route("/<string:project>/facsimile/page/<int:collection_id>_<int:publication_id>/")
@facsimiles.route("/<string:project>/facsimiles/page/<int:collection_id>_<int:publication_id>/<string:section_id>")
@reader_auth_required()
def get_facsimile_pages(project: str, collection_id: int, publication_id: int, section_id: str | None = None) -> ResponseReturnValue:
    logger.info("Getting facsimile page")
    section_number = int_or_none(section_id.replace("ch", "")) if section_id is not None else None
    if section_id is not None and section_number is None:
        return jsonify({"msg": "Invalid section_id."}), 400

    try:
        connection = db_engine.connect()
        stmnt = "SELECT pf.*, pf.page_nr as page_number, pfc.number_of_pages, pfc.start_page_number, pfc.id as collection_id\
            FROM publication_facsimile pf\
            JOIN publication_facsimile_collection pfc on pfc.id = pf.publication_facsimile_collection_id\
            WHERE pf.deleted != 1 AND pfc.deleted != 1 AND pf.publication_id = :pub_id"
        if section_id is not None:
            stmnt = " ".join([stmnt, "and pf.section_id = :section"])
            sql = sqlalchemy.sql.text(stmnt)
            statement = sql.bindparams(pub_id=publication_id, section=section_number)
        else:
            sql = sqlalchemy.sql.text(stmnt)
            statement = sql.bindparams(pub_id=publication_id)
        result = connection.execute(statement).fetchone()
        if result is not None:
            result = result._asdict()
        connection.close()
        return jsonify(result), 200
    except Exception:
        logger.exception("Exception while getting facsimile page from database")
        return Response("Couldn't get facsimile page.", status=404, content_type="text/json")


@facsimiles.route("/<string:project>/<string:facsimile_type>/page/image/<string:facs_id>/<int:facs_nr>")
@reader_auth_required()
def get_facsimile_page_image(project: str, facsimile_type: str, facs_id: str, facs_nr: int) -> ResponseReturnValue:
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting facsimile page image")
    try:
        zoom_level = "4"
        if facsimile_type == 'facsimile':
            file_path = safe_join(config["file_root"],
                                  "facsimiles",
                                  facs_id,
                                  zoom_level,
                                  "{}.jpg".format(facs_nr))
        elif facsimile_type == 'song-example':
            file_path = safe_join(config["file_root"],
                                  "song-example-images",
                                  facs_id,
                                  "{}.jpg".format(facs_nr))
        else:
            # TODO placeholder page image file?
            file_path = ""

        output = io.BytesIO()
        try:
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            logger.exception(f"Failed to read facsimile page from {file_path}")
            return Response("File not found: " + file_path, status=404, content_type="text/json")
    except Exception:
        logger.exception(f"Failed to interpret facsimile page image request {request.url}")
        return Response("Couldn't get facsimile page.", status=404, content_type="text/json")
