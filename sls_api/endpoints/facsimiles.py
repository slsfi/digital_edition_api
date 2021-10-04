from flask import Blueprint, jsonify, request, Response, safe_join
import io
import logging
import os
import sqlalchemy
import subprocess
from werkzeug.utils import secure_filename

from sls_api.endpoints.generics import ALLOWED_EXTENSIONS_FOR_FACSIMILE_UPLOAD, allowed_facsimile, db_engine, \
    FACSIMILE_IMAGE_SIZES, FACSIMILE_UPLOAD_FOLDER, get_project_config, get_project_id_from_name

facsimiles = Blueprint('facsimiles', __name__)
logger = logging.getLogger("sls_api.facsimiles")

# Facsimile metadata and file functions


@facsimiles.route("/<project>/facsimiles/<publication_id>")
@facsimiles.route("/<project>/facsimiles/<publication_id>/<section_id>")
def get_facsimiles(project, publication_id, section_id=None):
    config = get_project_config(project)
    if publication_id is None or str(publication_id) == "undefined":
        return False, "No such publication_id."
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting facsimiles /{}/facsimiles/{}".format(project, publication_id))

        connection = db_engine.connect()

        sql = 'select * from publication_facsimile as f \
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

        pub_id = publication_id.split('_')[1]

        if section_id is not None:
            section_id = str(section_id).replace('ch', '')
            statement = sqlalchemy.sql.text(sql).bindparams(p_id=pub_id, section=section_id)
        else:
            statement = sqlalchemy.sql.text(sql).bindparams(p_id=pub_id)

        result = []
        for row in connection.execute(statement).fetchall():
            facsimile = dict(row)
            if row.folder_path != '' and row.folder_path is not None:
                facsimile["start_url"] = row.folder_path
            else:
                facsimile["start_url"] = safe_join(
                    "digitaledition",
                    project,
                    "facsimile",
                    str(row["publication_facsimile_collection_id"])
                )
            pre_pages = row["start_page_number"] or 0

            facsimile["first_page"] = pre_pages + row["page_nr"]

            sql2 = "SELECT * FROM publication_facsimile WHERE deleted != 1 AND publication_facsimile_collection_id=:fc_id AND page_nr>:page_nr ORDER BY page_nr ASC LIMIT 1"
            statement2 = sqlalchemy.sql.text(sql2).bindparams(fc_id=row["publication_facsimile_collection_id"],
                                                              page_nr=row["page_nr"])
            for row2 in connection.execute(statement2).fetchall():
                facsimile["last_page"] = pre_pages + row2["page_nr"] - 1

            if "last_page" not in facsimile.keys():
                facsimile["last_page"] = row["number_of_pages"]

            result.append(facsimile)
        connection.close()

        return_data = result
        return jsonify(return_data), 200


@facsimiles.route("/<project>/publication-facsimile-relations/")
def get_project_publication_facsimile_relations(project):
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
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@facsimiles.route("/<project>/facsimiles/collections/<facsimile_collection_ids>")
def get_facsimile_collections(project, facsimile_collection_ids):
    logger.info("Getting facsimiles /{}/facsimiles/collections/{}".format(project, facsimile_collection_ids))
    connection = db_engine.connect()
    sql = """SELECT * FROM publication_facsimile_collection where deleted != 1 and id in :ids"""
    statement = sqlalchemy.sql.text(sql).bindparams(ids=tuple(facsimile_collection_ids.split(',')))
    return_data = []
    for row in connection.execute(statement).fetchall():
        return_data.append(dict(row))
    connection.close()
    return jsonify(return_data), 200


def convert_resize_uploaded_facsimile(uploaded_file_path, collection_folder_path, page_number):
    """
    Given an uploaded file, a destination folder for the facsimile collection, and a page number - create a .jpg file for each zoom level for the page
    Files are stored as <collection_folder_path>/<zoom_level>/<page_number>.jpg
    Where zoom_level is determined by FACSIMILE_IMAGE_SIZES in generics.py (1-4)

    Returns True if all conversions succeeded, otherwise returns False.
    """
    successful_conversions = []
    for zoom_level, resolution in FACSIMILE_IMAGE_SIZES.items():
        convert_cmd = ["convert", "-resize", resolution, "-quality", "77", "-colorspace", "sRGB",
                       uploaded_file_path, safe_join(collection_folder_path, zoom_level, f"{page_number}.jpg")]
        success = subprocess.run(convert_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        if success.returncode == 0:
            successful_conversions.append(zoom_level)
        else:
            logger.error("Failed to convert uploaded facsimile!")
            logger.error(success.stdout)
            logger.error(success.stderr)
    # remove uploaded source file once conversions are complete
    os.remove(uploaded_file_path)
    return len(successful_conversions) == len(FACSIMILE_IMAGE_SIZES.keys())


@facsimiles.route("/<project>/facsimiles/<collection_id>/<page_number>", methods=["PUT", "POST"])
def upload_facsimile_file(project, collection_id, page_number):
    """
    Upload a facsimile file in image format.

    Endpoint accepts requests with enctype=multipart/form-data
    Endpoint assumes facsimile is provided as form parameter named 'facsimile'
    (for example, curl -F 'facsimile=@path/to/local/file' https://api.sls.fi/digitaledition/<project>/facsimiles/<collection_id>/<page_number>)

    ---
    First and foremost, only accept images. Reject with 400 anything that allowed_facsimile() doesn't accept.
    Then, attempt to convert image to 4 different "zoom levels" of .jpg with imagemagick

    Lastly, store the images in root/facsimiles/<collection_id>/<zoom_level>/<page_number>.jpg
    Where zoom_level is determined by FACSIMILE_IMAGE_SIZES in generics.py (1-4)
    """
    # TODO OpenStack Swift support for ISILON file storage - config param for root 'facsimiles' path
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    if "facsimile" not in request.files:
        return jsonify({"msg": "No file provided!"}), 400

    # get a folder path for the facsimile collection from the database if set, otherwise use project file root
    connection = db_engine.connect()
    collection_check_statement = sqlalchemy.sql.text("SELECT * FROM publication_facsimile_collection WHERE deleted != 1 AND id=:coll_id").bindparams(coll_id=collection_id)
    row = connection.execute(collection_check_statement).fetchone()
    if row is None:
        return jsonify({
            "msg": "Desired facsimile collection was not found in database!"
        }), 404
    elif row.folder_path != '' and row.folder_path is not None:
        collection_folder_path = safe_join(row.folder_path, collection_id)
    else:
        collection_folder_path = safe_join(config["file_root"], "facsimiles", collection_id)
    connection.close()

    # handle received file
    uploaded_file = request.files["facsimile"]
    # if user selects no file, some libraries send a POST with an empty file and filename
    if uploaded_file.filename == "":
        return jsonify({"msg": "No file provided!"}), 400

    if uploaded_file and allowed_facsimile(uploaded_file.filename):
        # handle potentially malicious filename and save file to temp folder
        temp_path = os.path.join(FACSIMILE_UPLOAD_FOLDER, secure_filename(uploaded_file.filename))
        uploaded_file.save(temp_path)

        # resize file using imagemagick
        resize = convert_resize_uploaded_facsimile(temp_path, collection_folder_path, page_number)

        if resize:
            return jsonify({"msg": "OK"})
        else:
            return jsonify({"msg": "Failed to resize uploaded facsimile!"}), 500
    else:
        return jsonify({"msg": f"Invalid facsimile provided. Allowed filetypes are {ALLOWED_EXTENSIONS_FOR_FACSIMILE_UPLOAD}. TIFF files are preferred."}), 400


@facsimiles.route("/<project>/facsimiles/<collection_id>/<number>/<zoom_level>")
def get_facsimile_file(project, collection_id, number, zoom_level):
    """
    Retrieve a single facsimile image file from project root

    Facsimile files are stored as follows: root/facsimiles/<collection_id>/<zoom_level>/<page_number>.jpg
    The collection_id these are sorted by is the publication_facsimile_collection id, stored as publication_id in the old database structure?

    However, the first page of a publication is not necessarily 1.jpg, as facsimiles often contain title pages and blank pages
    Thus, calling for facsimiles/1/1/1 may require fetching a file from root/facsimiles/1/1/5.jpg
    """
    # TODO OpenStack Swift support for ISILON file storage - config param for root 'facsimiles' path
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
            except Exception:
                logger.exception(f"Couldn't convert {row[0]} to integer.")
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
            file_path = safe_join(row.folder_path, collection_id, zoom_level, "{}.jpg".format(int(number)))
        else:
            file_path = safe_join(config["file_root"],
                                  "facsimiles",
                                  collection_id,
                                  zoom_level,
                                  "{}.jpg".format(int(number)))
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


@facsimiles.route("/<project>/facsimile/page/<col_pub>/")
@facsimiles.route("/<project>/facsimiles/page/<col_pub>/<section_id>")
def get_facsimile_pages(project, col_pub, section_id=None):
    logger.info("Getting facsimile page")
    try:
        pub_id = col_pub.split('_')[1]
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT pf.*, pf.page_nr as page_number, pfc.number_of_pages, pfc.start_page_number, pfc.id as collection_id\
            FROM publication_facsimile pf\
            JOIN publication_facsimile_collection pfc on pfc.id = pf.publication_facsimile_collection_id\
            WHERE pf.deleted != 1 AND pfc.deleted != 1 AND pf.publication_id = :pub_id")
        if section_id is not None:
            section_id = str(section_id).replace('ch', '')
            sql = " ".join([sql, "and pf.section_id = :section"])
            statement = sql.bindparams(pub_id=pub_id, section=section_id)
        else:
            statement = sql.bindparams(pub_id=pub_id)
        result = connection.execute(statement).fetchone()
        facs = dict(result)
        connection.close()
        return jsonify(facs), 200
    except Exception:
        logger.exception("Exception while getting facsimile page from database")
        return Response("Couldn't get facsimile page.", status=404, content_type="text/json")


@facsimiles.route("/<project>/<facsimile_type>/page/image/<facs_id>/<facs_nr>")
def get_facsimile_page_image(project, facsimile_type, facs_id, facs_nr):
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
                                  "{}.jpg".format(int(facs_nr)))
        elif facsimile_type == 'song-example':
            file_path = safe_join(config["file_root"],
                                  "song-example-images",
                                  facs_id,
                                  "{}.jpg".format(int(facs_nr)))
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
