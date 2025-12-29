from flask import Blueprint, jsonify, request
import logging
import os
import sqlalchemy
import subprocess
from werkzeug.security import safe_join
from werkzeug.utils import secure_filename

from sls_api.endpoints.generics import ALLOWED_EXTENSIONS_FOR_FACSIMILE_UPLOAD, allowed_facsimile, db_engine, \
    FACSIMILE_IMAGE_SIZES, FACSIMILE_UPLOAD_FOLDER, get_project_config, project_permission_required


facsimile_tools = Blueprint('facsimile_tools', __name__)
logger = logging.getLogger("sls_api.tools.facsimiles")


def convert_resize_uploaded_facsimile(uploaded_file_path, collection_folder_path, page_number):
    """
    Given an uploaded file, a destination folder for the facsimile collection, and a page number - create a .jpg file for each zoom level for the page
    Files are stored as <collection_folder_path>/<zoom_level>/<page_number>.jpg
    Where zoom_level is determined by FACSIMILE_IMAGE_SIZES in generics.py (1-4)

    Returns True if all conversions succeeded, otherwise returns False.
    """
    successful_conversions = []
    for zoom_level, resolution in FACSIMILE_IMAGE_SIZES.items():
        os.makedirs(safe_join(collection_folder_path, str(zoom_level)), exist_ok=True)
        convert_cmd = ["convert", "-resize", resolution, "-quality", "77", "-colorspace", "sRGB",
                       uploaded_file_path, safe_join(collection_folder_path, str(zoom_level), f"{page_number}.jpg")]
        try:
            subprocess.run(convert_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError as ex:
            logger.exception("Failed to convert uploaded facsimile!")
            logger.error(ex.stdout)
            logger.error(ex.stderr)
        else:
            successful_conversions.append(str(zoom_level))
    # remove uploaded source file once conversions are complete
    os.remove(uploaded_file_path)
    return len(successful_conversions) == len(FACSIMILE_IMAGE_SIZES.keys())


@project_permission_required
@facsimile_tools.route("/<project>/facsimiles/<collection_id>/<page_number>", methods=["PUT", "POST"])
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
    # ensure temporary facsimile upload folder exists
    os.makedirs(FACSIMILE_UPLOAD_FOLDER, exist_ok=True)
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    if request.files is None:
        return jsonify({"msg": "Request.files is none!"}), 400
    if "facsimile" not in request.files:
        return jsonify({"msg": "No file provided in request (facsimile)!"}), 400
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
        return jsonify({"msg": "No file provided in uploaded_file.filename!"}), 400

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
