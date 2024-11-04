import base64
from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity
import io
import json
import logging
import os
import subprocess
from typing import Any, Dict, Optional, Tuple
import xml.etree.ElementTree as ET
from werkzeug.security import safe_join

from sls_api.endpoints.generics import get_project_config, \
    project_permission_required, create_error_response, \
    create_success_response, is_valid_year, is_valid_date, \
    is_valid_year_month


file_tools = Blueprint("file_tools", __name__)
logger = logging.getLogger("sls_api.tools.files")


def check_project_config(project):
    """
    Check the config file for project webfiles repository configuration.
    Returns True if config okay, otherwise False and a message
    """
    config = get_project_config(project)
    if config is None:
        return False, "Project config not found."
    if not is_a_test(project) and "git_repository" not in config:
        return False, "git_repository not in project config."
    if "git_branch" not in config:
        return False, "git_branch information not in project config."
    if "file_root" not in config:
        return False, "file_root information not in project config."
    return True, "Project config OK."


def file_exists_in_file_root(project, file_path):
    """
    Check if the given file exists in the webfiles repository for the given project
    Returns True if the file exists, otherwise False.
    """
    config = get_project_config(project)
    if config is None:
        return False
    return os.path.exists(safe_join(config["file_root"], file_path))


def run_git_command(project, command):
    """
    Helper method to run arbitrary git commands as if in the project's webfiles repository root folder
    @type project: str
    @type command: list
    """
    config = get_project_config(project)
    git_root = config["file_root"]
    git_command = ["git", "-C", git_root]
    for c in command:
        git_command.append(c)
    return subprocess.check_output(git_command, stderr=subprocess.STDOUT)


def update_files_in_git_repo(project, specific_file=False):
    """
    Helper method to sync local repositories with remote to get latest changes
    """
    config = get_project_config(project)
    if config is None:
        return False, "No such project."
    git_branch = config["git_branch"]

    # First, fetch latest changes from remote, but don't update local
    try:
        run_git_command(project, ["fetch"])
    except subprocess.CalledProcessError as e:
        return False, str(e.output)

    if not specific_file:
        # If we're updating all files, get the list of changed files and then merge in remote changes to local repo
        try:
            output = run_git_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(git_branch)])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        try:
            run_git_command(project, ["merge", "origin/{}".format(git_branch)])
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        return True, new_and_changed_files
    else:
        # If we're only updating one file, checkout that specific file, ignoring the others
        # This makes things go faster if we're not concerned with the changes in other files at the moment
        try:
            run_git_command(project, ["checkout", "origin/{}".format(git_branch), "--", specific_file])
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        return True, specific_file


@file_tools.route("/<project>/config/get")
def get_config_file(project):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        file_path = os.path.join(config["file_root"], "config.json")
        if not os.path.exists(file_path):
            return jsonify({})
        with open(file_path) as f:
            json_data = json.load(f)
        return jsonify(json_data)


@file_tools.route("/<project>/config/update", methods=["POST"])
@project_permission_required
def update_config(project):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        request_data = request.get_json()
        file_path = os.path.join(config["file_root"], "config.json")
        with open(file_path, "w") as f:
            json.dump(request_data, f)
        return jsonify({"msg": "received"})


@file_tools.route("/<project>/sync_files/", methods=["POST"])
@project_permission_required
def pull_changes_from_git_remote(project):
    """
    Sync API's local repo with the git remote, ensuring that all files are updated to their latest versions
    """
    # verify git config
    config_okay = check_project_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    sync_repo = update_files_in_git_repo(project)

    # TODO merge conflict handling, if necessary. wait and see how things pan out - may not be an issue.

    if sync_repo[0]:
        return jsonify({
            "msg": "Git repository successfully synced for project {}".format(project),
            "changed_files": sync_repo[1]
        })
    else:
        return jsonify({
            "msg": "Git update failed to execute properly.",
            "reason": sync_repo[1]
        }), 500


def is_a_test(project):
    """
    Returns true if running in debug mode and project git_repository not configured, indicating that this is a test
    """
    config = get_project_config(project)
    if config is None and int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        return True
    elif config is not None and config["git_repository"] is None and int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        return True


def git_commit_and_push_file(project, author, message, file_path, force=False):
    # verify git config
    config_okay = check_project_config(project)
    if not config_okay[0]:
        logger.error("Error in git config, check project configuration!")
        return False

    config = get_project_config(project)

    # fetch latest changes from remote
    if not is_a_test(project):
        try:
            run_git_command(project, ["fetch"])
        except subprocess.CalledProcessError:
            logger.exception("Git fetch failed to execute properly.")
            return False

        # check if desired file has changed in remote since last update
        # if so, fail and return both user file and repo file to user, unless force=True
        try:
            output = run_git_command(project, ["show", "--pretty=format:", "--name-only",
                                               "..origin/{}".format(config["git_branch"])])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            logger.error("Git show failed to execute properly.")
            logger.error(str(e.output))
            return False

        if safe_join(config["file_root"], file_path) in new_and_changed_files and not force:
            logger.error("File {} has been changed in git repository since last update, please manually check file changes.".format(file_path))
            return False

        # merge in latest changes so that the local repository is updated
        try:
            run_git_command(project, ["merge", "origin/{}".format(config["git_branch"])])
        except subprocess.CalledProcessError as e:
            logger.error("Git merge failed to execute properly.")
            logger.error(str(e.output))
            return False

    # git add file
    try:
        run_git_command(project, ["add", file_path])
    except subprocess.CalledProcessError as e:
        logger.error("Git add failed to execute properly!")
        logger.error(str(e.output))
        return False

    # Commit changes to local repo, noting down user and commit message
    try:
        run_git_command(project, ["commit", "--author={}".format(author), "-m", message])
    except subprocess.CalledProcessError as e:
        logger.error("Git commit failed to execute properly.")
        logger.error(str(e.output))
    else:
        logger.info("git commit of {} succeeded".format(file_path))

    # push new commit to remote repository
    if not is_a_test(project):
        try:
            if force:
                run_git_command(project, ["push", "-f"])
            else:
                run_git_command(project, ["push"])
        except subprocess.CalledProcessError as e:
            logger.error("Git push failed to execute properly.")
            logger.error(str(e.output))
            return False
        else:
            logger.info("git push of {} succeeded".format(file_path))
    # if we reach this point, the file has been commited (and possibly pushed)
    return True


@file_tools.route("/<project>/update_file/by_path/<path:file_path>", methods=["PUT"])
@project_permission_required
def update_file(project, file_path):
    """
    Add new or update existing file in git remote.

    PUT data MUST be in JSON format

    PUT data MUST contain the following:
    file: xml file data in base64, to be created or updated in git repository

    PUT data MAY contain the following override information:
    author: email of the person authoring this change, if not given, JWT identity is used instead
    message: commit message for this change, if not given, generic "File update by <author>" message is used instead
    force: boolean value, if True uses force-push to override errors and possibly mangle the git remote to get the update through
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    # Check if request has valid JSON and set author/message/force accordingly
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No JSON in PUT request."}), 400
    elif "file" not in request_data:
        return jsonify({"msg": "No file in JSON data."}), 400

    author_email = request_data.get("author", get_jwt_identity()["sub"])
    message = request_data.get("message", "File update by {}".format(author_email))
    force = bool(request_data.get("force", False))

    # git commit requires author info to be in the format "Name <email>"
    # As we only have an email address to work with, split email on @ and give first part as name
    # - foo@bar.org becomes "foo <foo@bar.org>"
    author = "{} <{}>".format(
        author_email.split("@")[0],
        author_email
    )

    # Read the file from request and decode the base64 string into raw binary data
    file = io.BytesIO(base64.b64decode(request_data["file"]))

    # verify git config
    config_okay = check_project_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    # fetch latest changes from remote
    if not is_a_test(project):
        try:
            run_git_command(project, ["fetch"])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git fetch failed to execute properly.",
                "reason": str(e.output)
            }), 500

        # check if desired file has changed in remote since last update
        # if so, fail and return both user file and repo file to user, unless force=True
        try:
            output = run_git_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(config["git_branch"])])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git show failed to execute properly.",
                "reason": str(e.output)
            }), 500
        if safe_join(config["file_root"], file_path) in new_and_changed_files and not force:
            with io.open(safe_join(config["file_root"], file_path), mode="rb") as repo_file:
                file_bytestring = base64.b64encode(repo_file.read())
                return jsonify({
                    "msg": "File {} has been changed in git repository since last update, please manually check file changes.",
                    "your_file": request_data["file"],
                    "repo_file": file_bytestring.decode("utf-8")
                }), 409

        # merge in latest changes so that the local repository is updated
        try:
            run_git_command(project, ["merge", "origin/{}".format(config["git_branch"])])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git merge failed to execute properly.",
                "reason": str(e.output)
            }), 500

    # check the status of the git repo, so we know if we need to git add later
    file_exists = file_exists_in_file_root(project, file_path)

    # Secure filename and save new file to local repo
    # Could be more secure...
    pos = file_path.find('.xml')
    if pos > 0:
        filename = safe_join(config["file_root"], file_path)
        if file and filename:
            with io.open(filename, mode="wb") as new_file:
                new_file.write(file.getvalue())
    else:
        return jsonify({
                "msg": "File path error"
            }), 500

    # Add file to local repo if it wasn't already in the repository
    if not file_exists:
        try:
            run_git_command(project, ["add", filename])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git add failed to execute properly.",
                "reason": str(e.output)
            }), 500

    # Commit changes to local repo, noting down user and commit message
    try:
        run_git_command(project, ["commit", "--author={}".format(author), "-m", message])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git commit failed to execute properly.",
            "reason": str(e.output)
        }), 500

    # push new commit to remote repository
    if not is_a_test(project):
        try:
            if force:
                run_git_command(project, ["push", "-f"])
            else:
                run_git_command(project, ["push"])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git push failed to execute properly.",
                "reason": str(e.output)
            }), 500

    return jsonify({
        "msg": "File updated successfully in repository."
    })


@file_tools.route("/<project>/get_file/by_path/<path:file_path>")
@project_permission_required
def get_file(project, file_path):
    """
    Get latest file from git remote
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    # TODO swift and/or S3 support for large files (images/facsimiles)
    config_okay = check_project_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    if not is_a_test(project):
        # Sync the desired file from remote repository to local API repository
        update_repo = update_files_in_git_repo(project, file_path)
        if not update_repo[0]:
            return jsonify({
                "msg": "Git update failed to execute properly.",
                "reason": update_repo[1]
            }), 500

    if file_exists_in_file_root(project, file_path):
        # read file, encode as base64 string and return to user as JSON data.
        with io.open(safe_join(config["file_root"], file_path), mode="rb") as file:
            file_bytestring = base64.b64encode(file.read())
            return jsonify({
                "file": file_bytestring.decode("utf-8"),
                "filepath": file_path
            })
    else:
        return jsonify({"msg": "The requested file was not found in the git repository."}), 404


@file_tools.route("/<project>/get_tree/")
@file_tools.route("/<project>/get_tree/<path:file_path>")
@project_permission_required
def get_file_tree(project, file_path=None):
    """
    Get a file listing from the git remote
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    # Fetch changes (to update index) but don't merge, and then run ls-files to get file listing.
    try:
        if not is_a_test(project):
            run_git_command(project, ["fetch"])
        if file_path is None:
            output = run_git_command(project, ["ls-files"])
        else:
            output = run_git_command(project, ["ls-files", file_path])
        file_listing = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git file listing failed.",
            "reason": str(e.output)
        }), 500
    tree = path_list_to_tree(file_listing)
    return jsonify(tree)


@file_tools.route("/<project>/get_metadata_from_xml/by_path/<path:file_path>")
@project_permission_required
def get_metadata_from_xml_file(project: str, file_path: str):
    """
    Retrieve metadata from a TEI XML file within a given project.

    This endpoint parses a TEI (Text Encoding Initiative) XML file
    specified by `file_path` within the given `project` and extracts
    publication metadata, including the title, original publication date
    (date of origin), language, and genre.

    URL Path Parameters:

    - `project` (str, required): The name of the projectcontaining the
      XML file.
    - `file_path` (str, required): The path to the XML file within the
      project's git repository.

    Returns:

    - A tuple containing a Flask Response object with JSON data and an
      HTTP status code. The JSON response has the following structure:

        {
            "success": bool,
            "message": str,
            "data": object or null
        }

      - `success`: A boolean indicating whether the operation was successful.
      - `message`: A string containing a descriptive message about the result.
      - `data`: On success, an object containing the extracted metadata;
        `null` on error.

    Example Request:

        GET /my_project/get_metadata_from_xml/by_path/documents/file.xml

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Metadata retrieved from XML file.",
            "data": {
                "name": "Publication Title",
                "original_publication_date": "1854-07-20",
                "language": "en",
                "genre": "prose"
            }
        }

    Example Error Response (HTTP 404):

        {
            "success": false,
            "message": "Error: the requested file was not found in the git repository.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the metadata is returned.
    - 400 - Bad Request: Invalid request parameters (e.g., invalid file
            path, missing .xml extension, file size exceeds limit).
    - 403 - Forbidden: Permission denied when trying to read the XML file.
    - 404 - Not Found: The requested file does not exist.
    - 500 - Internal Server Error: An unexpected error occurred on the server.
    """
    # Validate project config
    config = get_project_config(project)
    if config is None:
        return create_error_response("Error: project config not found on the server.", 500)

    config_ok = check_project_config(project)
    if not config_ok[0]:
        return create_error_response(f"Error: {config_ok[1]}", 500)

    # Safely join the base directory and file path
    full_path = safe_join(config["file_root"], file_path)
    if full_path is None:
        return create_error_response("Error: invalid file path.", 400)

    # Resolve the real, absolute paths
    base_dir = os.path.realpath(config["file_root"])
    full_path = os.path.realpath(full_path)

    # Verify that full_path is within base_dir, i.e. file_root specified
    # in config
    if os.path.commonpath([base_dir, full_path]) != base_dir:
        return create_error_response("Error: invalid file path.", 400)

    # Check if the file exists
    try:
        if not os.path.isfile(full_path):
            return create_error_response("Error: the requested file was not found on the server.", 404)
    except Exception:
        logger.exception(f"Error accessing file at {full_path}")
        return create_error_response(f"Error accessing file at {file_path}", 500)

    # Check that the file has a .xml extension
    if not full_path.endswith(".xml"):
        return create_error_response("Error: the file path must point to a file with a .xml extension.", 400)

    # Check file size so we don't parse overly large XML files
    max_file_size = 5 * 1024 * 1024  # 5 MB
    if os.path.getsize(full_path) > max_file_size:
        return create_error_response("Error: file size exceeds the maximum allowed limit (5 MB).", 400)

    # Process the XML file
    metadata, error_message, status_code = extract_publication_metadata_from_tei_xml(full_path)
    if error_message:
        return create_error_response(error_message, status_code=status_code)
    else:
        return create_success_response("Metadata retrieved from XML file.", data=metadata)


def extract_publication_metadata_from_tei_xml(file_path: str) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[int]]:
    """
    Extracts publication metadata (document title, date of origin and main
    language) from a TEI XML file located at the given file path.

    Args:

        file_path (str): The absolute path to the TEI XML file.

    Returns:

    - A tuple containing:
        - metadata (dict or None): A dictionary with the extracted
          metadata:
            - "name" (str or None): The title extracted from the XML.
            - "original_publication_date" (str or None): The date of
              origin in "YYYY", "YYYY-MM", or "YYYY-MM-DD" format.
            - "language" (str or None): The language code.
            - "genre" (str or None): The genre of the text.
            Returns `None` if an error occurred.
        - error_message (str or None): An error message if an error
          occurred; otherwise `None`.
        - status_code (int or None): The HTTP status code corresponding to
        the result (e.g., 200 on success, 404 if file not found).

    Examples:

        >>> metadata, error_message, status_code = extract_publication_metadata_from_tei_xml('/path/to/file.xml')
    """
    try:
        # Parse the XML file and extract relevant metadata from it
        with open(file_path, "r", encoding="utf-8-sig") as xml_file:
            tree = ET.parse(xml_file)
        root = tree.getroot()

        # Determine namespace
        ns = {'tei': 'http://www.tei-c.org/ns/1.0'}

        # Helper function to get full text including subelements
        def get_full_text(element):
            return "".join(element.itertext()) if element is not None else None

        # Extract the full text of <title> inside <titleStmt>
        title_element = root.find("./tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title", namespaces=ns)
        title = get_full_text(title_element)

        # Extract the @when attribute value in <origDate> within <sourceDesc>
        orig_date_element = root.find("./tei:teiHeader/tei:fileDesc/tei:sourceDesc//tei:origDate", namespaces=ns)
        orig_date = orig_date_element.get("when") if orig_date_element is not None else None
        if not orig_date:
            # Search for a <date> with @when in <bibl> within <sourceDesc>
            date_element = root.find("./tei:teiHeader/tei:fileDesc/tei:sourceDesc/tei:bibl//tei:date", namespaces=ns)
            orig_date = date_element.get("when") if date_element is not None else None

            # Validate orig_date, must conform to YYYY, YYYY-MM
            # or YYYY-MM-DD date formats
            if (
                orig_date is not None
                and not (
                    is_valid_year(str(orig_date))
                    or is_valid_year_month(str(orig_date))
                    or is_valid_date(str(orig_date))
                )
            ):
                orig_date = None

        # Extract the @xml:lang attribute in <text>
        text_element = root.find("./tei:text", namespaces=ns)
        language = (text_element.get("{http://www.w3.org/XML/1998/namespace}lang")
                    if text_element is not None
                    else None)

        metadata = {
            "name": title,
            "original_publication_date": orig_date,
            "language": language,
            "genre": None  # Currently, genre is not extractable from the XML files
        }
        return metadata, None, 200

    except FileNotFoundError:
        logger.exception("File not found error when trying to open XML file for metadata extraction.")
        return None, "Error: file not found.", 404
    except ET.ParseError:
        logger.exception("Parse error when trying to extract metadata from XML file.")
        return None, "Error: the XML file is not well-formed or could not be parsed.", 500
    except PermissionError:
        logger.exception("Permission denied error when trying to extract metadata from XML file.")
        return None, "Error: permission denied when trying to read the XML file.", 403
    except Exception:
        logger.exception("Exception extracting metadata from XML file.")
        return None, "Unexpected error: unable to extract metadata from XML file.", 500


def path_list_to_tree(path_list):
    """
    Turn a list of filepaths into a nested dict
    """
    file_tree = {}
    for path in path_list:
        _recurse(path, file_tree)
    return file_tree


def _recurse(path, container):
    """
    Recurse over path and container to make a nested dict of path in container
    """
    parts = path.split("/")
    head = parts[0]
    tail = parts[1:]
    if not tail:
        container[head] = None
    else:
        if head not in container:
            container[head] = {}
        _recurse("/".join(tail), container[head])
