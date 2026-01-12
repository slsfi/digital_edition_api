import base64
from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity
import io
import json
import logging
import os
import subprocess
from typing import Any, Dict, Optional, Tuple
from lxml import etree as ET
from sqlalchemy import select
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, get_project_config, \
    project_permission_required, create_error_response, \
    create_success_response, is_any_valid_date_format, \
    int_or_none, is_valid_language, get_table, get_project_id_from_name


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


def apply_updates_to_collection_toc(
    data: Dict[str, Any] | list,
    update_map: Dict[str, Dict[str, Any]]
) -> int:
    """
    Recursively traverse a nested JSON structure (collection table of
    contents) and update nodes (in-place) that match itemIds found in the
    `update_map`. Returns the count of updated nodes.

    Behavior:
        - If a node contains an `itemId` that exists in `update_map`, its
          `text`, `date`, and/or `language` fields will be updated.
        - The original structure of the JSON is preserved.
        - This function modifies `data` in-place.

    Args:
        data (dict or list): The JSON data structure (a dictionary or
            list of dictionaries) containing items to potentially
            update. This structure may be deeply nested and include
            "children" arrays.
        update_map (dict): A dictionary mapping itemIds (str) to
            dictionaries with the updated field values (e.g.,
            {"text": ..., "date": ..., "language": ...}). Only the keys
            present in the mapped value will be updated.

    Returns:
        int: Number of nodes where at least one field was changed.
    """
    updated_count = 0

    def _apply(node):
        nonlocal updated_count

        if isinstance(node, dict):
            item_id = node.get("itemId")
            if item_id and item_id in update_map:
                updated_fields = update_map[item_id]
                changed = False
                for key, new_value in updated_fields.items():
                    if node.get(key) != new_value:
                        node[key] = new_value
                        changed = True
                if changed:
                    updated_count += 1

            # Recurse into children
            if "children" in node:
                for child in node["children"]:
                    _apply(child)

        elif isinstance(node, list):
            for item in node:
                _apply(item)

    _apply(data)
    return updated_count


def normalize_toc_key_order(node, *, children_key: str = "children"):
    """
    Return a deep-copied structure where, in every dict:
      1) All keys except `children_key` are sorted alphabetically, and
      2) `children_key` (if present) is placed as the last key.

    This improves human readability and keeps a stable, deterministic
    order in serialized JSON regardless of the input order of keys.
    The function preserves semantics and data types; it only reorders
    keys.

    Behavior:
      - Dicts: Recursively processes values. Non-`children_key` keys are
        sorted alphabetically (case-insensitive) and emitted first. If
        `children_key` exists, it is emitted last (recursively processed).
      - Lists: Each element is processed recursively.
      - Other types: Returned as-is.

    Parameters:
      node: Any JSON-serializable Python structure (dict/list/primitive).
      children_key: The key to move to the end when present (default:
      "children").

    Notes:
      - Python 3.7+ preserves insertion order in dicts; `json.dump` will
        respect the order produced here. Keep `sort_keys=False` when
        dumping JSON.
      - If `children_key` is present but not a list, it is still
        processed recursively.
    """
    # Dict
    if isinstance(node, dict):
        other_keys = sorted((k for k in node.keys() if k != children_key),
                            key=lambda s: s.casefold())
        new_d = {}
        for k in other_keys:
            new_d[k] = normalize_toc_key_order(node[k],
                                               children_key=children_key)
        if children_key in node:
            cv = node[children_key]
            if isinstance(cv, list):
                new_d[children_key] = [
                    normalize_toc_key_order(
                        c, children_key=children_key
                    ) for c in cv
                ]
            else:
                new_d[children_key] = normalize_toc_key_order(
                    cv, children_key=children_key
                )
        return new_d

    # List
    if isinstance(node, list):
        return [
            normalize_toc_key_order(
                x, children_key=children_key
            ) for x in node
        ]

    # Primitives (str, int, float, bool, None) or other JSON-safe types
    return node


@file_tools.route("/<project>/git-repo-details")
@project_permission_required
def get_git_repo_details(project):
    """
    Retrieve details of a project's git repository.

    This endpoint fetches and returns details of a git repository
    associated with a given `project`, including the repository name
    and current branch. The details are extracted from the project's
    configuration on the server.

    URL Path Parameters:

    - `project` (str, required): The name of the project for which
      Git repository details are being retrieved.

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
      - `data`: On success, an object containing the Git repository details;
        `null` on error.

    Example Request:

        GET /my_project/git-repo-details/get

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Project git repository details successfully retrieved.",
            "data": {
                "name": "my_repo",
                "branch": "main"
            }
        }

    Example Error Response (HTTP 500):

        {
            "success": false,
            "message": "Error: project config not found on the server.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the repository details are
            returned.
    - 404 - Not Found: The project configuration does not exist.
    - 500 - Internal Server Error: An unexpected error occurred on the
            server.
    """
    # Validate project config
    config = get_project_config(project)
    if config is None:
        return create_error_response("Error: project config not found on the server.", 404)

    config_ok = check_project_config(project)
    if not config_ok[0]:
        return create_error_response(f"Error: {config_ok[1]}", 404)

    # Get the repo name from the repo URL
    repo_name = str(config["git_repository"]).split('/')[-1].replace(".git", "")

    return create_success_response(
        message="Project git repository details successfully retrieved.",
        data={"name": repo_name, "branch": config["git_branch"]}
    )


@file_tools.route("/<project>/sync_files/", methods=["POST"])
@project_permission_required
def pull_changes_from_git_remote(project):
    """
    Sync the local git repository of a given project with its remote origin.

    URL Path Parameters:

    - project (str, required): The name of the project for which the git
      repository sync is being performed.

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
    - `data`: On success, an object containing a list of changed files;
      `null` on error.

    Example Request:

        POST /projectname/sync_files/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Git repository successfully synced.",
            "data": {
                "changed_files": [
                    "file1.txt",
                    "dir1/file2.txt"
                ]
            }
        }

    Example Error Response (HTTP 500):

        {
            "success": false,
            "message": "Error: update of git repository failed.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the git repository was
            synced.
    - 500 - Internal Server Error: The project configuration is invalid,
            or an unexpected error occurred during the sync operation.
    """
    # verify git config
    config_ok = check_project_config(project)
    if not config_ok[0]:
        return create_error_response(f"Error: {config_ok[1]}", 500)

    sync_repo = update_files_in_git_repo(project)

    # TODO merge conflict handling, if necessary. wait and see how things pan out - may not be an issue.

    if sync_repo[0]:
        return create_success_response(
            message=f"Git repository for project '{project}' successfully synced.",
            data={"changed_files": sync_repo[1]}
        )
    else:
        logger.error(f"Git update failed: {sync_repo[1]}")
        return create_error_response("Error: update of git repository failed.", 500)


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
    Retrieve a file tree from the local git repository of a given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which the file
      tree is being requested.
    - file_path (str, optional): The path to a specific directory or file
      within the project's git repository. If omitted, the root file tree
      of the project's git repository will be retrieved.

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
    - `data`: On success, an object representing the file tree structure;
      `null` on error.

    Example Request:

        GET /projectname/get_tree/
        GET /projectname/get_tree/path/to/directory/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "File tree retrieved successfully.",
            "data": {
                "\"documents": {
                    "Manuskript": {
                        "Lasning_for_barn_manuskript": {
                        "Lasning_for_barn_1": {
                            "Den_tappade_kangan": {
                                "Madamen och tiggarflickan ms NB 244_106_0201 Lfb Den tappade k\\303\\244ngan.xml\"": null
                            }
                        },
                    ...
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Error: invalid file path.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the file tree is returned.
    - 400 - Bad Request: The file path is invalid or outside the allowed
            directory.
    - 500 - Internal Server Error: The project configuration is invalid,
            or an unexpected error occurred while retrieving the file
            tree.
    """
    # Validate project config
    config = get_project_config(project)
    if config is None:
        return create_error_response("Error: project config does not exist on server.", 500)

    config_ok = check_project_config(project)
    if not config_ok[0]:
        return create_error_response(f"Error: {config_ok[1]}", 500)

    try:
        # List files from the local repository
        if file_path is None:
            output = run_git_command(project, ["ls-files"])
        else:
            # Validate file_path
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

            output = run_git_command(project, ["ls-files", file_path])

        # Decode and process the output
        file_listing = [s.strip().decode("utf-8", "ignore") for s in output.splitlines()]
        tree = path_list_to_tree(file_listing)

    except subprocess.CalledProcessError as e:
        # Handle git command errors
        logger.error(f"Git file listing failed: {e.output.decode('utf-8', 'ignore')}")
        return create_error_response("Error: git file listing failed.", 500)
    except Exception:
        # Handle any other unexpected errors
        logger.exception("Unexpected error retrieving file tree.")
        return create_error_response("Unexpected error: could not get file tree.", 500)

    # Return the file tree in a standardized success response
    return create_success_response(
        message="File tree retrieved successfully.",
        data=tree
    )


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

    # Resolve the real, absolute path
    full_path = os.path.realpath(full_path)

    # Check if the file exists
    try:
        if not os.path.isfile(full_path):
            return create_error_response("Error: the requested file was not found on the server.", 404)
    except Exception:
        logger.exception(f"Error accessing file at {full_path}")
        return create_error_response(f"Error accessing file at {file_path}", 500)

    # Check that the file has a .xml extension
    if os.path.splitext(full_path)[1] != ".xml":
        return create_error_response("Error: the file path must point to a file with a .xml extension.", 400)

    # Check file size so we don't parse overly large XML files
    # Use default value if max size not specified in config
    if "xml_max_file_size" in config:
        max_file_size = config["xml_max_file_size"] * 1024 * 1024
    else:
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
    Extracts publication metadata (document title, date of origin, main
    language and genre) from a TEI XML file located at the given file path.

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

        # Declare namespace
        ns = {'tei': 'http://www.tei-c.org/ns/1.0'}

        # Helper function to get full text including subelements
        def get_full_text(element):
            return "".join(element.itertext()) if element is not None else None

        # Extract the full text of the first <title> without @type attribute inside <titleStmt>
        title_element = root.xpath("./tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title[not(@type)]", namespaces=ns)
        title_element = title_element[0] if title_element else None
        title = get_full_text(title_element)

        # Extract the @when attribute value in <origDate> within <sourceDesc>
        orig_date_element = root.find("./tei:teiHeader/tei:fileDesc/tei:sourceDesc//tei:origDate[@when]", namespaces=ns)
        orig_date = orig_date_element.get("when") if orig_date_element is not None else None

        # Fallbacks in case <origDate> not found in <sourceDesc>:
        if not orig_date:
            # Search for a <date> with @when in <bibl> within <sourceDesc>
            date_element = root.find("./tei:teiHeader/tei:fileDesc/tei:sourceDesc/tei:bibl//tei:date[@when]", namespaces=ns)
            orig_date = date_element.get("when") if date_element is not None else None

            if not orig_date:
                # Search for a <date> with @when in <correspDesc> within <profileDesc>
                date_element = root.find("./tei:teiHeader/tei:profileDesc/tei:correspDesc/tei:correspAction[@type='sent']/tei:date[@when]", namespaces=ns)
                orig_date = date_element.get("when") if date_element is not None else None

        # Validate orig_date, must conform to YYYY, YYYY-MM or YYYY-MM-DD
        # date formats if not None. Set to None if invalid format.
        if (
            orig_date is not None
            and not is_any_valid_date_format(str(orig_date).strip())
        ):
            orig_date = None

        # Extract the @xml:lang attribute in <text>
        text_element = root.find("./tei:text", namespaces=ns)
        language = (text_element.get("{http://www.w3.org/XML/1998/namespace}lang")
                    if text_element is not None
                    else None)

        # Extract genre from <textClass>
        genre_element = root.find("./tei:teiHeader/tei:profileDesc/tei:textClass/tei:keywords/tei:term[@type='genre']", namespaces=ns)
        genre = genre_element.text if genre_element is not None else None

        metadata = {
            "name": title,
            "original_publication_date": orig_date,
            "language": language,
            "genre": genre
        }

        # Strip whitespace from extracted values and convert
        # empty string values to None
        for key, value in metadata.items():
            if value is not None:
                new_value = str(value).strip()
                metadata[key] = None if new_value == "" else new_value

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


@file_tools.route("/<project>/collection-toc/<collection_id>/<language>", methods=["GET", "PUT"])
@file_tools.route("/<project>/collection-toc/<collection_id>", methods=["GET", "PUT"])
@project_permission_required
def handle_collection_toc(project, collection_id, language=None):
    """
    Get or save the table of contents file for the specified collection
    in the specified language and project. This endpoint requires
    authentication; there is a separate GET-only endpoint in metadata.py
    for digital edition frontends.

    URL Path Parameters:

    - project (str): Project name.
    - collection_id (int): The id of the collection.
    - language (str or None, optional): The language of the table of
      contents.

    PUT Data Parameters in JSON Format:

    - A valid JSON object with the new table of contents data of the
      collection.

    Returns:

    - A tuple containing a Flask Response object with JSON data and an
      HTTP status code. The JSON response has the following structure:

        {
            "success": bool,
            "message": str,
            "data": object if GET method; null if PUT method
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: If GET method, an object; if PUT method, `null`.

    Status Codes:

    - 200 - OK: Successfully retrieved or updated the table of contents.
    - 201 - OK: Successfully added the table of contents as a new file.
    - 400 - Bad Request: Invalid data.
    - 403 - Forbidden: Permission denied performing the operation.
    - 404 - Not Found: The table of contens file was not found on the
            server.
    - 415 - Unsupported Media Type: The JSON data of the table of contents
            file on the server, or the JSON data in the request, is
            invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Validate project config
    config = get_project_config(project)
    if config is None:
        return create_error_response("Error: project config not found on the server.", 500)

    config_ok = check_project_config(project)
    if not config_ok[0]:
        logger.warning(f"Project '{project}' has an incomplete config: {config_ok[1]}")
        return create_error_response(f"Error: {config_ok[1]}", 500)

    # Validate collection_id
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    # Validate language
    if language is not None and not is_valid_language(language):
        return create_error_response("Validation error: 'language' is not among valid language codes.")

    filename = f"{collection_id}_{language}.json" if language else f"{collection_id}.json"
    filepath = safe_join(config["file_root"], "toc", filename)

    if filepath is None:
        return create_error_response("Error: invalid table of contents file path.", 400)

    # Resolve the real, absolute path
    filepath = os.path.realpath(filepath)

    toc_file_exists = os.path.isfile(filepath)

    if request.method == "GET":
        try:
            # Check if the file exists
            if not toc_file_exists:
                return create_error_response(f"Error: the table of contents file {filename} was not found on the server.", 404)

            # Read file contents and parse json into a dict
            with open(filepath, "r", encoding="utf-8-sig") as json_file:
                contents = json.load(json_file)

            return create_success_response(f"Loaded {filename}.", contents)

        except FileNotFoundError:
            logger.exception(f"File not found error when trying to read ToC-file at {filepath}.")
            return create_error_response(f"Error: {filename} not found.", 404)
        except PermissionError:
            logger.exception(f"Permission denied error when trying to read ToC-file at {filepath}.")
            return create_error_response(f"Error: permission denied when trying to read {filename}.", 403)
        except UnicodeDecodeError:
            logger.exception(f"Invalid encoding in ToC file at {filepath}.")
            return create_error_response(f"Error: file {filename} is not valid UTF-8 encoded JSON.", 415)
        except json.JSONDecodeError:
            logger.exception(f"Invalid JSON in ToC file at {filepath}.")
            return create_error_response(f"Error: file {filename} contains invalid JSON.", 415)
        except Exception:
            logger.exception(f"Error accessing file at {filepath}.")
            return create_error_response(f"Error accessing file at {filepath}.", 500)

    elif request.method == "PUT":
        # Verify that request data was provided
        request_data = request.get_json(silent=True)
        if not request_data:
            return create_error_response("No data provided or data not valid JSON.", 415)

        # Reorder keys so 'children' is last everywhere and rest of keys are
        # alphabetically sorted. Makes the serialized JSON more humanly readable
        # and diffs easier to spot.
        norm_toc = normalize_toc_key_order(request_data)

        try:
            # Ensure parent dir exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            # Save new ToC as filepath.new
            with open(f"{filepath}.new", "w", encoding="utf-8") as outfile:
                json.dump(norm_toc, outfile, ensure_ascii=False, indent=2)
        except Exception:
            logger.exception(f"Error saving file {filepath}.new.")
            # If saving file fails, remove it before returning an error
            try:
                os.remove(f"{filepath}.new")
            except FileNotFoundError:
                pass
            return create_error_response("Error: failed to save data to disk.", 500)
        else:
            # Check identity though it should never be invalid at this point,
            # we will use it later to get the user’s email.
            identity = get_jwt_identity()
            if not identity:
                return create_error_response("Error: permission denied.", 403)

            # Attempt to rename the new ToC file so it replaces the old file
            try:
                os.replace(f"{filepath}.new", filepath)
            except PermissionError:
                logger.exception(f"Permission error renaming ToC file {filepath}.new.")
                try:
                    os.remove(f"{filepath}.new")
                except FileNotFoundError:
                    pass
                return create_error_response("Error: permission denied when trying to rename file saving data to disk.", 403)
            except OSError:
                logger.exception(f"Error renaming ToC file {filepath}.new.")
                try:
                    os.remove(f"{filepath}.new")
                except FileNotFoundError:
                    pass
                return create_error_response("Error: renaming file failed while saving data to disk.", 500)

            # Get author and construct git commit message
            author_email = str(identity)
            author = f"{author_email.split('@')[0]} <{author_email}>"
            message = f"ToC {filename} update by {author_email}"

            # git commit (and possibly push) file
            commit_result = git_commit_and_push_file(project, author, message, filepath)
            if commit_result:
                return create_success_response(f"Saved {filename}.",
                                               None,
                                               200 if toc_file_exists else 201)
            else:
                return create_error_response("Error: git commit failed. Possible configuration fault or git conflict.", 500)

    else:
        return create_error_response("Error: invalid request method. Only GET and PUT are allowed.")


@file_tools.route("/<project>/collection-toc-update-items/<collection_id>/<language>", methods=["POST"])
@file_tools.route("/<project>/collection-toc-update-items/<collection_id>", methods=["POST"])
@project_permission_required
def get_collection_toc_updated_from_db(project, collection_id, language=None):
    """
    Get an existing table of contents for the specified collection in the
    specified language (optional) and project, with publication names,
    dates, and languages updated from the database.

    Note: This endpoint does not modify the table of contents file in the
    project repository – it simply loads the table of contents on the
    server, updates it with the latest publication data in the database,
    and returns it as a JSON object. The structure of the table of
    contents is not altered.

    URL Path Parameters:

    - project (str): Project name.
    - collection_id (int): The id of the collection.
    - language (str or None, optional): The language of the table of
      contents.

    POST Data Parameters in JSON Format:

    - update (array, required): an array of strings with the names of the
      fields to update. Valid field names are "text", "date", and
      "language.

    Returns:

    - A tuple containing a Flask Response object with JSON data and an
      HTTP status code. The JSON response has the following structure:

        {
            "success": bool,
            "message": str,
            "data": object
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, an object with the updated table of contents,
      `null` on error.

    Status Codes:

    - 200 - OK: Successfully retrieved the updated table of contents.
    - 400 - Bad Request: Invalid request data.
    - 403 - Forbidden: Permission denied trying to read the table of
            contents file.
    - 404 - Not Found: The table of contens file was not found on the
            server.
    - 415 - Unsupported Media Type: The JSON data of the table of contents
            file on the server is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Validate project config
    config = get_project_config(project)
    if config is None:
        return create_error_response("Error: project config not found on the server.", 500)

    config_ok = check_project_config(project)
    if not config_ok[0]:
        logger.warning(f"Project '{project}' has an incomplete config: {config_ok[1]}")
        return create_error_response(f"Error: {config_ok[1]}", 500)

    # Validate collection_id
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    # Validate language
    if language is not None and not is_valid_language(language):
        return create_error_response("Validation error: 'language' is not among valid language codes.")

    # Validate request data
    payload = request.get_json()
    update_fields = payload.get("update", []) if payload else []
    valid_fields = ["text", "date", "language"]
    if not update_fields or not any(f in valid_fields for f in update_fields):
        return create_error_response("Request must include a valid update list with one or more of 'date', 'text', and 'language'.")

    filename = f"{collection_id}_{language}.json" if language else f"{collection_id}.json"
    filepath = safe_join(config["file_root"], "toc", filename)

    if filepath is None:
        return create_error_response("Error: invalid table of contents file path.")

    # Resolve the real, absolute path
    filepath = os.path.realpath(filepath)

    try:
        # Check if the file exists
        if not os.path.isfile(filepath):
            return create_error_response(f"Error: the table of contents file {filename} was not found on the server.", 404)

        # Read file contents and parse json into a dict
        with open(filepath, "r", encoding="utf-8-sig") as json_file:
            toc = json.load(json_file)

    except FileNotFoundError:
        logger.exception(f"File not found error when trying to read ToC-file at {filepath}.")
        return create_error_response(f"Error: {filename} not found.", 404)
    except PermissionError:
        logger.exception(f"Permission denied error when trying to read ToC-file at {filepath}.")
        return create_error_response(f"Error: permission denied when trying to read {filename}.", 403)
    except json.JSONDecodeError:
        logger.exception(f"Invalid JSON in ToC file at {filepath}.")
        return create_error_response(f"Error: file {filename} contains invalid JSON.", 415)
    except Exception:
        logger.exception(f"Error accessing file at {filepath}.")
        return create_error_response(f"Error accessing file at {filepath}.", 500)

    if not toc:
        return create_error_response(f"The table of contents file {filename} is empty.")

    # Fetch publication data
    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    try:
        with db_engine.connect() as connection:
            # Check for publication_collection existence in project
            select_coll_stmt = (
                select(collection_table.c.id)
                .where(collection_table.c.id == collection_id)
                .where(collection_table.c.project_id == project_id)
                .where(collection_table.c.deleted < 1)
            )
            result = connection.execute(select_coll_stmt).first()

            if not result:
                return create_error_response("Validation error: could not find publication collection, either 'project' or 'collection_id' is invalid.")

            # Proceed to selecting the publications
            select_pub_stmt = (
                select(
                    publication_table.c.id,
                    publication_table.c.name,
                    publication_table.c.original_publication_date,
                    publication_table.c.language
                )
                .where(publication_table.c.publication_collection_id == collection_id)
                .where(publication_table.c.deleted < 1)
            )
            publication_rows = connection.execute(select_pub_stmt).mappings().all()

    except Exception:
        logger.exception("Exception retrieving publications data for collection ToC update.")
        return create_error_response("Unexpected error: failed to retrieve publications data.", 500)

    if len(publication_rows) == 0:
        return create_error_response(f"No publications in collection with ID '{collection_id}'.")

    # Build update map
    update_map = {}
    for row in publication_rows:
        item_id = f"{collection_id}_{row['id']}"
        entry = {}

        if "text" in update_fields:
            entry["text"] = row["name"] or ""

        if "date" in update_fields:
            entry["date"] = row["original_publication_date"] or ""

        if "language" in update_fields:
            entry["language"] = row["language"] or ""

        if entry:
            update_map[item_id] = entry

    # Update fields in the collection ToC (in-place)
    updated_item_count = apply_updates_to_collection_toc(toc, update_map)

    resp_message = f"Updated {updated_item_count} items in the table of contents with publication data from the database. The changes are unsaved."

    if updated_item_count == 0:
        resp_message = "Publication data already up to date, no changes made to the table of contents."

    # Respond with the updated ToC, don't write it to disk
    return create_success_response(resp_message, toc)
