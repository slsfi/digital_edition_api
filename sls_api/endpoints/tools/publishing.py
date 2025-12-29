import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt
from sqlalchemy import select
from datetime import datetime

from sls_api.endpoints.generics import cms_required, db_engine, get_project_id_from_name, get_table, \
    int_or_none, validate_project_name, validate_int, create_error_response, \
    create_success_response, update_publication_related_table, handle_deleted_flag
from sls_api.exceptions import CascadeUpdateError


publishing_tools = Blueprint("publishing_tools", __name__)
logger = logging.getLogger("sls_api.tools.publishing")


@publishing_tools.route("/projects/list/")
@cms_required()
def list_user_projects():
    """
    List all (non-deleted) projects the current user has access to.

    Returns:

    - A tuple containing a Flask Response object with JSON data and an
      HTTP status code. The JSON response has the following structure:

        {
            "success": bool,
            "message": str,
            "data": array of objects or null
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, an array of project objects; `null` on error.

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # projects.",
            "data": [
                {
                    "id": 1,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "deleted": 0,
                    "published": 1,
                    "name": "project_name"
                },
                ...
            ]
        }

    Example Error Response (HTTP 404):

        {
            "success": false,
            "message": "Permissions error: user lacks access to any project.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the projects are returned.
    - 403 - Forbidden: The user doesn't have project permissions.
    - 404 - Not Found: The user doesn't have access to any (non-deleted)
            project.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Get claims of current JWT user along with projects the user
    # has permissions to.
    claims = get_jwt()

    if "projects" not in claims:
        return create_error_response("Permissions error: user lacks project permissions.", 403)

    if not claims["projects"]:
        return create_error_response("Permissions error: user lacks access to any project.", 404)

    user_projects = [str(project) for project in claims["projects"]]
    project_table = get_table("project")

    try:
        with db_engine.connect() as connection:
            # Get projects from the database and filter by
            # non-deleted and names of projects the user has access to.
            stmt = (
                select(project_table)
                .where(project_table.c.deleted < 1)
                .where(project_table.c.name.in_(user_projects))
                .order_by(project_table.c.name)
            )
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} projects.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving user projects.")
        return create_error_response("Unexpected error: failed to retrieve user projects.", 500)


@publishing_tools.route("/projects/new/", methods=["POST"])
@cms_required()
def add_new_project():
    """
    Create a new project.
    TODO: grant the current user permission to the new project.

    POST Data Parameters in JSON Format:

    - name (str, required): The name/title of the new project. The name
      can only contain lowercase letters (a-z), digits (0-9) and
      underscores (_), and must be between 3 and 32 characters long
      (inclusive). The project name must be unique.
    - published (int): The published status of the project.
      Must be an integer with value 0, 1 or 2. Defaults to 1.

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
    - `data`: On success, an object containing the inserted project data;
      `null` on error.

    Example Request:

        POST /projects/new/
        Body:
        {
            "name": "My New Project"
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Project created.",
            "data": {
                "id": 123,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": null,
                "deleted": 0,
                "published": 1,
                "name": "My New Project"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'name' required.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The project was inserted successfully.
    - 400 - Bad Request: No data was provided in the request,
            or required fields are missing.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Validate request data and construct dict with insert values
    values = {}

    # Validate project name
    name = request_data.get("name")
    if name is None:
        return create_error_response("Validation error: 'name' required.")

    name = str(name)
    is_valid_name, name_error_msg = validate_project_name(name)
    if not is_valid_name:
        return create_error_response(f"Validation error: {name_error_msg}")

    values["name"] = name

    published = request_data.get("published")
    if published is not None:
        if not validate_int(published, 0, 2):
            return create_error_response("Validation error: 'published' must be either 0, 1 or 2.")
        values["published"] = published
    else:
        values["published"] = 1

    project_table = get_table("project")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Check for existing project with the same name
                select_stmt = (
                    select(project_table.c.id)
                    .where(project_table.c.name == name)
                )
                result = connection.execute(select_stmt).first()

                if result:
                    return create_error_response("Validation error: a project with the provided name already exists.")

                # Proceed to insert the new project
                insert_stmt = (
                    project_table.insert()
                    .values(**values)
                    .returning(*project_table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(insert_stmt).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Project created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new project.")
        return create_error_response("Unexpected error: failed to create new project.", 500)


@publishing_tools.route("/projects/<project_id>/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_project(project_id):
    """
    Edit fields of the specified project.

    URL Path Parameters:

    - project_id (int, required): The ID of the project to edit.

    POST Data Parameters in JSON Format (at least one required):

    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
      Setting `deleted` value to 1 will force `published` value to 0.
    - published (int): The publication status of the project.
      Must be an integer with value 0, 1 or 2.

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
    - `data`: On success, an object containing the updated project data;
      `null` on error.

    Example Request:

        POST /projects/123/edit/
        Body:
        {
            "published": 2
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Project updated.",
            "data": {
                "id": 123,
                "date_created": "2023-01-01T10:00:00",
                "date_modified": "2023-10-17T12:34:56",
                "deleted": 0,
                "published": 2,
                "name": "projectname"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The project was successfully updated.
    - 400 - Bad Request: Invalid `project_id`, field values or no data provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Convert project_id to integer and verify
    project_id = int_or_none(project_id)
    if not project_id or project_id < 1:
        return create_error_response("Validation error: 'project_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List allowed fields in POST data
    fields = ["deleted", "published"]

    # Verify that POST data contains at least one valid field
    if all(field not in request_data for field in fields):
        return create_error_response("Validation error: either 'deleted' or 'published' required.")

    # Start building values dictionary for update statement
    values = {}

    # Loop over all fields and validate them
    for field in fields:
        if field in request_data:
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the field_names list for the query construction
            values[field] = request_data[field]

    if values:
        values["date_modified"] = datetime.now()
        # If "deleted" set to 1, force "published" to 0
        if values.get("deleted"):
            values["published"] = 0

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                project_table = get_table("project")
                stmt = (
                    project_table.update()
                    .where(project_table.c.id == project_id)
                    .values(**values)
                    .returning(*project_table.c)  # Return the updated row
                )
                updated_row = connection.execute(stmt).first()

                if updated_row is None:
                    # No row was returned; project_id invalid
                    return create_error_response("Update failed: no project with the provided 'project_id' found.")

                return create_success_response(
                    message="Project updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating project.")
        return create_error_response("Unexpected error: failed to update project.", 500)


@publishing_tools.route("/<project>/publication_collection/<collection_id>/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_publication_collection(project, collection_id):
    """
    Edit a publication collection in the specified project by updating
    its fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - collection_id (str): The id of the publication collection to be updated.
      Must be a valid positive integer.

    POST Data Parameters in JSON Format (at least one required):

    - name (str or null): The name/title of the publication collection.
    - published (int): The publication status. Must be an integer with
      value 0, 1 or 2.
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
      Setting `deleted` value to 1 will force `published` value to 0.
    - cascade_deleted (bool): If `true`, all publications in the
      collection including comments, manuscripts and versions linked to
      the publications will be marked with the same 'deleted' value as
      this publication collection. Also titles and introductions linked
      to the collection will be affected. If the value of 'deleted' is 1,
      the 'published' value will be set to 0 also for these. Defaults
      to `false`.
    - cascade_published (bool): If `true`, all publications in the
      collection including comments, manuscripts and versions linked to
      the publications will be marked with the same 'published' value as
      this publication collection. Also titles and introductions linked
      to the collection will be affected. Defaults to `false`.

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
    - `data`: On success, an object containing the updated publication
      collection data; `null` on error.

    Example Request:

        POST /projectname/publication_collection/456/edit/
        Body:
        {
            "name": "Updated Collection Name",
            "published": 2
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Publication collection updated.",
            "data": {
                "id": 456,
                "publication_collection_introduction_id": null,
                "publication_collection_title_id": null,
                "project_id": 4,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": "2023-08-14T14:29:02",
                "date_published_externally": null,
                "deleted": 0,
                "published": 2,
                "name": "Updated Collection Name",
                "legacy_id": null,
                "name_translation_id": 4297
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'published' must be either 0, 1 or 2.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The publication collection was updated successfully.
    - 400 - Bad Request: Invalid collection_id, invalid field values,
            or no valid fields provided for the update.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project is valid
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that collection_id is an integer
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["name", "published", "deleted"]

    # Start building values dictionary for update statement
    values = {}

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Validate integer field values and ensure all other
            # fields are strings or None
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the values list for the query construction
            values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()
    # If "deleted" set to 1, force "published" to 0
    if values.get("deleted"):
        values["published"] = 0

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                collection_table = get_table("publication_collection")
                upd_stmt = (
                    collection_table.update()
                    .where(collection_table.c.id == collection_id)
                    .where(collection_table.c.project_id == project_id)
                    .values(**values)
                    .returning(*collection_table.c)  # Return the updated row
                )
                updated_row = connection.execute(upd_stmt).first()

                if updated_row is None:
                    return create_error_response("Update failed: no publication collection with the provided 'collection_id' and 'project' found.")

                updated_row_dict = updated_row._asdict()

                # Check if the "deleted" or "published" status should be cascaded
                # to publications and their comments, manuscripts and versions
                if (
                    ("deleted" in values and request_data.get("cascade_deleted"))
                    or ("published" in values and request_data.get("cascade_published"))
                ):
                    casc_values = {"date_modified": values["date_modified"]}
                    for field in ["published", "deleted"]:
                        if field in values and request_data.get(f"cascade_{field}"):
                            casc_values[field] = values[field]

                    # Force 'published' value to 0 if 'deleted' set to 1
                    casc_values = handle_deleted_flag(casc_values)

                    # Update collection introduction and title
                    for text_type in ["collection_introduction", "collection_title"]:
                        upd_id = updated_row_dict.get(f"publication_{text_type}_id")
                        if upd_id is None:
                            continue

                        casc_upd_result = update_publication_related_table(
                            connection, text_type, upd_id, casc_values
                        )
                        if casc_upd_result is None:
                            raise CascadeUpdateError(f"failed to update 'deleted' or 'published' field for {text_type} linked to the collection.")

                    # Get all non-deleted publications in the collection
                    publication_table = get_table("publication")
                    pub_stmt = (
                        select(
                            publication_table.c.id,
                            publication_table.c.publication_comment_id
                        )
                        .where(publication_table.c.deleted < 1)
                        .where(publication_table.c.publication_collection_id == collection_id)
                    )
                    pub_rows = connection.execute(pub_stmt).fetchall()

                    # Update each publication
                    for pub_data in pub_rows:
                        # Update the "deleted" and/or "published" value of
                        # the publication and any comment, manuscript and
                        # version linked to the publication.
                        for text_type in ["publication", "comment", "manuscript", "version"]:
                            upd_id = (pub_data.publication_comment_id
                                      if text_type == "comment"
                                      else pub_data.id)
                            if upd_id is None:
                                continue

                            casc_upd_result = update_publication_related_table(
                                connection, text_type, upd_id, casc_values
                            )
                            if casc_upd_result is None:
                                raise CascadeUpdateError(f"failed to update 'deleted' or 'published' field for {text_type} linked to publication in the collection.")

                return create_success_response(
                    message="Publication collection updated.",
                    data=updated_row_dict
                )

    except CascadeUpdateError as ce:
        logger.error(f"Error updating publication collection: {ce.message}")
        return create_error_response(f"Unexpected error: {ce.message}", 500)

    except Exception:
        logger.exception("Exception updating publication collection.")
        return create_error_response("Unexpected error: failed to update publication collection.", 500)


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/")
@cms_required()
def get_intro(project, collection_id):
    collections = get_table("publication_collection")
    introductions = get_table("publication_collection_introduction")
    query = select(collections.c.publication_collection_introduction_id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query)
    if result.fetchone() is None:
        result.close()
        return jsonify("No such publication collection exists."), 404

    query = select(introductions).where(introductions.c.id == int(result[collections.c.publication_collection_introduction_id]))

    row = connection.execute(query).fetchone()
    if row is not None:
        row = row._asdict()
    connection.close()
    return jsonify(row)


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_intro(project, collection_id):
    """
    Takes "filename" and/or "published" as JSON data
    Returns "msg" and "introduction_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    collections = get_table("publication_collection")
    introductions = get_table("publication_collection_introduction")
    connection = db_engine.connect()
    with connection.begin():
        query = select(collections.c.publication_collection_introduction_id).where(collections.c.id == int_or_none(collection_id))
        result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        intro_id = int(result[0])
        with connection.begin():
            update = introductions.update().where(introductions.c.id == intro_id).values(**values)
            connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated publication collection introduction {} with values {}".format(intro_id, str(values)),
            "introduction_id": intro_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/")
@cms_required()
def get_title(project, collection_id):
    collections = get_table("publication_collection")
    titles = get_table("publication_collection_title")
    query = select(collections.c.publication_collection_title_id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    query = select(titles).where(titles.c.id == int(result[collections.c.publication_collection_title_id]))

    row = connection.execute(query).fetchone()
    if row is not None:
        row = row._asdict()
    connection.close()
    return jsonify(row)


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_title(project, collection_id):
    """
    Takes "filename" and/or "published" as JSON data
    Returns "msg" and "title_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    collections = get_table("publication_collection")
    titles = get_table("publication_collection_title")
    connection = db_engine.connect()
    with connection.begin():
        query = select(collections.c.publication_collection_title_id).where(collections.c.id == int_or_none(collection_id))
        result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        title_id = int(result[0])
        with connection.begin():
            update = titles.update().where(titles.c.id == title_id).values(**values)
            connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated publication collection title {} with values {}".format(title_id, str(values)),
            "title_id": title_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication/<publication_id>/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_publication(project, publication_id):
    """
    Edit a publication in the specified project by updating its fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - publication_id (str): The ID of the publication to be updated.
      Must be a valid integer.

    POST Data Parameters in JSON Format (at least one required):

    - publication_collection_id (int): The ID of the publication collection.
      Must be a positive integer.
    - publication_comment_id (int): The ID of the publication comment.
      Must be a positive integer.
    - name (str): The name/title of the publication.
    - original_filename (str): Path to the publication’s XML-file in the
      project GitHub repository.
    - original_publication_date (str): The original publication date or
      year (formatted as a string).
    - published (int): The publication status. Must be an integer with
      value 0, 1 or 2.
    - language (str): The language code of the main language of the
      publication (ISO 639-1).
    - genre (str): The genre of the publication.
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
      Setting `deleted` value to 1 will force `published` value to 0.
    - cascade_deleted (bool): If `true`, all comments, manuscripts and
      versions linked to the publication will be marked with the same
      'deleted' value as the publication. If the value of 'deleted' is 1,
      the 'published' value will be set to 0 also for these. Defaults
      to `false`.
    - cascade_published (bool): If `true`, all comments, manuscripts and
      versions linked to the publication will be marked with the same
      'published' value as the publication. Defaults to `false`.

    Additionally, all POST data parameter values can be set to null,
    except 'deleted'.

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
    - `data`: On success, an object containing the updated publication
      data; `null` on error.

    Example Request:

        POST /projectname/publication/123/edit/
        Body:
        {
            "name": "New Publication Name",
            "published": 1,
            "language": "en"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Publication updated.",
            "data": {
                "id": 123,
                "publication_collection_id": 585,
                "publication_comment_id": 5487,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "/path/to/file.xml",
                "name": "New Publication Name",
                "genre": "non-fiction",
                "publication_group_id": null,
                "original_publication_date": "1854",
                "zts_id": null,
                "language": "en"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'published' must be either 0, 1 or 2.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The publication was updated successfully.
    - 400 - Bad Request: Invalid publication_id, invalid field values,
            or no valid fields provided for the update.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that publication_id is an integer
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["publication_collection_id",
              "publication_comment_id",
              "name",
              "original_filename",
              "original_publication_date",
              "published",
              "language",
              "genre",
              "deleted"]

    # Start building values dictionary for update statement
    values = {}

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                # Validate integer field values and ensure all other
                # fields are strings
                if field in ["publication_collection_id", "publication_comment_id"]:
                    if not validate_int(request_data[field], 1):
                        return create_error_response(f"Validation error: '{field}' must be a positive integer.")
                elif field == "published":
                    if not validate_int(request_data[field], 0, 2):
                        return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
                elif field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
                else:
                    # Convert remaining fields to string
                    request_data[field] = str(request_data[field])

                # Add the field to the values list for the query construction
                values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()
    # If "deleted" set to 1, force "published" to 0
    values = handle_deleted_flag(values)

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Verify publication_id and that the publication is
                # in the project
                collection_table = get_table("publication_collection")
                publication_table = get_table("publication")
                stmt = (
                    select(publication_table.c.id)
                    .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                    .where(collection_table.c.project_id == project_id)
                    .where(publication_table.c.id == publication_id)
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return create_error_response("Validation error: could not find publication, either 'project' or 'publication_id' is invalid.")

                # Execute the update statement
                upd_stmt = (
                    publication_table.update()
                    .where(publication_table.c.id == publication_id)
                    .values(**values)
                    .returning(*publication_table.c)  # Return the updated row
                )
                updated_row = connection.execute(upd_stmt).first()

                if updated_row is None:
                    return create_error_response("Update failed: no publication with the provided 'publication_id' found.")

                # Check if the "deleted" or "published" status should be cascaded
                # to comments, manuscripts and versions linked to the publication
                if (
                    ("deleted" in values and request_data.get("cascade_deleted"))
                    or ("published" in values and request_data.get("cascade_published"))
                ):
                    casc_values = {"date_modified": values["date_modified"]}
                    for field in ["published", "deleted"]:
                        if field in values and request_data.get(f"cascade_{field}"):
                            casc_values[field] = values[field]

                    # Force 'published' value to 0 if 'deleted' set to 1
                    casc_values = handle_deleted_flag(casc_values)

                    # Update the "deleted" and/or "published" value of any
                    # comment, manuscript and version linked to the
                    # publication.
                    for text_type in ["comment", "manuscript", "version"]:
                        upd_id = (updated_row.publication_comment_id
                                  if text_type == "comment"
                                  else publication_id)
                        if upd_id is None:
                            continue

                        casc_upd_result = update_publication_related_table(
                            connection, text_type, upd_id, casc_values
                        )
                        if casc_upd_result is None:
                            raise CascadeUpdateError(f"failed to update 'deleted' or 'published' field for {text_type} linked to the publication.")

                return create_success_response(
                    message="Publication updated.",
                    data=updated_row._asdict()
                )

    except CascadeUpdateError as ce:
        logger.error(f"Error updating publication: {ce.message}")
        return create_error_response(f"Unexpected error: {ce.message}", 500)

    except Exception:
        logger.exception("Exception updating publication.")
        return create_error_response("Unexpected error: failed to update publication.", 500)


@publishing_tools.route("/<project>/publication/<publication_id>/comment/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_comment(project, publication_id):
    """
    Edit a comment of the specified publication in the given project by
    updating its fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - publication_id (str): The ID of the publication whose comment is
      to be updated. Must be a valid integer.

    POST Data Parameters in JSON Format (at least one required):

    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
      Setting `deleted` value to 1 will force `published` value to 0.
    - published (int): The publication status of the comment. Must be an
      integer with value 0, 1 or 2.
    - original_filename (str): Path to the comment’s XML-file in the
      project GitHub repository.

    Returns:

    - A tuple containing a Flask Response object with JSON data and an
      HTTP status code. The JSON response has the following structure:

        {
            "success": bool,
            "message": str,
            "data":  object or null
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, an object containing the updated comment data;
      `null` on error.

    Example Request:

        POST /projectname/publication/123/comment/edit/
        Body:
        {
            "published": 1,
            "deleted": 0,
            "original_filename": "path/to/updated_comment_file.xml"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Publication comment updated.",
            "data": {
                "id": 456,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": "2023-08-14T14:29:02",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "path/to/updated_comment_file.xml"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The comment was updated successfully.
    - 400 - Bad Request: Invalid publication_id, invalid field values,
            no data provided, or no valid fields provided to update.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that publication_id is an integer
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["deleted", "published", "original_filename"]

    # Start building values dictionary for update statement
    values = {}

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Validate integer field values and ensure all other
            # fields are strings or None
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the values list for the query construction
            values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()
    # If "deleted" set to 1, force "published" to 0
    if values.get("deleted"):
        values["published"] = 0

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                collection_table = get_table("publication_collection")
                publication_table = get_table("publication")
                comment_table = get_table("publication_comment")

                # Get publicatiom_comment_id
                stmt = (
                    select(publication_table.c.publication_comment_id)
                    .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                    .where(collection_table.c.project_id == project_id)
                    .where(publication_table.c.id == publication_id)
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return create_error_response("Validation error: could not find publication, either 'project' or 'publication_id' is invalid.")

                com_id = getattr(result, "publication_comment_id", None)

                if com_id is None:
                    return create_error_response("Update failed: ID of comment linked to the publication is NULL.", 500)

                # Execute the update statement
                upd_stmt = (
                    comment_table.update()
                    .where(comment_table.c.id == com_id)
                    .values(**values)
                    .returning(*comment_table.c)  # Return the updated row
                )
                updated_row = connection.execute(upd_stmt).first()

                if updated_row is None:
                    return create_error_response("Update failed: no comment linked to the publication with the provided 'publication_id' found.")

                return create_success_response(
                    message="Publication comment updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating publication comment.")
        return create_error_response("Unexpected error: failed to update publication comment.", 500)


@publishing_tools.route("/<project>/manuscripts/<manuscript_id>/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_manuscript(project, manuscript_id):
    """
    Edit a manuscript of the specified project by updating its fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - manuscript_id (str): The ID of the manuscript to be updated.
      Must be a valid integer.

    POST Data Parameters in JSON Format (at least one required):

    - publication_id (int): The ID of the publication linked to the
      manuscript. Must be a positive integer or null.
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
      Setting `deleted` value to 1 will force `published` value to 0.
    - published (int): The publication status of the manuscript.
      Must be an integer with value 0, 1 or 2.
    - original_filename (str): Path to the manuscript’s XML file
      in the project repository.
    - name (str): The name of the manuscript.
    - section_id (int): The ID of the section/chapter of the manuscript.
      Must be a non-negative integer.
    - sort_order (int): The sorting order of the manuscript. Must be a
      non-negative integer.
    - language (str): The language (ISO 639-1) code of the main language
      of the manuscript.

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
    - `data`: On success, an object containing the updated manuscript
      data; `null` on error.

    Example Request:

        POST /projectname/manuscripts/123/edit/
        Body:
        {
            "published": 1,
            "deleted": 0,
            "original_filename": "path/to/updated_manuscript.xml",
            "name": "Updated Manuscript Title"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Publication manuscript updated.",
            "data": {
                "id": 123,
                "publication_id": 456,
                "date_created": "2024-08-02T05:13:49",
                "date_modified": "2024-10-17T14:23:01",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "path/to/updated_manuscript.xml",
                "name": "Updated Manuscript Title",
                "type": null,
                "section_id": 2,
                "sort_order": 3,
                "language": "en"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'manuscript_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The manuscript was updated successfully.
    - 400 - Bad Request: Invalid manuscript_id, invalid field values, no
            data provided, or no valid fields provided to update.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that manuscript_id is an integer
    manuscript_id = int_or_none(manuscript_id)
    if not manuscript_id or manuscript_id < 1:
        return create_error_response("Validation error: 'manuscript_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["publication_id",
              "deleted",
              "published",
              "original_filename",
              "name",
              "section_id",
              "sort_order",
              "language"]

    # Start building values dictionary for update statement
    values = {}

    # Loop over all fields and validate them
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                # Validate integer field values and ensure all other
                # fields are strings
                if field == "publication_id":
                    if not validate_int(request_data[field], 1):
                        return create_error_response(f"Validation error: '{field}' must be either a positive integer or null.")
                elif field == "published":
                    if not validate_int(request_data[field], 0, 2):
                        return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
                elif field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
                elif field in ["section_id", "sort_order"]:
                    if not validate_int(request_data[field], 0):
                        return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
                else:
                    # Convert remaining fields to string
                    request_data[field] = str(request_data[field])

                # Add the field to the values list for the query construction
                values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()
    # If "deleted" set to 1, force "published" to 0
    if values.get("deleted"):
        values["published"] = 0

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # We are not verifying that the manuscript belongs to
                # a publication in the specified project.

                # Verify that publication_id is valid if it is updated
                if (
                    "publication_id" in values
                    and values["publication_id"] is not None
                ):
                    publication_table = get_table("publication")
                    stmt = (
                        select(publication_table.c.id)
                        .where(publication_table.c.id == values["publication_id"])
                    )
                    result = connection.execute(stmt).first()

                    if result is None:
                        return create_error_response("Validation error: could not find publication with the provided 'publication_id'.")

                # Proceed to updating the manuscript
                manuscript_table = get_table("publication_manuscript")
                upd_stmt = (
                    manuscript_table.update()
                    .where(manuscript_table.c.id == manuscript_id)
                    .values(**values)
                    .returning(*manuscript_table.c)  # Return the updated row
                )
                updated_row = connection.execute(upd_stmt).first()

                if updated_row is None:
                    return create_error_response("Update failed: no publication manuscript with the provided 'manuscript_id' found.")

                return create_success_response(
                    message="Publication manuscript updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating publication manuscript.")
        return create_error_response("Unexpected error: failed to update publication manuscript.", 500)


@publishing_tools.route("/<project>/versions/<version_id>/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_version(project, version_id):
    """
    Edit a publication version of the specified project by updating its fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - version_id (str): The ID of the publication version to be updated.
      Must be a valid integer.

    POST Data Parameters in JSON Format (at least one required):

    - publication_id (int): The ID of the publication linked to the version.
      Must be a positive integer or null.
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
      Setting `deleted` value to 1 will force `published` value to 0.
    - published (int): The publication status of the version. Must be an
      integer with value 0, 1 or 2.
    - original_filename (str): Path to the version’s XML file in the
      project repository.
    - name (str): The name of the version.
    - type (int): The type of the version. Must be a non-negative integer.
      1 denotes a base text and 2 some other variant.
    - section_id (int): The ID of the section/chapter of the version.
      Must be a non-negative integer.
    - sort_order (int): The sorting order of the version. Must be a
      non-negative integer.

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
    - `data`: On success, an object containing the updated publication
      version data; `null` on error.

    Example Request:

        POST /projectname/versions/123/edit/
        Body:
        {
            "published": 1,
            "deleted": 0,
            "original_filename": "path/to/updated_version.xml",
            "name": "Updated Version Title"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Publication version updated.",
            "data": {
                "id": 123,
                "publication_id": 456,
                "date_created": "2024-08-02T05:13:49",
                "date_modified": "2024-10-17T14:23:01",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "path/to/updated_version.xml",
                "name": "Updated Version Title",
                "type": 1,
                "section_id": 2,
                "sort_order": 3
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The version was updated successfully.
    - 400 - Bad Request: Invalid version_id, invalid field values, no data
            provided, or no valid fields provided to update.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that version_id is an integer
    version_id = int_or_none(version_id)
    if not version_id or version_id < 1:
        return create_error_response("Validation error: 'version_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["publication_id",
              "deleted",
              "published",
              "original_filename",
              "name",
              "type",
              "section_id",
              "sort_order"]

    # Start building values dictionary for update statement
    values = {}

    # Loop over all fields and validate them
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                # Validate integer field values and ensure all other
                # fields are strings
                if field == "publication_id":
                    if not validate_int(request_data[field], 1):
                        return create_error_response(f"Validation error: '{field}' must be either a positive integer or null.")
                elif field == "published":
                    if not validate_int(request_data[field], 0, 2):
                        return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
                elif field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
                elif field in ["type", "section_id", "sort_order"]:
                    if not validate_int(request_data[field], 0):
                        return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
                else:
                    # Convert remaining fields to string
                    request_data[field] = str(request_data[field])

                # Add the field to the values list for the query construction
                values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()
    # If "deleted" set to 1, force "published" to 0
    if values.get("deleted"):
        values["published"] = 0

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # We are not verifying that the version belongs to
                # a publication in the specified project.

                # Verify that publication_id is valid if it is updated
                if (
                    "publication_id" in values
                    and values["publication_id"] is not None
                ):
                    publication_table = get_table("publication")
                    stmt = (
                        select(publication_table.c.id)
                        .where(publication_table.c.id == values["publication_id"])
                    )
                    result = connection.execute(stmt).first()

                    if result is None:
                        return create_error_response("Validation error: could not find publication with the provided 'publication_id'.")

                # Proceed to updating the version
                version_table = get_table("publication_version")
                upd_stmt = (
                    version_table.update()
                    .where(version_table.c.id == version_id)
                    .values(**values)
                    .returning(*version_table.c)  # Return the updated row
                )
                updated_row = connection.execute(upd_stmt).first()

                if updated_row is None:
                    return create_error_response("Update failed: no publication version with the provided 'version_id' found.")

                return create_success_response(
                    message="Publication version updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating publication version.")
        return create_error_response("Unexpected error: failed to update publication version.", 500)


@publishing_tools.route("/<project>/publication_collection/<collection_id>/info")
@cms_required()
def get_publication_collection_info(project, collection_id):
    """
    Returns published status for publication_collection and associated introduction and title objects
    Also returns the original_filename for the introduction and title objects
    """
    collections = get_table("publication_collection")
    intros = get_table("publication_collection_introduction")
    titles = get_table("publication_collection_title")

    query = select(collections).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    collection_result = connection.execute(query).fetchone()
    if collection_result is None:
        connection.close()
        return jsonify("No such publication collection exists"), 404
    else:
        collection_result = collection_result._asdict()

    intro_id = int_or_none(collection_result["publication_collection_introduction_id"])
    title_id = int_or_none(collection_result["publication_collection_title_id"])
    intro_query = select(intros.c.published, intros.c.original_filename).where(intros.c.id == intro_id)
    title_query = select(titles.c.published, titles.c.original_filename).where(titles.c.id == title_id)

    intro_result = connection.execute(intro_query).fetchone()._asdict()
    title_result = connection.execute(title_query).fetchone()._asdict()

    connection.close()
    result = {
        "collection_id": int(collection_id),
        "collection_published": collection_result["published"],
        "intro_id": intro_id,
        "intro_published": None if intro_result is None else intro_result["published"],
        "intro_original_filename": None if intro_result is None else intro_result["original_filename"],
        "title_id": title_id,
        "title_published": None if title_result is None else title_result["published"],
        "title_original_filename": None if title_result is None else title_result["original_filename"]
    }
    return jsonify(result)
