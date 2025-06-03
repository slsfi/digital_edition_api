import logging
import os
from flask import Blueprint, request, Response
from flask_jwt_extended import jwt_required
from sqlalchemy import collate, select, text
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, \
    get_table, int_or_none, validate_int, project_permission_required, \
    create_error_response, create_success_response, get_project_config, \
    get_project_collation


publication_tools = Blueprint("publication_tools", __name__)
logger = logging.getLogger("sls_api.tools.publications")


@publication_tools.route("/<project>/publications/")
@publication_tools.route("/<project>/publications/<order_by>/<direction>/")
@jwt_required()
def get_publications(project, order_by="id", direction="asc"):
    """
    List all (non-deleted) publications for a given project, with optional
    sorting by publication table columns.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publications.
    - order_by (str, optional): The column by which to order the publications.
      For example "id" or "name". Defaults to "id".
    - direction (str, optional): The sort direction, valid values are `asc`
      (ascending, default) and `desc` (descending).

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
    - `data`: On success, an array of publication objects; `null` on error.

    Example Request:

        GET /projectname/publications/
        GET /projectname/publications/date_modified/desc/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publications.",
            "data": [
                {
                    "id": 1,
                    "publication_collection_id": 123,
                    "publication_comment_id": 5487,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "date_published_externally": null,
                    "deleted": 0,
                    "published": 1,
                    "legacy_id": null,
                    "published_by": null,
                    "original_filename": "/path/to/file.xml",
                    "name": "Publication Title",
                    "genre": "non-fiction",
                    "publication_group_id": null,
                    "original_publication_date": "1854",
                    "zts_id": null,
                    "language": "en"
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publications are returned.
    - 400 - Bad Request: The project name is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    # Verify order_by and direction
    if order_by not in publication_table.c:
        return create_error_response("Validation error: 'order_by' must be a valid column in the publication table.")

    if direction not in ["asc", "desc"]:
        return create_error_response("Validation error: 'direction' must be either 'asc' or 'desc'.")

    try:
        with db_engine.connect() as connection:
            # Left join collection table on publication table and
            # select only the columns from the publication table
            stmt = (
                select(*publication_table.c)
                .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                .where(collection_table.c.project_id == project_id)
                .where(publication_table.c.deleted < 1)
                .order_by(publication_table.c.publication_collection_id)
            )

            order_column = publication_table.c[order_by]

            # Apply collation if the column needs it
            if order_by == "name":
                collation_name = get_project_collation(project)
                order_column = collate(order_column, collation_name)

            # Apply ordering direction
            if direction == "asc":
                stmt = stmt.order_by(order_column.asc())
            else:
                stmt = stmt.order_by(order_column.desc())

            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publications.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publications.")
        return create_error_response("Unexpected error: failed to retrieve publications.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/")
@project_permission_required
def get_publication(project, publication_id):
    """
    Retrieve a single publication for a given project.

    URL Path Parameters:

    - project (str, required): The name of the project to which the
      publication belongs.
    - publication_id (int, required): The id of the publication to retrieve.

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
    - `data`: On success, an object containing the publication data; `null`
      on error.

    Example Request:

        GET /projectname/publication/123/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved 1 publication.",
            "data": {
                "id": 123,
                "publication_collection_id": 456,
                "publication_comment_id": 789,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "/path/to/file.xml",
                "name": "Publication Title",
                "genre": "fiction",
                "publication_group_id": null,
                "original_publication_date": "1854",
                "zts_id": null,
                "language": "en"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication is returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    try:
        with db_engine.connect() as connection:
            # Left join collection table on publication table and
            # select only the columns from the publication table
            # with matching publication_id and project_id
            statement = (
                select(*publication_table.c)
                .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                .where(collection_table.c.project_id == project_id)
                .where(publication_table.c.id == publication_id)
            )
            result = connection.execute(statement).first()

            if result is None:
                return create_error_response("Validation error: could not find publication, either 'project' or 'publication_id' is invalid.")

            return create_success_response(
                message="Retrieved 1 publication.",
                data=result._asdict()
            )

    except Exception:
        logger.exception("Exception retrieving publication.")
        return create_error_response("Unexpected error: failed to retrieve publication.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/versions/")
@jwt_required()
def get_publication_versions(project, publication_id):
    """
    List all (non-deleted) versions (i.e. variants) of the specified
    publication in a given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publication versions.
    - publication_id (int, required): The id of the publication to retrieve
      versions for. Must be a positive integer.

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
    - `data`: On success, an array of publication version objects; `null` on
      error.

    Example Request:

        GET /projectname/publication/456/versions/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication versions.",
            "data": [
                {
                    "id": 1,
                    "publication_id": 456,
                    "date_created": "2023-07-12T09:23:45",
                    "date_modified": "2023-07-13T10:00:00",
                    "date_published_externally": null,
                    "deleted": 0,
                    "published": 1,
                    "legacy_id": null,
                    "published_by": null,
                    "original_filename": "path/to/file.xml",
                    "name": "Publication Title version 2",
                    "type": 1,
                    "section_id": 5,
                    "sort_order": 1
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication versions
            are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    publication_version = get_table("publication_version")

    try:
        with db_engine.connect() as connection:
            # We are simply retrieving matching rows based on
            # publication_id, not verifying that the publication
            # actually belongs to the project.
            stmt = (
                select(publication_version)
                .where(publication_version.c.publication_id == publication_id)
                .where(publication_version.c.deleted < 1)
                .order_by(publication_version.c.sort_order)
            )
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication versions.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication versions.")
        return create_error_response("Unexpected error: failed to retrieve publication versions.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/manuscripts/")
@jwt_required()
def get_publication_manuscripts(project, publication_id):
    """
    List all (non-deleted) manuscripts of the specified publication in
    a given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publication manuscripts.
    - publication_id (int, required): The id of the publication to retrieve
      manuscripts for. Must be a positive integer.

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
    - `data`: On success, an array of publication manuscript objects; `null`
      on error.

    Example Request:

        GET /projectname/publication/456/manuscripts/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication manuscripts.",
            "data": [
                {
                    "id": 1,
                    "publication_id": 456,
                    "date_created": "2023-07-12T09:23:45",
                    "date_modified": "2023-07-13T10:00:00",
                    "date_published_externally": null,
                    "deleted": 0,
                    "published": 1,
                    "legacy_id": null,
                    "published_by": null,
                    "original_filename": "path/to/file.xml",
                    "name": "Publication Title manuscript 1",
                    "type": 1,
                    "section_id": 5,
                    "sort_order": 1,
                    "language": "en"
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication manuscripts
            are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    publication_manuscript = get_table("publication_manuscript")

    try:
        with db_engine.connect() as connection:
            # We are simply retrieving matching rows based on
            # publication_id, not verifying that the publication
            # actually belongs to the project.
            stmt = (
                select(publication_manuscript)
                .where(publication_manuscript.c.publication_id == publication_id)
                .where(publication_manuscript.c.deleted < 1)
                .order_by(publication_manuscript.c.sort_order)
            )
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication manuscripts.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication manuscripts.")
        return create_error_response("Unexpected error: failed to retrieve publication manuscripts.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/tags/")
@jwt_required()
def get_publication_tags(project, publication_id):
    """
    List all (non-deleted) tags for the specified publication.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publication tags.
    - publication_id (int, required): The id of the publication to retrieve
      tags for. Must be a positive integer.

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
    - `data`: On success, an array of tag objects; `null` on error.

    Example Request:

        GET /projectname/publication/456/tags/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication tags.",
            "data": [
                {
                    "id": 1,
                    ...
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication tags are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    statement = """
        SELECT
            t.*, e_o.*
        FROM
            event_occurrence e_o
        JOIN
            event_connection e_c
            ON e_o.event_id = e_c.event_id
        JOIN
            tag t
            ON t.id = e_c.tag_id
        WHERE
            e_o.publication_id = :pub_id
            AND e_c.tag_id IS NOT NULL
            AND e_c.deleted < 1
            AND e_o.deleted < 1
            AND t.deleted < 1
    """

    try:
        with db_engine.connect() as connection:
            rows = connection.execute(
                text(statement),
                {"pub_id": publication_id}
            ).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication tags.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication tags.")
        return create_error_response("Unexpected error: failed to retrieve publication tags.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/facsimiles/")
@jwt_required()
def get_publication_facsimiles(project, publication_id):
    """
    List all (non-deleted) fascimiles for the specified publication in
    the given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      fascimiles.
    - publication_id (int, required): The id of the publication to retrieve
      fascimiles for. Must be a positive integer.

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
    - `data`: On success, an array of facsimile objects; `null` on error.

    Example Request:

        GET /projectname/publication/456/fascimiles/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication facsimiles.",
            "data": [
                {
                    "id": 123,
                    "publication_facsimile_collection_id": 5830,
                    "publication_id": 456,
                    "publication_manuscript_id": null,
                    "publication_version_id": null,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "deleted": 0,
                    "page_nr": 4,
                    "section_id": 1,
                    "priority": 1,
                    "type": 0,
                    "title": "Facsimile Collection Title",
                    "description": "Some details about the collection.",
                    "external_url": null
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication facsimiles
            are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    facs_table = get_table("publication_facsimile")
    facs_collection_table = get_table("publication_facsimile_collection")

    try:
        with db_engine.connect() as connection:
            stmt = (
                select(
                    facs_table,
                    facs_collection_table.c.title,
                    facs_collection_table.c.description,
                    facs_collection_table.c.external_url
                )
                .join(
                    facs_collection_table,
                    facs_table.c.publication_facsimile_collection_id == facs_collection_table.c.id
                )
                .where(facs_table.c.publication_id == publication_id)
                .where(facs_table.c.deleted < 1)
                .where(facs_collection_table.c.deleted < 1)
                .order_by(facs_table.c.priority)
            )
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication facsimiles.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication facsimiles.")
        return create_error_response("Unexpected error: failed to retrieve publication facsimiles.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/comments/")
@jwt_required()
def get_publication_comments(project, publication_id):
    """
    List all (non-deleted) comments of the specified publication
    in a given project. Since only one comment is allowed per publication,
    the list will have only one item (or none).

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publication comments.
    - publication_id (int, required): The id of the publication to retrieve
      comments for. Must be a positive integer.

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
    - `data`: On success, an array of publication comment objects; `null`
      on error.

    Example Request:

        GET /projectname/publication/456/comments/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication comments.",
            "data": [
                {
                    "id": 2582,
                    "publication_id": 456,
                    "date_created": "2023-07-12T09:23:45",
                    "date_modified": "2023-07-13T10:00:00",
                    "date_published_externally": null,
                    "deleted": 0,
                    "published": 1,
                    "legacy_id": null,
                    "published_by": null,
                    "original_filename": "path/to/comment_file.xml"
                }
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication comments
            are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    publication_table = get_table("publication")
    comment_table = get_table("publication_comment")

    try:
        with db_engine.connect() as connection:
            # We are simply retrieving matching rows based on
            # publication_id, not verifying that the publication
            # actually belongs to the project.

            # Left join publication table on publication_comment
            # table and filter on publication_id and non-deleted
            # comments. Publications can have only one comment,
            # so this should return only one row (or none).
            stmt = (
                select(*comment_table.c)
                .join(publication_table, comment_table.c.id == publication_table.c.publication_comment_id)
                .where(publication_table.c.id == publication_id)
                .where(comment_table.c.deleted < 1)
            )
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication comments.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication comments.")
        return create_error_response("Unexpected error: failed to retrieve publication comments.", 500)


@publication_tools.route("/<project>/publication/<publication_id>/link_text/", methods=["POST"])
@project_permission_required
def link_text_to_publication(project, publication_id):
    """
    Create a new comment, manuscript or version for the specified publication
    in the given project. Observe that publications can have only one comment.
    Attempting to create a new comment for a publication that already has one
    will fail.

    URL Path Parameters:

    - project (str): The name of the project.
    - publication_id (int): The ID of the publication to which the comment,
      manuscript or version will be linked.

    POST Data Parameters in JSON Format:

    - text_type (str, required): The type of text to create.
      Must be one of "comment", "manuscript" or "version".
    - original_filename (str, required): File path to the XML file of the
      text. Cannot be empty.

    Optional POST data parameters (depending on text_type):

    For "manuscript" and "version":

    - name (str, optional): The name or title of the text.
    - type (int, optional): A non-negative integer representing the type of
      the text. Defaults to 1 for "version" (1=base text, 2=other variant).
    - section_id (int, optional): A non-negative integer representing the
      section ID.
    - sort_order (int, optional): A non-negative integer indicating the
      sort order. Defaults to 1.

    For "manuscript" only:

    - language (str, optional): The language code (ISO 639-1) of the main
      language in the manuscript text.

    For all text types:

    - published (int, optional): The publication status. Must be an integer
      with value 0, 1 or 2. Defaults to 1.
    - published_by (str, optional): The name of the person who published
      the text.
    - legacy_id (str, optional): A legacy identifier for the text.

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
    - `data`: On success, an object containing the inserted data; `null`
      on error.

    Example Request:

        POST /projectname/publication/456/link_text/
        Body:
        {
            "text_type": "manuscript",
            "original_filename": "path/to/ms_file1.xml",
            "name": "Publication Title manuscript 1",
            "language": "en",
            "published": 1
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Publication manuscript created and linked to publication.",
            "data":  {
                "id": 284,
                "publication_id": 456,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": null,
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "path/to/ms_file1.xml",
                "name": "Publication Title manuscript 1",
                "type": null,
                "section_id": null,
                "sort_order": null,
                "language": "en"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'original_filename' and 'text_type' required. Valid values for 'text_type' are 'comment', 'manuscript' or 'version'.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The publication text type was created successfully.
    - 400 - Bad Request: Invalid project name, publication ID, field values,
            or no data provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List required and optional fields in POST data
    required_fields = ["text_type", "original_filename"]
    optional_fields = [
        "name",         # only manuscript and version
        "published",
        "published_by",
        "legacy_id",
        "type",         # only manuscript and version
        "section_id",   # only manuscript and version
        "sort_order",   # only manuscript and version
        "language"      # only manuscript
    ]

    text_type = request_data.get("text_type", None)

    # Check that required fields are in the request data,
    # that their values are non-empty
    # and that text_type is among valid values
    valid_text_types = ["comment", "manuscript", "version"]
    if (
        any(field not in request_data or not request_data[field] for field in required_fields)
        or text_type not in valid_text_types
    ):
        return create_error_response("Validation error: 'original_filename' and 'text_type' required. Valid values for 'text_type' are 'comment', 'manuscript' or 'version'.")

    # Start building values dictionary for insert statement
    values = {}

    # Loop over all fields and validate them
    for field in required_fields + optional_fields:
        if field in request_data:
            # Skip inapplicable fields
            if (
                field == "text_type"
                or (
                    text_type == "comment"
                    and field in ["name", "type", "section_id", "sort_order", "language"]
                )
                or (text_type == "version" and field == "language")
            ):
                continue

            # Validate integer field values and ensure all other fields are
            # strings or None
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
            elif field in ["type", "section_id", "sort_order"]:
                if not validate_int(request_data[field], 0):
                    return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the values list for the query construction
            values[field] = request_data[field]

    # Set published to default value 1 if not in provided values
    if "published" not in values:
        values["published"] = 1

    # For manuscript and version set publication_id and default values
    # for sort_order and type (version only)
    if text_type != "comment":
        values["publication_id"] = publication_id
        if "sort_order" not in values:
            values["sort_order"] = 1
        if text_type == "version" and "type" not in values:
            values["type"] = 1

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Verify publication_id and that the publication is
                # in the project
                collection_table = get_table("publication_collection")
                publication_table = get_table("publication")
                stmt = (
                    select(
                        publication_table.c.id,
                        publication_table.c.publication_comment_id
                    )
                    .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                    .where(collection_table.c.project_id == project_id)
                    .where(publication_table.c.id == publication_id)
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return create_error_response("Validation error: could not find publication, either 'project' or 'publication_id' is invalid.")

                # Since publications can have only one comment linked to them,
                # we need to check if the publication already has a comment.
                if (
                    text_type == "comment"
                    and getattr(result, "publication_comment_id", None) is not None
                ):
                    return create_error_response("Failed to add comment to publication: a comment is already linked to the publication.")

                table = get_table(f"publication_{text_type}")
                ins_stmt = (
                    table.insert()
                    .values(**values)
                    .returning(*table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(ins_stmt).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                if (
                    text_type == "comment"
                    and getattr(inserted_row, "id", None) is not None
                ):
                    # Update the publication with the comment id
                    upd_stmt = (
                        publication_table.update()
                        .where(publication_table.c.id == publication_id)
                        .values(publication_comment_id=inserted_row.id)
                    )
                    connection.execute(upd_stmt)

                return create_success_response(
                    message=f"Publication {text_type} created and linked to publication.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception(f"Exception creating new publication {text_type}.")
        return create_error_response(f"Unexpected error: failed to create new publication {text_type}.", 500)


@publication_tools.route("/<project>/verify-facsimile-file/<collection_id>/<file_nr>/<zoom_level>")
@project_permission_required
def verify_facsimile_file_exists(project, collection_id, file_nr, zoom_level):
    """
    Verify the existence of a single facsimile file or all files in a
    specific facsimile collection in the given project.

    URL Path Parameters:

    - project (str, required): The name of the project containing the
      facsimile collection.
    - collection_id (int, required): The ID of the facsimile collection.
      Must be a positive integer.
    - file_nr (int or str, required): The number of the facsimile file to
      verify. Must be either a positive integer to verify a single
      file, or the fixed string 'all' to verify all image files in the
      facsimile collection.
    - zoom_level (int, required): The zoom level of the facsimile file(s).
      Must be an integer with value 1, 2, 3 or 4.

    Returns:

    - A tuple containing a Flask Response object with JSON data and an
      HTTP status code. The JSON Response has the following structure:

        {
            "success": bool,
            "message": str,
            "data": null or object
        }

    - `success`: A boolean indicating whether the file exists.
    - `message`: A string containing a descriptive message about the result.
    - `data`: `null` or an object with an array of missing file numbers.

    Example Request:

        GET /projectname/verify-facsimile-file/1234/5/2
        GET /projectname/verify-facsimile-file/1234/all/2

    Example Success Response For Single File (HTTP 200):

        {
            "success": true,
            "message": "Facsimile file exists.",
            "data": null
        }

    Example Error Response For Single File (HTTP 404):

        {
            "success": false,
            "message": "Facsimile file not found.",
            "data": null
        }

    Example Error Response For All Files (HTTP 404):

        {
            "success": false,
            "message": "5 facsimile files not found.",
            "data": {
                "missing_file_numbers": [4, 11, 12, 27, 61]
            }
        }

    Status Codes:

    - 200 - OK: The request was successful; the file(s) exist(s).
    - 400 - Bad Request: One or more URL path parameters are invalid.
    - 403 - Forbidden: Permission denied to access facsimile file.
    - 404 - Not Found: The specified facsimile file does not exist.
    - 500 - Internal Server Error: Database query or file reading failed.
    """
    # Validate URL path parameters
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    if file_nr != "all":
        file_nr = int_or_none(file_nr)
        if not validate_int(file_nr, 1):
            return create_error_response("Validation error: 'file_nr' must be a positive integer or 'all'.")

    zoom_level = int_or_none(zoom_level)
    if not validate_int(zoom_level, 1, 4):
        return create_error_response("Validation error: 'zoom_level' must be an integer with value 1, 2, 3 or 4.")

    # Verify facsimile collection exists in database
    try:
        with db_engine.connect() as connection:
            facs_coll_table = get_table("publication_facsimile_collection")
            stmt = (
                select(facs_coll_table)
                .where(facs_coll_table.c.id == collection_id)
            )
            result = connection.execute(stmt).first()
    except Exception:
        logger.exception(f"Database error retrieving facsimile collection with ID {collection_id}.")
        return create_error_response("Unexpected error: failed to get facsimile collection from database.", 500)

    if result is None:
        return create_error_response("Validation error: could not find facsimile collection with given ID.")

    # Set the folder path based on the database or configuration
    folder_path = getattr(result, "folder_path", None)
    if folder_path:
        base_path = safe_join(folder_path)
    else:
        config = get_project_config(project)
        if config is None:
            return create_error_response("Error: project config does not exist on server.", 500)
        base_path = safe_join(config["file_root"], "facsimiles")

    # Create list of file paths where each item is a tuple with the file
    # number as the first part and the path as the second part.
    file_paths = []

    if file_nr == "all":
        image_count = int_or_none(result.number_of_pages)
        if not validate_int(image_count, 1):
            return create_error_response("Error: 'number_of_pages' for the facsimile collection in the database is NULL or a non-positive integer.")

        for i in range(1, image_count + 1):
            file_paths.append(
                (i, safe_join(base_path, str(collection_id), str(zoom_level), f"{str(i)}.jpg"))
            )
    else:
        file_paths.append(
            (file_nr, safe_join(base_path, str(collection_id), str(zoom_level), f"{file_nr}.jpg"))
        )

    # Check if one or more files exist
    missing_file_numbers = []
    for file_number, path in file_paths:
        try:
            if not os.path.isfile(path):
                missing_file_numbers.append(file_number)
        except Exception:
            logger.exception(f"Error checking file existence at {path}.")
            return create_error_response("Error accessing facsimile files.", 500)

    if len(file_paths) > 1:
        if len(missing_file_numbers) > 0:
            return create_error_response(
                f"{len(missing_file_numbers)} facsimile files not found.",
                404,
                {"missing_file_numbers": missing_file_numbers}
            )
        else:
            return create_success_response("All facsimile files exist.")
    else:
        if len(missing_file_numbers) > 0:
            return create_error_response("Facsimile file not found.", 404)
        else:
            return create_success_response("Facsimile file exists.")


@publication_tools.route("/<project>/get-single-facsimile-file/<collection_id>/<file_nr>/<zoom_level>")
@project_permission_required
def get_single_facsimile_file(project, collection_id, file_nr, zoom_level):
    """
    Retrieve a single facsimile file in a specific facsimile collection
    in the given project. Unlike the public `get_facsimile_file` endpoint
    in `facsimiles.py`, this endpoint does not require the facsimile
    collection to be linked to a publication. Also, the file number of the
    image to retrieve is not adjusted with the `start_page_number` value
    of the collection in the database.

    URL Path Parameters:

    - project (str, required): The name of the project containing the
      facsimile collection.
    - collection_id (int, required): The ID of the facsimile collection.
      Must be a positive integer.
    - file_nr (int, required): The number of the facsimile file to
      retrieve. Must be a positive integer.
    - zoom_level (int, required): The zoom level of the facsimile file.
      Must be an integer with value 1, 2, 3 or 4.

    Returns:

    - On success, the image file's binary data, otherwise a tuple
      containing a Flask Response object with JSON data and an
      HTTP status code. On failure, the JSON Response has the following
      structure:

        {
            "success": bool,
            "message": str,
            "data": null
        }

    - `success`: `false`.
    - `message`: A string containing a descriptive message about the result.
    - `data`: `null`.

    Example Request:

        GET /projectname/get-single-facsimile-file/1234/5/2

    Example Success Response (HTTP 200):

        Binary content of the image file with 'Content-Type: image/jpeg'.

    Example Error Response (HTTP 404):

        {
            "success": false,
            "message": "Facsimile file not found.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful; the image binary data is
                returned.
    - 400 - Bad Request: One or more URL path parameters are invalid.
    - 403 - Forbidden: Permission denied to access facsimile file.
    - 404 - Not Found: The specified facsimile file does not exist.
    - 500 - Internal Server Error: Database query or file reading failed.
    """
    # Validate URL path parameters
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    file_nr = int_or_none(file_nr)
    if not validate_int(file_nr, 1):
        return create_error_response("Validation error: 'file_nr' must be a positive integer.")

    zoom_level = int_or_none(zoom_level)
    if not validate_int(zoom_level, 1, 4):
        return create_error_response("Validation error: 'zoom_level' must be an integer with value 1, 2, 3 or 4.")

    # Verify facsimile collection exists in database
    try:
        with db_engine.connect() as connection:
            facs_coll_table = get_table("publication_facsimile_collection")
            stmt = (
                select(facs_coll_table)
                .where(facs_coll_table.c.id == collection_id)
            )
            result = connection.execute(stmt).first()
    except Exception:
        logger.exception(f"Database error retrieving facsimile collection with ID {collection_id}.")
        return create_error_response("Unexpected error: failed to get facsimile collection from database.", 500)

    if result is None:
        return create_error_response("Validation error: could not find facsimile collection with given ID.")

    # Set the file path based on the database or configuration
    folder_path = getattr(result, "folder_path", None)
    if folder_path:
        base_path = folder_path
    else:
        config = get_project_config(project)
        if config is None:
            return create_error_response("Error: project config does not exist on server.", 500)
        base_path = safe_join(config["file_root"], "facsimiles")

    file_path = safe_join(base_path,
                          str(collection_id),
                          str(zoom_level),
                          f"{file_nr}.jpg")

    # Retrieve image file
    try:
        with open(file_path, "rb") as img_file:
            content = img_file.read()
        return Response(content, status=200, content_type="image/jpeg")
    except FileNotFoundError:
        logger.exception(f"Error reading facsimile: file not found at {file_path}.")
        return create_error_response("Facsimile file not found.", 404)
    except PermissionError:
        logger.exception(f"Permission denied when accessing facsimile file at {file_path}.")
        return create_error_response("Error: permission denied to access facsimile file.", 403)
    except OSError:
        logger.exception(f"I/O error accessing facsimile file at {file_path}.")
        return create_error_response("Error accessing facsimile file.", 500)
