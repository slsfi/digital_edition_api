from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import select, text

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, \
    get_table, int_or_none, validate_int, project_permission_required


publication_tools = Blueprint("publication_tools", __name__)


@publication_tools.route("/<project>/publications/")
@jwt_required()
def get_publications(project):
    """
    List all (non-deleted) publications for a given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publications.

    Returns:

        JSON: A list of publication objects within the specified project,
        an empty list if there are no publications, or an error message.

    Example Request:

        GET /projectname/publications/

    Example Response (Success):

        [
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

    Example Response (Error):

        {
            "msg": "Invalid project name."
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publications are returned.
    - 400 - Bad Request: The project name is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    try:
        with db_engine.connect() as connection:
            # Left join collection table on publication table and
            # select only the columns from the publication table
            statement = (
                select(*publication_table.c)
                .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                .where(collection_table.c.project_id == project_id)
                .where(publication_table.c.deleted < 1)
                .order_by(publication_table.c.publication_collection_id)
            )
            rows = connection.execute(statement).fetchall()
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve project publications.",
                        "reason": str(e)}), 500


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

        JSON: A publication object within the specified project, or an error
        message if the publication is not found.

    Example Request:

        GET /projectname/publication/123/

    Example Response (Success):

        {
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

    Example Response (Error):

        {
            "msg": "Publication not found. Either project name or
                    publication_id is invalid."
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication is returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 404 - Not Found: The publication was not found within the specified
            project.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

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
                return jsonify({"msg": "Publication not found. Either project name or publication_id is invalid."}), 404
            return jsonify(result._asdict())

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publication.",
                        "reason": str(e)}), 500


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

        JSON: A list of publication version objects for the specified
        publication, or an error message.

    Example Request:

        GET /projectname/publication/456/versions/

    Example Response (Success):

        [
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

    Example Response (Error):

        {
            "msg": "Invalid publication_id, must be a positive integer."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

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
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publication versions.",
                        "reason": str(e)}), 500


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

        JSON: A list of manuscript objects for the specified publication,
        or an error message.

    Example Request:

        GET /projectname/publication/456/manuscripts/

    Example Response (Success):

        [
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

    Example Response (Error):

        {
            "msg": "Invalid publication_id, must be a positive integer."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

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
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publication manuscripts.",
                        "reason": str(e)}), 500


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

        JSON: A list of tag objects for the specified publication, or an
        error message.

    Example Request:

        GET /projectname/publication/456/tags/

    Status Codes:

    - 200 - OK: The request was successful, and the publication tags are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

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
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publication tags.",
                        "reason": str(e)}), 500


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

        JSON: A list of fascimile objects for the specified publication,
        or an error message.

    Example Request:

        GET /projectname/publication/456/fascimiles/

    Status Codes:

    - 200 - OK: The request was successful, and the publication facsimiles
            are returned.
    - 400 - Bad Request: The project name or publication_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

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
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publication facsimiles.",
                        "reason": str(e)}), 500


@publication_tools.route("/<project>/publication/<publication_id>/comments/")
@jwt_required()
def get_publication_comments(project, publication_id):
    """
    List all (non-deleted) comments of the specified publication
    in a given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which to retrieve
      publication comments.
    - publication_id (int, required): The id of the publication to retrieve
      comments for. Must be a positive integer.

    Returns:

        JSON: A list of publication comment objects for the specified
        publication, or an error message. Currently, publications can have
        only one comment, so the list will have either only one item or no
        items.

    Example Request:

        GET /projectname/publication/456/comments/

    Example Response (Success):

        [
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

    Example Response (Error):

        {
            "msg": "Invalid publication_id, must be a positive integer."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

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
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publication comments.",
                        "reason": str(e)}), 500


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
      the file. Defaults to 1 for "version" (1=base text, 2=other variant).
    - section_id (int, optional): A non-negative integer representing the
      section ID.
    - sort_order (int, optional): A non-negative integer indicating the
      sort order. Defaults to 1.

    For "manuscript" only:

    - language (str, optional): The language code (ISO 639-1) of the main
      language in the manuscript text.

    For all file types:

    - published (int, optional): The publication status. Must be an integer
      with value 0, 1 or 2. Defaults to 1.
    - published_by (str, optional): The name of the person who published
      the text.
    - legacy_id (str, optional): A legacy identifier for the text.

    Returns:

        JSON: A success message with the inserted row data or an error message.

    Example Request:

        POST /projectname/publication/456/link_file/
        Body:
        {
            "text_type": "manuscript",
            "original_filename": "path/to/ms_file1.xml",
            "name": "Publication Title manuscript 1",
            "language": "en",
            "published": 1
        }

    Example Response (Success):

        {
            "msg": "Publication manuscript with ID 123 created successfully.",
            "row": {
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

    Example Response (Error):

        {
            "msg": "POST data is invalid: required fields are missing or empty, or 'text_type' has an invalid value."
        }

    Status Codes:

    - 201 - Created: The publication text type was created successfully.
    - 400 - Bad Request: Invalid project name, publication ID, field values,
            or no data provided.
    - 404 - Not Found: Publication not found or does not belong to the
            specified project.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert publication_id to integer and verify
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id, must be a positive integer."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

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
        return jsonify({"msg": "POST data is invalid: required fields are missing or empty, or 'text_type' has an invalid value."}), 400

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
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
            elif field in ["type", "section_id", "sort_order"]:
                if not validate_int(request_data[field], 0):
                    return jsonify({"msg": f"Field '{field}' must be a non-negative integer."}), 400
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
                    return jsonify({"msg": "Publication not found. Either project name or publication_id is invalid."}), 404

                # Since publications can have only one comment linked to them,
                # we need to check if the publication already has a comment.
                if (
                    text_type == "comment"
                    and result["publication_comment_id"] is not None
                ):
                    return jsonify({"msg": "Comment already linked to publication. Publications can have only one comment."}), 400

                table = get_table(f"publication_{text_type}")
                ins_stmt = (
                    table.insert()
                    .values(**values)
                    .returning(*table.c)  # Return the inserted row
                )
                result = connection.execute(ins_stmt)
                inserted_row = result.fetchone()  # Fetch the inserted row

                if inserted_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Insertion failed: no row returned.",
                        "reason": "The insert statement did not return any data."
                    }), 500

                if text_type == "comment":
                    # Update the publication with the comment id
                    upd_stmt = (
                        publication_table.update()
                        .where(publication_table.c.id == publication_id)
                        .values(publication_comment_id=inserted_row["id"])
                    )
                    connection.execute(upd_stmt)

                # Convert the inserted row to a dict for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return jsonify({
                    "msg": f"Publication {text_type} with ID {inserted_row['id']} created successfully.",
                    "row": inserted_row_dict
                }), 201

    except Exception as e:
        return jsonify({
            "msg": f"Failed to create new publication {text_type}.",
            "reason": str(e)
        }), 500
