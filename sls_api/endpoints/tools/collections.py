import logging
from flask import Blueprint, request
from sqlalchemy import collate, select, and_, or_, not_
from datetime import datetime

from sls_api.endpoints.generics import cms_required, db_engine, get_project_id_from_name, \
    get_table, int_or_none, validate_int, create_error_response, \
    create_success_response, get_project_collation


collection_tools = Blueprint("collection_tools", __name__)
logger = logging.getLogger("sls_api.tools.collections")


@collection_tools.route("/<project>/facsimile_collection/new/", methods=["POST"])
@cms_required(edit=True)
def create_facsimile_collection(project):
    """
    Create a new facsimile collection.

    URL Path Parameters:

    - project (str): The name of the project (must be a valid project
      name, but the created facsimile collection is not associated with it).

    POST Data Parameters in JSON Format:

    - title (str, required): The title of the facsimile collection.
      Cannot be empty.
    - description (str): A description of the facsimile collection.
      Recommended.
    - number_of_pages (int): The total number of pages in the facsimile
      collection. Must be a non-negative integer.
    - start_page_number (int): The starting page number of the facsimile
      collection. Must be a non-negative integer. Generally, this should
      be set to 0, which is also the default if 'external_url' is empty.
    - folder_path (str): File system path to the folder containing
      the facsimile files. Generally not used.
    - page_comment (str): Comments or notes related to the pages in the
      facsimile collection.
    - external_url (str): External URL where the image files of the
      facsimile collection are located.

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
    - `data`: On success, an object containing the inserted facsimile
       collection data; `null` on error.

    Example Request:

        POST /projectname/facsimile_collection/new/
        Body:
        {
            "title": "New Facsimile Collection",
            "description": "Some details about the collection.",
            "number_of_pages": 100
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Facsimile collection created.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "deleted": 0,
                "title": "New Facsimile Collection",
                "number_of_pages": 100,
                "start_page_number": 0,
                "description": "Some details about the collection.",
                "folder_path": null,
                "page_comment": null,
                "external_url": null
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'title' required.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The facsimile collection was inserted successfully.
    - 400 - Bad Request: Invalid project name, field values, or no data
            provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Verify that the required 'title' field was provided
    if "title" not in request_data or not request_data["title"]:
        return create_error_response("Validation error: 'title' required.")

    # List of fields to check in request_data
    fields = ["title",
              "number_of_pages",
              "start_page_number",
              "description",
              "folder_path",
              "page_comment",
              "external_url"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if request_data[field] is None:
                values[field] = None
            else:
                if field in ["number_of_pages", "start_page_number"]:
                    if not validate_int(request_data[field], 0):
                        return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
                else:
                    # Ensure remaining fields are strings
                    request_data[field] = str(request_data[field])

                # Add the field to the insert values
                values[field] = request_data[field]

    # Set default value for "start_page_number" if not set and
    # "external_url" not set
    if (
        ("external_url" not in values or values["external_url"] is None)
        and ("start_page_number" not in values or values["start_page_number"] is None)
    ):
        values["start_page_number"] = 0

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                facs_coll_table = get_table("publication_facsimile_collection")
                stmt = (
                    facs_coll_table.insert()
                    .values(**values)
                    .returning(*facs_coll_table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(stmt).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Facsimile collection created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new facsimile collection.")
        return create_error_response("Unexpected error: failed to create new facsimile collection.", 500)


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_facsimile_collection(project, collection_id):
    """
    Edit a facsimile collection.

    URL Path Parameters:

    - project (str): The name of the project (must be a valid project name).
    - collection_id (int): The ID of the facsimile collection to be edited.
      Must be a positive integer.

    POST Data Parameters in JSON Format (at least one required):

    - title (str): The title of the facsimile collection. If
      provided, it cannot be empty.
    - description (str): A description of the facsimile collection.
    - number_of_pages (int): The total number of pages in the
      facsimile collection. Must be a non-negative integer.
    - start_page_number (int): The starting page number of the
      facsimile collection. Must be a non-negative integer.
    - folder_path (str): File system path to the folder containing
      the facsimile files. Generally not used.
    - page_comment (str): Comments or notes related to the pages in
      the facsimile collection.
    - external_url (str): External URL where the image files of the
      facsimile collection are located.
    - deleted (int): Marks the facsimile collection as deleted if set
      to 1. Must be either 0 or 1.

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
    - `data`: On success, an object containing the updated facsimile
       collection data; `null` on error.

    Example Request:

        POST /projectname/facsimile_collection/123/edit/
        Body:
        {
            "title": "Updated Facsimile Collection",
            "description": "Updated description of the collection.",
            "number_of_pages": 150
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Facsimile collection updated.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "deleted": 0,
                "title": "Updated Facsimile Collection",
                "number_of_pages": 150,
                "start_page_number": 0,
                "description": "Updated description of the collection.",
                "folder_path": null,
                "page_comment": null,
                "external_url": null
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'collection_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The facsimile collection was updated successfully.
    - 400 - Bad Request: Invalid project name, field values, or no data
            provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert facsimile collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Verify that the 'title' field is not empty if provided
    if "title" in request_data and not request_data["title"]:
        return create_error_response("Validation error: 'title' required.")

    # List of fields to check in request_data
    fields = ["title",
              "number_of_pages",
              "start_page_number",
              "description",
              "folder_path",
              "page_comment",
              "external_url",
              "deleted"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                if field in ["number_of_pages", "start_page_number"]:
                    if not validate_int(request_data[field], 0):
                        return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
                elif field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
                else:
                    # Ensure remaining fields are strings
                    request_data[field] = str(request_data[field])

                # Add the field to the insert values
                values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                facs_coll_table = get_table("publication_facsimile_collection")
                stmt = (
                    facs_coll_table.update()
                    .where(facs_coll_table.c.id == collection_id)
                    .values(**values)
                    .returning(*facs_coll_table.c)  # Return the updated row
                )
                updated_row = connection.execute(stmt).first()

                if updated_row is None:
                    # No row was returned: invalid facsimile collection_id
                    return create_error_response("Update failed: no facsimile collection with the provided 'collection_id' found.")

                return create_success_response(
                    message="Facsimile collection updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating facsimile collection.")
        return create_error_response("Unexpected error: failed to update facsimile collection.", 500)


@collection_tools.route("/<project>/facsimile_collection/list/")
@collection_tools.route("/<project>/facsimile_collection/list/<order_by>/<direction>/")
@cms_required(edit=True)
def list_facsimile_collections(project, order_by="id", direction="desc"):
    """
    List all facsimile collections linked to the specified project and
    all orphan facsimile collections (i.e. not linked to any project),
    with optional sorting by facsimile collection table columns.

    URL Path Parameters:

    - project (str, required): The name of the project to retrieve facsimile
      collections for (must be a valid project name).
    - order_by (str, optional): The column by which to order the facsimile
      collections. For example "id", "title", "date_modified". Defaults to "id".
    - direction (str, optional): The sort direction, valid values are `asc`
      (ascending) and `desc` (descending, default).

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
    - `data`: On success, an array of facsimile collection objects; `null`
       on error.

    Example Request:

        GET /projectname/facsimile_collection/list/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # facsimile collections.",
            "data": [
                {
                    "id": 123,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "deleted": 0,
                    "title": "Facsimile Collection",
                    "number_of_pages": 150,
                    "start_page_number": 0,
                    "description": "Description of the collection.",
                    "folder_path": null,
                    "page_comment": null,
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

    - 200 - OK: The facsimile collections are retrieved successfully.
    - 400 - Bad Request: Invalid project name.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    publication_facsimile_collection = get_table("publication_facsimile_collection")
    publication_facsimile = get_table("publication_facsimile")
    publication = get_table("publication")
    publication_collection = get_table("publication_collection")

    # Verify order_by and direction
    if order_by not in publication_facsimile_collection.c:
        return create_error_response("Validation error: 'order_by' must be a valid column in the publication_facsimile_collection table.")

    if direction not in ["asc", "desc"]:
        return create_error_response("Validation error: 'direction' must be either 'asc' or 'desc'.")

    # The endpoint has been refactored to use SQLAlchemy's Core methods
    # to build the query statement. The original raw SQL query is below
    # for reference.
    # The query does the following:
    # Select all publication_collection objects that are linked to the
    # project through publication_facsimile -> publication ->
    # publication_collection
    # AND all publication_collection objects that are orphans, i.e. they
    # are not linked to any publication_collection
    """
    SELECT *
    FROM publication_facsimile_collection
    WHERE deleted != 1 AND (
        id IN (
            SELECT publication_facsimile_collection_id
            FROM publication_facsimile
            WHERE publication_id IN (
                SELECT id
                FROM publication
                WHERE publication_collection_id IN (
                    SELECT id
                    FROM publication_collection
                    WHERE project_id = :project_id AND deleted != 1
                )
            )
        )
        OR id NOT IN (
            SELECT publication_facsimile_collection_id
            FROM publication_facsimile
            WHERE publication_id IN (
                SELECT id
                FROM publication
                WHERE publication_collection_id IN (
                    SELECT id
                    FROM publication_collection
                    WHERE deleted != 1
                )
            )
        )
    )
    """

    try:
        with db_engine.connect() as connection:
            # Subquery to get publication_collection IDs for the project
            pub_coll_subq = select(publication_collection.c.id).where(
                and_(
                    publication_collection.c.project_id == project_id,
                    publication_collection.c.deleted < 1
                )
            )

            # Subquery to get publication IDs linked to the
            # publication_collections
            publication_subq = select(publication.c.id).where(
                publication.c.publication_collection_id.in_(pub_coll_subq)
            )

            # Subquery to get publication_facsimile_collection_ids linked
            # to the publications
            facsimile_coll_linked_subq = select(publication_facsimile.c.publication_facsimile_collection_id).where(
                publication_facsimile.c.publication_id.in_(publication_subq)
            )

            # Subquery to get all publication_collection IDs where deleted < 1
            all_pub_coll_subq = select(publication_collection.c.id).where(
                publication_collection.c.deleted < 1
            )

            # Subquery to get publication IDs linked to all
            # publication_collections
            all_publication_subq = select(publication.c.id).where(
                publication.c.publication_collection_id.in_(all_pub_coll_subq)
            )

            # Subquery to get all publication_facsimile_collection_ids
            # linked to any publication_collection
            facsimile_coll_all_linked_subq = select(publication_facsimile.c.publication_facsimile_collection_id).where(
                publication_facsimile.c.publication_id.in_(all_publication_subq)
            )

            # Main query
            stmt = select(publication_facsimile_collection).where(
                and_(
                    publication_facsimile_collection.c.deleted < 1,
                    or_(
                        publication_facsimile_collection.c.id.in_(facsimile_coll_linked_subq),
                        not_(publication_facsimile_collection.c.id.in_(facsimile_coll_all_linked_subq))
                    )
                )
            )

            # Columns that should use collation-aware sorting
            collation_columns = {"title", "description"}
            collation_name = get_project_collation(project)

            order_column = publication_facsimile_collection.c[order_by]

            # Apply collation if the column needs it
            if order_by in collation_columns:
                order_column = collate(order_column, collation_name)

            # Apply ordering direction
            if direction == "desc":
                stmt = stmt.order_by(order_column.desc())
            else:
                stmt = stmt.order_by(order_column.asc())

            rows = connection.execute(stmt).fetchall()
            return create_success_response(
                message=f"Retrieved {len(rows)} facsimile collections.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving facsimile collections.")
        return create_error_response("Unexpected error: failed to retrieve facsimile collections.", 500)


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/data/")
@cms_required(edit=True)
def get_facsimile_collection_data(project, collection_id):
    """
    Retrieve a single facsimile collection.

    URL Path Parameters:

    - project (str, required): The name of the project to which the
      facsimile collection belongs (currently not enforced, any valid
      project name will do).
    - collection_id (int, required): The id of the facsimile collection
      to retrieve.

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
    - `data`: On success, an object containing the facsimile collection
      data; `null` on error.

    Example Request:

        GET /projectname/facsimile_collection/123/data/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved 1 facsimile collection.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "deleted": 0,
                "title": "Facsimile Collection",
                "number_of_pages": 150,
                "start_page_number": 0,
                "description": "Description of the collection.",
                "folder_path": null,
                "page_comment": null,
                "external_url": null
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'project' does not exist.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the facsimile collection
            is returned.
    - 400 - Bad Request: The project name or collection_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert facsimile collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    facs_coll_table = get_table("publication_facsimile_collection")
    statement = (
        select(*facs_coll_table.c)
        .where(facs_coll_table.c.deleted < 1)
        .where(facs_coll_table.c.id == collection_id)
    )

    try:
        with db_engine.connect() as connection:
            result = connection.execute(statement).mappings().first()
    except Exception:
        logger.exception("Exception retrieving facsimile collection data.")
        return create_error_response("Unexpected error: failed to retrieve facsimile collection.", 500)

    if result is None:
        return create_error_response("Validation error: could not find facsimile collection, either it’s deleted or 'collection_id' is invalid.")

    return create_success_response(
        message="Retrieved 1 facsimile collection.",
        data=dict(result)
    )


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/link/", methods=["POST"])
@cms_required(edit=True)
def link_facsimile_collection_to_publication(project, collection_id):
    """
    Link a facsimile collection to a publication by creating a
    publication facsimile entry in the `publication_facsimile` table.

    URL Path Parameters:

    - project (str): The name of the project. Must be a valid project
      name and associated with the publication.
    - collection_id (int): The ID of the facsimile collection. Must be a
      positive integer.

    POST Data Parameters in JSON Format:

    - publication_id (int, required): The ID of the publication to which the
      facsimile collection should be linked. Must be a positive integer.
    - page_nr (int): The page number where the facsimile starts.
      Must be an integer. Defaults to 0 if not provided.
    - section_id (int): The ID of the section where the facsimile is categorized.
      Must be a non-negative integer. Defaults to 0 if not provided.
    - priority (int): Sort order of the facsimile. Must be a non-negative
      integer. Defaults to 0 if not provided.
    - type (int): The type of the facsimile (not in use). Must be a
      non-negative integer. Defaults to 0 if not provided.

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
    - `data`: On success, an object containing the inserted facsimile data;
      `null` on error.

    Example Request:

        POST /projectname/facsimile_collection/123/link/
        Body:
        {
            "publication_id": 456,
            "page_nr": 10,
            "priority": 1
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Facsimile created.",
            "data": {
                "id": 789,
                "publication_facsimile_collection_id": 123,
                "publication_id": 456,
                "publication_manuscript_id": null,
                "publication_version_id": null,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": "2023-07-13T10:00:00",
                "deleted": 0,
                "page_nr": 10,
                "section_id": 0,
                "priority": 1,
                "type": 0
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'publication_id' required.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The facsimile was created successfully.
    - 400 - Bad Request: Invalid project name, collection ID, publication ID,
            or no data provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if collection_id is None or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    if "publication_id" not in request_data:
        return create_error_response("Validation error: 'publication_id' required.")

    # List of fields to check in request_data
    fields = ["publication_id",
              "page_nr",
              "section_id",
              "priority",
              "type"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if field == "publication_id":
                if not validate_int(request_data[field], 1):
                    return create_error_response(f"Validation error: '{field}' must be a positive integer.")
            elif field == "page_nr":
                if not validate_int(request_data[field]):
                    return create_error_response(f"Validation error: '{field}' must be an integer.")
            elif field in ["section_id", "priority", "type"]:
                if not validate_int(request_data[field], 0):
                    return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
            else:
                # Ensure remaining fields are strings
                request_data[field] = str(request_data[field])

            # Add the field to the insert values
            values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Set default values for "page_nr", "section_id", "priority", "type"
    for field in ["page_nr", "section_id", "priority", "type"]:
        if field not in values:
            values[field] = 0

    values["publication_facsimile_collection_id"] = collection_id

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                collection_table = get_table("publication_collection")
                publication_table = get_table("publication")
                facsimile_table = get_table("publication_facsimile")

                # Verify that the publication_id is valid and belongs to the project
                stmt = (
                    select(publication_table.c.id)
                    .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                    .where(collection_table.c.project_id == project_id)
                    .where(publication_table.c.id == values["publication_id"])
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return create_error_response("Validation error: could not find publication, either 'project' or 'publication_id' is invalid.")

                # Proceed to inserting the new publication facsimile
                ins_stmt = (
                    facsimile_table.insert()
                    .values(**values)
                    .returning(*facsimile_table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(ins_stmt).first()  # Fetch the inserted row

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Facsimile created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new publication facsimile.")
        return create_error_response("Unexpected error: failed to create new publication facsimile.", 500)


@collection_tools.route("/<project>/facsimile_collection/facsimile/edit/", methods=["POST"])
@cms_required(edit=True)
def edit_facsimile(project):
    """
    Edit a publication facsimile object by updating its fields.

    URL Path Parameters:

    - project (str): The name of the project. Must be a valid project name
      and associated with the facsimile.

    POST Data Parameters in JSON Format:

    - id (int, required): The ID of the facsimile to be updated. Must be a
      positive integer.
    - page_nr (int, optional): The page number of the facsimile. Must be an integer.
    - section_id (int, optional): The section ID where the facsimile is categorized.
      Must be a non-negative integer.
    - priority (int, optional): The sort order of the facsimile. Must be a
      non-negative integer.
    - type (int, optional): The type of the facsimile. Must be a non-negative integer.
    - deleted (int, optional): Marks the facsimile as deleted. Must be 0 (not
      deleted) or 1 (deleted).

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
    - `data`: On success, an object containing the updated facsimile data;
      `null` on error.

    Example Request:

        POST /projectname/facsimile_collection/facsimile/edit/
        Body:
        {
            "id": 789,
            "page_nr": 12,
            "priority": 2
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Facsimile updated.",
            "data": {
                "id": 789,
                "publication_facsimile_collection_id": 123,
                "publication_id": 456,
                "publication_manuscript_id": null,
                "publication_version_id": null,
                "date_created": "2023-06-01T08:00:00",
                "date_modified": "2023-07-13T10:00:00",
                "deleted": 0,
                "page_nr": 12,
                "section_id": 0,
                "priority": 2,
                "type": 0
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The facsimile was updated successfully.
    - 400 - Bad Request: Invalid project name, facsimile ID, or no valid
            data provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Verify that required id field in request data
    facsimile_id = int_or_none(request_data.get("id"))
    if facsimile_id is None or facsimile_id < 1:
        return create_error_response("Validation error: 'id' must be a positive integer.")

    # List of fields to check in request_data
    fields = ["page_nr",
              "section_id",
              "priority",
              "type",
              "deleted"]

    # Start building the dictionary of updated values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if field == "page_nr":
                if not validate_int(request_data[field]):
                    return create_error_response(f"Validation error: '{field}' must be an integer.")
            elif field in ["section_id", "priority", "type"]:
                if not validate_int(request_data[field], 0):
                    return create_error_response(f"Validation error: '{field}' must be an integer greater than or equal to 0.")
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")

            # Add the field to the insert values
            values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    # Add date_modified
    values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                facsimile_table = get_table("publication_facsimile")
                publication_table = get_table("publication")
                pub_collection_table = get_table("publication_collection")

                # Verify that facsimile id is valid and belongs to project
                stmt = (
                    select(facsimile_table.c.id)
                    .join(publication_table, facsimile_table.c.publication_id == publication_table.c.id)
                    .join(pub_collection_table, publication_table.c.publication_collection_id == pub_collection_table.c.id)
                    .where(facsimile_table.c.deleted < 1)
                    .where(facsimile_table.c.id == facsimile_id)
                    .where(pub_collection_table.c.project_id == project_id)
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return create_error_response("Validation error: could not find facsimile, either 'project' or facsimile 'id' is invalid.")

                # Proceed to updating the facsimile
                upd_stmt = (
                    facsimile_table.update()
                    .where(facsimile_table.c.id == facsimile_id)
                    .values(**values)
                    .returning(*facsimile_table.c)  # Return the updated row
                )
                updated_row = connection.execute(upd_stmt).first()

                if updated_row is None:
                    return create_error_response("Update failed: no facsimile with the provided 'id' found.")

                return create_success_response(
                    message="Publication facsimile updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating publication facsimile.")
        return create_error_response("Unexpected error: failed to update publication facsimile.", 500)


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/list_links/")
@collection_tools.route("/<project>/facsimile_collection/<collection_id>/list_links/<order_by>/<direction>/")
@cms_required(edit=True)
def list_facsimile_collection_links(project, collection_id, order_by="id", direction="asc"):
    """
    List all publication facsimile objects in the specified publication
    facsimile collection, with optional sorting by facsimile table columns
    or the publication name.

    URL Path Parameters:

    - project (str, required): The name of the project associated with the
      publication facsimile collection (must be a valid project name).
    - collection_id (int, required): The ID of the publication facsimile
      collection to retrieve facsimiles for (must be a positive integer).
    - order_by (str, optional): The column by which to order the facsimile
      objects. Valid options include columns from the publication_facsimile
      table like "id", "page_number", as well as "publication_name" for the
      name of the publication. Defaults to "id".
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
    - `data`: On success, an array of facsimile objects; `null` on error.

    Example Request:

        GET /projectname/facsimile_collection/123/list_links/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication facsimiles.",
            "data": [
                {
                    "id": 456,
                    "publication_facsimile_collection_id": 123,
                    "publication_id": 789,
                    "publication_manuscript_id": null,
                    "publication_version_id": null,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "deleted": 0,
                    "page_nr": 12,
                    "section_id": 1,
                    "priority": 1,
                    "type": 0,
                    "publication_name": "Publication Title"
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'collection_id' must be a positive integer.",
            "data": null

    Status Codes:

    - 200 - OK: The facsimiles are retrieved successfully.
    - 400 - Bad Request: Invalid project name or collection ID.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    facsimile_table = get_table("publication_facsimile")
    publication_table = get_table("publication")

    valid_order_columns = list(facsimile_table.c.keys()) + ["publication_name"]

    if order_by not in valid_order_columns:
        return create_error_response("Validation error: 'order_by' must be a valid column in the publication_facsimile table or the string 'publication_name'.")

    if direction not in ["asc", "desc"]:
        return create_error_response("Validation error: 'direction' must be either 'asc' or 'desc'.")

    try:
        with db_engine.connect() as connection:
            # Select facsimiles in the facsimile collection and join in
            # the publication table so we can also get the publication
            # name.
            stmt = (
                select(
                    *facsimile_table.c,
                    publication_table.c.name.label("publication_name")
                )
                .join(publication_table, facsimile_table.c.publication_id == publication_table.c.id)
                .where(facsimile_table.c.publication_facsimile_collection_id == collection_id)
                .where(facsimile_table.c.deleted < 1)
                .where(publication_table.c.deleted < 1)
            )

            # Order by facsimile table column or publication name
            if order_by == "publication_name":
                # Use collation-aware sorting
                collation_name = get_project_collation(project)
                order_column = collate(publication_table.c.name, collation_name)
            else:
                order_column = facsimile_table.c[order_by]

            if direction == "asc":
                stmt = stmt.order_by(order_column.asc())
            else:
                stmt = stmt.order_by(order_column.desc())

            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication facsimiles.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication facsimiles.")
        return create_error_response("Unexpected error: failed to retrieve publication facsimiles.", 500)


@collection_tools.route("/<project>/publication_collection/list/")
@collection_tools.route("/<project>/publication_collection/list/<order_by>/<direction>/")
@cms_required(edit=True)
def list_publication_collections(project, order_by="id", direction="asc"):
    """
    List all (non-deleted) publication collections for a given project,
    with optional sorting by publication collection table columns.

    URL Path Parameter:

    - project (str, required): The name of the project to retrieve
      publication collections for.
    - order_by (str, optional): The column by which to order the publication
      collections. For example "id", "name", "date_modified". Defaults to "id".
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
    - `data`: On success, an array of publication collection objects; `null`
      on error.

    Example Request:

        GET /projectname/publication_collection/list/
        GET /projectname/publication_collection/list/name/asc/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # publication collections.",
            "data": [
                {
                    "id": 1,
                    "name": "Collection Title",
                    "published": 1,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "date_published_externally": null,
                    "deleted": 0,
                    "legacy_id": null,
                    "project_id": 101,
                    "publication_collection_title_id": 55,
                    "publication_collection_introduction_id": 75,
                    "name_translation_id": null,
                    "collection_title_filename": "title_file.xml",
                    "collection_intro_filename": "intro_file.xml",
                    "collection_title_published": 1,
                    "collection_intro_published": 1
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

    - 200 - OK: The request was successful, and the publication collections
            are returned.
    - 400 - Bad Request: The project does not exist.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    try:
        # Get table metadata
        collection_table = get_table("publication_collection")
        title_table = get_table("publication_collection_title")
        introduction_table = get_table("publication_collection_introduction")

        # Verify order_by and direction
        if order_by not in collection_table.c:
            return create_error_response("Validation error: 'order_by' must be a valid column in the publication_collection table.")

        if direction not in ["asc", "desc"]:
            return create_error_response("Validation error: 'direction' must be either 'asc' or 'desc'.")

        # Select all columns from the collection table and the
        # `original_filename` and `published` columns from the
        # collection title and introduction tables.
        stmt = select(
            *collection_table.c,
            title_table.c.original_filename.label("collection_title_filename"),
            introduction_table.c.original_filename.label("collection_intro_filename"),
            title_table.c.published.label("collection_title_published"),
            introduction_table.c.published.label("collection_intro_published")
        ).select_from(
            collection_table.outerjoin(
                title_table,
                collection_table.c.publication_collection_title_id == title_table.c.id
            ).outerjoin(
                introduction_table,
                collection_table.c.publication_collection_introduction_id == introduction_table.c.id
            )
        ).where(
            and_(
                collection_table.c.project_id == project_id,
                collection_table.c.deleted < 1
            )
        )

        order_column = collection_table.c[order_by]

        # Apply collation if the column needs it
        if order_by == "name":
            collation_name = get_project_collation(project)
            order_column = collate(order_column, collation_name)

        if direction == "asc":
            stmt = stmt.order_by(order_column.asc())
        else:
            stmt = stmt.order_by(order_column.desc())

        with db_engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} publication collections.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving publication collections.")
        return create_error_response("Unexpected error: failed to retrieve publication collections.", 500)


@collection_tools.route("/<project>/publication_collection/new/", methods=["POST"])
@cms_required(edit=True)
def new_publication_collection(project):
    """
    Create a new publication collection in the specified project.

    URL Path Parameters:

    - project (str): The name of the project.

    POST Data Parameters in JSON Format:

    - name (str, required): The name/title of the publication collection.
      Cannot be empty.
    - published (int): The publication status of the collection. Must be an
      integer with value 0, 1 or 2. Default value is 1.

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
    - `data`: On success, an object containing the inserted publication
      collection data; `null` on error.

    Example Request:

        POST /projectname/publication_collection/new/
        Body:
        {
            "name": "My New Collection",
            "published": 1
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Publication collection created.",
            "data": {
                "id": 1,
                "publication_collection_introduction_id": null,
                "publication_collection_title_id": null,
                "project_id": 101,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": null,
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "name": "My New Collection",
                "legacy_id": null,
                "name_translation_id": null
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'name' required.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The publication collection was inserted successfully.
    - 400 - Bad Request: Invalid project name, invalid field values, or no
            valid fields provided for insertion.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project is valid
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Verify that the required 'name' field was provided
    if "name" not in request_data or not request_data["name"]:
        return create_error_response("Validation error: 'name' required.")

    # Start building values dictionary for the insert
    values = {}

    # List of fields to check in request_data
    fields = ["name", "published"]

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Validate integer field values and ensure all other fields are strings
            if field == "published":
                if not validate_int(request_data["published"], 0, 2):
                    return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
            else:
                # Convert remaining fields to string
                request_data[field] = str(request_data[field])

            # Add the field to the field_names list for the query construction
            values[field] = request_data[field]

    # Set published to default value 1 if not in provided values
    if "published" not in values:
        values["published"] = 1

    # Add project_id to insert values
    values["project_id"] = project_id

    publication_collection = get_table("publication_collection")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Execute the insert statement
                statement = (
                    publication_collection.insert()
                    .values(**values)
                    .returning(*publication_collection.c)  # Return the inserted row
                )
                inserted_row = connection.execute(statement).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Publication collection created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new publication collection.")
        return create_error_response("Unexpected error: failed to create new publication collection.", 500)


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/")
@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/<order_by>/")
@cms_required(edit=True)
def list_publications(project, collection_id, order_by="id"):
    """
    List all (non-deleted) publications within a specific publication
    collection for a given project.

    URL Path Parameters:

    - project (str, required): The name of the project for which to
      retrieve publications.
    - collection_id (int, required): The id of the publication collection
      to retrieve publications from.
    - order_by (str, optional): The column by which to order the publications.
      For example "id", "name", "original_filename" or "date_modified".
      Defaults to "id".

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

        GET /projectname/publication_collection/123/publications/
        GET /projectname/publication_collection/123/publications/name/

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
            "message": "Validation error: 'collection_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publications are returned.
    - 400 - Bad Request: The project name or collection_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    if order_by not in publication_table.c:
        return create_error_response("Validation error: 'order_by' must be a valid column in the publication table.")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
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

                order_column = publication_table.c[order_by]

                # Apply collation if the column needs it
                if order_by == "name":
                    collation_name = get_project_collation(project)
                    order_column = collate(order_column, collation_name)

                # Proceed to selecting the publications
                select_pub_stmt = (
                    select(publication_table)
                    .where(publication_table.c.publication_collection_id == collection_id)
                    .where(publication_table.c.deleted < 1)
                    .order_by(order_column)
                )
                rows = connection.execute(select_pub_stmt).fetchall()

                return create_success_response(
                    message=f"Retrieved {len(rows)} publications.",
                    data=[row._asdict() for row in rows]
                )

    except Exception:
        logger.exception("Exception retrieving publications.")
        return create_error_response("Unexpected error: failed to retrieve publications.", 500)


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/new/", methods=["POST"])
@cms_required(edit=True)
def new_publication(project, collection_id):
    """
    Create a new publication as part of the specified publication collection.

    URL Path Parameters:

    - project (str): The name of the project.
    - collection_id (int): The id of the publication collection to which
      the new publication will be added.

    POST Data Parameters in JSON Format:

    - name (str, required): The name/title of the publication. Cannot be empty.
    - publication_comment_id (int, optional): id of the associated
      publication comment. Must be a positive integer, and the comment must
      exist in the 'publication_comment' table.
    - published (int, optional): The publication status. Must be an integer
      with value 0, 1 or 2. Defaults to 1.
    - legacy_id (str, optional): Legacy id for the publication.
    - original_filename (str, optional): File path to the publication XML file.
    - genre (str, optional): The genre of the publication.
    - original_publication_date (str, optional): Date when the publication
      was originally published.
    - language (str, optional): Language code (ISO 639-1) of the main
      language of the publication.

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
    - `data`: On success, an object containing the inserted publication
      data; `null` on error.

    Example Request:

        POST /projectname/publication_collection/123/publications/new/
        Body:
        {
            "name": "New Publication",
            "original_filename": "/path/to/file.xml",
            "original_publication_date": "1854",
            "language": "en"
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Publication created.",
            "data":  {
                "id": 1,
                "publication_collection_id": 123,
                "publication_comment_id": null,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": null,
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "/path/to/file.xml",
                "name": "New Publication",
                "genre": null,
                "publication_group_id": null,
                "original_publication_date": "1854",
                "zts_id": null,
                "language": "en"
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'collection_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The publication was inserted successfully.
    - 400 - Bad Request: Invalid project name, collection id, field values,
            or no data provided.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return create_error_response("Validation error: 'collection_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Verify that the required 'name' field was provided
    if "name" not in request_data or not request_data["name"]:
        return create_error_response("Validation error: 'name' required.")

    # List of fields to check in request_data
    fields = ["publication_comment_id",
              "published",
              "legacy_id",
              "original_filename",
              "name",
              "genre",
              "original_publication_date",
              "language"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if request_data[field] is None:
                # Skip None values, they will be set to NULL by the database anyway
                continue
            else:
                if field == "publication_comment_id":
                    if not validate_int(request_data[field], 1):
                        return create_error_response(f"Validation error: '{field}' must be a positive integer.")
                elif field == "published":
                    if not validate_int(request_data[field], 0, 2):
                        return create_error_response(f"Validation error: '{field}' must be either 0, 1 or 2.")
                else:
                    # Ensure remaining fields are strings
                    request_data[field] = str(request_data[field])

            # Add the field to the insert values
            values[field] = request_data[field]

    # Set published to default value 1 if not in provided values
    if "published" not in values:
        values["published"] = 1

    values["publication_collection_id"] = collection_id

    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Check for publication_collection existence
                select_stmt = (
                    select(collection_table.c.id)
                    .where(collection_table.c.id == collection_id)
                    .where(collection_table.c.project_id == project_id)
                    .where(collection_table.c.deleted < 1)
                )
                result = connection.execute(select_stmt).first()

                if not result:
                    return create_error_response("Validation error: could not find publication collection, either 'project' or 'collection_id' is invalid.")

                # If a publication_comment_id was provided, check that it
                # exists and is not deleted
                if "publication_comment_id" in values:
                    comment_table = get_table("publication_comment")
                    select_com_stmt = (
                        select(comment_table.c.id)
                        .where(comment_table.c.id == values["publication_comment_id"])
                        .where(comment_table.c.deleted < 1)
                    )
                    result = connection.execute(select_com_stmt).first()

                    if not result:
                        return create_error_response("Validation error: could not find publication comment with the provided 'publication_comment_id'.")

                # Proceed to insert the new publication with provided values
                insert_stmt = (
                    publication_table.insert()
                    .values(**values)
                    .returning(*publication_table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(insert_stmt).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Publication created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new publication.")
        return create_error_response("Unexpected error: failed to create new publication.", 500)
