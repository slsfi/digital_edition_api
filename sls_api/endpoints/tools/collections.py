from flask import Blueprint, jsonify, request
from sqlalchemy import select, text, and_, or_, not_, asc, desc
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, \
    int_or_none, project_permission_required, validate_int


collection_tools = Blueprint("collection_tools", __name__)


@collection_tools.route("/<project>/facsimile_collection/new/", methods=["POST"])
@project_permission_required
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

        JSON: A success message with the inserted row or an error message.

    Example Request:

        POST /projectname/facsimile_collection/new/
        Body:
        {
            "title": "New Facsimile Collection",
            "description": "Some details about the collection.",
            "number_of_pages": 100
        }

    Example Response (Success):

        {
            "msg": "Facsimile collection with ID 123 created successfully.",
            "row": {
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

    Example Response (Error):

        {
            "msg": "Field 'title' must be provided and cannot be empty."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # Verify that the required 'title' field was provided
    if "title" not in request_data or not request_data["title"]:
        return jsonify({"msg": "Field 'title' must be provided and cannot be empty."}), 400

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
                        return jsonify({"msg": f"Field '{field}' must be a non-negative integer."}), 400
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
                result = connection.execute(stmt)
                inserted_row = result.fetchone()  # Fetch the inserted row

                if inserted_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Insertion failed: no row returned.",
                        "reason": "The insert statement did not return any data."
                    }), 500

                # Convert the inserted_row to a dictionary for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return jsonify({
                    "msg": f"Facsimile collection with ID {inserted_row['id']} created successfully.",
                    "row": inserted_row_dict
                }), 201

    except Exception as e:
        return jsonify({"msg": "Failed to create new facsimile collection.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/facsimile_collection/<facsimile_collection_id>/edit/", methods=["POST"])
@project_permission_required
def edit_facsimile_collection(project, facsimile_collection_id):
    """
    Edit a facsimile collection.

    URL Path Parameters:

    - project (str): The name of the project (must be a valid project name).
    - facsimile_collection_id (int): The ID of the facsimile collection to
      be edited. Must be a positive integer.

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

        JSON: A success message with the updated row or an error message.

    Example Request:

        POST /projectname/facsimile_collection/123/edit/
        Body:
        {
            "title": "Updated Facsimile Collection",
            "description": "Updated description of the collection.",
            "number_of_pages": 150
        }

    Example Response (Success):

        {
            "msg": "Updated facsimile collection with ID 123 successfully.",
            "row": {
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

    Example Response (Error):

        {
            "msg": "Invalid facsimile_collection_id, must be a positive integer."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert facsimile_collection_id to integer and verify
    facsimile_collection_id = int_or_none(facsimile_collection_id)
    if not facsimile_collection_id or facsimile_collection_id < 1:
        return jsonify({"msg": "Invalid facsimile_collection_id, must be a positive integer."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # Verify that the 'title' field is not empty if provided
    if "title" in request_data and not request_data["title"]:
        return jsonify({"msg": "Field 'title' cannot be empty."}), 400

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
                        return jsonify({"msg": f"Field '{field}' must be a non-negative integer."}), 400
                elif field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
                else:
                    # Ensure remaining fields are strings
                    request_data[field] = str(request_data[field])

                # Add the field to the insert values
                values[field] = request_data[field]

    if not values:
        return jsonify({"msg": "No valid fields provided to update."}), 400

    # Add date_modified
    values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                facs_coll_table = get_table("publication_facsimile_collection")
                stmt = (
                    facs_coll_table.update()
                    .where(facs_coll_table.c.id == facsimile_collection_id)
                    .values(**values)
                    .returning(*facs_coll_table.c)  # Return the updated row
                )
                result = connection.execute(stmt)
                updated_row = result.fetchone()  # Fetch the updated row

                if updated_row is None:
                    # No row was returned: invalid facsimile_collection_id
                    return jsonify({"msg": "Update failed: invalid facsimile_collection_id."}), 400

                # Convert the inserted row to a dict for JSON serialization
                updated_row_dict = updated_row._asdict()

                return jsonify({
                    "msg": f"Updated facsimile collection with ID {facsimile_collection_id} successfully.",
                    "row": updated_row_dict
                })

    except Exception as e:
        return jsonify({"msg": f"Failed to update facsimile collection with ID {facsimile_collection_id}.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/facsimile_collection/list/")
@collection_tools.route("/<project>/facsimile_collection/list/<order_by>/<direction>/")
@project_permission_required
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

        JSON: A list with facsimile collection objects, an empty list
        or an error message.

    Example Request:

        GET /projectname/facsimile_collection/list/

    Example Response (Success):

        [
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

    Example Response (Error):

        {
            "msg": "Invalid project name."
        }

    Status Codes:

    - 200 - OK: The facsimile collections are retrieved successfully.
    - 400 - Bad Request: Invalid project name.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    publication_facsimile_collection = get_table("publication_facsimile_collection")
    publication_facsimile = get_table("publication_facsimile")
    publication = get_table("publication")
    publication_collection = get_table("publication_collection")

    # Verify order_by and direction
    if order_by not in publication_facsimile_collection.c:
        return jsonify({"msg": "Invalid order_by field."}), 400

    if direction not in ["asc", "desc"]:
        return jsonify({"msg": "Invalid sort order, must be either 'asc' or 'desc'."}), 400

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

            if direction == "desc":
                stmt = stmt.order_by(
                    desc(publication_facsimile_collection.c[order_by])
                )
            else:
                stmt = stmt.order_by(
                    asc(publication_facsimile_collection.c[order_by])
                )

            rows = connection.execute(stmt).fetchall()
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Error trying to retrieve facsimile collections.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/link/", methods=["POST"])
@project_permission_required
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

        JSON: A success message with the inserted row or an error message.

    Example Request:

        POST /projectname/facsimile_collection/123/link/
        Body:
        {
            "publication_id": 456,
            "page_nr": 10,
            "priority": 1
        }

    Example Response (Success):

        {
            "msg": "Publication facsimile with ID 789 created successfully.",
            "row": {
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

    Example Response (Error):

        {
            "msg": "Publication not found. Either project name or publication_id is invalid."
        }

    Status Codes:

    - 201 - Created: The facsimile was linked successfully.
    - 400 - Bad Request: Invalid project name, collection ID, publication ID, or no data
            provided.
    - 404 - Not Found: The publication was not found for the given project or ID.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if collection_id is None or collection_id < 1:
        return jsonify({"msg": "Invalid facsimile collection_id, must be a positive integer."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    if "publication_id" not in request_data:
        return jsonify({"msg": "Field 'publication_id' must be provided."}), 400

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
                    return jsonify({"msg": f"Field '{field}' must be a positive integer."}), 400
            elif field == "page_nr":
                if not validate_int(request_data[field]):
                    return jsonify({"msg": f"Field '{field}' must be an integer."}), 400
            elif field in ["section_id", "priority", "type"]:
                if not validate_int(request_data[field], 0):
                    return jsonify({"msg": f"Field '{field}' must be a non-negative integer."}), 400
            else:
                # Ensure remaining fields are strings
                request_data[field] = str(request_data[field])

            # Add the field to the insert values
            values[field] = request_data[field]

    if not values:
        return jsonify({"msg": "No valid fields provided to update."}), 400

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
                    return jsonify({"msg": "Publication not found. Either project name or publication_id is invalid."}), 404

                # Proceed to inserting the new publication facsimile
                try:
                    ins_stmt = (
                        facsimile_table.insert()
                        .values(**values)
                        .returning(*facsimile_table.c)  # Return the inserted row
                    )
                    inserted_row = connection.execute(ins_stmt).first()  # Fetch the inserted row

                    if inserted_row is None:
                        # No row was returned; handle accordingly
                        return jsonify({
                            "msg": "Insertion failed: no row returned.",
                            "reason": "The insert statement did not return any data."
                        }), 500

                    # Convert the inserted row to a dict for JSON serialization
                    inserted_row_dict = inserted_row._asdict()

                    return jsonify({
                        "msg": f"Publication facsimile with ID {inserted_row['id']} created successfully.",
                        "row": inserted_row_dict
                    }), 201

                except Exception as e:
                    return jsonify({"msg": "Failed to create new publication facsimile.",
                                    "reason": str(e)}), 400

    except Exception as e:
        return jsonify({"msg": "Unexpected error occurred while creating publication facsimile.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/facsimile_collection/facsimile/edit/", methods=["POST"])
@project_permission_required
def edit_facsimile(project):
    """
    Edit a facsimile object in the database

    POST data MUST be in JSON format.

    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    facsimile_id = request_data.get("id", None)
    facsimile = get_table("publication_facsimile")

    connection = db_engine.connect()
    with connection.begin():
        facsimile_query = select(facsimile.c.id).where(facsimile.c.id == int_or_none(facsimile_id))
        facsimile_row = connection.execute(facsimile_query).fetchone()
    if facsimile_row is None:
        return jsonify({"msg": "No facsimile with an ID of {} exists.".format(facsimile_id)}), 404

    # facsimile_collection_id = request_data.get("facsimile_collection_id", None)
    page = request_data.get("page", None)
    priority = request_data.get("priority", None)
    type = request_data.get("type", None)

    values = {}
    if page is not None:
        values["page_nr"] = page
    if type is not None:
        values["type"] = type
    if priority is not None:
        values["priority"] = priority

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = facsimile.update().where(facsimile.c.id == int(facsimile_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated facsimile {} with values {}".format(int(facsimile_id), str(values)),
                    "facsimile_id": int(facsimile_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update facsimile.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/list_links/")
@collection_tools.route("/<project>/facsimile_collection/<collection_id>/list_links/<order_by>/<direction>/")
@project_permission_required
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
      table like "id", "page_number", as well as "publication_name" for the name of the publication. Defaults to "id".
    - direction (str, optional): The sort direction, valid values are `asc`
      (ascending, default) and `desc` (descending).

    Returns:

        JSON: A list of facsimile objects with details or an error message.

    Example Request:

        GET /projectname/facsimile_collection/123/list_links/

    Example Response (Success):

        [
            {
                "id": 456,
                "publication_id": 789,
                "publication_name": "Publication Title",
                "page_number": 12,
                "date_created": "2023-07-01T14:22:33",
                "deleted": 0
            },
            ...
        ]

    Example Response (Error):

        {
            "msg": "Invalid collection_id, must be a positive integer."
        }

    Status Codes:

    - 200 - OK: The facsimiles are retrieved successfully.
    - 400 - Bad Request: Invalid project name or collection ID.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return jsonify({"msg": "Invalid collection_id, must be a positive integer."}), 400

    facsimile_table = get_table("publication_facsimile")
    publication_table = get_table("publication")

    valid_order_columns = list(facsimile_table.c.keys()) + ["publication_name"]

    if order_by not in valid_order_columns:
        return jsonify({"msg": "Invalid order_by field."}), 400

    if direction not in ["asc", "desc"]:
        return jsonify({"msg": "Invalid sort order, must be either 'asc' or 'desc'."}), 400

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
                order_column = publication_table.c.name
            else:
                order_column = facsimile_table.c[order_by]

            if direction == "asc":
                stmt = stmt.order_by(asc(order_column))
            else:
                stmt = stmt.order_by(desc(order_column))

            rows = connection.execute(stmt).fetchall()
            result = [row._asdict() for row in rows]
            return jsonify(result)

    except Exception as e:
        return jsonify({"msg": f"Error trying to retrieve facsimiles for facsimile collection with ID {collection_id}.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/facsimile_publication/delete/<f_pub_id>", methods=["DELETE"])
@project_permission_required
def delete_facsimile_collection_link(project, f_pub_id):
    """
    Delete the specified publication facsimile.
    """
    connection = db_engine.connect()
    publication_facsimile = get_table("publication_facsimile")
    values = {
        'deleted': 1,
        "date_modified": datetime.now()
    }
    with connection.begin():
        update = publication_facsimile.update().where(publication_facsimile.c.id == int(f_pub_id)).values(**values)
        connection.execute(update)
    connection.close()
    result = {
        "msg": "Deleted publication_facsimile"
    }
    return jsonify(result), 200


@collection_tools.route("/<project>/publication_collection/list/")
@project_permission_required
def list_publication_collections(project):
    """
    List all (non-deleted) publication collections for a given project.

    URL Path Parameter:

    - project (str, required): The name of the project to retrieve
      publication collections for.

    Returns:

        JSON: A list of all publication collection objects associated
        with the given project, an empty list if there are none,
        or an error message.

    Example Request:

        GET /<project>/publication_collection/list/

    Example Response (Success):

        [
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

    Example Response (Error):

        {
            "msg": "No such project exists."
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publication collections
            are returned.
    - 400 - Bad Request: The project does not exist.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "No such project exists."}), 400

    statement = """
        SELECT
            pc.id,
            pc.name,
            pc.published,
            pc.date_created,
            pc.date_modified,
            pc.date_published_externally,
            pc.deleted,
            pc.legacy_id,
            pc.project_id,
            pc.publication_collection_title_id,
            pc.publication_collection_introduction_id,
            pc.name_translation_id,
            pct.original_filename AS collection_title_filename,
            pci.original_filename AS collection_intro_filename,
            pct.published AS collection_title_published,
            pci.published AS collection_intro_published
        FROM
            publication_collection pc
        LEFT JOIN
            publication_collection_title pct
            ON pct.id = pc.publication_collection_title_id
        LEFT JOIN
            publication_collection_introduction pci
            ON pci.id = pc.publication_collection_introduction_id
        WHERE
            pc.project_id = :project_id
            AND pc.deleted < 1
        ORDER BY
            pc.id
    """

    try:
        with db_engine.connect() as connection:
            rows = connection.execute(
                text(statement),
                {"project_id": project_id}
            ).fetchall()
            result = [row._asdict() for row in rows]
            return jsonify(result)
    except Exception as e:
        return jsonify({"msg": "Failed to retrieve project publication collections.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/publication_collection/new/", methods=["POST"])
@project_permission_required
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

        JSON: A success message and the inserted publication collection
        row, or an error message.

    Example Request:

        POST /projectname/publication_collection/new/
        Body:
        {
            "name": "My New Collection",
            "published": 1
        }

    Example Response (Success):

        {
            "msg": "Publication collection created successfully.",
            "row": {
                "id": 789,
                "name": "My New Collection",
                "published": 1,
                ...
            }
        }

    Example Response (Error):

        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # Verify that the required 'name' field was provided
    if "name" not in request_data or not request_data["name"]:
        return jsonify({"msg": "'name' field is required and must be non-empty."}), 400

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
                    return jsonify({"msg": "Field 'published' must be an integer with value 0, 1 or 2."}), 400
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
                result = connection.execute(statement)
                inserted_row = result.fetchone()  # Fetch the inserted row

                if inserted_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Insertion failed: no row returned.",
                        "reason": "The insert statement did not return any data."
                    }), 500

                # Convert the inserted_row to a dictionary for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return jsonify({"msg": "Publication collection inserted successfully.",
                                "row": inserted_row_dict}), 201

    except Exception as e:
        return jsonify({"msg": "Failed to create new publication collection.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/")
@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/<order_by>/")
@project_permission_required
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

        JSON: A list of publication objects in the specified collection, an
        empty list if there are no publications, or an error message.

    Example Request:

        GET /projectname/publication_collection/123/publications/
        GET /projectname/publication_collection/123/publications/name/

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
            "msg": "Invalid collection_id, does not exist."
        }

    Status Codes:

    - 200 - OK: The request was successful, and the publications are returned.
    - 400 - Bad Request: The project name or collection_id is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return jsonify({"msg": "Invalid collection_id, must be an integer."}), 400

    collection_table = get_table("publication_collection")
    publication_table = get_table("publication")

    if order_by not in publication_table.c:
        return jsonify({"msg": "Invalid order_by field."}), 400

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Check for publication_collection existence
                select_coll_stmt = (
                    select(collection_table.c.project_id)
                    .where(collection_table.c.id == collection_id)
                    .where(collection_table.c.deleted < 1)
                )
                result = connection.execute(select_coll_stmt).first()

                if not result:
                    return jsonify({"msg": "Invalid collection_id, does not exist."}), 400

                # Check that the publication collection belongs to the provided project
                if project_id != result[0]:
                    return jsonify({"msg": f"The publication collection with id '{collection_id}' does not belong to project '{project}'."}), 400

                # Proceed to selecting the publications
                select_pub_stmt = (
                    select(publication_table)
                    .where(publication_table.c.publication_collection_id == collection_id)
                    .where(publication_table.c.deleted < 1)
                    .order_by(publication_table.c[order_by])
                )
                rows = connection.execute(select_pub_stmt).fetchall()
                result = [row._asdict() for row in rows]
                return jsonify(result)

    except Exception as e:
        return jsonify({"msg": "Failed to retrieve publications.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/new/", methods=["POST"])
@project_permission_required
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

        JSON: A success message with the inserted row or an error message.

    Example Request:

        POST /projectname/publication_collection/123/publications/new/
        Body:
        {
            "name": "New Publication",
            "published": 1
        }

    Example Response (Success):

        {
            "msg": "Publication created successfully.",
            "row": {
                "id": 789,
                "name": "New Publication",
                "published": 1,
                ...
            }
        }

    Example Response (Error):

        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
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
        return jsonify({"msg": "Invalid project name."}), 400

    # Convert collection_id to integer and verify
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return jsonify({"msg": "Invalid collection_id, must be an integer."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # Verify that the required 'name' field was provided
    if "name" not in request_data or not request_data["name"]:
        return jsonify({"msg": "Invalid publication name, cannot be empty."}), 400

    # List of fields to check in request_data
    fields = [
        "publication_comment_id",
        "published",
        "legacy_id",
        "original_filename",
        "name",
        "genre",
        "original_publication_date",
        "language"
    ]

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
                        return jsonify({"msg": f"Field '{field}' must be a positive integer."}), 400
                elif field == "published":
                    if not validate_int(request_data[field], 0, 2):
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
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
                    select(collection_table.c.project_id)
                    .where(collection_table.c.id == collection_id)
                )
                result = connection.execute(select_stmt).first()

                if not result:
                    return jsonify({"msg": "Invalid collection_id, does not exist."}), 400

                # Check that the publication collection belongs to the provided project
                if project_id != result[0]:
                    return jsonify({"msg": f"The publication collection with id '{collection_id}' does not belong to project '{project}'."}), 400

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
                        return jsonify({"msg": "Invalid publication_comment_id, does not exist."}), 400

                # Proceed to insert the new publication with provided values
                insert_stmt = (
                    publication_table.insert()
                    .values(**values)
                    .returning(*publication_table.c)  # Return the inserted row
                )
                result = connection.execute(insert_stmt)
                inserted_row = result.fetchone()  # Fetch the inserted row

                if inserted_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Insertion failed: no row returned.",
                        "reason": "The insert statement did not return any data."
                    }), 500

                # Convert the inserted_row to a dictionary for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return jsonify({
                    "msg": "Publication created successfully.",
                    "row": inserted_row_dict
                }), 201

    except Exception as e:
        return jsonify({"msg": "Failed to create new publication.",
                        "reason": str(e)}), 500
