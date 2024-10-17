from flask import Blueprint, jsonify, request
from sqlalchemy import select, text
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
    Edit a facsimile_collection object in the database

    POST data MUST be in JSON format.
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    facsimile_collections = get_table("publication_facsimile_collection")

    connection = db_engine.connect()
    with connection.begin():
        facsimile_collections_query = select(facsimile_collections.c.id).where(facsimile_collections.c.id == int_or_none(facsimile_collection_id))
        facsimile_collections_row = connection.execute(facsimile_collections_query).fetchone()
    if facsimile_collections_row is None:
        return jsonify({"msg": "No facsimile collection with an ID of {} exists.".format(facsimile_collection_id)}), 404

    title = request_data.get("title", None)
    number_of_pages = request_data.get("number_of_pages", 0)
    start_page_number = request_data.get("start_page_number", 0)
    description = request_data.get("description", None)
    folder_path = request_data.get("folder_path", None)
    page_comment = request_data.get("page_comment", None)
    external_url = request_data.get("external_url", None)

    values = {}
    if title is not None:
        values["title"] = title
    if number_of_pages is not None:
        values["number_of_pages"] = number_of_pages
    if start_page_number is not None:
        values["start_page_number"] = start_page_number
    if description is not None:
        values["description"] = description
    if folder_path is not None:
        values["folder_path"] = folder_path
    if page_comment is not None:
        values["page_comment"] = page_comment
    if external_url is not None:
        values["external_url"] = external_url

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = facsimile_collections.update().where(facsimile_collections.c.id == int(facsimile_collection_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated facsimile_collection {} with values {}".format(int(facsimile_collection_id), str(values)),
                    "facsimile_collection_id": int(facsimile_collection_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update facsimile_collections.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@collection_tools.route("/<project>/facsimile_collection/list/")
@project_permission_required
def list_facsimile_collections(project):
    """
    List all publication_collection objects for a given project
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    # Select all publication_collection objects that are linked to the
    # project through publication_facsimile -> publication ->
    # publication_collection
    # AND all publication_collection objects that are orphans, i.e. they
    # are not linked to any publication_collection
    statement = """ select * from publication_facsimile_collection where deleted != 1 AND (
                    id in
                    (
                        select publication_facsimile_collection_id from publication_facsimile where publication_id in (
                            select id from publication where publication_collection_id in (
                                select id from publication_collection where project_id = :project_id and deleted != 1
                            )
                        )
                    ) or
                    id not in
                    (
                        select publication_facsimile_collection_id from publication_facsimile where publication_id in (
                            select id from publication where publication_collection_id in (
                                select id from publication_collection where deleted != 1
                            )
                        )
                    )
                )"""
    statement = text(statement).bindparams(project_id=project_id)
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/link/", methods=["POST"])
@project_permission_required
def link_facsimile_collection_to_publication(project, collection_id):
    """
    Link a publication_facsimile_collection to a publication through publication_facsimile table

    POST data MUST be in JSON format.

    POST data MUST contain the following:
    publication_id: ID for the publication to link to

    POST data MAY also contain the following:
    publicationManuscript_id: ID for the specific publication manuscript to link to
    publicationVersion_id: ID for the specific publication version to link to
    sectionId: Section or chapter number for this particular facsimile
    pageNr: Page number for link
    priority: Priority number for this link
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "publication_id" not in request_data:
        return jsonify({"msg": "No publication_id in POST data."}), 400

    connection = db_engine.connect()
    publication_id = int_or_none(request_data["publication_id"])
    project_id = get_project_id_from_name(project)

    publication_facsimiles = get_table("publication_facsimile")
    publication_collections = get_table("publication_collection")
    publications = get_table("publication")
    with connection.begin():
        statement = select(publications.c.publication_collection_id).where(publications.c.id == publication_id)
        result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404
    publication_collection_id = int_or_none(result[0].publication_collection_id)

    with connection.begin():
        statement = select(publication_collections.c.project_id).where(publication_collections.c.id == publication_collection_id)
        result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404

    if result[0].project_id != project_id:
        return jsonify(
            {
                "msg": "Publication {} appears to not belong to project {!r}".format(publication_id, project)
            }
        ), 400

    new_facsimile = {
        "publication_facsimile_collection_id": collection_id,
        "publication_id": publication_id,
        "publication_manuscript_id": request_data.get("publication_manuscript_id", None),
        "publication_version_id": request_data.get("publication_version_id", None),
        "page_nr": request_data.get("page", 0),
        "section_id": request_data.get("section_id", 0),
        "priority": request_data.get("priority", 0),
        "type": request_data.get("type", 0)
    }
    try:
        with connection.begin():
            insert = publication_facsimiles.insert().values(**new_facsimile)
            result = connection.execute(insert)
            new_row = select(publication_facsimiles).where(publication_facsimiles.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            else:
                new_row = {}
            result = {
                "msg": "Created new publication_facsimile with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new publication_facsimile",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


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
@project_permission_required
def list_facsimile_collection_links(project, collection_id):
    """
    List all publication_facsimile objects in the given publication_facsimile_collection
    """
    connection = db_engine.connect()
    facsimiles = get_table("publication_facsimile")
    statement = select(facsimiles).where(facsimiles.c.publication_facsimile_collection_id == int_or_none(collection_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/facsimile_publication/delete/<f_pub_id>", methods=["DELETE"])
@project_permission_required
def delete_facsimile_collection_link(project, f_pub_id):
    """
    List all publication_facsimile objects in the given publication_facsimile_collection
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
