import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import and_, cast, collate, select, text, Text
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required, select_all_from_table, create_translation, create_translation_text, \
    get_translation_text_id, validate_int, create_error_response, create_success_response, \
    build_select_with_filters, get_project_collation


event_tools = Blueprint("event_tools", __name__)
logger = logging.getLogger("sls_api.tools.events")


@event_tools.route("/<project>/locations/new/", methods=["POST"])
@project_permission_required
def add_new_location(project):
    """
    Add a new location object to the database

    POST data MUST be in JSON format.

    POST data MUST contain:
    name: location name

    POST data SHOULD also contain:
    description: location description

    POST data CAN also contain:
    legacy_id: legacy id for location
    latitude: latitude coordinate for location
    longitude: longitude coordinate for location
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "name" not in request_data:
        return jsonify({"msg": "No name in POST data"}), 400

    # Create the translation id
    translation_id = create_translation(request_data["name"])
    # Add a default translation for the location
    create_translation_text(translation_id, "location")

    locations = get_table("location")
    connection = db_engine.connect()

    new_location = {
        "name": request_data["name"],
        "description": request_data.get("description", None),
        "project_id": get_project_id_from_name(project),
        "legacy_id": request_data.get("legacy_id", None),
        "latitude": request_data.get("latitude", None),
        "longitude": request_data.get("longitude", None),
        "translation_id": translation_id
    }
    try:
        with connection.begin():
            insert = locations.insert().values(**new_location)
            result = connection.execute(insert)
            new_row = select(locations).where(locations.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new location with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new location",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/<project>/locations/<location_id>/edit/", methods=["POST"])
@project_permission_required
def edit_location(project, location_id):
    """
    Edit a location object in the database

    POST data MUST be in JSON format.

    POST data CAN contain:
    name: location name
    description: location description
    legacy_id: legacy id for location
    latitude: latitude coordinate for location
    longitude: longitude coordinate for location
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    locations = get_table("location")

    connection = db_engine.connect()
    with connection.begin():
        location_query = select(locations.c.id).where(locations.c.id == int_or_none(location_id))
        location_row = connection.execute(location_query).fetchone()
    if location_row is None:
        return jsonify({"msg": "No location with an ID of {} exists.".format(location_id)}), 404

    name = request_data.get("name", None)
    description = request_data.get("description", None)
    legacy_id = request_data.get("legacy_id", None)
    latitude = request_data.get("latitude", None)
    longitude = request_data.get("longitude", None)
    city = request_data.get("city", None)
    region = request_data.get("region", None)
    source = request_data.get("source", None)
    alias = request_data.get("alias", None)
    deleted = request_data.get("deleted", 0)
    country = request_data.get("country", None)

    values = {}
    if name is not None:
        values["name"] = name
    if description is not None:
        values["description"] = description
    if legacy_id is not None:
        values["legacy_id"] = legacy_id
    if latitude is not None:
        values["latitude"] = latitude
    if longitude is not None:
        values["longitude"] = longitude
    if city is not None:
        values["city"] = city
    if country is not None:
        values["country"] = country
    if region is not None:
        values["region"] = region
    if source is not None:
        values["source"] = source
    if alias is not None:
        values["alias"] = alias
    if deleted is not None:
        values["deleted"] = deleted

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = locations.update().where(locations.c.id == int(location_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated location {} with values {}".format(int(location_id), str(values)),
                    "location_id": int(location_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update location.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@event_tools.route("/<project>/subjects/list/")
@event_tools.route("/<project>/subjects/list/<order_by>/<direction>/")
@project_permission_required
def list_project_subjects(project, order_by="last_name", direction="asc"):
    """
    List all (non-deleted) subjects (persons) for a specified project,
    with optional sorting by subject table columns.

    URL Path Parameters:

    - project (str, required): The name of the project for which to
      retrieve subjects.
    - order_by (str, optional): The column by which to order the subjects.
      For example "last_name" or "first_name". Defaults to "last_name"
      (which applies secondary ordering by the "full_name" column).
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
    - `data`: On success, an array of subject objects; `null` on error.

    Example Request:

        GET /projectname/subjects/list/
        GET /projectname/subjects/list/last_name/asc/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # person records.",
            "data": [
                {
                    "id": 1,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "deleted": 0,
                    "type": "Historical person",
                    "first_name": "John",
                    "last_name": "Doe",
                    "place_of_birth": "Fantasytown",
                    "occupation": "Doctor",
                    "preposition": "von",
                    "full_name": "John von Doe",
                    "description": "a brief description about the person.",
                    "legacy_id": "pe1",
                    "date_born": "1870",
                    "date_deceased": "1915",
                    "project_id": 123,
                    "source": "Encyclopaedia Britannica",
                    "alias": "JD",
                    "previous_last_name": "Crow",
                    "translation_id": 4287
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

    - 200 - OK: The request was successful, and the subjects are returned.
    - 400 - Bad Request: The project name, order_by field, or sort direction
            is invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    subject_table = get_table("subject")

    # Verify order_by and direction
    if order_by not in subject_table.c:
        return create_error_response("Validation error: 'order_by' must be a valid column in the subject table.")

    if direction not in ["asc", "desc"]:
        return create_error_response("Validation error: 'direction' must be either 'asc' or 'desc'.")

    try:
        with db_engine.connect() as connection:
            stmt = (
                select(subject_table)
                .where(subject_table.c.deleted < 1)
                .where(subject_table.c.project_id == project_id)
            )

            # Columns that should use collation-aware sorting
            collation_columns = {
                "last_name", "first_name", "full_name", "type",
                "place_of_birth", "occupation", "description",
                "source", "alias", "previous_last_name"
            }
            collation_name = get_project_collation(project)

            # Build the order_by clause based on multiple columns
            # if ordering by last_name
            order_columns = []

            # Primary column to order by
            col = subject_table.c[order_by]
            if order_by in collation_columns:
                col = collate(col, collation_name)

            # Apply primary ordering
            order_columns.append(col.asc() if direction == "asc" else col.desc())

            # Secondary ordering by full_name if sorting by last_name
            if order_by == "last_name":
                full_name_col = subject_table.c.full_name
                if "full_name" in collation_columns:
                    full_name_col = collate(full_name_col, collation_name)
                order_columns.append(full_name_col.asc()
                                     if direction == "asc"
                                     else full_name_col.desc())

            # Apply multiple order_by clauses
            stmt = stmt.order_by(*order_columns)

            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} person records.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving project subjects.")
        return create_error_response("Unexpected error: failed to retrieve person records in project.", 500)


@event_tools.route("/<project>/subjects/new/", methods=["POST"])
@project_permission_required
def add_new_subject(project):
    """
    Add a new subject (person) object to the specified project.

    URL Path Parameters:

    - project (str, required): The name of the project to which the new person will
      be added.

    POST Data Parameters in JSON Format:

    - type (str): The type of person.
    - first_name (str): The first name of the person.
    - last_name (str): The last name of the person.
    - place_of_birth (str): The place where the person was born.
    - occupation (str): The person's occupation.
    - preposition (str): Prepositional or nobiliary particle used in the
      surname of the person.
    - full_name (str): The full name of the person.
    - description (str): A brief description of the person.
    - legacy_id (str): An identifier from a legacy system.
    - date_born (str, optional): The birth date or year of the person
      (max length 30 characters), in YYYY-MM-DD or YYYY format.
    - date_deceased (str, optional): The date of death of the person
      (max length 30 characters), in YYYY-MM-DD or YYYY format.
    - source (str): The source of the information.
    - alias (str, optional): An alias for the person.
    - previous_last_name (str, optional): The person's previous last name.

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
    - `data`: On success, an object containing the inserted subject
      data; `null` on error.

    Example Request:

        POST /projectname/subjects/new/
        {
            "type": "Historical person",
            "first_name": "Jane",
            "last_name": "Doe",
            "place_of_birth": "Fantasytown",
            "occupation": "Scientist",
            "preposition": "van",
            "full_name": "Jane van Doe",
            "description": "A brief description about the person.",
            "legacy_id": "pe2",
            "date_born": "1850",
            "date_deceased": "1920",
            "source": "Historical Archive",
            "alias": "JD",
            "previous_last_name": "Smith"
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Person record created.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "deleted": 0,
                "type": "Historical person",
                "first_name": "Jane",
                "last_name": "Doe",
                "place_of_birth": "Fantasytown",
                "occupation": "Scientist",
                "preposition": "van",
                "full_name": "Jane van Doe",
                "description": "A brief description about the person.",
                "legacy_id": "pe2",
                "date_born": "1850",
                "date_deceased": "1920",
                "project_id": 123,
                "source": "Historical Archive",
                "alias": "JD",
                "previous_last_name": "Smith",
                "translation_id": 4288
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'date_born' must be 30 or less characters in length.",
            "data": null
        }

    Status Codes:

    - 201 - Created: The subject was created successfully.
    - 400 - Bad Request: No data provided or fields are invalid.
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

    # Verify that "date_born" and "date_deceased" fields are within length limits.
    date_born = request_data.get("date_born")
    if date_born is not None and len(str(date_born)) > 30:
        return create_error_response("Validation error: 'date_born' must be 30 or less characters in length.")

    date_deceased = request_data.get("date_deceased")
    if date_deceased is not None and len(str(date_deceased)) > 30:
        return create_error_response("Validation error: 'date_deceased' must be 30 or less characters in length.")

    # List of fields to check in request_data
    fields = ["type",
              "first_name",
              "last_name",
              "place_of_birth",
              "occupation",
              "preposition",
              "full_name",
              "description",
              "legacy_id",
              "date_born",
              "date_deceased",
              "source",
              "alias",
              "previous_last_name"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if request_data[field] is None:
                values[field] = None
            else:
                # Ensure remaining fields are strings
                request_data[field] = str(request_data[field])

                # Add the field to the insert values
                values[field] = request_data[field]

    values["project_id"] = project_id

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                subject_table = get_table("subject")
                stmt = (
                    subject_table.insert()
                    .values(**values)
                    .returning(*subject_table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(stmt).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Person record created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new subject.")
        return create_error_response("Unexpected error: failed to create new person record.", 500)


@event_tools.route("/<project>/subjects/<subject_id>/edit/", methods=["POST"])
@project_permission_required
def edit_subject(project, subject_id):
    """
    Edit an existing subject (person) object in the specified project by
    updating its fields.

    URL Path Parameters:

    - project (str, required): The name of the project containing the subject
      to be edited.
    - subject_id (int, required): The unique identifier of the subject to be
      updated.

    POST Data Parameters in JSON Format (at least one required):

    - deleted (int): Indicates if the subject is deleted (0 for no,
      1 for yes).
    - type (str): The type of person.
    - first_name (str): The first name of the person.
    - last_name (str): The last name of the person.
    - place_of_birth (str): The place where the person was born.
    - occupation (str): The person's occupation.
    - preposition (str): Prepositional or nobiliary particle used in the
      surname of the person.
    - full_name (str): The full name of the person.
    - description (str): A brief description of the person.
    - legacy_id (str): An identifier from a legacy system.
    - date_born (str, optional): The birth date or year of the person
      (max length 30 characters), in YYYY-MM-DD or YYYY format.
    - date_deceased (str, optional): The date of death of the person
      (max length 30 characters), in YYYY-MM-DD or YYYY format.
    - source (str): The source of the information.
    - alias (str, optional): An alias for the person.
    - previous_last_name (str, optional): The person's previous last name.

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
    - `data`: On success, an object containing the updated subject data;
      `null` on error.

    Example Request:

        POST /projectname/subjects/123/edit/
        {
            "type": "Historical person",
            "first_name": "Jane",
            "last_name": "Doe",
            "place_of_birth": "Fantasytown",
            "occupation": "Scientist",
            "preposition": "van",
            "full_name": "Jane van Doe",
            "description": "An updated description about the person.",
            "legacy_id": "pe2",
            "date_born": "1850",
            "date_deceased": "1920",
            "source": "Historical Archive",
            "alias": "JD",
            "previous_last_name": "Smith",
            "deleted": 0
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Person record updated.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2024-01-01T09:00:00",
                "deleted": 0,
                "type": "Historical person",
                "first_name": "Jane",
                "last_name": "Doe",
                "place_of_birth": "Fantasytown",
                "occupation": "Scientist",
                "preposition": "van",
                "full_name": "Jane van Doe",
                "description": "An updated description about the person.",
                "legacy_id": "pe2",
                "date_born": "1850",
                "date_deceased": "1920",
                "project_id": 123,
                "source": "Historical Archive",
                "alias": "JD",
                "previous_last_name": "Smith",
                "translation_id": 4288
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'subject_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The subject was updated successfully.
    - 400 - Bad Request: No data provided or fields are invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert subject_id to integer and verify
    subject_id = int_or_none(subject_id)
    if not subject_id or subject_id < 1:
        return create_error_response("Validation error: 'subject_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["deleted",
              "type",
              "first_name",
              "last_name",
              "place_of_birth",
              "occupation",
              "preposition",
              "full_name",
              "description",
              "legacy_id",
              "date_born",
              "date_deceased",
              "source",
              "alias",
              "previous_last_name"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                if field == "deleted":
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
                subject_table = get_table("subject")
                stmt = (
                    subject_table.update()
                    .where(subject_table.c.id == subject_id)
                    .where(subject_table.c.project_id == project_id)
                    .values(**values)
                    .returning(*subject_table.c)  # Return the updated row
                )
                updated_row = connection.execute(stmt).first()

                if updated_row is None:
                    # No row was returned: invalid subject_id or project name
                    return create_error_response("Update failed: no person record with the provided 'subject_id' found in project.")

                return create_success_response(
                    message="Person record updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating subject.")
        return create_error_response("Unexpected error: failed to update person record.", 500)


@event_tools.route("/<project>/translation/new/", methods=["POST"])
@project_permission_required
def add_new_translation(project):
    """
    Add a new translation, either for a record that has no previous
    translations, or add a translation in a new language to a record
    that has previous translations.

    URL Path Parameters:

    - project (str, required): The name of the project the translation
      belongs to (must be a valid project name).

    POST Data Parameters in JSON Format:

    - table_name (str, required): name of the table containing the record
      to be translated.
    - field_name (str, required): name of the field to be translated (if
      applicable).
    - text (str, required): the translated text.
    - language (str, required): the language code for the translation
      (ISO 639-1).
    - translation_id (int): the ID of an existing translation record in
      the `translation` table. Required if you intend to add a translation
      in a new language to an entry that already has one or more
      translations.
    - parent_id (int): the ID of the record in the `table_name` table.
    - parent_translation_field (str): the name of the field holding the
      translation_id (defaults to 'translation_id').
    - neutral_text (str): the base text before translation.

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
    - `data`: On success, an object containing the inserted translation
      text data; `null` on error.

    Example Request:

        POST /projectname/translation/new/
        Body:
        {
            "table_name": "subject",
            "field_name": "description",
            "text": "a description of the person",
            "language": "en",
            "parent_id": 958,
            "neutral_text": "en beskrivning av personen"
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Translation created.",
            "data": {
                "id": 123,
                "translation_id": 7387,
                "language": "en",
                "text": "a description of the person",
                "field_name": "description",
                "table_name": "subject",
                "date_created": "2023-05-12T12:34:56",
                "date_modified": null,
                "deleted": 0
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'text' and 'language' required.",
            "data": null
        }

    Return Codes:

    - 201 - Created: Successfully created new translation.
    - 400 - Bad Request: Invalid input.
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

    # List required and optional fields in POST data
    required_fields = ["text", "language"]

    # Check that required fields are in the request data,
    # and that their values are non-empty
    if any(field not in request_data or not request_data[field] for field in required_fields):
        return create_error_response("Validation error: 'text' and 'language' required.")

    table_name = request_data.get("table_name")
    translation_id = request_data.get("translation_id")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Create a new translation base object if not provided
                if translation_id is None:
                    if table_name is None:
                        return create_error_response("Validation error: 'table_name' required when no 'translation_id' provided.")
                    table_name = str(table_name)

                    parent_id = int_or_none(request_data.get("parent_id"))
                    if not validate_int(parent_id, 1):
                        return create_error_response("Validation error: 'parent_id' must be a positive integer.")

                    # Create a new translation base object
                    translation_id = create_translation(
                        request_data.get("neutral_text"),
                        connection
                    )

                    if translation_id is None:
                        return create_error_response("Unexpected error: failed to create new translation.", 500)

                    # Add the translation_id to the record in the parent table.
                    # If the field name for translation_id is something else than
                    # 'translation_id' it must be given in the
                    # "parent_translation_field" in the request data
                    # (in some tables the field name is 'name_translation_id').
                    target_table = get_table(table_name)
                    upd_values = {
                        str(request_data.get("parent_translation_field", "translation_id")): translation_id,
                        "date_modified": datetime.now()
                    }
                    upd_stmt = (
                        target_table.update()
                        .where(target_table.c.id == parent_id)
                        .values(**upd_values)
                        .returning(*target_table.c)
                    )
                    upd_result = connection.execute(upd_stmt).first()

                    # Check if the update in the parent table was successful,
                    # if not, clean up ...
                    if upd_result is None:
                        translation_table = get_table("translation")
                        upd_values = {
                            "deleted": 1,
                            "date_modified": datetime.now()
                        }
                        upd_stmt2 = (
                            translation_table.update()
                            .where(translation_table.c.id == translation_id)
                            .values(**upd_values)
                            .returning(*translation_table.c)
                        )
                        upd_result2 = connection.execute(upd_stmt2).first()

                        upd_error_message = "Update failed: could not link translation to record with 'parent_id' in 'table_name'."
                        if upd_result2 is None:
                            upd_error_message += f" Also failed to mark a created base translation object with ID {translation_id} in the table `translation` as deleted. Please contact support."
                        return create_error_response(upd_error_message, 500)

                # The translation_id has been provided in the POST data.
                # Validate translation_id
                if not validate_int(translation_id, 1):
                    return create_error_response("Validation error: 'translation_id' must be a positive integer.")

                ins_values = {
                    "table_name": table_name,
                    "field_name": request_data.get("field_name"),
                    "text": request_data.get("text"),
                    "language": request_data.get("language"),
                    "translation_id": translation_id
                }

                translation_text = get_table("translation_text")

                ins_stmt = (
                    translation_text.insert()
                    .values(**ins_values)
                    .returning(*translation_text.c)  # Return the inserted row
                )
                inserted_row = connection.execute(ins_stmt).first()

                if inserted_row is None:
                    return create_error_response("Insertion failed: no row returned.", 500)

                return create_success_response(
                    message="Translation created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new translation.")
        return create_error_response("Unexpected error: failed to create new translation.", 500)


@event_tools.route("/<project>/translations/<translation_id>/edit/", methods=["POST"])
@project_permission_required
def edit_translation(project, translation_id):
    """
    Edit a translation object in the database.

    URL Path Parameters:

    - project (str, required): The name of the project.
    - translation_id (int, required): The unique identifier of the
      translation object to be updated.

    POST Data Parameters in JSON Format (at least one required):

    - translation_text_id (int, recommended): ID of the translation text
      object in the `translation_text` table.
    - table_name (str): Name of the table being translated.
    - field_name (str): Name of the field being translated.
    - text (str): The translation text.
    - language (str): Language code of the translation (ISO 639-1).
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.

    If translation_text_id is omitted, an attempt to find the translation
    object which is to be updated is made based on translation_id,
    table_name, field_name and language. If that fails, a new translation
    object will be created.

    In practice, it's always recommended to provide translation_text_id in
    requests to this endpoint. To create a new translation, the
    add_new_translation() endpoint should be used.

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
    - `data`: On success, an object containing the updated translation
      text data; `null` on error.

    Example Request:

        POST /projectname/translations/123/edit/
        Body:
        {
            "translation_text_id": 456,
            "text": "an edited translated text"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Translation text updated.",
            "data": {
                "id": 456,
                "translation_id": 123,
                "language": "en",
                "text": "an edited translated text",
                "field_name": "description",
                "table_name": "subject",
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-10-22T14:17:02",
                "deleted": 0
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'translation_text_id' must be a positive integer.",
            "data": null
        }

    Response Codes:

    - 201 - Created: Successfully created new translation text.
    - 200 - OK: Existing translation text updated.
    - 400 - Bad Request: Invalid input.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert translation_id to integer and verify
    translation_id = int_or_none(translation_id)
    if translation_id is None or translation_id < 1:
        return create_error_response("Validation error: 'translation_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # List of fields to check in request_data
    fields = ["translation_text_id",
              "table_name",
              "field_name",
              "text",
              "language",
              "deleted"]

    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if field == "translation_text_id":
                continue
            elif request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                if field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return create_error_response(f"Validation error: '{field}' must be either 0 or 1.")
                else:
                    # Ensure remaining fields are strings
                    request_data[field] = str(request_data[field])

                # Add the field to the insert values
                values[field] = request_data[field]

    if not values:
        return create_error_response("Validation error: no valid fields provided to update.")

    translation_text_id = request_data.get("translation_text_id")
    if translation_text_id is None:
        # Attempt to get the id of the record in translation_text based on
        # translation id, table name, field name and language in the data
        translation_text_id = get_translation_text_id(translation_id,
                                                      values.get("table_name"),
                                                      values.get("field_name"),
                                                      values.get("language"))

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                translation_text = get_table("translation_text")
                if translation_text_id is None:
                    # Add new row to the translation_text table
                    values["deleted"] = 0
                    values["translation_id"] = translation_id

                    try:
                        ins_stmt = (
                            translation_text.insert()
                            .values(**values)
                            .returning(*translation_text.c)  # Return the inserted row
                        )
                        inserted_row = connection.execute(ins_stmt).first()

                        if inserted_row is None:
                            return create_error_response("Insertion failed: no row returned.", 500)

                        return create_success_response(
                            message="Translation text created.",
                            data=inserted_row._asdict(),
                            status_code=201
                        )

                    except Exception:
                        logger.exception("Exception creating new translation text.")
                        return create_error_response("Unexpected error: failed to create new translation text.", 500)

                else:
                    # Update data of existing translation

                    # Validate translation_text_id
                    translation_text_id = int_or_none(translation_text_id)
                    if translation_text_id is None or not validate_int(translation_text_id, 1):
                        return create_error_response("Validation error: 'translation_text_id' must be a positive integer.")

                    # Add date_modified
                    values["date_modified"] = datetime.now()

                    upd_stmt = (
                        translation_text.update()
                        .where(translation_text.c.id == translation_text_id)
                        .values(**values)
                        .returning(*translation_text.c)  # Return the updated row
                    )
                    updated_row = connection.execute(upd_stmt).first()

                    if updated_row is None:
                        return create_error_response("Update failed: no translation text with the provided 'translation_text_id' found.")

                    return create_success_response(
                        message="Translation text updated.",
                        data=updated_row._asdict()
                    )

    except Exception:
        logger.exception("Exception updating translation text.")
        return create_error_response("Unexpected error: failed to update translation text.", 500)


@event_tools.route("/<project>/translations/<translation_id>/list/", methods=["POST"])
@project_permission_required
def list_translations(project, translation_id):
    """
    List all (non-deleted) translations for a given translation_id
    with optional filters.

    URL Path Parameters:

    - project (str): project name.
    - translation_id (str): The id of the translation object in the
      `translation` table. Must be a valid integer.

    POST Data Parameters in JSON Format (optional):

    - table_name (str): Filter translations by a specific table name.
    - field_name (str): Filter translations by a specific field name.
    - language (str): Filter translations by a specific language.
    - translation_text_id (int): Filter translations by a specific id
      in the `translation_text` table.

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
    - `data`: On success, an array of translation text objects; `null` on
      error.

    Example Request:

        POST /projectname/translations/1/list/
        Body:
        {
            "language": "en"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # translation texts.",
            "data": [
                {
                    "translation_text_id": 123,
                    "translation_id": 1,
                    "language": "en",
                    "text": "Some description in English",
                    "field_name": "description",
                    "table_name": "subject"
                },
                ...
            ]
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'translation_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: Successfully retrieved the list of translation texts.
    - 400 - Bad Request: Invalid or missing translation_id.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Convert translation_id to integer
    translation_id = int_or_none(translation_id)
    if translation_id is None or translation_id < 1:
        return create_error_response("Validation error: 'translation_id' must be a positive integer.")

    # Get optional filters from the request JSON body
    filters = request.get_json(silent=True) or {}
    translation_text_id = int_or_none(filters.get("translation_text_id"))

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Base SQL query
                query = """
                    SELECT
                        id AS translation_text_id,
                        translation_id,
                        language,
                        text,
                        field_name,
                        table_name
                    FROM
                        translation_text
                    WHERE
                        translation_id = :translation_id
                        AND deleted < 1
                """
                # Add additional filters dynamically if present
                query_params = {"translation_id": translation_id}

                # Check if 'table_name' exists in filters
                if "table_name" in filters:
                    if filters["table_name"] is None:
                        query += " AND table_name IS NULL"
                    else:
                        query += " AND table_name = :table_name"
                        query_params["table_name"] = filters["table_name"]

                # Check if 'field_name' exists in filters
                if "field_name" in filters:
                    if filters["field_name"] is None:
                        query += " AND field_name IS NULL"
                    else:
                        query += " AND field_name = :field_name"
                        query_params["field_name"] = filters["field_name"]

                # Check if 'language' exists in filters
                if "language" in filters:
                    if filters["language"] is None:
                        query += " AND language IS NULL"
                    else:
                        query += " AND language = :language"
                        query_params["language"] = filters["language"]

                if translation_text_id:
                    query += " AND id = :translation_text_id"
                    query_params["translation_text_id"] = translation_text_id

                # Add ordering to query
                query += " ORDER BY field_name, language"

                # Execute the query
                statement = text(query).bindparams(**query_params)
                rows = connection.execute(statement).fetchall()

                return create_success_response(
                    message=f"Retrieved {len(rows)} translation texts.",
                    data=[row._asdict() for row in rows]
                )

    except Exception:
        logger.exception("Exception retrieving translations.")
        return create_error_response("Unexpected error: failed to retrieve translations.", 500)


@event_tools.route("/<project>/keywords/list/")
@project_permission_required
def list_project_keywords(project):
    """
    List all non-deleted keywords in the specified project.
    The keywords are alphabetically ordered by name.
    (Note: keywords are named 'tags' in the database.)

    URL Path Parameters:

    - project (str, required): The name of the project to retrieve
      keywords for (must be a valid project name).

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
    - `data`: On success, an array of keyword objects; `null`
       on error.

    Keyword object keys and their data types:

    {
        "id": number,
        "date_created": string | null,
        "date_modified": string | null,
        "deleted": number,
        "type": string | null,
        "name": string | null,
        "description": string | null,
        "legacy_id": string | null,
        "project_id": number,
        "source": string | null,
        "name_translation_id": number | null
    }

    Example Request:

        GET /projectname/keywords

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # keywords.",
            "data": [
                {
                    "id": 123,
                    "date_created": "2023-05-12T12:34:56",
                    "date_modified": "2023-06-01T08:22:11",
                    "deleted": 0,
                    "type": "filosofiska",
                    "name": "spelrumsmodellen",
                    "description": "Description of the keyword.",
                    "legacy_id": "k3524",
                    "project_id": 5,
                    "source": "Encyclopaedia Britannica",
                    "name_translation_id": 86
                },
                ...
            ]
        }

    Status Codes:

    - 200 - OK: The keywords are retrieved successfully.
    - 400 - Bad Request: Invalid project name.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    tag_table = get_table("tag")

    try:
        with db_engine.connect() as connection:
            collation_name = get_project_collation(project)

            stmt = (
                select(*tag_table.c)
                .where(tag_table.c.project_id == project_id)
                .where(tag_table.c.deleted < 1)
                .order_by(
                    collate(tag_table.c.name, collation_name)
                )
            )
            rows = connection.execute(stmt).fetchall()
            return create_success_response(
                message=f"Retrieved {len(rows)} keywords.",
                data=[row._asdict() for row in rows]
            )

    except Exception:
        logger.exception("Exception retrieving project keywords.")
        return create_error_response("Unexpected error: failed to retrieve project keywords.", 500)


@event_tools.route("/<project>/keywords/new/", methods=["POST"])
@project_permission_required
def add_new_keyword(project):
    """
    Add a new keyword object to the specified project.
    (Note: keywords are named 'tags' in the database.)

    URL Path Parameters:

    - project (str, required): The name of the project to add the
      keyword to (must be a valid project name).

    POST Data Parameters in JSON Format:

    - name (str, required): The name of the keyword. Cannot be empty.
    - type (str, optional): The type or classification of the keyword.
      Can be used to group or categorise the keywords.
    - description (str, optional): A description or explanation of the keyword.
    - source (str, optional): A reference to a source where the keyword
      is defined.
    - legacy_id (str, optional): Alternate or legacy ID of the keyword.

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
    - `data`: On success, an object containing the inserted keyword
      data; `null` on error.

    Response object keys and their data types:

    {
        "id": number,
        "date_created": string | null,
        "date_modified": string | null,
        "deleted": number,
        "type": string | null,
        "name": string | null,
        "description": string | null,
        "legacy_id": string | null,
        "project_id": number,
        "source": string | null,
        "name_translation_id": number | null
    }

    Example Request:

        POST /projectname/keywords/new/
        {
            "name": "spelrumsmodellen",
            "type": "filosofiska",
            "description": "metaforisk modell som används för att beskriva balansen mellan frihet och regler i mänsklig handling"
            "source": "Wikipedia",
            "legacy_id": "t42"
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Keyword record created.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": null,
                "deleted": 0,
                "type": "filosofiska",
                "name": "spelrumsmodellen",
                "description": "metaforisk modell som används för att beskriva balansen mellan frihet och regler i mänsklig handling",
                "legacy_id": "t42",
                "project_id": 5,
                "source": "Wikipedia",
                "name_translation_id": null
            }
        }

    Status Codes:

    - 201 - OK: The keyword was created successfully.
    - 400 - Bad Request: No data provided or fields are invalid.
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

    # Verify that the required 'name' field was provided
    if "name" not in request_data or not request_data["name"]:
        return create_error_response("Validation error: 'name' required.")

    # List of fields to check in request_data
    fields = ["name",
              "type",
              "description",
              "source",
              "legacy_id"]

    # Start building the dictionary of inserted values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if not request_data[field]:
                # Field is empty or null
                values[field] = None
            else:
                # Ensure field is saved as a string
                values[field] = str(request_data[field])

    values["project_id"] = project_id

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                tag_table = get_table("tag")
                stmt = (
                    tag_table.insert()
                    .values(**values)
                    .returning(*tag_table.c)  # Return the inserted row
                )
                inserted_row = connection.execute(stmt).first()

                if inserted_row is None:
                    return create_error_response("Failed to create keyword record: no row returned.", 500)

                return create_success_response(
                    message="Keyword record created.",
                    data=inserted_row._asdict(),
                    status_code=201
                )

    except Exception:
        logger.exception("Exception creating new keyword.")
        return create_error_response("Unexpected error: failed to create new keyword record.", 500)


@event_tools.route("/<project>/keywords/<keyword_id>/edit/", methods=["POST"])
@project_permission_required
def edit_keyword(project, keyword_id):
    """
    Edit an existing keyword object in the specified project by
    updating its fields. If the keyword is deleted, it’s connections
    to event occurrences are also deleted.
    (Note: keywords are named 'tags' in the database.)

    URL Path Parameters:

    - project (str, required): The name of the project containing the keyword
      to be edited.
    - keyword_id (int, required): The unique identifier of the keyword to be
      updated.

    POST Data Parameters in JSON Format (at least one required):

    - name (str): The name of the keyword. Cannot be empty.
    - type (str): The type or classification of the keyword.
      Can be used to group or categorise the keywords.
    - description (str): A description or explanation of the keyword.
    - source (str): A reference to a source where the keyword
      is defined.
    - legacy_id (str): Alternate or legacy ID of the keyword.
    - deleted (int): Indicates if the keyword is deleted (0 for no,
      1 for yes).

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
    - `data`: On success, an object containing the updated keyword data;
      `null` on error.

    Response object keys and their data types:

    {
        "id": number,
        "date_created": string | null,
        "date_modified": string | null,
        "deleted": number,
        "type": string | null,
        "name": string | null,
        "description": string | null,
        "legacy_id": string | null,
        "project_id": number,
        "source": string | null,
        "name_translation_id": number | null
    }

    Example Request:

        POST /projectname/keywords/123/edit/
        {
            "name": "spelrumsmodellen"
        }

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Keyword record updated.",
            "data": {
                "id": 123,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2025-05-28T10:08:17",
                "deleted": 0,
                "type": "filosofiska",
                "name": "spelrumsmodellen",
                "description": "metaforisk modell som används för att beskriva balansen mellan frihet och regler i mänsklig handling",
                "legacy_id": "t42",
                "project_id": 5,
                "source": "Wikipedia",
                "name_translation_id": null
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'keyword_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The keyword was updated successfully.
    - 400 - Bad Request: No data provided or fields are invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Convert keyword_id to integer and verify
    keyword_id = int_or_none(keyword_id)
    if not keyword_id or keyword_id < 1:
        return create_error_response("Validation error: 'keyword_id' must be a positive integer.")

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return create_error_response("No data provided.")

    # Verify that the 'name' field is non-empty if provided
    if "name" in request_data and not request_data["name"]:
        return create_error_response("Validation error: 'name' must not be empty.")

    # List of fields to check in request_data
    fields = ["deleted",
              "name",
              "type",
              "description",
              "source",
              "legacy_id"]

    # Start building the dictionary of updated values
    values = {}

    # Loop over the fields list, check each one in request_data and validate
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                if field == "deleted":
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
                tag_table = get_table("tag")
                stmt = (
                    tag_table.update()
                    .where(tag_table.c.id == keyword_id)
                    .where(tag_table.c.project_id == project_id)
                    .values(**values)
                    .returning(*tag_table.c)  # Return the updated row
                )
                updated_row = connection.execute(stmt).first()

                if updated_row is None:
                    # No row was returned: invalid keyword_id or project name
                    return create_error_response(f"Update failed: no keyword with ID '{keyword_id}' found in project.")

                # If the keyword is deleted, also delete any events related to it
                if "deleted" in values and values["deleted"]:
                    connection_table = get_table("event_connection")
                    occurrence_table = get_table("event_occurrence")
                    event_table = get_table("event")

                    del_upd_value = {
                        "deleted": 1,
                        "date_modified": values["date_modified"]
                    }

                    # Subquery: Get event IDs for tag_id (used in two of the updates)
                    event_id_subquery = (
                        select(connection_table.c.event_id)
                        .where(connection_table.c.tag_id == keyword_id)
                    ).scalar_subquery()

                    # 1. Update event_occurrence where event_id matches subquery
                    upd_occ_stmt = (
                        occurrence_table.update()
                        .where(occurrence_table.c.event_id.in_(event_id_subquery))
                        .values(**del_upd_value)
                        .returning(occurrence_table.c.id)
                    )

                    # 2. Update event where id matches same subquery
                    upd_event_stmt = (
                        event_table.update()
                        .where(event_table.c.id.in_(event_id_subquery))
                        .values(**del_upd_value)
                        .returning(event_table.c.id)
                    )

                    # 3. Update event_connection directly using tag_id filter
                    upd_conn_stmt = (
                        connection_table.update()
                        .where(connection_table.c.tag_id == keyword_id)
                        .values(**del_upd_value)
                        .returning(connection_table.c.id)
                    )

                    connection.execute(upd_occ_stmt)
                    connection.execute(upd_event_stmt)
                    connection.execute(upd_conn_stmt)

                return create_success_response(
                    message="Keyword record updated.",
                    data=updated_row._asdict()
                )

    except Exception:
        logger.exception("Exception updating keyword.")
        return create_error_response("Unexpected error: failed to update keyword record.", 500)


@event_tools.route("/<project>/work_manifestation/new/", methods=["POST"])
@project_permission_required
def add_new_work_manifestation(project):
    """
    Add a new work, work_manifestation and work_reference object to the database
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "title" not in request_data:
        return jsonify({"msg": "No name in POST data"}), 400

    works = get_table("work")
    work_manifestations = get_table("work_manifestation")
    work_references = get_table("work_reference")
    connection = db_engine.connect()

    new_work = {
        "title": request_data.get("title", None),
        "description": request_data.get("description", None)
    }

    new_work_manifestation = {
        "title": request_data.get("title", None),
        "description": request_data.get("description", None),
        "type": request_data.get("type", None),
        "legacy_id": request_data.get("legacy_id", None),
        "source": request_data.get("source", None),
        "translated_by": request_data.get("translated_by", None),
        "journal": request_data.get("journal", None),
        "publication_location": request_data.get("publication_location", None),
        "publisher": request_data.get("publisher", None),
        "published_year": request_data.get("published_year", None),
        "volume": request_data.get("volume", None),
        "total_pages": request_data.get("total_pages", None),
        "ISBN": request_data.get("ISBN", None)
    }

    new_work_reference = {
        "reference": request_data.get("reference", None),
        "project_id": get_project_id_from_name(project),
    }

    try:
        with connection.begin():
            insert = works.insert().values(**new_work)
            result = connection.execute(insert)

            work_id = result.inserted_primary_key[0]
            new_work_manifestation["work_id"] = work_id
            insert = work_manifestations.insert().values(**new_work_manifestation)
            result = connection.execute(insert)

            work_manifestation_id = result.inserted_primary_key[0]
            new_work_reference["work_manifestation_id"] = work_manifestation_id
            insert = work_references.insert().values(**new_work_reference)
            result = connection.execute(insert)

            new_row = select(work_manifestations).where(work_manifestations.c.id == work_manifestation_id)
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new work_manifestation with ID {}".format(work_manifestation_id),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new work_manifestation",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/<project>/work_manifestations/<man_id>/edit/", methods=["POST"])
@project_permission_required
def edit_work_manifestation(project, man_id):
    """
    Update work_manifestation object to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: manifestation type
    title: manifestation title

    POST data CAN also contain:
    description: tag description
    legacy_id: Legacy id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    manifestations = get_table("work_manifestation")
    references = get_table("work_reference")

    connection = db_engine.connect()

    # get manifestation data
    with connection.begin():
        query = select(manifestations.c.id).where(manifestations.c.id == int_or_none(man_id))
        row = connection.execute(query).fetchone()
    if row is None:
        return jsonify({"msg": "No manifestation with an ID of {} exists.".format(man_id)}), 404

    # get reference data
    reference = request_data.get("reference", None)
    reference_id = request_data.get("reference_id", None)

    type = request_data.get("type", None)
    title = request_data.get("title", None)
    description = request_data.get("description", None)
    legacy_id = request_data.get("legacy_id", None)
    source = request_data.get("source", None)
    translated_by = request_data.get("translated_by", None)
    journal = request_data.get("journal", None)
    publication_location = request_data.get("publication_location", None)
    publisher = request_data.get("publisher", None)
    published_year = request_data.get("published_year", None)
    volume = request_data.get("volume", None)
    total_pages = request_data.get("total_pages", None)
    isbn = request_data.get("isbn", None)

    values = {}
    if type is not None:
        values["type"] = type
    if title is not None:
        values["title"] = title
    if description is not None:
        values["description"] = description
    if legacy_id is not None:
        values["legacy_id"] = legacy_id
    if source is not None:
        values["source"] = source
    if translated_by is not None:
        values["translated_by"] = translated_by
    if journal is not None:
        values["journal"] = journal
    if publication_location is not None:
        values["publication_location"] = publication_location
    if publisher is not None:
        values["publisher"] = publisher
    if published_year is not None:
        values["published_year"] = published_year
    if volume is not None:
        values["volume"] = volume
    if total_pages is not None:
        values["total_pages"] = total_pages
    if isbn is not None:
        values["isbn"] = isbn

    values["date_modified"] = datetime.now()

    reference_values = {}
    if reference is not None:
        reference_values["reference"] = reference

    if len(values) > 0:
        try:
            with connection.begin():
                update = manifestations.update().where(manifestations.c.id == int(man_id)).values(**values)
                connection.execute(update)
                if len(reference_values) > 0:
                    update_ref = references.update().where(references.c.id == int(reference_id)).values(**reference_values)
                    connection.execute(update_ref)
                return jsonify({
                    "msg": "Updated manifestation {} with values {}".format(int(man_id), str(values)),
                    "man_id": int(man_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update manifestation.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@event_tools.route("/locations/")
@jwt_required()
def get_locations():
    """
    Get all locations from the database
    """
    return select_all_from_table("location")


@event_tools.route("/subjects/")
@jwt_required()
def get_subjects():
    """
    Get all subjects from the database
    """
    connection = db_engine.connect()
    subject = get_table("subject")
    columns = [
        subject.c.id, cast(subject.c.date_created, Text), subject.c.date_created.label('date_created'),
        cast(subject.c.date_modified, Text), subject.c.date_modified.label('date_modified'),
        subject.c.deleted, subject.c.type, subject.c.first_name, subject.c.last_name,
        subject.c.place_of_birth, subject.c.occupation, subject.c.preposition,
        subject.c.full_name, subject.c.description, subject.c.legacy_id,
        cast(subject.c.date_born, Text), subject.c.date_born.label('date_born'),
        cast(subject.c.date_deceased, Text), subject.c.date_deceased.label('date_deceased'),
        subject.c.project_id, subject.c.source
    ]
    stmt = select(columns)
    rows = connection.execute(stmt).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@event_tools.route("/keywords/")
@jwt_required()
def get_keywords():
    """
    Get all keywords from the database
    """
    return select_all_from_table("tag")


@event_tools.route("/work_manifestations/")
@jwt_required()
def get_work_manifestations():
    """
    Get all work_manifestations from the database
    """
    connection = db_engine.connect()
    stmt = """ SELECT w_m.id as id,
                w_m.date_created,
                w_m.date_modified,
                w_m.deleted,
                w_m.title,
                w_m.type,
                w_m.description,
                w_m.source,
                w_m.linked_work_manifestation_id,
                w_m.work_id,
                w_m.work_manuscript_id,
                w_m.translated_by,
                w_m.journal,
                w_m.publication_location,
                w_m.publisher,
                w_m.published_year,
                w_m.volume,
                w_m.total_pages,
                w_m."ISBN",
                w_r.project_id,
                w_r.reference,
                w_r.id as reference_id
                FROM work_manifestation w_m
                JOIN work_reference w_r ON w_r.work_manifestation_id = w_m.id
                ORDER BY w_m.title """
    rows = connection.execute(stmt).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@event_tools.route("/events/")
@jwt_required()
def get_events():
    """
    Get a list of all available events in the database
    """
    return select_all_from_table("event")


@event_tools.route("/events/search/", methods=["POST"])
@jwt_required()
def find_event_by_description():
    """
    List all events whose description contains a given phrase

    POST data MUST be in JSON format.

    POST data MUST contain the following:
    phrase: search-phrase for event description
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "phrase" not in request_data:
        return jsonify({"msg": "No phrase in POST data"}), 400

    events = get_table("event")
    connection = db_engine.connect()

    statement = select(events).where(events.c.description.ilike("%{}%".format(request_data["phrase"])))
    rows = connection.execute(statement).fetchall()

    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@event_tools.route("/<project>/events/new/", methods=["POST"])
@project_permission_required
def add_new_event(project):
    """
    Add a new event to the specified project.

    URL Path Parameters:

    - project (str, required): The name of the project to add the
      event to (must be a valid project name).

    POST Data Parameters in JSON Format:

    - publication_id (int, required): ID of the publication the event is
      related to.
    - Exactly one of the following:
        - subject_id (int): ID of a person/subject record.
        - tag_id (int): ID of a keyword record.
        - location_id (int): ID of a place/location record.
        - work_manifestation_id (int):  ID of a work title record.
        - correspondence_id (int): ID of a correspondence.
    - Optionally exactly one of the following:
        - publication_comment_id (int): ID of a publication comment.
        - publication_facsimile_id (int): ID of a publication facsimile.
        - publication_manuscript_id (int): ID of a publication manuscript.
        - publication_song_id (int): ID of a publication song.
        - publication_version_id (int): ID of a publication version.
    - publication_facsimile_page (int, required if publication_facsimile_id set,
      otherwise ignored): page/image number of a publication facsimile.

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
    - `data`: On success, an object containing the inserted event related
      data; `null` on error.

    Response object keys and their data types:

    {
        "event_id": number,
        "event_connection_id": number,
        "event_occurrence_id": number,
        "publication_id": number,
        "subject_id": number | null,
        "tag_id": number | null,
        "location_id": number | null,
        "work_manifestation_id": number | null,
        "correspondence_id": number | null,
        "publication_comment_id": number | null,
        "publication_facsimile_id": number | null,
        "publication_manuscript_id": number | null,
        "publication_song_id": number | null,
        "publication_version_id": number | null,
        "publication_facsimile_page": number | null
    }

    Example Request:

        POST /projectname/events/new/
        {
            "publication_id": 4751,
            "tag_id": 12
        }

    Example Success Response (HTTP 201):

        {
            "success": true,
            "message": "Connection created.",
            "data": {
                "event_id": 4,
                "event_connection_id": 32,
                "event_occurrence_id": 7,
                "publication_id": 4751,
                "subject_id": null,
                "tag_id": 12,
                "location_id": null,
                "work_manifestation_id": null,
                "correspondence_id": null,
                "publication_comment_id": null,
                "publication_facsimile_id": null,
                "publication_manuscript_id": null,
                "publication_song_id": null,
                "publication_version_id": null,
                "publication_facsimile_page": null
            }
        }

    Status Codes:

    - 201 - OK: The event was created successfully.
    - 400 - Bad Request: No data provided or fields are invalid.
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

    # Verify that the required 'publication_id' field was provided
    publication_id = int_or_none(request_data["publication_id"]) if "publication_id" in request_data else None
    if not publication_id or publication_id < 1:
        return create_error_response("Validation error: 'publication_id' required and must be a positive integer.")

    # List of connection fields to check in request_data, one must be
    # present (if multiple, respond with error)
    connection_fields = ["subject_id",
                         "tag_id",
                         "location_id",
                         "work_manifestation_id",
                         "correspondence_id"]

    present_connection_fields = [key for key in connection_fields if key in request_data and request_data[key] is not None]

    if len(present_connection_fields) != 1:
        return create_error_response("Validation error: exactly one of 'subject_id', 'tag_id', 'location_id', 'work_manifestation_id' and 'correspondence_id' must be provided.")

    connection_field = present_connection_fields[0]

    if not validate_int(request_data[connection_field], 1):
        return create_error_response(f"Validation error: '{connection_field}' must be a positive integer.")

    # List of optional occurrence fields to check in request data,
    # one or none must be present (if multiple, respond with error)
    occurrence_fields = ["publication_comment_id",
                         "publication_facsimile_id",
                         "publication_manuscript_id",
                         "publication_song_id",
                         "publication_version_id"]

    present_occurrence_fields = [key for key in occurrence_fields if key in request_data and request_data[key] is not None]

    if len(present_occurrence_fields) > 1:
        return create_error_response("Validation error: no more than one of 'publication_comment_id', 'publication_facsimile_id', 'publication_manuscript_id', 'publication_song_id' and 'publication_version_id' can be provided.")

    occurrence_field = present_occurrence_fields[0] if len(present_occurrence_fields) == 1 else None

    if occurrence_field and not validate_int(request_data[occurrence_field], 1):
        return create_error_response(f"Validation error: '{occurrence_field}' must be a positive integer.")

    if occurrence_field == "publication_facsimile_id" and ("publication_facsimile_page" not in request_data or int_or_none(request_data["publication_facsimile_page"]) is None):
        return create_error_response("Validation error: 'publication_facsimile_page' must be provided and be an integer.")

    # Form values objects
    connection_values = {}
    occurrence_values = {}

    connection_values[connection_field] = request_data[connection_field]
    connection_values["deleted"] = 0
    occurrence_values["publication_id"] = publication_id
    occurrence_values["deleted"] = 0

    if occurrence_field:
        occurrence_values[occurrence_field] = request_data[occurrence_field]
        if occurrence_field == "publication_facsimile_id":
            occurrence_values["publication_facsimile_page"] = request_data["publication_facsimile_page"]

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                event_table = get_table("event")
                event_connection_table = get_table("event_connection")
                event_occurrence_table = get_table("event_occurrence")

                # Check if an event for this connection and occurrence already exists
                check_ev_conn_subq = build_select_with_filters(event_connection_table,
                                                               connection_values,
                                                               "event_id").scalar_subquery()
                check_ev_occ_subq = build_select_with_filters(event_occurrence_table,
                                                              occurrence_values,
                                                              "event_id").scalar_subquery()

                event_exists_stmt = (
                    select(event_table.c.id)
                    .where(
                        and_(
                            event_table.c.id.in_(check_ev_conn_subq),
                            event_table.c.id.in_(check_ev_occ_subq),
                            event_table.c.deleted == 0
                        )
                    )
                )

                event_ids = connection.execute(event_exists_stmt).fetchall()

                if len(event_ids) > 1:
                    return create_error_response("Unable to create connection: multiple instances of this connection already exist.", 400)
                elif len(event_ids) == 1:
                    return create_error_response("Unable to create connection: a connection with the given publication and reference already exists.")

                # Proceed with creating an event for this connection and occurrence,
                # as there is no existing event.
                event_values = {"description": f"project {project_id}"}

                # Insert event query
                insert_ev_stmt = (
                    event_table.insert()
                    .values(**event_values)
                    .returning(event_table.c.id)  # Return the ID of the inserted row
                )

                # Execute insert event query first to get an event ID
                insert_ev_result = connection.execute(insert_ev_stmt).first()
                if insert_ev_result is None:
                    return create_error_response("Unexpected error: failed to insert new event in the database.", 500)

                event_id = insert_ev_result[0]
                connection_values["event_id"] = event_id
                occurrence_values["event_id"] = event_id

                # Insert event connection query
                insert_ev_conn_stmt = (
                    event_connection_table.insert()
                    .values(**connection_values)
                    .returning(*event_connection_table.c)  # Return the inserted row
                )

                # Insert event occurrence query
                insert_ev_occu_stmt = (
                    event_occurrence_table.insert()
                    .values(**occurrence_values)
                    .returning(*event_occurrence_table.c)  # Return the inserted row
                )

                insert_conn_result = connection.execute(insert_ev_conn_stmt).first()
                insert_occu_result = connection.execute(insert_ev_occu_stmt).first()

                if insert_conn_result is None:
                    return create_error_response("Unexpected error: failed to insert new event connection in the database.", 500)
                if insert_occu_result is None:
                    return create_error_response("Unexpected error: failed to insert new event occurrence in the database.", 500)

                response_data = {
                    "event_id":                   event_id,
                    "event_connection_id":        insert_conn_result["id"],
                    "event_occurrence_id":        insert_occu_result["id"],
                    "publication_id":             insert_occu_result["publication_id"],
                    "subject_id":                 insert_conn_result["subject_id"],
                    "tag_id":                     insert_conn_result["tag_id"],
                    "location_id":                insert_conn_result["location_id"],
                    "work_manifestation_id":      insert_conn_result["work_manifestation_id"],
                    "correspondence_id":          insert_conn_result["correspondence_id"],
                    "publication_comment_id":     insert_occu_result["publication_comment_id"],
                    "publication_facsimile_id":   insert_occu_result["publication_facsimile_id"],
                    "publication_manuscript_id":  insert_occu_result["publication_manuscript_id"],
                    "publication_song_id":        insert_occu_result["publication_song_id"],
                    "publication_version_id":     insert_occu_result["publication_version_id"],
                    "publication_facsimile_page": insert_occu_result["publication_facsimile_page"]
                }

                return create_success_response(
                    message="Connection created.",
                    data=response_data,
                    status_code=201
                )

    except ValueError:
        logger.exception("Invalid query parameters for building select statement.")
        return create_error_response("Unexpected error: ValueError building select statement.", 500)
    except Exception:
        logger.exception("Exception creating new connection.")
        return create_error_response("Unexpected error: failed to create new connection.", 500)


@event_tools.route("/<project>/events/<event_id>/delete/", methods=["POST"])
@project_permission_required
def delete_event(project, event_id):
    """
    Delete the event, event connection and event occurrence with the
    specified event ID.

    URL Path Parameters:

    - project (str, required): The name of the project the event occurs
      in. Strictly it doesn’t matter which project name is given as long
      as it’s a valid project name because currently events in the
      database don’t include project information.
    - event_id (int, required): The unique identifier of the event to
      be deleted.

    POST Data Parameters in JSON Format:

    - None.

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
    - `data`: On success, an object containing the updated event data;
      `null` on error.

    Response object keys and their data types:

    {
        "event_id": number,
        "event_connection_id": number,
        "event_occurrence_id": number,
        "deleted": number
    }

    Example Request:

        POST /projectname/events/123/delete/
        {}

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Connection deleted.",
            "data": {
                "event_id": 123,
                "event_connection_id": 46,
                "event_occurrence_id": 94,
                "deleted": 1
            }
        }

    Example Error Response (HTTP 400):

        {
            "success": false,
            "message": "Validation error: 'event_id' must be a positive integer.",
            "data": null
        }

    Status Codes:

    - 200 - OK: The event was successfully deleted.
    - 400 - Bad Request: Fields are invalid.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return create_error_response("Validation error: 'project' does not exist.")

    # Verify that event_id is a positive integer
    event_id = int_or_none(event_id)
    if not validate_int(event_id, 1):
        return create_error_response("Validation error: 'event_id' must be a positive integer.")

    upd_values = {
        "deleted": 1,
        "date_modified": datetime.now()
    }

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                event_table = get_table("event")
                event_connection_table = get_table("event_connection")
                event_occurrence_table = get_table("event_occurrence")

                # Check that the event_id is valid and non-deleted
                event_exists_stmt = (
                    select(event_table.c.id)
                    .where(event_table.c.id == event_id)
                    .where(event_table.c.deleted == 0)
                )

                event_ids = connection.execute(event_exists_stmt).fetchall()

                if len(event_ids) < 1:
                    return create_error_response("Failed to delete connection: invalid event ID or an event for the connection does not exist.")
                elif len(event_ids) > 1:
                    return create_error_response("Failed to delete connection: event ID is referenced by multiple connection or occurrence rows. This may be legacy data and must be reviewed manually.")

                # Delete event
                upd_ev_stmt = (
                    event_table.update()
                    .where(event_table.c.id == event_id)
                    .where(event_table.c.deleted == 0)
                    .values(**upd_values)
                    .returning(*event_table.c)  # Return the updated row
                )
                upd_ev_row = connection.execute(upd_ev_stmt).first()

                if upd_ev_row is None:
                    # No row was returned: invalid event_id
                    return create_error_response("Failed to delete connection: invalid event ID or the event is already deleted.")

                # Delete event connection
                upd_ev_conn_stmt = (
                    event_connection_table.update()
                    .where(event_connection_table.c.event_id == event_id)
                    .where(event_connection_table.c.deleted == 0)
                    .values(**upd_values)
                    .returning(*event_connection_table.c)  # Return the updated row
                )
                upd_ev_conn_row = connection.execute(upd_ev_conn_stmt).first()

                if upd_ev_conn_row is None:
                    # No row was returned: invalid event_id
                    return create_error_response("Failed to delete connection: invalid event ID or the event connection is already deleted.")

                # Delete event occurrence
                upd_ev_occu_stmt = (
                    event_occurrence_table.update()
                    .where(event_occurrence_table.c.event_id == event_id)
                    .where(event_occurrence_table.c.deleted == 0)
                    .values(**upd_values)
                    .returning(*event_occurrence_table.c)  # Return the updated row
                )
                upd_ev_occu_row = connection.execute(upd_ev_occu_stmt).first()

                if upd_ev_occu_row is None:
                    # No row was returned: invalid event_id
                    return create_error_response("Failed to delete connection: invalid event ID or the event occurrence is already deleted.")

                response_data = {
                    "event_id":            upd_ev_row["id"],
                    "event_connection_id": upd_ev_conn_row["id"],
                    "event_occurrence_id": upd_ev_occu_row["id"],
                    "deleted":             upd_ev_row["deleted"]
                }

                return create_success_response(
                    message="Connection deleted.",
                    data=response_data
                )

    except Exception:
        logger.exception("Exception deleting event.")
        return create_error_response("Unexpected error: failed to delete the connection.", 500)
