import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import asc, cast, desc, select, text, Text
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required, select_all_from_table, create_translation, create_translation_text, \
    get_translation_text_id, validate_int, create_error_response, create_success_response


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
            "data": dict or None
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, a list of subject objects; `null` on error.

    Example Request:

        GET /projectname/subjects/list/
        GET /projectname/subjects/list/last_name/asc/

    Example Success Response (HTTP 200):

        {
            "success": true,
            "message": "Retrieved # records.",
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

            # Build the order_by clause based on multiple columns
            # if ordering by last_name
            order_columns = []

            if direction == "asc":
                order_columns.append(
                    asc(subject_table.c[order_by])
                )
                if order_by == "last_name":
                    order_columns.append(
                        asc(subject_table.c.full_name)
                    )
            else:
                order_columns.append(
                    desc(subject_table.c[order_by])
                )
                if order_by == "last_name":
                    order_columns.append(
                        desc(subject_table.c.full_name)
                    )

            # Apply multiple order_by clauses
            stmt = stmt.order_by(*order_columns)
            rows = connection.execute(stmt).fetchall()

            return create_success_response(
                message=f"Retrieved {len(rows)} records.",
                data=[row._asdict() for row in rows]
            )

    except Exception as e:
        logger.exception(f"Exception retrieving project subjects: {str(e)}")
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
            "data": dict or None
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, a dictionary containing the inserted subject
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
                    # No row was returned; handle accordingly
                    return create_error_response("Insertion failed: no row returned.", 500)

                # Convert the inserted_row to a dictionary for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return create_success_response(
                    message="Person record created.",
                    data=inserted_row_dict,
                    status_code=201
                )

    except Exception as e:
        logger.exception(f"Exception creating new subject: {str(e)}")
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
            "data": dict or None
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, a dictionary containing the updated subject
      data; `null` on error.

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

                # Convert the inserted row to a dict for JSON serialization
                updated_row_dict = updated_row._asdict()

                return create_success_response(
                    message="Person record updated.",
                    data=updated_row_dict
                )

    except Exception as e:
        logger.exception(f"Exception updating subject: {str(e)}")
        return create_error_response("Unexpected error: failed to update person record.", 500)


@event_tools.route("/<project>/translation/new/", methods=["POST"])
@project_permission_required
def add_new_translation(project):
    """
    Add a new translation, either for a record that has no previous
    translations, or add a translation in a new language to a record
    that has previous translations.

    URL Path Parameters:

    - project (str, required): The name of the project the translation belongs to
      (must be a valid project name).

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
            "data": dict or None
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, a dictionary containing the inserted translation
      text object; `null` on error.

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
                    translation_id = create_translation(request_data.get("neutral_text"))

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

                # Convert the inserted_row to a dictionary for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return create_success_response(
                    message="Translation created.",
                    data=inserted_row_dict,
                    status_code=201
                )

    except Exception as e:
        logger.exception(f"Exception creating new translation: {str(e)}")
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
    - table_name (str): name of the table being translated.
    - field_name (str): name of the field being translated.
    - text (str) the translation text.
    - language (str): language code of the translation (ISO 639-1).
    - deleted (int): flag to mark as deleted (0 for no and 1 for yes).

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
            "data": dict or None
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, a dictionary containing the updated translation
      text object; `null` on error.

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

                        # Convert the inserted_row to a dictionary for JSON serialization
                        inserted_row_dict = inserted_row._asdict()

                        return create_success_response(
                            message="Translation text created.",
                            data=inserted_row_dict,
                            status_code=201
                        )

                    except Exception as e:
                        logger.exception(f"Exception creating new translation text: {str(e)}")
                        return create_error_response("Unexpected error: failed to create new translation text.", 500)

                else:
                    # Update data of existing translation

                    # Validate translation_text_id
                    translation_text_id = int_or_none(translation_text_id)
                    if translation_text_id is None or validate_int(translation_text_id, 1):
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

                    # Convert the inserted row to a dict for JSON serialization
                    updated_row_dict = updated_row._asdict()

                    return create_success_response(
                        message="Translation text updated.",
                        data=updated_row_dict
                    )

    except Exception as e:
        logger.exception(f"Exception updating translation text: {str(e)}")
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
            "data": dict or None
        }

    - `success`: A boolean indicating whether the operation was successful.
    - `message`: A string containing a descriptive message about the result.
    - `data`: On success, a list of translation text objects; `null` on
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
            "message": "Retrieved # records.",
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
                    message=f"Retrieved {len(rows)} records.",
                    data=[row._asdict() for row in rows]
                )

    except Exception as e:
        logger.exception(f"Exception retrieving translations: {str(e)}")
        return create_error_response("Unexpected error: failed to retrieve translations.", 500)


@event_tools.route("/<project>/tags/new/", methods=["POST"])
@project_permission_required
def add_new_tag(project):
    """
    Add a new tag object to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: tag type
    name: tag name

    POST data CAN also contain:
    description: tag description
    legacy_id: Legacy id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    tags = get_table("tag")
    connection = db_engine.connect()

    new_tag = {
        "type": request_data.get("type", None),
        "name": request_data.get("name", None),
        "project_id": get_project_id_from_name(project),
        "description": request_data.get("description", None),
        "legacy_id": request_data.get("legacy_id", None)
    }
    try:
        with connection.begin():
            insert = tags.insert().values(**new_tag)
            result = connection.execute(insert)
            new_row = select(tags).where(tags.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new tag with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new tag",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/<project>/tags/<tag_id>/edit/", methods=["POST"])
@project_permission_required
def edit_tag(project, tag_id):
    """
    Update tag object to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: tag type
    name: tag name

    POST data CAN also contain:
    description: tag description
    legacy_id: Legacy id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    tags = get_table("tag")

    connection = db_engine.connect()
    with connection.begin():
        tag_query = select(tags.c.id).where(tags.c.id == int_or_none(tag_id))
        tag_row = connection.execute(tag_query).fetchone()
    if tag_row is None:
        return jsonify({"msg": "No tag with an ID of {} exists.".format(tag_id)}), 404

    type = request_data.get("type", None)
    name = request_data.get("name", None)
    description = request_data.get("description", None)
    legacy_id = request_data.get("legacy_id", None)

    values = {}
    if type is not None:
        values["type"] = type
    if name is not None:
        values["name"] = name
    if description is not None:
        values["description"] = description
    if legacy_id is not None:
        values["legacy_id"] = legacy_id

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = tags.update().where(tags.c.id == int(tag_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated tag {} with values {}".format(int(tag_id), str(values)),
                    "tag_id": int(tag_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update tag.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


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


@event_tools.route("/tags/")
@jwt_required()
def get_tags():
    """
    Get all tags from the database
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


@event_tools.route("/events/new/", methods=["POST"])
@jwt_required()
def add_new_event():
    """
    Add a new event to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: event type
    description: event description
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = get_table("event")
    connection = db_engine.connect()

    new_event = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
    }
    try:
        with connection.begin():
            insert = events.insert().values(**new_event)
            result = connection.execute(insert)
            new_row = select(events).where(events.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new event with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new event",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/event/<event_id>/connections/new/", methods=["POST"])
@jwt_required()
def connect_event(event_id):
    """
    Link an event to a location, subject, or tag through event_connection

    POST data MUST be in JSON format.

    POST data MUST contain at least one of the following:
    subject_id: ID for the subject involved in the given event
    location_id: ID for the location involved in the given event
    tag_id: ID for the tag involved in the given event
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = get_table("event")
    connection = db_engine.connect()
    with connection.begin():
        select_event = select(events).where(events.c.id == int_or_none(event_id))
        event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404
    event_connections = get_table("event_connection")
    new_event_connection = {
        "event_id": int(event_id),
        "subject_id": int(request_data["subject_id"]) if request_data.get("subject_id", None) else None,
        "location_id": int(request_data["location_id"]) if request_data.get("location_id", None) else None,
        "tag_id": int(request_data["tag_id"]) if request_data.get("tag_id", None) else None
    }
    try:
        with connection.begin():
            insert = event_connections.insert().values(**new_event_connection)
            result = connection.execute(insert)
            new_row = select(event_connections).where(event_connections.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new event_connection with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new event_connection",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/event/<event_id>/connections/")
@jwt_required()
def get_event_connections(event_id):
    """
    List all event_connections for a given event, to find related locations, subjects, and tags
    """
    event_connections = get_table("event_connection")
    connection = db_engine.connect()
    statement = select(event_connections).where(event_connections.c.event_id == int_or_none(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@event_tools.route("/event/<event_id>/occurrences/")
@jwt_required()
def get_event_occurrences(event_id):
    """
    Get a list of all event_occurrence in the database, optionally limiting to a given event
    """
    event_occurrences = get_table("event_occurrence")
    connection = db_engine.connect()
    statement = select(event_occurrences).where(event_occurrences.c.event_id == int_or_none(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@event_tools.route("/event/<event_id>/occurrences/new/", methods=["POST"])
@jwt_required()
def new_event_occurrence(event_id):
    """
    Add a new event_occurrence to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain the following:
    type: event occurrence type
    description: event occurrence description

    POST data SHOULD also contain at least one of the following:
    publication_id: ID for publication the event occurs in
    publicationVersion_id: ID for publication version the event occurs in
    publicationManuscript_id: ID for publication manuscript the event occurs in
    publicationFacsimile_id: ID for publication facsimile the event occurs in
    publicationComment_id: ID for publication comment the event occurs in
    publicationFacsimile_page: Number for publication facsimile page the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = get_table("event")
    connection = db_engine.connect()
    with connection.begin():
        select_event = select(events).where(events.c.id == int_or_none(event_id))
        event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404

    event_occurrences = get_table("event_occurrence")
    new_occurrence = {
        "event_id": int(event_id),
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "publication_id": int(request_data["publication_id"]) if request_data.get("publication_id", None) else None,
        "publication_version_id": int(request_data["publicationVersion_id"]) if request_data.get("publicationVersion_id", None) else None,
        "publication_manuscript_id": int(request_data["publicationManuscript_id"]) if request_data.get("publicationManuscript_id", None) else None,
        "publication_facsimile_id": int(request_data["publicationFacsimile_id"]) if request_data.get("publicationFacsimile_id", None) else None,
        "publication_comment_id": int(request_data["publicationComment_id"]) if request_data.get("publicationComment_id", None) else None,
        "publication_facsimile_page": int(request_data["publicationFacsimile_page"]) if request_data.get("publicationFacsimile_page", None) else None,
    }
    try:
        with connection.begin():
            insert = event_occurrences.insert().values(**new_occurrence)
            result = connection.execute(insert)
            new_row = select(event_occurrences).where(event_occurrences.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new event_occurrence with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new event_occurrence",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/event/<publication_id>/occurrences/add/", methods=["POST"])
@jwt_required()
def new_publication_event_occurrence(publication_id):
    """
    Add a new event_occurrence to the publication

    POST data MUST be in JSON format.

    POST data MUST contain the following:
    publication_id: ID for publication the event occurs in
    tag_id: ID for publication the event occurs in

    POST data MAY contain the following:
    publicationFacsimile_page: Number for publication facsimile page the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    event_occ = get_table("event_occurrence")
    connection = db_engine.connect()
    with connection.begin():
        select_event = select(event_occ.c.event_id).where(event_occ.c.publication_id == int_or_none(publication_id)).where(event_occ.c.deleted != 1)
        result = connection.execute(select_event).fetchone()
    if int_or_none(result["event_id"]) is None:
        event_id = int_or_none(result)
    else:
        event_id = int_or_none(result["event_id"])
    # No existing connection between publication and event, we need to create an event
    if event_id is None:
        # create event
        events = get_table("event")
        new_event = {
            "type": "publication",
            "description": "publication->tag",
        }
        try:
            with connection.begin():
                insert = events.insert().values(**new_event)
                result = connection.execute(insert)
                event_id = result.inserted_primary_key[0]
        except Exception as e:
            result = {
                "msg": "Failed to create new event",
                "reason": str(e)
            }
            return jsonify(result), 500

        # Create the occurrence, connection between publication and event
        new_occurrence = {
            "event_id": int(event_id),
            "type": request_data.get("type", None),
            "description": request_data.get("description", None),
            "publication_id": int(request_data["publication_id"]) if request_data.get("publication_id", None) else None,
            "publication_facsimile_page": int(request_data["publication_facsimile_page"]) if request_data.get("publication_facsimile_page", None) else None,
        }
        try:
            with connection.begin():
                insert = event_occ.insert().values(**new_occurrence)
                connection.execute(insert)
        except Exception as e:
            result = {
                "msg": "Failed to create new event_occurrence",
                "reason": str(e)
            }
            return jsonify(result), 500

        # Create the connection between tag and event
        event_conn = get_table("event_connection")
        new_connection = {
            "event_id": int(event_id),
            "tag_id": request_data.get("tag_id", None)
        }
        try:
            with connection.begin():
                insert = event_conn.insert().values(**new_connection)
                connection.execute(insert)
        except Exception as e:
            result = {
                "msg": "Failed to create new event_connection",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        try:
            new_connection = {
                "event_id": int(event_id),
                "tag_id": request_data.get("tag_id", None)
            }
            with connection.begin():
                event_conn = get_table("event_connection")
                insert = event_conn.insert().values(**new_connection)
                result = connection.execute(insert)
                new_row = select(event_conn).where(event_conn.c.id == result.inserted_primary_key[0])
                if new_row is not None:
                    new_row = new_row._asdict()
                result = {
                    "msg": "Created new event_connection with ID {}".format(result.inserted_primary_key[0]),
                    "row": new_row
                }
                return jsonify(result), 201
        except Exception as e:
            result = {
                "msg": "Failed to create new event_connection",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()


@event_tools.route("/event/<occ_id>/occurrences/edit/", methods=["POST"])
@jwt_required()
def edit_event_occurrence(occ_id):
    """
    Edit a event_occurrence
    id of the event_occurrence: Number for publication facsimile page the event occurs in
    publication_facsimile_page: Number for publication facsimile page the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    publication_facsimile_page = request_data.get("publication_facsimile_page", None)

    values = {}
    if publication_facsimile_page is not None:
        values["publication_facsimile_page"] = publication_facsimile_page

    values["date_modified"] = datetime.now()
    connection = db_engine.connect()
    event_occurrences = get_table("event_occurrence")
    try:
        with connection.begin():
            update = event_occurrences.update().where(event_occurrences.c.id == int(occ_id)).values(**values)
            connection.execute(update)
            return jsonify({
                "msg": "Updated event_occurrences {} with values {}".format(int(occ_id), str(values)),
                "occ_id": int(occ_id)
            })
    except Exception as e:
        result = {
            "msg": "Failed to update event_occurrences.",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/event/<occ_id>/occurrences/delete/", methods=["POST"])
@jwt_required()
def delete_event_occurrence(occ_id):
    """
    Logical delete a event_occurrence
    id of the event_occurrence: Number for publication facsimile page the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    values = {
        "date_modified": datetime.now(),
        "deleted": 1
    }

    connection = db_engine.connect()
    event_occurrences = get_table("event_occurrence")
    try:
        with connection.begin():
            update = event_occurrences.update().where(event_occurrences.c.id == int(occ_id)).values(**values)
            connection.execute(update)
            return jsonify({
                "msg": "Delete event_occurrences {} with values {}".format(int(occ_id), str(values)),
                "occ_id": int(occ_id)
            })
    except Exception as e:
        result = {
            "msg": "Failed to delete event_occurrences.",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()
