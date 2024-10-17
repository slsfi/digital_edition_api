from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import cast, select, text, Text
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required, select_all_from_table, create_translation, create_translation_text, \
    get_translation_text_id

event_tools = Blueprint("event_tools", __name__)


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


@event_tools.route("/<project>/subjects/new/", methods=["POST"])
@project_permission_required
def add_new_subject(project):
    """
    Add a new subject object to the database

    POST data MUST be in JSON format

    POST data SHOULD contain:
    type: subject type
    description: subject description

    POST data CAN also contain:
    first_name: Subject first or given name
    last_name Subject surname
    preposition: preposition for subject
    full_name: Subject full name
    legacy_id: Legacy id for subject
    date_born: Subject date of birth
    date_deceased: Subject date of death
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    subjects = get_table("subject")
    connection = db_engine.connect()

    new_subject = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "project_id": get_project_id_from_name(project),
        "first_name": request_data.get("first_name", None),
        "last_name": request_data.get("last_name", None),
        "preposition": request_data.get("preposition", None),
        "full_name": request_data.get("full_name", None),
        "legacy_id": request_data.get("legacy_id", None),
        "date_born": request_data.get("date_born", None),
        "date_deceased": request_data.get("date_deceased", None)
    }
    try:
        with connection.begin():
            insert = subjects.insert().values(**new_subject)
            result = connection.execute(insert)
            new_row = select(subjects).where(subjects.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new subject with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new subject.",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/<project>/subjects/<subject_id>/edit/", methods=["POST"])
@project_permission_required
def edit_subject(project, subject_id):
    """
    Edit a subject object in the database

    POST data MUST be in JSON format

    POST data CAN contain:
    type: subject type
    description: subject description
    first_name: Subject first or given name
    last_name: Subject surname
    preposition: preposition for subject
    full_name: Subject full name
    legacy_id: Legacy id for subject
    date_born: Subject date of birth
    date_deceased: Subject date of death
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    subjects = get_table("subject")

    connection = db_engine.connect()
    with connection.begin():
        subject_query = select(subjects.c.id).where(subjects.c.id == int_or_none(subject_id))
        subject_row = connection.execute(subject_query).fetchone()
    if subject_row is None:
        return jsonify({"msg": "No subject with an ID of {} exists.".format(subject_id)}), 404

    subject_type = request_data.get("type", None)
    description = request_data.get("description", None)
    first_name = request_data.get("first_name", None)
    last_name = request_data.get("last_name", None)
    preposition = request_data.get("preposition", None)
    full_name = request_data.get("full_name", None)
    legacy_id = request_data.get("legacy_id", None)
    date_born = request_data.get("date_born", None)
    date_deceased = request_data.get("date_deceased", None)

    values = {}
    if subject_type is not None:
        values["type"] = subject_type
    if description is not None:
        values["description"] = description
    if first_name is not None:
        values["first_name"] = first_name
    if last_name is not None:
        values["last_name"] = last_name
    if preposition is not None:
        values["preposition"] = preposition
    if full_name is not None:
        values["full_name"] = full_name
    if legacy_id is not None:
        values["legacy_id"] = legacy_id
    if date_born is not None:
        values["date_born"] = date_born
    if date_deceased is not None:
        values["date_deceased"] = date_deceased

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = subjects.update().where(subjects.c.id == int(subject_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated subject {} with values {}".format(int(subject_id), str(values)),
                    "subject_id": int(subject_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update subject.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@event_tools.route("/<project>/translation/new/", methods=["POST"])
@project_permission_required
def add_new_translation(project):
    """
    Add a new translation, either for a record that has no previous
    translations, or add a translation in a new language to a record
    that has previous translations.

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    - table_name: str, name of the table containing the record to be translated.
    - field_name: str, name of the field to be translated (if applicable).
    - text: str, the translated text.
    - language: str, the language code for the translation (ISO 639-1).

    POST data CAN contain:
    - translation_id: int, the id of an existing translation record in the `translation` table.
      Required if you intend to add a translation in a new language to an entry that
      already has one or more translations.
    - parent_id: int, the id of the record in the table_name table.
    - parent_translation_field: str, the name of the field holding the translation_id
      (defaults to 'translation_id').
    - neutral_text: str, the base text before translation.

    Returns:
    - 201: Created new translation.
    - 400: Invalid input.
    - 500: Internal server error.
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    translation_text = get_table("translation_text")
    connection = db_engine.connect()
    result = None
    translation_id = request_data.get("translation_id", None)
    # create a new translation if not supplied
    if translation_id is None:
        if "table_name" not in request_data:
            return jsonify({"msg": "No table_name in POST data, required when no translation_id"}), 400
        if "parent_id" not in request_data:
            return jsonify({"msg": "No parent_id in POST data, required when no translation_id"}), 400
        translation_id = create_translation(request_data.get("neutral_text", None))
        # need to add the new id to the location, subject ... table
        # update table_name set translation_id = translation_id where id = ?
        # if the field name is something else than translation_id it must be given
        # in "parent_translation_field" in the request data (in some tables the
        # field name is name_translation_id)
        target_table = get_table(request_data.get("table_name", None))
        values = {}
        if translation_id is not None:
            translation_id_field_name = request_data.get("parent_translation_field", "translation_id")
            values[translation_id_field_name] = translation_id
        with connection.begin():
            update = target_table.update().where(target_table.c.id == int(request_data.get("parent_id", None))).values(**values)
            connection.execute(update)

    new_translation = {
        "table_name": request_data.get("table_name", None),
        "field_name": request_data.get("field_name", None),
        "text": request_data.get("text", None),
        "language": request_data.get("language", None),
        "translation_id": translation_id
    }
    try:
        with connection.begin():
            insert = translation_text.insert().values(**new_translation)
            result = connection.execute(insert)
            new_row = select(translation_text).where(translation_text.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new translation with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new translation.",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()
        return result


@event_tools.route("/<project>/translations/<translation_id>/edit/", methods=["POST"])
@project_permission_required
def edit_translation(project, translation_id):
    """
    Edit a translation object in the database.

    POST data must be in JSON format.

    POST data can include the following fields:
    - translation_text_id: int, id of the translation object in the `translation_text` table
    - table_name: str, name of the table being translated.
    - field_name: str, name of the field being translated.
    - text: str, the translation text.
    - language: str, language code of the translation (ISO 639-1).
    - deleted: int, flag to mark as deleted (0 or 1).

    If translation_text_id is omitted, an attempt to find the translation object
    which is to be updated is made based on translation_id, table_name, field_name
    and language. If that fails, a new tranlation object will be created.

    Response:
    - 201: New translation created.
    - 200: Existing translation updated.
    - 400: Invalid input.
    - 500: Server error.
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    translation_text = get_table("translation_text")

    translation_text_id = request_data.get("translation_text_id", None)
    if translation_text_id is None:
        # Attempt to get the id of the record in translation_text based on translation id,
        # table name, field name and language in the data
        translation_text_id = get_translation_text_id(translation_id,
                                                      request_data.get("table_name", None),
                                                      request_data.get("field_name", None),
                                                      request_data.get("language", None))

    connection = db_engine.connect()

    # if translation_text_id is None we should add a new row to the translation_text table
    if translation_text_id is None:
        new_translation = {
            "table_name": request_data.get("table_name", None),
            "field_name": request_data.get("field_name", None),
            "text": request_data.get("text", None),
            "language": request_data.get("language", None),
            "translation_id": translation_id
        }
        try:
            with connection.begin():
                insert = translation_text.insert().values(**new_translation)
                result = connection.execute(insert)
                new_row = select(translation_text).where(translation_text.c.id == result.inserted_primary_key[0])
                new_row = connection.execute(new_row).fetchone()
                if new_row is not None:
                    new_row = new_row._asdict()
                result = {
                    "msg": "Created new translation_text with ID {}".format(result.inserted_primary_key[0]),
                    "row": new_row
                }
                return jsonify(result), 201
        except Exception as e:
            result = {
                "msg": "Failed to create new translation_text.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    # if translation_text_id is not None, we should update the data
    else:
        edited_translation = {}
        # Update only fields that are provided in the POST data
        if "table_name" in request_data:
            edited_translation["table_name"] = request_data.get("table_name", None)
        if "field_name" in request_data:
            edited_translation["field_name"] = request_data.get("field_name", None)
        if "text" in request_data:
            edited_translation["text"] = request_data.get("text", None)
        if "language" in request_data:
            edited_translation["language"] = request_data.get("language", None)
        if "deleted" in request_data:
            edited_translation["deleted"] = request_data.get("deleted", 0)
        edited_translation["date_modified"] = datetime.now()

        if len(edited_translation) > 1:
            try:
                with connection.begin():
                    update = translation_text.update().where(translation_text.c.id == int(translation_text_id)).values(**edited_translation)
                    connection.execute(update)
                    return jsonify({
                        "msg": "Updated translation_text {} with values {}".format(int(translation_text_id), str(edited_translation)),
                        "location_id": int(translation_text_id)
                    })
            except Exception as e:
                result = {
                    "msg": "Failed to update translation_text.",
                    "reason": str(e)
                }
                return jsonify(result), 500
            finally:
                connection.close()
        else:
            connection.close()
            return jsonify("No valid update values given."), 400


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

        JSON: A list of translation records or an error message.

    Example Request:

        POST /projectname/translations/1/list/
        Body:
        {
            "table_name": "subject",
            "field_name": "description",
            "language": "en",
            "translation_text_id": 123
        }

    Example Response:

        [
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

    Status Codes:

    - 200 - OK: Returns the list of translations.
    - 400 - Bad Request: Invalid or missing translation_id.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Convert translation_id to integer
    translation_id = int_or_none(translation_id)
    if not translation_id:
        return jsonify({"msg": "Invalid translation_id parameter."}), 400

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

                # Convert rows to dictionary format
                result = []
                for row in rows:
                    if row is not None:
                        result.append(row._asdict())
                return jsonify(result)

    except Exception as e:
        # Handle errors and return error response
        result = {
            "msg": "Failed to retrieve translations.",
            "reason": str(e)
        }
        return jsonify(result), 500


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
