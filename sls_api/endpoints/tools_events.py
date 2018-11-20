from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import Table
from sqlalchemy.sql import select, text

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, metadata, \
    project_permission_required, select_all_from_table

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
    legacyId: legacy id for location
    latitude: latitude coordinate for location
    longitude: longitude coordinate for location
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "name" not in request_data:
        return jsonify({"msg": "No name in POST data"}), 400

    locations = Table('location', metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_location = {
        "name": request_data["name"],
        "description": request_data.get("desription", None),
        "project_id": get_project_id_from_name(project),
        "legacy_id": request_data.get("legacyId", None),
        "latitude": request_data.get("latitude", None),
        "longitude": request_data.get("longitude", None)
    }
    try:
        insert = locations.insert()
        result = connection.execute(insert, **new_location)
        new_row = select([locations]).where(locations.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
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


@event_tools.route("/<project>/subjects/new/", methods=["POST"])
@project_permission_required
def add_new_subject(project):
    """
    Add a new subject object to the database

    POST data MUST be in JSON format

    POST data SHOULD contain:
    type: subject type
    description: subject descrtiption

    POST data CAN also contain:
    firstName: Subject first or given name
    lastName Subject surname
    preposition: preposition for subject
    fullName: Subject full name
    legacyId: Legacy id for subject
    dateBorn: Subject date of birth
    dateDeceased: Subject date of death
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    subjects = Table('subject', metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_subject = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "project_id": get_project_id_from_name(project),
        "first_name": request_data.get("firstName", None),
        "last_name": request_data.get("lastName", None),
        "preposition": request_data.get("preposition", None),
        "full_name": request_data.get("fullName", None),
        "legacy_id": request_data.get("legacyId", None),
        "date_born": request_data.get("dateBorn", None),
        "date_deceased": request_data.get("dateDeceased", None)
    }
    try:
        insert = subjects.insert()
        result = connection.execute(insert, **new_subject)
        new_row = select([subjects]).where(subjects.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
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
    legacyId: Legacy id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    tags = Table("tag", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_tag = {
        "type": request_data.get("type", None),
        "name": request_data.get("name", None),
        "project_id": get_project_id_from_name(project),
        "description": request_data.get("description", None),
        "legacy_id": request_data.get("legacyId", None)
    }
    try:
        insert = tags.insert()
        result = connection.execute(insert, **new_tag)
        new_row = select([tags]).where(tags.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
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


@event_tools.route("/locations/")
@jwt_required
def get_locations():
    """
    Get all locations from the database
    """
    return select_all_from_table("location")


@event_tools.route("/subjects/")
@jwt_required
def get_subjects():
    """
    Get all subjects from the database
    """
    # TODO better workaround because subject.date_born and subject.date_death may contain BC dates
    connection = db_engine.connect()
    stmt = text("SELECT id, date_created::text, date_modified::text, deleted, type, first_name, last_name, "
                "place_of_birth, occupation, preposition, full_name, description, legacy_id, "
                "date_born::text, date_deceased::text, project_id, source FROM subject")
    rows = connection.execute(stmt).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/tags/")
@jwt_required
def get_tags():
    """
    Get all tags from the database
    """
    return select_all_from_table("tag")


@event_tools.route("/events/")
@jwt_required
def get_events():
    """
    Get a list of all available events in the database
    """
    return select_all_from_table("event")


@event_tools.route("/events/search/", methods=["POST"])
@jwt_required
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

    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    statement = select([events]).where(events.c.description.ilike("%{}%".format(request_data["phrase"])))
    rows = connection.execute(statement).fetchall()

    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/events/new/", methods=["POST"])
@jwt_required
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
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_event = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
    }
    try:
        insert = events.insert()
        result = connection.execute(insert, **new_event)
        new_row = select([events]).where(events.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
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
@jwt_required
def connect_event(event_id):
    """
    Link an event to a location, subject, or tag through event_connection

    POST data MUST be in JSON format.

    POST data MUST contain at least one of the following:
    subject_id: ID for the subject involved in the given event
    location_id: ID for the location involced in the given event
    tag_id: ID for the tag involved in the given event
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    select_event = select([events]).where(events.c.id == int(event_id))
    event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404
    event_connections = Table("event_connection", metadata, autoload=True, autoload_with=db_engine)
    insert = event_connections.insert()
    new_event_connection = {
        "event_id": int(event_id),
        "subject_id": int(request_data["subject_id"]) if request_data.get("subject_id", None) else None,
        "location_id": int(request_data["location_id"]) if request_data.get("location_id", None) else None,
        "tag_id": int(request_data["tag_id"]) if request_data.get("tag_id", None) else None
    }
    try:
        result = connection.execute(insert, **new_event_connection)
        new_row = select([event_connections]).where(event_connections.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
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
@jwt_required
def get_event_connections(event_id):
    """
    List all event_connections for a given event, to find related locations, subjects, and tags
    """
    event_connections = Table("event_connection", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([event_connections]).where(event_connections.c.event_id == int(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/event/<event_id>/occurrences/")
@jwt_required
def get_event_occurrences(event_id):
    """
    Get a list of all event_occurrence in the database, optionally limiting to a given event
    """
    event_occurrences = Table("event_occurrence", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([event_occurrences]).where(event_occurrences.c.event_id == int(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/event/<event_id>/occurrences/new/", methods=["POST"])
@jwt_required
def new_event_occurrence(event_id):
    """
    Add a new event_occurrence to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain the following:
    type: event occurance type
    description: event occurance description

    POST data SHOULD also contain at least one of the following:
    publication_id: ID for publication the event occurs in
    publicationVersion_id: ID for publication version the event occurs in
    publicationManuscript_id: ID for publication manuscript the event occurs in
    publicationFacsimile_id: ID for publication facsimile the event occurs in
    publicationComment_id: ID for publication comment the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    select_event = select([events]).where(events.c.id == int(event_id))
    event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404

    event_occurrences = Table("event_occurrence", metadata, autoload=True, autoload_with=db_engine)
    insert = event_occurrences.insert()
    new_occurrence = {
        "event_id": int(event_id),
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "publication_id": int(request_data["publication_id"]) if request_data.get("publication_id", None) else None,
        "publication_version_id": int(request_data["publicationVersion_id"]) if request_data.get("publicationVersion_id", None) else None,
        "publication_manuscript_id": int(request_data["publicationManuscript_id"]) if request_data.get("publicationManuscript_id", None) else None,
        "publication_facsimile_id": int(request_data["publicationFacsimile_id"]) if request_data.get("publicationFacsimile_id", None) else None,
        "publication_comment_id": int(request_data["publicationComment_id"]) if request_data.get("publicationComment_id", None) else None,
    }
    try:
        result = connection.execute(insert, **new_occurrence)
        new_row = select([event_occurrences]).where(event_occurrences.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
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
