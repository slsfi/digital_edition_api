from flask.typing import ResponseReturnValue
from flask import Blueprint, jsonify, request
from sqlalchemy import select

from sls_api.endpoints.generics import cms_required, db_engine, get_table, int_or_none

group_tools = Blueprint("group_tools", __name__, url_prefix="/digitaledition")


@group_tools.route("/<string:project>/publication_groups/")
@cms_required()
def list_publication_groups(project: str) -> ResponseReturnValue:
    """
    List all available publication groups
    """
    connection = db_engine.connect()
    groups = get_table("publication_group")
    statement = select(groups.c.id, groups.c.published, groups.c.name)
    result = connection.execute(statement).first()
    if result is not None:
        result = result._asdict()
    connection.close()
    return jsonify(result)


@group_tools.route("/<string:project>/publication_group/<int:group_id>/")
@cms_required()
def get_publication_group(project: str, group_id: int) -> ResponseReturnValue:
    """
    Get all data for a single publication group
    """
    connection = db_engine.connect()
    groups = get_table("publication_group")
    statement = select(groups).where(groups.c.id == group_id)
    result = connection.execute(statement).first()
    if result is not None:
        result = result._asdict()
    connection.close()
    return jsonify(result)


@group_tools.route("/<string:project>/publication_group/<int:group_id>/publications/")
@cms_required()
def get_publications_in_group(project: str, group_id: int) -> ResponseReturnValue:
    """
    List all publications in a given publication_group
    """
    connection = db_engine.connect()
    publications = get_table("publication")
    statement = select(publications.c.id, publications.c.name).where(publications.c.publication_group_id == group_id)
    result = []
    for row in connection.execute(statement).fetchall():
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@group_tools.route("/<string:project>/publication/<int:publication_id>/add_group/", methods=["POST"])
@cms_required(edit=True)
def add_publication_to_group(project: str, publication_id: int) -> ResponseReturnValue:
    """
    Add a publication to a publication_group

    POST data MUST be in JSON format

    POST data MUST contain the following:
    group_id: numerical ID for the publication_group
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "group_id" not in request_data:
        return jsonify({"msg": "group_id not in POST data."}), 400

    group_id = int_or_none(request_data["group_id"])

    connection = db_engine.connect()
    publications = get_table("publication")
    statement = publications.update().where(publications.c.id == publication_id).values(publication_group_id=group_id)
    transaction = connection.begin()
    try:
        connection.execute(statement)
        statement = select(publications).where(publications.c.id == publication_id)
        updated = connection.execute(statement).fetchone()
        if updated is not None:
            updated = updated._asdict()
        transaction.commit()
        result = {
            "msg": "Updated publication object",
            "row": updated
        }
        return jsonify(result)
    except Exception as e:
        transaction.rollback()
        result = {
            "msg": "Failed to create new object",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@group_tools.route("/<string:project>/publication_group/new/", methods=["POST"])
@cms_required(edit=True)
def add_new_publication_group(project: str) -> ResponseReturnValue:
    """
    Create a new publication_group

    POST data MUST be in JSON format

    POST data SHOULD contain the following:
    name: name for the group
    published: publication status for the group, 0 meaning unpublished
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    groups = get_table("publication_group")
    connection = db_engine.connect()
    new_group = {
        "name": request_data.get("name", None),
        "published": request_data.get("published", 0)
    }
    try:
        with connection.begin():
            insert = groups.insert().values(**new_group)
            result = connection.execute(insert)
            new_row = select(groups).where(groups.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new group with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new group",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()
