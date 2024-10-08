from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import join, select, sql

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
     project_permission_required


publication_tools = Blueprint("publication_tools", __name__)


@publication_tools.route("/<project>/publications/")
@jwt_required()
def get_publications(project):
    """
    List all available publications in the given project
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    publication_collections = get_table("publication_collection")
    publications = get_table("publication")
    statement = select(publication_collections.c.id).where(publication_collections.c.project_id == project_id)
    collection_ids = connection.execute(statement).fetchall()
    collection_ids = [int(row["id"]) for row in collection_ids]
    statement = select(publications).where(publications.c.id.in_(collection_ids))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/")
@project_permission_required
def get_publication(project, publication_id):
    """
    Get a publication object from the database
    """
    connection = db_engine.connect()
    publications = get_table("publication")
    statement = select(publications).where(publications.c.id == int_or_none(publication_id))
    result = connection.execute(statement).first()
    if result is not None:
        result = result._asdict()
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/manuscript/<publication_id>/")
@project_permission_required
def get_publication_manuscript(project, publication_id):
    """
    Get a publication object from the database
    """
    connection = db_engine.connect()
    publication_ms = get_table("publication_manuscript")
    statement = select(publication_ms).where(publication_ms.c.publication_id == int_or_none(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/version/<publication_id>/")
@project_permission_required
def get_publication_version(project, publication_id):
    """
    Get a publication object from the database
    """
    connection = db_engine.connect()
    publication_v = get_table("publication_version")
    statement = select(publication_v).where(publication_v.c.publication_id == int_or_none(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/versions/")
@jwt_required()
def get_publication_versions(project, publication_id):
    """
    List all versions of the given publication
    """
    connection = db_engine.connect()
    publication_versions = get_table("publication_version")
    statement = select(publication_versions).where(publication_versions.c.publication_id == int_or_none(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/manuscripts/")
@jwt_required()
def get_publication_manuscripts(project, publication_id):
    """
    List all manuscripts for the given publication
    """
    connection = db_engine.connect()
    publication_manuscripts = get_table("publication_manuscript")
    statement = select(publication_manuscripts).where(publication_manuscripts.c.publication_id == int_or_none(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/tags/")
@jwt_required()
def get_publication_tags(project, publication_id):
    """
    List all manuscripts for the given publication
    """
    connection = db_engine.connect()
    sql_text = """ select t.*, e_o.* from event_occurrence e_o
    join event_connection e_c on e_o.event_id = e_c.event_id
    join tag t on t.id = e_c.tag_id
    where e_o.publication_id = :pub_id
    and e_c.tag_id is not null
    and e_c.deleted != 1 and e_o.deleted != 1
    and t.deleted != 1 """

    statement = sql.text(sql_text).bindparams(pub_id=publication_id)
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/facsimiles/")
@jwt_required()
def get_publication_facsimiles(project, publication_id):
    """
    List all fascimilies for the given publication
    """
    connection = db_engine.connect()
    publication_facsimiles = get_table("publication_facsimile")
    facsimile_collections = get_table("publication_facsimile_collection")

    # join in facsimile_collections to we can get the collection title as well
    tables = join(publication_facsimiles, facsimile_collections, publication_facsimiles.c.publication_facsimile_collection_id == facsimile_collections.c.id)

    statement = select(publication_facsimiles, facsimile_collections.c.title)\
        .where(publication_facsimiles.c.publication_id == int_or_none(publication_id))\
        .where(publication_facsimiles.c.deleted != 1)\
        .select_from(tables)

    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/comments/")
@jwt_required()
def get_publication_comments(project, publication_id):
    """
    List all comments for the given publication
    """
    connection = db_engine.connect()
    publications = get_table("publication")
    publication_comments = get_table("publication_comment")
    statement = select(publications.c.publication_comment_id).where(publications.c.id == int_or_none(publication_id))
    comment_ids = []
    for row in connection.execute(statement).fetchall():
        if row:
            comment_ids.append(int(row._asdict()['publication_comment_id']))
    statement = select(publication_comments).where(publication_comments.c.id.in_(comment_ids))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/link_file/", methods=["POST"])
@project_permission_required
def link_file_to_publication(project, publication_id):
    """
    Link an XML file to a publication,
    creating the appropriate publication_comment, publication_manuscript, or publication_version object.

    POST data MUST be in JSON format

    POST data MUST contain the following:
    file_type: one of [comment, manuscript, version] indicating which type of file the given file_path points to
    file_path: path to the file to be linked

    POST data SHOULD also contain the following:
    datePublishedExternally: date of external publication
    published: 0 or 1, is this file published and ready for viewing
    publishedBy: person responsible for publishing

    POST data MAY also contain:
    legacyId: legacy ID for this publication file object
    type: Type of file link, for Manuscripts and Versions
    sectionId: Publication section or chapter number, for Manuscripts and Versions
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "file_path" not in request_data or "file_type" not in request_data or request_data.get("file_type", None) not in ["comment", "manuscript", "version"]:
        return jsonify({"msg": "POST data JSON doesn't contain required data."}), 400

    file_type = request_data["file_type"]

    connection = db_engine.connect()

    if file_type == "comment":
        comments = get_table("publication_comment")
        publications = get_table("publication")
        transaction = connection.begin()
        new_comment = {
            "original_file_name": request_data.get("file_path"),
            "date_published_externally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "published_by": request_data.get("publishedBy", None),
            "legacy_id": request_data.get("legacyId", None)
        }
        try:
            ins = comments.insert().values(**new_comment)
            result = connection.execute(ins)
            new_row = select(comments).where(comments.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()

            # update publication object in database with new publication_comment ID
            update_stmt = publications.update().where(publications.c.id == int_or_none(publication_id)). \
                values(publications.c.publication_comment_id == result.inserted_primary_key[0])
            connection.execute(update_stmt)

            transaction.commit()
            result = {
                "msg": "Created new publication_comment with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
        except Exception as e:
            transaction.rollback()
            result = {
                "msg": "Failed to create new publication_comment object",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        new_object = {
            "original_file_name": request_data.get("file_path"),
            "publication_id": int(publication_id),
            "date_published_externally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "published_by": request_data.get("publishedBy", None),
            "legacy_id": request_data.get("legacyId", None),
            "type": request_data.get("type", None),
            "section_id": request_data.get("sectionId", None)
        }
        if file_type == "manuscript":
            table = get_table("publication_manuscript")
        else:
            table = get_table("publication_version")
        try:
            ins = table.insert().values(**new_object)
            result = connection.execute(ins)
            new_row = select(table).where(table.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            result = {
                "msg": "Created new publication{} with ID {}".format(file_type.capitalize(), result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
        except Exception as e:
            result = {
                "msg": "Failed to create new object",
                "reason": str(e)
            }
            return jsonify(result), 500

        finally:
            connection.close()
