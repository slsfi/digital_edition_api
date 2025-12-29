from flask import Blueprint, jsonify, Response, request, send_file
import logging
import sqlalchemy
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, get_project_config

songs = Blueprint('songs', __name__)
logger = logging.getLogger("sls_api.songs")

# Song metadata and file functions


@songs.route("/<project>/song/<song_id>")
@reader_auth_required()
def get_publication_song(project, song_id):
    logger.info("Getting songs /{}/song/{}".format(project, song_id))
    connection = db_engine.connect()
    song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.original_id as song_original_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype, \
                ps.note as song_note, ps.comment as song_comment, ps.lyrics as song_lyrics \
                FROM publication_song ps  "

    # Check if song_id is a number
    try:
        song_id = int(song_id)
        song_sql = song_sql + " WHERE ps.id = :song_id "
    except ValueError:
        song_id = song_id
        song_sql = song_sql + " WHERE ps.original_id = :song_id "

    statement = sqlalchemy.sql.text(song_sql).bindparams(song_id=song_id)
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        return jsonify({"msg": "Desired song not found in database."}), 404
    else:
        return jsonify(return_data._asdict()), 200


@songs.route("/<project>/song/id/<song_id>/")
@reader_auth_required()
def get_song_by_id(project, song_id):
    logger.info("Getting song by id")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT * FROM song WHERE id=:s_id")
        statement = sql.bindparams(s_id=song_id)
        result = connection.execute(statement).fetchone()
        if result is not None:
            result = result._asdict()
        connection.close()
        return jsonify(result), 200
    except Exception:
        logger.exception(f"Failed to get song by id {song_id}.")
        return Response("Couldn't get song by id.", status=404, content_type="text/json")


@songs.route("/<project>/songs/filtered", methods=["GET"])
@reader_auth_required()
def get_songs_filtered(project):
    """
    Filter songs either by subject name, location or category.
    If no filter is provided all songs are returned.
    Sql injection is also prevented.

    Example: /<project>/songs/filtered?subject_name=Foo Bar
    """

    logger.info("Getting songs filtered...")
    try:
        connection = db_engine.connect()

        filter_query_sql = ''
        subject_name = ''
        category = ''
        location = ''

        if request.args.get('subject_name') and request.args.get('subject_name') != '':
            subject_name = request.args.get('subject_name')
            # Prevent sql injection
            filter_query_sql += 'and (s1.full_name = :s_name OR s2.full_name = :s_name )'
        elif request.args.get('category') and request.args.get('category') != '':
            category = request.args.get('category')
            # Prevent sql injection
            filter_query_sql += 'and t.name=:ca'
        elif request.args.get('location') and request.args.get('location') != '':
            location = request.args.get('location')
            # Prevent sql injection
            filter_query_sql += 'and l.city=:lo'

        song_query = "SELECT \
                        e.description as song_name, \
                        t.name as song_type, \
                        eo.publication_facsimile_page, \
                        ec1.subject_id as playman_id, \
                        s1.full_name as playman_name, \
                        ec2.subject_id as recorder_id, \
                        s2.full_name as recorder_name, \
                        t.id as tag_id, \
                        l.id as location_id, \
                        l.name as location_name, \
                        l.city as city, \
                        l.region as region, \
                        pf.publication_facsimile_collection_id, \
                        eo.publication_song_id, \
                        pf.page_nr \
                        FROM event e \
                        join event_occurrence eo \
                        on eo.event_id=e.id \
                        join event_connection ec1 \
                        on ec1.event_id = e.id \
                        join tag t \
                        on t.id = ec1.tag_id \
                        join subject s1 \
                        on ec1.subject_id = s1.id \
                        join event_connection ec2 \
                        on ec2.event_id = e.id \
                        join subject s2 \
                        on ec2.subject_id = s2.id \
                        join location l \
                        on l.id=ec1.location_id \
                        left join publication_facsimile pf \
                        on pf.id=eo.publication_facsimile_id \
                        where e.type='song' \
                        and s1.type='playman' \
                        and s2.type='recorder' \
                        {} \
                        order by e.id".format(filter_query_sql)

        sql = sqlalchemy.sql.text(song_query)

        if subject_name != '':
            # Filter songs involving person
            statement = sql.bindparams(s_name=subject_name)
        elif category != '':
            # Filter songs by category
            statement = sql.bindparams(ca=category)
        elif location != '':
            # Filter songs by location
            statement = sql.bindparams(lo=location)
        else:
            # All songs
            statement = sql

        result = connection.execute(statement).fetchall()
        return_data = []
        for row in result:
            if row is not None:
                return_data.append(row._asdict())
        connection.close()
        return jsonify(return_data), 200
    except Exception as e:
        return Response("Couldn't get songs filtered." + str(e), status=404, content_type="text/json")


@songs.route("/<project>/song-files/<file_type>/<file_name>/")
@reader_auth_required()
def get_song_file(project, file_type, file_name):
    """
    Retrieve a single file from project root that belongs to a song
    It can be musicxml, midi
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    file_path = ""
    if 'musicxml' in str(file_type):
        file_path = safe_join(config["file_root"],
                              "musicxml",
                              "{}.xml".format(str(file_name)))
        file_name = "{}.xml".format(str(file_name))
    elif 'midi' in str(file_type):
        file_path = safe_join(config["file_root"],
                              "midi-files",
                              "{}.mid".format(str(file_name)))

    try:
        return send_file(file_path, as_attachment=True, mimetype='application/octet-stream',
                         download_name=file_name)
    except Exception:
        logger.exception(f"Failed sending file from {file_path}")
        return Response("File not found.", status=404, content_type="text/json")
