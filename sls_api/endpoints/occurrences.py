from flask import abort, Blueprint, jsonify
import logging
from operator import itemgetter
import sqlalchemy

from sls_api.endpoints.generics import db_engine, get_project_id_from_name

occurrences = Blueprint('occurrences', __name__)
logger = logging.getLogger("sls_api.occurrences")

# Occurrence functions


@occurrences.route("/occurrences/<object_type>/<ident>")
@reader_auth_required()
def get_occurrences(object_type, ident):
    """
    Get event occurrence info and related publication IDs for a given subject, tag, or location
    Given a numerical or legacy ID for an object, returns a list of events and occurance information for the object
    """
    if object_type not in ["subject", "tag", "location", "work_manifestation"]:
        abort(404)
    else:
        connection = db_engine.connect()
        try:
            object_id = int(ident)
        except ValueError:
            object_sql = "SELECT id FROM {} WHERE legacy_id=:l_id".format(object_type)
            stmt = sqlalchemy.sql.text(object_sql).bindparams(l_id=ident)
            row = connection.execute(stmt).fetchone()
            if row is not None:
                object_id = row.id
            else:
                connection.close()
                return jsonify([])

        events_sql = "SELECT id, type, description FROM event WHERE id IN " \
                     "(SELECT event_id FROM event_connection WHERE deleted != 1 AND {}_id=:o_id)".format(object_type)
        occurrence_sql = "SELECT original_id as song_original_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                        variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                        recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                        performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, note as song_note, comment as song_comment, \
                        lyrics as song_lyrics, original_collection_location as song_original_collection_location, original_collection_signature as song_original_collection_signature,\
                        ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype, event_occurrence.id,\
                        event_occurrence.publication_facsimile_page AS publication_facsimile_page, publication.publication_collection_id AS collection_id, event_occurrence.id, event_occurrence.type, description, \
                        event_occurrence.publication_id, event_occurrence.publication_version_id, event_occurrence.publication_facsimile_id, \
        event_occurrence.publication_comment_id, event_occurrence.publication_manuscript_id, publication.published as publication_published, \
        pc.name as publication_collection_name, publication.name as publication_name  \
        FROM event_occurrence, publication \
        JOIN publication_collection pc ON pc.id = publication.publication_collection_id \
        LEFT OUTER JOIN publication_song ps ON ps.publication_id = publication.id \
        WHERE event_occurrence.event_id=:e_id AND event_occurrence.publication_id=publication.id AND publication.deleted != 1 AND event_occurrence.deleted != 1 AND pc.deleted != 1 \
        AND (event_occurrence.publication_song_id = ps.id OR event_occurrence.publication_song_id is null)"

        events_stmnt = sqlalchemy.sql.text(events_sql).bindparams(o_id=object_id)
        results = []
        for row in connection.execute(events_stmnt).fetchall():
            if row is not None:
                results.append(row._asdict())

        for event in results:
            event["occurrences"] = []
            occurrence_stmnt = sqlalchemy.sql.text(occurrence_sql).bindparams(e_id=event["id"])
            for row in connection.execute(occurrence_stmnt).fetchall():
                if row is not None:
                    event["occurrences"].append(row._asdict())
        connection.close()
        return jsonify(results)


@occurrences.route("/<project>/occurrences/<object_type>")
@occurrences.route("/occurrences/<object_type>")
@reader_auth_required()
def get_all_occurrences_by_type(object_type, project=None):
    """
    Get occurrences for each person
    TODO: refactor and divide into multiple functions
    """
    if object_type not in ["subject", "tag", "location", "work_manifestation"]:
        abort(404)
    else:
        connection = db_engine.connect()
        ob_id = object_type + "_id"
        if object_type == "subject":
            name_attr = "full_name"
        else:
            name_attr = "name"

        if project is None:
            ob_sql = "SELECT DISTINCT event_connection.{}, {}.{} FROM event_connection, event_occurrence, {} \
            WHERE event_connection.event_id=event_occurrence.event_id AND event_connection.{}={}.id"
            ob_sql = ob_sql.format(ob_id, object_type, name_attr, object_type, ob_id, object_type)
        else:
            project_id = get_project_id_from_name(project)
            ob_sql = "SELECT DISTINCT event_connection.{}, {}.{} FROM event_connection, event_occurrence, {} \
            WHERE event_connection.event_id=event_occurrence.event_id AND event_connection.{}={}.id AND {}.project_id={}"
            ob_sql = ob_sql.format(ob_id, object_type, name_attr, object_type, ob_id, object_type, object_type,
                                   project_id)

        ob_statement = sqlalchemy.sql.text(ob_sql)
        obs = []
        for row in connection.execute(ob_statement).fetchall():
            if row is not None:
                obs.append(row._asdict())

        occur = []
        for o in obs:
            ident = o[ob_id]
            try:
                object_id = int(ident)
            except ValueError:
                object_sql = "SELECT id FROM {} WHERE legacy_id=:l_id and deleted != 1 ".format(object_type)
                stmt = sqlalchemy.sql.text(object_sql).bindparams(l_id=ident)
                row = connection.execute(stmt).fetchone()
                object_id = row.id
            events_sql = "SELECT id FROM event WHERE id IN " \
                         "(SELECT event_id FROM event_connection WHERE {}_id=:o_id)".format(object_type)
            occurrence_sql = "SELECT publication_collection.name AS collection_name, publication.publication_collection_id AS collection_id,\
             event_occurrence.id, type, description, event_occurrence.publication_id, event_occurrence.publication_version_id,\
             event_occurrence.publication_facsimile_id, event_occurrence.publication_comment_id, event_occurrence.publication_facsimile_page, \
             event_occurrence.publication_manuscript_id, event_occurrence.publication_song_id FROM event_occurrence, publication, publication_collection \
             WHERE publication.publication_collection_id=publication_collection.id AND event_occurrence.event_id=:e_id AND event_occurrence.publication_id=publication.id"

            events_stmnt = sqlalchemy.sql.text(events_sql).bindparams(o_id=object_id)
            results = []
            for row in connection.execute(events_stmnt).fetchall():
                if row is None:
                    continue
                row = row._asdict()
                if object_type == "subject":
                    type_stmnt = sqlalchemy.sql.text(
                        "SELECT type, subject.first_name::text, subject.last_name::text, subject.source::text, subject.description::text, subject.occupation::text, subject.place_of_birth::text, subject.date_born::text, subject.date_deceased::text FROM subject WHERE id=:ty_id").bindparams(
                        ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    if type_object is None:
                        continue
                    type_object = type_object._asdict()
                    row["object_type"] = type_object["type"]
                    row["date_born"] = type_object["date_born"]
                    row["date_deceased"] = type_object["date_deceased"]
                    row["first_name"] = type_object["first_name"]
                    row["last_name"] = type_object["last_name"]
                    row["source"] = type_object["source"]
                    row["description"] = type_object["description"]
                    row["occupation"] = type_object["occupation"]
                    row["place_of_birth"] = type_object["place_of_birth"]
                if object_type == "tag":
                    type_stmnt = sqlalchemy.sql.text(
                        "SELECT tag.type::text, tag.description::text, tag.source::text, tag.name::text FROM tag WHERE id=:ty_id").bindparams(
                        ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    if type_object is None:
                        continue
                    type_object = type_object._asdict()
                    row["description"] = type_object["description"]
                    row["source"] = type_object["source"]
                    row["name"] = type_object["name"]
                    row["type"] = type_object["type"]
                if object_type == "work_manifestation":
                    type_stmnt = sqlalchemy.sql.text("""SELECT work_manifestation.type::text, work.description::text, work.source::text, work.title::text,
                        journal::text, publisher::text, published_year::text, volume::text,, total_pages::text, ISBN::text,
                        publication_location::text, translated_by::text, work_id::text, work_manuscript_id::text, linked_work_manifestation_id::text
                        FROM work_manifestation WHERE id=:ty_id""").bindparams(
                        ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    if type_object is None:
                        continue
                    type_object = type_object._asdict()
                    row["description"] = type_object["description"]
                    row["source"] = type_object["source"]
                    row["name"] = type_object["title"]
                    row["type"] = type_object["type"]
                    row["journal"] = type_object["journal"]
                    row["publisher"] = type_object["publisher"]
                    row["published_year"] = type_object["published_year"]
                    row["volume"] = type_object["volume"]
                    row["total_pages"] = type_object["total_pages"]
                    row["ISBN"] = type_object["ISBN"]
                    row["publication_location"] = type_object["publication_location"]
                    row["translated_by"] = type_object["translated_by"]
                    row["work_id"] = type_object["work_id"]
                    row["work_manuscript_id"] = type_object["work_manuscript_id"]
                    row["linked_work_manifestation_id"] = type_object["linked_work_manifestation_id"]
                if object_type == "location":
                    type_stmnt = sqlalchemy.sql.text("SELECT location.description::text, location.source::text, location.name::text, location.country::text, location.city::text, \
                                                        location.latitude::text, location.longitude::text, location.region::text FROM location WHERE id=:ty_id").bindparams(
                        ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    if type_object is None:
                        continue
                    type_object = type_object._asdict()
                    row["description"] = type_object["description"]
                    row["source"] = type_object["source"]
                    row["name"] = type_object["name"]
                    row["country"] = type_object["country"]
                    row["city"] = type_object["city"]
                    row["latitude"] = type_object["latitude"]
                    row["longitude"] = type_object["longitude"]
                    row["region"] = type_object["region"]
                results.append(row)

            # set occurrences for each object
            for event in results:
                event["occurrences"] = []
                occurrence_stmnt = sqlalchemy.sql.text(occurrence_sql).bindparams(e_id=event["id"])
                for row in connection.execute(occurrence_stmnt).fetchall():
                    if row is None:
                        continue
                    row = row._asdict()

                    if row["publication_manuscript_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT publication_manuscript.id AS id, publication_manuscript.original_filename, publication_manuscript.name \
                        FROM publication_manuscript WHERE id=:m_id").bindparams(m_id=row["publication_manuscript_id"])
                        manu = connection.execute(type_sql).fetchone()
                        if manu is not None:
                            row["publication_manuscript"] = manu._asdict()
                    if row["publication_version_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT publication_version.id AS id, publication_version.original_filename, publication_version.name \
                        FROM publication_version WHERE id=:v_id").bindparams(v_id=row["publication_version_id"])
                        variation = connection.execute(type_sql).fetchone()
                        if variation is not None:
                            row["publication_version"] = variation._asdict()
                    if row["publication_comment_id"] is not None:
                        type_sql = ""
                    if row["publication_facsimile_id"] is not None:
                        type_sql = "SELECT publication_facsimile.id, publication_facsimile.page_nr, publication_facsimile_collection.title AS name, \
                        publication_facsimile.section_id, publication_facsimile_collection.start_page_number, publication_facsimile_collection.folder_path, \
                        publication_facsimile_collection.page_comment FROM publication_facsimile, publication_facsimile_collection \
                        WHERE publication_facsimile.id=:f_id AND \
                        publication_facsimile_collection.id=publication_facsimile.publication_facsimile_collection_id"
                        type_sql = sqlalchemy.sql.text(type_sql).bindparams(f_id=row["publication_facsimile_id"])
                        facs = connection.execute(type_sql).fetchone()
                        if facs is not None:
                            row["publication_facsimile"] = facs._asdict()
                    if row["publication_id"] is not None \
                            and row["publication_facsimile_id"] is None \
                            and row["publication_facsimile_id"] is None \
                            and row["publication_comment_id"] is None \
                            and row["publication_version_id"] is None \
                            and row["publication_manuscript_id"] is None:
                        type_sql = sqlalchemy.sql.text("SELECT publication.id AS publication_id, "
                                                       "publication.original_filename, "
                                                       "publication.name "
                                                       "FROM publication WHERE id=:pub_id").bindparams(pub_id=row["publication_id"])
                        publication = connection.execute(type_sql).fetchone()
                        if publication is not None:
                            row["publication"] = publication._asdict()
                    if row["publication_song_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT \
                            original_id as song_original_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                        variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                        recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                        performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, note as song_note, comment as song_comment, \
                        lyrics as song_lyrics, original_collection_location as song_original_collection_location, original_collection_signature as song_original_collection_signature,\
                        ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype \
                         FROM publication_song WHERE id=:song_id").bindparams(song_id=row["publication_song_id"])
                        publication_song = connection.execute(type_sql).fetchone()
                        if publication_song is not None:
                            row["publication_song"] = publication_song._asdict()

                    event["occurrences"].append(row)

            for i in results:
                if object_type == "subject":
                    i["name"] = o["full_name"]
                else:
                    i["name"] = o["name"]
                occur.append(i)
        connection.close()
        return jsonify(occur)


@occurrences.route("/<project>/subject/occurrences/<subject_id>/")
@occurrences.route("/<project>/subject/occurrences/")
@reader_auth_required()
def get_subject_occurrences(project=None, subject_id=None):
    if project == 'all':
        subject_sql = " SELECT id, date_born::text, date_deceased::text, description, first_name, last_name, full_name as name, \
                        type as object_type, occupation, place_of_birth, source \
                        FROM subject WHERE deleted != 1"
        if subject_id is not None:
            subject_sql = subject_sql + " AND id = :sub_id "
            statement_subj = sqlalchemy.sql.text(subject_sql).bindparams(sub_id=subject_id)
        else:
            statement_subj = sqlalchemy.sql.text(subject_sql)
    else:
        subject_sql = " SELECT id, date_born::text, date_deceased::text, description, first_name, last_name, full_name as name, \
                        type as object_type, occupation, place_of_birth, source \
                        FROM subject WHERE deleted != 1 AND project_id = :project_id"
        project_id = get_project_id_from_name(project)
        if subject_id is not None:
            subject_sql = subject_sql + " AND id = :sub_id "
            statement_subj = sqlalchemy.sql.text(subject_sql).bindparams(sub_id=subject_id, project_id=project_id)
        else:
            statement_subj = sqlalchemy.sql.text(subject_sql).bindparams(project_id=project_id)

    connection = db_engine.connect()
    subjects = []
    result = connection.execute(statement_subj)
    subject = result.fetchone()
    while subject is not None:
        subject = subject._asdict()
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id, \
                            ev_c.id as ev_c_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN subject sub ON ev_c.subject_id = sub.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND sub.id = :sub_id ORDER BY pub_c.name ASC "

        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(sub_id=subject['id'])
        subject['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = occurrence._asdict()
            if subject_id is not None:
                song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype\
                FROM publication_song ps WHERE ps.id = :song_id\
                ORDER BY ps.type ASC"
                song_sql = sqlalchemy.sql.text(song_sql).bindparams(song_id=occurrenceData['publication_song_id'])
                song_result = connection_2.execute(song_sql)
                song_data = song_result.fetchone()
                if song_data is not None:
                    song_data = song_data._asdict()
                    occurrenceData.update(song_data)
            subject['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(subject['occurrences']) > 0:
            subjects.append(subject)

        connection_2.close()
        subject = result.fetchone()
    connection.close()

    subjects = sorted(subjects, key=itemgetter("last_name"))

    return jsonify(subjects)


@occurrences.route("/<project>/location/occurrences/<location_id>/")
@occurrences.route("/<project>/location/occurrences/")
@reader_auth_required()
def get_location_occurrences(project=None, location_id=None):
    if project == 'all':
        location_sql = " SELECT id, city, country, description, latitude, longitude, name, region, source \
                        FROM location WHERE deleted != 1"
        if location_id is not None:
            location_sql = location_sql + " AND id = :location_id "
            statement_loc = sqlalchemy.sql.text(location_sql).bindparams(location_id=location_id)
        else:
            statement_loc = sqlalchemy.sql.text(location_sql)
    else:
        location_sql = " SELECT id, city, country, description, latitude, longitude, name, region, source \
                        FROM location WHERE deleted != 1 AND project_id = :project_id"
        project_id = get_project_id_from_name(project)
        if location_id is not None:
            location_sql = location_sql + " AND id = :location_id "
            statement_loc = sqlalchemy.sql.text(location_sql).bindparams(location_id=location_id, project_id=project_id)
        else:
            statement_loc = sqlalchemy.sql.text(location_sql).bindparams(project_id=project_id)

    connection = db_engine.connect()
    locations = []
    result = connection.execute(statement_loc)
    location = result.fetchone()
    while location is not None:
        location = location._asdict()
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id, \
                            ev_c.id as ev_c_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN location loc ON ev_c.location_id = loc.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND loc.id = :loc_id ORDER BY pub_c.name ASC"
        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(loc_id=location['id'])
        location['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = occurrence._asdict()
            if location_id is not None:
                song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype\
                FROM publication_song ps WHERE ps.id = :song_id\
                ORDER BY ps.type ASC"
                song_sql = sqlalchemy.sql.text(song_sql).bindparams(song_id=occurrenceData['publication_song_id'])
                song_result = connection_2.execute(song_sql)
                song_data = song_result.fetchone()
                if song_data is not None:
                    song_data = song_data._asdict()
                    occurrenceData.update(song_data)
            location['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(location['occurrences']) > 0:
            locations.append(location)

        connection_2.close()
        location = result.fetchone()
    connection.close()

    locations = sorted(locations, key=itemgetter('name'))

    return jsonify(locations)


@occurrences.route("/<project>/tag/occurrences/<tag_id>/")
@occurrences.route("/<project>/tag/occurrences/")
@reader_auth_required()
def get_tag_occurrences(project=None, tag_id=None):
    if project == 'all':
        tag_sql = " SELECT id, type, name, description, source \
                        FROM tag WHERE deleted != 1"
        if tag_id is not None:
            tag_sql = tag_sql + " AND id = :tag_id "
            statement_tag = sqlalchemy.sql.text(tag_sql).bindparams(tag_id=tag_id)
        else:
            statement_tag = sqlalchemy.sql.text(tag_sql)
    else:
        tag_sql = " SELECT id, type, name, description, source \
                        FROM tag WHERE deleted != 1 AND project_id = :project_id"
        project_id = get_project_id_from_name(project)
        if tag_id is not None:
            tag_sql = tag_sql + " AND id = :tag_id "
            statement_tag = sqlalchemy.sql.text(tag_sql).bindparams(tag_id=tag_id, project_id=project_id)
        else:
            statement_tag = sqlalchemy.sql.text(tag_sql).bindparams(project_id=project_id)

    connection = db_engine.connect()
    tags = []
    result = connection.execute(statement_tag)
    tag = result.fetchone()
    while tag is not None:
        tag = tag._asdict()
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id, \
                            ev_c.id as ev_c_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN tag ON ev_c.tag_id = tag.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND tag.id = :tag_id ORDER BY pub_c.name ASC"
        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(tag_id=tag['id'])
        tag['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = occurrence._asdict()
            if tag_id is not None:
                song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype\
                FROM publication_song ps WHERE ps.id = :song_id\
                ORDER BY ps.type ASC"
                song_sql = sqlalchemy.sql.text(song_sql).bindparams(song_id=occurrenceData['publication_song_id'])
                song_result = connection_2.execute(song_sql)
                song_data = song_result.fetchone()
                if song_data is not None:
                    song_data = song_data._asdict()
                    occurrenceData.update(song_data)
            tag['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(tag['occurrences']) > 0:
            tags.append(tag)

        connection_2.close()
        tag = result.fetchone()

    connection.close()

    tags = sorted(tags, key=itemgetter("name"))

    return jsonify(tags)


@occurrences.route("/<project>/work_manifestation/occurrences/<work_manifestation_id>/")
@occurrences.route("/<project>/work_manifestation/occurrences/")
@reader_auth_required()
def get_work_manifestation_occurrences(project=None, work_manifestation_id=None):

    work_sql = """ SELECT id, title \
                    FROM work_manifestation WHERE deleted != 1 """
    if work_manifestation_id is not None:
        work_sql = work_sql + " AND id = :work_manifestation_id "
        statement = sqlalchemy.sql.text(work_sql).bindparams(work_manifestation_id=work_manifestation_id)
    else:
        statement = sqlalchemy.sql.text(work_sql)

    connection = db_engine.connect()
    work_manifestations = []
    result = connection.execute(statement)
    work_manifestation = result.fetchone()
    while work_manifestation is not None:
        work_manifestation = work_manifestation._asdict()
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id, \
                            ev_c.id as ev_c_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN work_manifestation ON ev_c.work_manifestation_id = work_manifestation.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND work_manifestation.id = :work_manifestation_id ORDER BY pub_c.name ASC"
        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(work_manifestation=work_manifestation['id'])
        work_manifestation['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = occurrence._asdict()
            work_manifestation['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(work_manifestation['occurrences']) > 0:
            work_manifestations.append(work_manifestation)

        connection_2.close()
        work_manifestation = result.fetchone()

    connection.close()

    return jsonify(work_manifestations)


@occurrences.route("/<project>/occurrences/collection/<object_type>/<collection_id>")
@reader_auth_required()
def get_person_occurrences_by_collection(project, object_type, collection_id):
    connection = db_engine.connect()
    occurrence_sql = "SELECT publication.publication_collection_id AS collection_id, event_occurrence.id, event_occurrence.event_id, \
    type, description, event_occurrence.publication_id, event_occurrence.publication_version_id, event_occurrence.publication_facsimile_id, \
    event_occurrence.publication_comment_id, event_occurrence.publication_manuscript_id FROM event_occurrence, publication \
    WHERE event_occurrence.publication_id=publication.id AND publication.publication_collection_id={} AND \
    event_occurrence.type='{}'".format(collection_id, object_type)

    occurrences = []
    result = connection.execute(occurrence_sql)
    row = result.fetchone()
    while row is not None:
        occurrences.append(row._asdict())
        row = result.fetchone()

    subjects = []
    for occurrence in occurrences:
        subject_sql = "SELECT DISTINCT event_connection.subject_id, subject.full_name, subject.legacy_id, subject.project_id \
        FROM event_occurrence, event_connection, subject \
        WHERE event_occurrence.event_id={} AND event_occurrence.event_id = event_connection.event_id AND \
        event_connection.subject_id =  subject.id".format(occurrence["event_id"])

        result = connection.execute(subject_sql)
        row = result.fetchone()
        while row is not None:
            subjects.append(row._asdict())
            row = result.fetchone()
    connection.close()

    return jsonify(subjects)
