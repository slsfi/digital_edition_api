import argparse
import logging
import os
import sys
from bs4 import BeautifulSoup
from io import StringIO
from lxml import etree as ET
from saxonche import PySaxonProcessor, PyXslt30Processor, PyXsltExecutable
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from subprocess import CalledProcessError
from typing import Any, Dict, List, Optional, Union

from sls_api.endpoints.generics import changed_by_size_or_hash, \
    config, db_engine, file_fingerprint, get_project_id_from_name
from sls_api.endpoints.tools.files import run_git_command, update_files_in_git_repo
from sls_api.scripts.CTeiDocument import CTeiDocument
from sls_api.scripts.saxon_xml_document import SaxonXMLDocument

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("publisher")
logger.setLevel(logging.DEBUG)

# list of projects in this API (useful for if we want to process all projects)
projects = [project for project in config if isinstance(config[project], dict)]

EST_WEB_XML_XSL_PATH_IN_FILE_ROOT = "xslt/publisher/generate-web-xml-est.xsl"
COM_WEB_XML_XSL_PATH_IN_FILE_ROOT = "xslt/publisher/generate-web-xml-com.xsl"
MS_WEB_XML_XSL_PATH_IN_FILE_ROOT = "xslt/publisher/generate-web-xml-ms.xsl"
LEGACY_COMMENTS_XSL_PATH_IN_FILE_ROOT = "xslt/comment_html_to_tei.xsl"
COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT = "templates/comment.xml"


def get_comments_from_database(project, document_note_ids):
    """
    Given the name of a project and a list of IDs of comments in a master file, returns data from the comments database with matching documentnote.id
    Returns a list of dicts, each dict representing one comment.
    """
    if not document_note_ids:
        return []

    # if project has comments database config, try and read comments from database
    if config[project].get("comments_database", False):
        connection = create_engine(config[project]["comments_database"], pool_pre_ping=True).connect()

        comment_query = text("SELECT documentnote.id, documentnote.shortenedSelection, note.description \
                            FROM documentnote INNER JOIN note ON documentnote.note_id = note.id \
                            WHERE documentnote.deleted = 0 AND note.deleted = 0 AND documentnote.id IN :docnote_ids")
        comment_query = comment_query.bindparams(docnote_ids=tuple(document_note_ids))
        comments = connection.execute(comment_query).fetchall()
        connection.close()
        if len(comments) <= 0:
            return []
        return [comment._asdict() for comment in comments if comment is not None]
    else:
        logger.warning(f"Project {project} lacks comments_database configuration.")
        return []


def get_letter_info_from_database(letter_id):
    logger.info("Getting correspondence info for letter: {}".format(letter_id))
    if letter_id is None:
        return []
    letter = dict()
    # Get Sender
    sender = get_letter_person(letter_id, 'avsändare')
    if sender is not None:
        letter['sender'] = sender.full_name
        letter['sender_id'] = sender.id
    else:
        letter['sender'] = ''
        letter['sender_id'] = ''
    # Get Reciever
    reciever = get_letter_person(letter_id, 'mottagare')
    if reciever is not None:
        letter['reciever'] = reciever.full_name
        letter['reciever_id'] = reciever.id
    else:
        letter['reciever'] = ''
        letter['reciever_id'] = ''
    # Get Sender Location
    sender_location = get_letter_location(letter_id, 'avsändarort')
    if sender_location is not None:
        letter['sender_location'] = sender_location.name
        letter['sender_location_id'] = sender_location.id
    else:
        letter['sender_location'] = ''
        letter['sender_location_id'] = ''
    # Get Reciever Location
    reciever_location = get_letter_location(letter_id, 'mottagarort')
    if reciever_location is not None:
        letter['reciever_location'] = reciever_location.name
        letter['reciever_location_id'] = reciever_location.id
    else:
        letter['reciever_location'] = ''
        letter['reciever_location_id'] = ''
    # Get Title and Status
    title = get_letter_info(letter_id)
    if title is not None:
        letter['title'] = title.title
        letter['title_id'] = title.id
    else:
        letter['title'] = ''
        letter['title_id'] = ''
    return letter


def get_letter_info(letter_id):
    if letter_id is None:
        return []
    connection = db_engine.connect()
    statement = text("SELECT c.id, c.title from correspondence c \
                     where c.legacy_id = :letter_id ")
    statement = statement.bindparams(letter_id=letter_id)
    data = connection.execute(statement).fetchone()
    connection.close()
    return data


def get_letter_person(letter_id, type):
    if letter_id is None:
        return []
    if type not in ['mottagare', 'avsändare']:
        return []
    connection = db_engine.connect()
    statement = text("SELECT s.id, s.full_name from correspondence c \
                     join event_connection ec on ec.correspondence_id = c.id \
                     join subject s on s.id = ec.subject_id \
                     where c.legacy_id = :letter_id and ec.type = :type ")
    statement = statement.bindparams(letter_id=letter_id, type=type)
    data = connection.execute(statement).fetchone()
    connection.close()
    return data


def get_letter_location(letter_id, type):
    if letter_id is None:
        return []
    if type not in ['mottagarort', 'avsändarort']:
        return []
    connection = db_engine.connect()
    statement = text("SELECT l.id, l.name from correspondence c \
                     join event_connection ec on ec.correspondence_id = c.id \
                     join location l on l.id = ec.location_id \
                     where c.legacy_id = :letter_id and ec.type = :type ")
    statement = statement.bindparams(letter_id=letter_id, type=type)
    data = connection.execute(statement).fetchone()
    connection.close()
    return data


def clean_comment_html_fragment(html_str: str) -> str:
    """
    Parses the provided HTML comment fragment (str) using 1) BeautifulSoup
    and 2) ElementTree from lxml to ensure the result is well-formed XML.
    Returns the valid XML, wrapped in a <noteText> element, in stringified
    form.
    """
    result = "<noteText></noteText>"
    html_str = html_str.strip()
    if len(html_str) > 0:
        try:
            soup = BeautifulSoup(html_str, "html.parser")
            soup.contents[0].unwrap()
            dom = ET.parse(StringIO('<noteText>' + str(soup) + '</noteText>'))
            result = ET.tostring(dom, encoding="unicode")
        except Exception:
            logger.exception("Failed to parse comment HTML fragment.")
            raise
    return result


def construct_note_position(comment_positions: Dict[str, Any], comment_id: str) -> str | None:
    """
    Given a dictionary of comment note IDs (keys) and note positions (values),
    and a specific note ID, returns a string representing the position of the
    lemma of the note, or None if the note ID is not found in the dictionary.
    """
    start_pos = comment_positions.get('start' + comment_id)
    end_pos = comment_positions.get('end' + comment_id)

    if start_pos is None or end_pos is None:
        return None

    if start_pos == end_pos or (start_pos != "null" and end_pos == "null") or start_pos == "null":
        return str(start_pos)
    else:
        return str(start_pos) + "–" + str(end_pos)


def construct_notes_xml(comments: List[Dict[str, Any]], comment_positions: Dict[str, Any]) -> str:
    """
    Given a list of dictionaries with comment notes data and a dictionary
    with note IDs (keys) and note positions (values), constructs an XML
    fragment of the notes and returns it in stringified form.
    """
    notes = []
    for comment in comments:
        note_position = construct_note_position(comment_positions, str(comment["id"]))
        if note_position is None:
            continue
        # Parse and clean the comment HTML to ensure it's well-formed
        try:
            note_text = clean_comment_html_fragment(comment["description"])
        except Exception:
            raise

        # Form the note XML
        note = '<note id="' + str(comment["id"]) + '">'
        note += '<notePosition>' + note_position + '</notePosition>'
        note += '<noteLemma>' + str(comment["shortenedSelection"]).replace('[...]', '<lemmaBreak>[...]</lemmaBreak>') + '</noteLemma>'
        note += note_text + '</note>'
        notes.append(note)

    return "\n".join(notes)


def compile_xslt_stylesheets(
        project: str,
        xslt_proc: PyXslt30Processor
) -> Dict[str, Optional[PyXsltExecutable]]:
    """
    Compiles the XSLT stylesheets in the project files to Saxon XSLT
    executables.

    Returns:
    - A dictionary where the text types (est, com, ms) are keys and the
    compiled stylesheets are values. If a stylesheet for a text type
    can't be compiled, it's value will be set to None.
    """
    xslt_execs: Dict[str, Optional[PyXsltExecutable]] = {}

    for type_key, xsl_path in [
        ("est", EST_WEB_XML_XSL_PATH_IN_FILE_ROOT),
        ("com", COM_WEB_XML_XSL_PATH_IN_FILE_ROOT),
        ("ms", MS_WEB_XML_XSL_PATH_IN_FILE_ROOT)
    ]:
        xsl_full_path = os.path.join(config[project]["file_root"], xsl_path)

        if os.path.exists(xsl_full_path):
            try:
                xslt_execs[type_key] = xslt_proc.compile_stylesheet(stylesheet_file=xsl_full_path, encoding="utf-8")
            except Exception:
                logger.exception(f"Failed to compile XSLT executable for '{type_key}' files. Make sure '{xsl_path}' exists and is valid in project root.")
                xslt_execs[type_key] = None
        else:
            xslt_execs[type_key] = None

    return xslt_execs


def generate_est_and_com_files(publication_info: Optional[Dict[str, Any]],
                               project: str,
                               est_master_file_path: str,
                               com_master_file_path: str,
                               est_target_path: str,
                               com_target_path: str,
                               com_xsl_path: Optional[str]=None):
    """
    Given a project name, and paths to valid EST/COM masters and targets, regenerates target files based on source files
    """
    # Generate est file for this document
    est_document = CTeiDocument()
    try:
        est_document.Load(est_master_file_path, bRemoveDelSpans=True)
        est_document.PostProcessMainText()
    except Exception as ex:
        logger.exception("Failed to handle est master file: {}".format(est_master_file_path))
        raise ex

    if publication_info is not None:
        est_document.SetMetadata(publication_info['original_publication_date'],
                                 publication_info['p_id'],
                                 publication_info['name'],
                                 publication_info['genre'],
                                 'est',
                                 publication_info['c_id'],
                                 publication_info['publication_group_id'])
        letterId = est_document.GetLetterId()
        if letterId is not None:
            letterData = get_letter_info_from_database(letterId)
            est_document.SetLetterTitleAndStatusAndMeta(letterData)

    est_document.Save(est_target_path)

    # Generate comments file for this document

    # If com_master_file_path doesn't exist, use COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT.
    # If the template file doesn't exist either, don't generate a comments file for this document.
    if not os.path.exists(com_master_file_path):
        com_master_file_path = os.path.join(config[project]["file_root"],
                                            COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)

        if not os.path.exists(com_master_file_path):
            logger.info(f"Skipping com file generation: no comments file associated with publication and no template file exists at {COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT}")
            return

    # Get all documentnote IDs from the main master file (these are the IDs of the comments for this document)
    note_ids = est_document.GetAllNoteIDs()
    # Use these note_ids to get all comments for this publication from the notes database
    comments = get_comments_from_database(project, note_ids)

    com_document = CTeiDocument()

    # load in com_master file
    try:
        com_document.Load(com_master_file_path)

        # if com_xsl_path is invalid or not given, try using COMMENTS_XSL_PATH_IN_FILE_ROOT
        if com_xsl_path is None or not os.path.exists(com_xsl_path):
            com_xsl_path = os.path.join(config[project]["file_root"],
                                        LEGACY_COMMENTS_XSL_PATH_IN_FILE_ROOT)

        # process comments and save
        com_document.ProcessCommments(comments, est_document, com_xsl_path)
        com_document.PostProcessOtherText()

        if publication_info is not None:
            com_document.SetMetadata(publication_info['original_publication_date'],
                                     publication_info['p_id'],
                                     publication_info['name'],
                                     publication_info['genre'],
                                     'com',
                                     publication_info['c_id'],
                                     publication_info['publication_group_id'])

        com_document.Save(com_target_path)
    except Exception as ex:
        logger.exception("Failed to handle com master file: {}".format(com_master_file_path))
        raise ex


def generate_est_and_com_files_with_xslt(publication_info: Optional[Dict[str, Any]],
                                         project: str,
                                         est_source_file_path: str,
                                         com_source_file_path: str,
                                         est_target_file_path: str,
                                         com_target_file_path: str,
                                         saxon_proc: PySaxonProcessor,
                                         xslt_execs: Dict[str, Optional[PyXsltExecutable]]):
    """
    Generates published est and com files using XSLT processing.
    """
    if xslt_execs["est"] is None:
        logger.warning(f"XSLT executable for 'est' is missing. '{EST_WEB_XML_XSL_PATH_IN_FILE_ROOT}' is invalid or does not exist in project root.")
        # Don't raise an exception here in case the XSLT for est is
        # intentionally missing, for example if the project doesn't
        # have est files. This still allows com files to be processed.
    else:
        try:
            est_document = SaxonXMLDocument(saxon_proc, xml_filepath=est_source_file_path)
            # Create a dictionary with publication metadata which will be
            # passed as a parameter to the XSLT processor.
            est_params = {}
            if publication_info is not None:
                est_params = {
                    "collectionId": publication_info["c_id"],
                    "publicationId": publication_info["p_id"],
                    "title": publication_info["name"],
                    "sourceFile": publication_info["original_filename"],
                    "publishedStatus": publication_info["published"],
                    "dateOrigin": publication_info["original_publication_date"],
                    "genre": publication_info["genre"],
                    "language": publication_info["language"]
                }
            est_params["textType"] = "est"

            est_document.transform_and_save(xslt_exec=xslt_execs["est"],
                                            output_filepath=est_target_file_path,
                                            parameters=est_params)
        except Exception:
            logger.exception(f"Failed to handle est master file: {est_source_file_path}")
            raise

    if not publication_info["publication_comment_id"]:
        # No publication_comment linked to publication, skip
        # generation of comments web XML
        logger.info("Skipping generation of comment file, no comment linked to publication.")
        return

    if xslt_execs["com"] is None:
        logger.warning(f"XSLT executable for 'com' is missing. '{COM_WEB_XML_XSL_PATH_IN_FILE_ROOT}' is invalid or does not exist in project root. Comment file not generated.")
        # Don't raise an exception here so the est file can still be committed.
        return

    notes_xml_str = ""

    if xslt_execs["est"] is not None:
        try:
            # Get all comment IDs from the reading text file
            comment_note_ids = est_document.get_all_comment_ids()
            # Use these IDs to get all comments from the notes database
            # comments is a list of dictionaries with the keys "id",
            # "shortenedSelection" and "description"
            comment_notes = get_comments_from_database(project, comment_note_ids)
            # Get positions of comment start and end tags from reading text file
            comment_positions = est_document.get_all_comment_positions(comment_note_ids)
            # Stringify the notes data
            notes_xml_str = construct_notes_xml(comment_notes, comment_positions)
        except Exception:
            logger.exception("Failed to get/process comment notes from database.")

    # If com_source_file_path doesn't exist, use
    # COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT
    if not os.path.exists(com_source_file_path):
        com_source_file_path = os.path.join(config[project]["file_root"],
                                            COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)

    try:
        com_document = SaxonXMLDocument(saxon_proc, xml_filepath=com_source_file_path)
        # Create a dictionary with publication comment metadata which will be
        # passed as a parameter to the XSLT processor.
        if est_params:
            com_params = est_params
            com_params["commentId"] = publication_info["publication_comment_id"]
            com_params["sourceFile"] = publication_info["com_original_filename"] or None
        else:
            com_params = {}
        com_params["textType"] = "com"
        com_params["notes"] = notes_xml_str

        com_document.transform_and_save(xslt_exec=xslt_execs["com"],
                                        output_filepath=com_target_file_path,
                                        parameters=com_params)
    except Exception:
        logger.exception(f"Failed to handle com master file: {com_source_file_path}")
        raise


def process_var_documents_and_generate_files(main_var_doc, main_var_path, var_docs, var_paths, publication_info):
    """
    Process generated CTeiDocument objects - comparing each var_doc in var_docs to the main_var_doc and saving target files
    """
    # First, compare the main variant against all other variants
    main_var_doc.ProcessVariants(var_docs)
    if publication_info is not None:
        main_var_doc.SetMetadata(publication_info['original_publication_date'], publication_info['p_id'], publication_info['name'],
                                 publication_info['genre'], 'com', publication_info['c_id'], publication_info['publication_group_id'])
    # Then save main variant web XML file
    main_var_doc.Save(main_var_path)
    # lastly, save all other variant web XML files
    for var_doc, var_path in zip(var_docs, var_paths):
        var_doc.Save(var_path)


def generate_ms_file(master_file_path, target_file_path, publication_info):
    """
    Given a project name, and valid master and target file paths for a publication manuscript, regenerates target file based on source file
    """
    try:
        ms_document = CTeiDocument()
        ms_document.Load(master_file_path)
        ms_document.PostProcessOtherText()
    except Exception as ex:
        logger.exception("Failed to handle manuscript file: {}".format(master_file_path))
        raise ex

    if publication_info is not None:
        ms_document.SetMetadata(publication_info['original_publication_date'], publication_info['p_id'], publication_info['name'],
                                publication_info['genre'], 'ms', publication_info['c_id'], publication_info['publication_group_id'])
    ms_document.Save(target_file_path)


def generate_ms_file_with_xslt(publication_info: Optional[Dict[str, Any]],
                               source_file_path: str,
                               target_file_path: str,
                               saxon_proc: PySaxonProcessor,
                               xslt_execs: Dict[str, Optional[PyXsltExecutable]]):
    """
    Generates a published ms file using XSLT processing.
    """
    try:
        if xslt_execs["ms"] is None:
            logger.error(f"XSLT executable for 'ms' is missing. '{MS_WEB_XML_XSL_PATH_IN_FILE_ROOT}' is invalid or does not exist in project root.")
            raise ValueError("XSLT executable for 'ms' is missing.")

        ms_document = SaxonXMLDocument(saxon_proc, xml_filepath=source_file_path)
        # Create a dictionary with publication manuscript metadata which will be
        # passed as a parameter to the XSLT processor.
        ms_params = {}
        if publication_info is not None:
            ms_params = {
                "collectionId": publication_info["c_id"],
                "publicationId": publication_info["p_id"],
                "manuscriptId": publication_info["m_id"],
                "title": publication_info["m_name"],
                "sourceFile": publication_info["original_filename"],
                "publishedStatus": publication_info["published"],
                "dateOrigin": publication_info["original_publication_date"],
                "genre": publication_info["genre"],
                "language": publication_info["language"]
            }
        ms_params["textType"] = "ms"

        ms_document.transform_and_save(xslt_exec=xslt_execs["ms"],
                                       output_filepath=target_file_path,
                                       parameters=ms_params)
    except Exception:
        logger.exception(f"Failed to handle manuscript file: {source_file_path}")
        raise


def check_publication_mtimes_and_publish_files(project: str, publication_ids: Union[tuple, None], git_author: str, no_git=False, force_publish=False, is_multilingual=False, use_xslt_processing=False):
    update_success, result_str = update_files_in_git_repo(project)
    if not update_success:
        logger.error("Git update failed! Reason: {}".format(result_str))
        return False
    project_id = get_project_id_from_name(project)
    project_settings = config.get(project, None)

    # if publication_ids is a tuple of ints, we're (re)publishing a certain publication(s)
    # explicitly set force_publish in this instance, so we force-generate files for publishing (this overrides mtime checks)
    if isinstance(publication_ids, tuple):
        force_publish = True

    if project_id is not None and project_settings is not None:
        file_root = project_settings.get("file_root", None)
        if file_root is not None:
            # open DB connection for publication, comment, and manuscript data fetch
            connection = db_engine.connect()

            # publication info
            publication_query = "SELECT \
                                p.id as p_id, \
                                p.publication_collection_id as c_id, \
                                pcol.id as c_id, \
                                p.original_filename as original_filename, \
                                p.published as published, \
                                p.original_publication_date as original_publication_date, \
                                p.genre as genre, \
                                p.language as language, \
                                p.publication_group_id as publication_group_id, \
                                p.publication_comment_id as publication_comment_id, \
                                p.name as name \
                                FROM publication p \
                                JOIN publication_collection pcol ON p.publication_collection_id=pcol.id \
                                WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 "

            if is_multilingual:
                # publication info
                publication_query = "SELECT \
                                    p.id as p_id, \
                                    p.publication_collection_id as c_id, \
                                    pcol.id as c_id, \
                                    tr.text as original_filename, \
                                    p.published as published, \
                                    p.original_publication_date as original_publication_date, \
                                    p.genre as genre, \
                                    p.publication_group_id as publication_group_id, \
                                    p.publication_comment_id as publication_comment_id, \
                                    p.name as name, \
                                    tr.language as language \
                                    FROM publication p \
                                    JOIN publication_collection pcol ON p.publication_collection_id=pcol.id \
                                    JOIN translation_text tr ON p.translation_id = tr.translation_id and tr.field_name='original_filename' \
                                    WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 "

            # publication_comment info, relating to "general comments" file for each publication
            comment_query = "SELECT \
                            p.id as p_id, \
                            p.publication_collection_id as c_id, \
                            pc.original_filename as original_filename, \
                            pc.published as published, \
                            p.original_publication_date as original_publication_date, \
                            p.genre as genre, \
                            p.publication_group_id as publication_group_id, \
                            p.publication_comment_id as publication_comment_id, \
                            p.name as name \
                            FROM publication p \
                            JOIN publication_collection pcol ON p.publication_collection_id = pcol.id \
                            JOIN publication_comment pc ON p.publication_comment_id = pc.id \
                            WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 AND pc.deleted != 1 "

            # publication_manuscript info
            manuscript_query = "SELECT \
                                pm.id as m_id, \
                                p.id as p_id, \
                                p.publication_collection_id as c_id, \
                                pcol.id as c_id, \
                                pm.original_filename as original_filename, \
                                pm.published as published, \
                                p.original_publication_date as original_publication_date, \
                                p.genre as genre, \
                                p.publication_group_id as publication_group_id, \
                                p.publication_comment_id as publication_comment_id, \
                                p.name as name, \
                                pm.name as m_name, \
                                pm.language as language \
                                FROM publication_manuscript pm \
                                JOIN publication p ON pm.publication_id = p.id \
                                JOIN publication_collection pcol ON p.publication_collection_id = pcol.id \
                                WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 AND pm.deleted != 1 "

            if force_publish and isinstance(publication_ids, tuple):
                # append publication.id checks if this is a forced (re)publication of certain publication(s)
                publication_query += " AND p.id IN :p_ids"
                publication_query = text(publication_query).bindparams(proj=project_id, p_ids=publication_ids)

                comment_query += " AND p.id IN :p_ids"
                comment_query = text(comment_query).bindparams(proj=project_id, p_ids=publication_ids)

                manuscript_query += " AND p.id IN :p_ids"
                manuscript_query = text(manuscript_query).bindparams(proj=project_id, p_ids=publication_ids)
            else:
                publication_query = text(publication_query).bindparams(proj=project_id)
                comment_query = text(comment_query).bindparams(proj=project_id)
                manuscript_query = text(manuscript_query).bindparams(proj=project_id)

            publication_info = connection.execute(publication_query).fetchall()
            manuscript_info = connection.execute(manuscript_query).fetchall()

            # comment_filenames can just be a dict of publication.id to publication_comment.original_filename
            comment_filenames = dict()
            for row in connection.execute(comment_query):
                comment_filenames[row.p_id] = row.original_filename

            # close DB connection for now, it won't be needed for a while
            connection.close()

            if use_xslt_processing:
                # Initialise a Saxon processor and Saxon XSLT 3.0 processor and
                # compile Saxon XSLT stylesheets so they can be reused for
                # each publication.
                saxon_proc: PySaxonProcessor = PySaxonProcessor(license=False)
                xslt_proc: PyXslt30Processor = saxon_proc.new_xslt30_processor()

                # Store the compiled XSLT stylesheets in a dictionary where the
                # text types (est, com, ms) are keys and the compiled stylesheets
                # are values. If a stylesheet for a text type can't be compiled,
                # it's value will be set to None.
                xslt_execs: Dict[str, Optional[PyXsltExecutable]] = compile_xslt_stylesheets(project,
                                                                                             xslt_proc)

            # Keep a list of changed files for later git commit
            changes = set()
            # For each publication belonging to this project, check the modification timestamp of its master files and compare them to the generated web XML files
            for row in publication_info:
                if row is None:
                    continue
                row = row._asdict()
                publication_id = row["p_id"]
                collection_id = row["c_id"]
                if not row["original_filename"]:
                    logger.info("Source file not set for publication {}".format(publication_id))
                    continue
                est_target_filename = "{}_{}_est.xml".format(collection_id, publication_id)
                com_target_filename = est_target_filename.replace("_est.xml", "_com.xml")

                if is_multilingual:
                    language = row["language"]
                    est_target_filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)

                est_target_file_path = os.path.join(file_root, "xml", "est", est_target_filename)
                com_target_file_path = os.path.join(file_root, "xml", "com", com_target_filename)
                # original_filename should be relative to the project root
                est_source_file_path = os.path.join(file_root, row["original_filename"])

                # Get comment filename if a comment is linked to the publication
                # in the database. Default to template comment file if no entry
                # in publication_comment pointing to a comments file for this
                # publication. If no comment linked to the publication, set
                # comment file to None, so we can skip the generation of a
                # comment web file.
                if row["publication_comment_id"]:
                    comment_file = comment_filenames.get(publication_id, COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)
                else:
                    comment_file = None

                # Add the comment filename to the row dict so it can be passed
                # to called functions
                row["com_original_filename"] = comment_file

                if os.path.isdir(est_source_file_path):
                    logger.warning("Source file {} for publication {} is a directory!".format(est_source_file_path, publication_id))
                    continue
                if not os.path.exists(est_source_file_path):
                    logger.warning("Source file {} for publication {} does not exist!".format(est_source_file_path, publication_id))
                    continue

                # Check comment file existence only if a comment is linked to the
                # publication in the database. If no comment linked to the
                # publication, set comment source file path to empty string, so
                # we can skip the generation of a comment web file.
                if comment_file:
                    com_source_file_path = os.path.join(file_root, comment_file)

                    if os.path.isdir(com_source_file_path):
                        logger.warning("Source file {} for publication {} comment is a directory!".format(com_source_file_path, publication_id))
                        continue
                    if not os.path.exists(com_source_file_path):
                        logger.warning("Source file {} for publication {} does not exist!".format(com_source_file_path, publication_id))
                        continue
                else:
                    com_source_file_path = ""

                if force_publish:
                    # during force_publish, just generate
                    logger.info("Generating new est/com files for publication {}...".format(publication_id))
                    try:
                        # calculate file fingerprints for existing files, so we can later
                        # compare if they have changed
                        pre_est = file_fingerprint(est_target_file_path)
                        pre_com = file_fingerprint(com_target_file_path)

                        if use_xslt_processing:
                            generate_est_and_com_files_with_xslt(row,
                                                                 project,
                                                                 est_source_file_path,
                                                                 com_source_file_path,
                                                                 est_target_file_path,
                                                                 com_target_file_path,
                                                                 saxon_proc,
                                                                 xslt_execs)
                        else:
                            generate_est_and_com_files(row,
                                                       project,
                                                       est_source_file_path,
                                                       com_source_file_path,
                                                       est_target_file_path,
                                                       com_target_file_path)
                    except Exception:
                        logger.exception("Failed to generate est/com files for publication {}!".format(publication_id))
                        continue
                    else:
                        # check if est and/or com files have changed
                        if changed_by_size_or_hash(pre_est, est_target_file_path):
                            changes.add(est_target_file_path)
                        if changed_by_size_or_hash(pre_com, com_target_file_path):
                            changes.add(com_target_file_path)

                else:
                    # otherwise, check if this publication's files need to be re-generated
                    try:
                        est_target_mtime = os.path.getmtime(est_target_file_path)
                        com_target_mtime = os.path.getmtime(com_target_file_path)
                        est_source_mtime = os.path.getmtime(est_source_file_path)
                        com_source_mtime = os.path.getmtime(com_source_file_path)
                    except OSError:
                        # If there is an error, the web XML files likely don't exist or are otherwise corrupt
                        # It is then easiest to just generate new ones
                        logger.warning("Error getting time_modified for target or source files for publication {}".format(publication_id))
                        logger.info("Generating new est/com files for publication {}...".format(publication_id))
                        try:
                            # calculate file fingerprints for existing files, so we can later
                            # compare if they have changed
                            pre_est = file_fingerprint(est_target_file_path)
                            pre_com = file_fingerprint(com_target_file_path)

                            if use_xslt_processing:
                                generate_est_and_com_files_with_xslt(row,
                                                                     project,
                                                                     est_source_file_path,
                                                                     com_source_file_path,
                                                                     est_target_file_path,
                                                                     com_target_file_path,
                                                                     saxon_proc,
                                                                     xslt_execs)
                            else:
                                generate_est_and_com_files(row,
                                                           project,
                                                           est_source_file_path,
                                                           com_source_file_path,
                                                           est_target_file_path,
                                                           com_target_file_path)
                        except Exception:
                            logger.exception("Failed to generate est/com files for publication {}!".format(publication_id))
                            continue
                        else:
                            # check if est and/or com files have changed
                            if changed_by_size_or_hash(pre_est, est_target_file_path):
                                changes.add(est_target_file_path)
                            if changed_by_size_or_hash(pre_com, com_target_file_path):
                                changes.add(com_target_file_path)
                    else:
                        if est_target_mtime >= est_source_mtime and com_target_mtime >= com_source_mtime:
                            # If both the est and com files are newer than the source files, just continue to the next publication
                            continue
                        else:
                            # If one or either is outdated, generate new ones
                            logger.info("Reading files for publication {} are outdated, generating new est/com files...".format(publication_id))
                            try:
                                # calculate file fingerprints for existing files, so we can later
                                # compare if they have changed
                                pre_est = file_fingerprint(est_target_file_path)
                                pre_com = file_fingerprint(com_target_file_path)

                                if use_xslt_processing:
                                    generate_est_and_com_files_with_xslt(row,
                                                                         project,
                                                                         est_source_file_path,
                                                                         com_source_file_path,
                                                                         est_target_file_path,
                                                                         com_target_file_path,
                                                                         saxon_proc,
                                                                         xslt_execs)
                                else:
                                    generate_est_and_com_files(row,
                                                               project,
                                                               est_source_file_path,
                                                               com_source_file_path,
                                                               est_target_file_path,
                                                               com_target_file_path)
                            except Exception:
                                logger.exception("Failed to generate est/com files for publication {}!".format(publication_id))
                                continue
                            else:
                                # check if est and/or com files have changed
                                if changed_by_size_or_hash(pre_est, est_target_file_path):
                                    changes.add(est_target_file_path)
                                if changed_by_size_or_hash(pre_com, com_target_file_path):
                                    changes.add(com_target_file_path)

                # Process all variants belonging to this publication
                # publication_version with type=1 is the "main" variant, the others should have type=2 and be versions of that main variant
                variant_query = text("SELECT id, original_filename "
                                     "FROM publication_version "
                                     "WHERE publication_version.publication_id = :pub_id AND publication_version.type = :vers_type AND publication_version.deleted != 1")

                # open new DB connection for variant data fetch
                connection = db_engine.connect()

                # fetch info for "main" variant
                main_variant_query = variant_query.bindparams(pub_id=publication_id, vers_type=1)
                # should only be one main variant per publication?
                main_variant_info = connection.execute(main_variant_query).fetchone()
                if main_variant_info is None:
                    logger.info("No main variant found for publication {}!".format(publication_id))
                else:
                    main_variant_info = main_variant_info._asdict()
                    logger.debug(f"Main variant query result: {str(main_variant_info)}")

                    # fetch info for all "other" variants
                    variants_query = variant_query.bindparams(pub_id=publication_id, vers_type=2)
                    variants_info = connection.execute(variants_query).fetchall()

                    # close DB connection, as it's no longer needed
                    connection.close()

                    # compile info and generate files if needed
                    if main_variant_info["original_filename"] is None:
                        continue

                    main_variant_source = os.path.join(file_root, main_variant_info["original_filename"])

                    if not main_variant_source:
                        logger.warning("Source file for main variant {} is not set.".format(main_variant_info["id"]))
                        continue

                    if os.path.isdir(main_variant_source):
                        logger.error("Source file {} for main variant {} (type=1) is a directory!".format(main_variant_source, main_variant_info["id"]))
                        continue

                    if not os.path.exists(main_variant_source):
                        logger.error("Source file {} for main variant {} (type=1) does not exist!".format(main_variant_source, main_variant_info["id"]))
                        continue

                    target_filename = "{}_{}_var_{}.xml".format(collection_id,
                                                                publication_id,
                                                                main_variant_info["id"])

                    # If any variants have changed, we need a CTeiDocument for the main variant to ProcessVariants() with
                    main_variant_target = os.path.join(file_root, "xml", "var", target_filename)

                    main_variant_doc = CTeiDocument()
                    main_variant_doc.Load(main_variant_source)

                    # For each "other" variant, create a new CTeiDocument if needed, but if main_variant_updated is True, just make a new for all
                    variant_docs = []
                    variant_paths = []
                    for variant in variants_info:
                        if variant is None:
                            continue
                        variant = variant._asdict()
                        target_filename = "{}_{}_var_{}.xml".format(collection_id,
                                                                    publication_id,
                                                                    variant["id"])
                        if variant["original_filename"] is None:
                            continue

                        source_filename = variant["original_filename"]
                        if not source_filename:
                            logger.error("Source file for variant {} is not set.".format(variant["id"]))
                            continue
                        target_file_path = os.path.join(file_root, "xml", "var", target_filename)
                        # original_filename should be relative to the project root
                        source_file_path = os.path.join(file_root, source_filename)

                        if os.path.isdir(source_file_path):
                            logger.error("Source file {} for variant {} is a directory!".format(source_file_path, variant["id"]))
                            continue
                        if not os.path.exists(source_file_path):
                            logger.error("Source file {} for variant {} does not exist!".format(source_file_path, variant["id"]))
                            continue

                        # in a force_publish, just load all variants for generation/processing
                        if force_publish:
                            logger.info("Generating new var file for publication_version {}...".format(variant["id"]))
                            variant_doc = CTeiDocument()
                            variant_doc.Load(source_file_path)
                            variant_docs.append(variant_doc)
                            variant_paths.append(target_file_path)
                        # otherwise, check which ones need to be updated and load only those
                        else:
                            try:
                                target_mtime = os.path.getmtime(target_file_path)
                                source_mtime = os.path.getmtime(source_file_path)
                            except OSError:
                                # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                                # It is then easiest to just generate a new one
                                logger.warning("Error getting time_modified for target or source files for publication_version {}".format(variant["id"]))
                                logger.info("Generating new file...")
                                variant_doc = CTeiDocument()
                                variant_doc.Load(source_file_path)
                                variant_docs.append(variant_doc)
                                variant_paths.append(target_file_path)
                            else:
                                if target_mtime < source_mtime:
                                    logger.info("File {} is older than source file {}, generating new file...".format(target_file_path, source_file_path))
                                    variant_doc = CTeiDocument()
                                    variant_doc.Load(source_file_path)
                                    variant_docs.append(variant_doc)
                                    variant_paths.append(target_file_path)
                                else:
                                    # If no changes, don't generate CTeiDocument and don't make a new web XML file
                                    continue

                    # calculate file fingerprints for existing main variant file and all
                    # variant files, so we can later compare if they have changed
                    pre_main_variant = file_fingerprint(main_variant_target)
                    pre_variants = {path: file_fingerprint(path) for path in variant_paths}

                    # lastly, actually process all generated CTeiDocument objects and create web XML files
                    process_var_documents_and_generate_files(main_variant_doc,
                                                             main_variant_target,
                                                             variant_docs,
                                                             variant_paths,
                                                             row)

                    # check if main variant has changed
                    if changed_by_size_or_hash(pre_main_variant, main_variant_target):
                        changes.add(main_variant_target)

                    # check if each variant has changed
                    for path, pre_fp in pre_variants.items():
                        if changed_by_size_or_hash(pre_fp, path):
                            changes.add(path)

            # For each publication_manuscript belonging to this project, check the modification timestamp of its master file and compare it to the generated web XML file
            for row in manuscript_info:
                if row is None:
                    continue
                row = row._asdict()
                collection_id = row["c_id"]
                publication_id = row["p_id"]
                manuscript_id = row["m_id"]
                target_filename = "{}_{}_ms_{}.xml".format(collection_id,
                                                           publication_id,
                                                           manuscript_id)

                source_filename = row["original_filename"]
                if not source_filename:
                    logger.info("Source file not set for manuscript {}".format(manuscript_id))
                    continue

                target_file_path = os.path.join(file_root, "xml", "ms", target_filename)
                # original_filename should be relative to the project root
                source_file_path = os.path.join(file_root, source_filename)

                if os.path.isdir(source_file_path):
                    logger.warning("Source file {} for manuscript {} is a directory!".format(source_file_path, manuscript_id))
                    continue

                if not os.path.exists(source_file_path):
                    logger.warning("Source file {} for manuscript {} does not exist!".format(source_file_path, manuscript_id))
                    continue

                # in a force_publish, just generate all ms files
                if force_publish:
                    logger.info("Generating new ms file for publication_manuscript {}".format(manuscript_id))
                    try:
                        # calculate file fingerprint for existing ms file, so we can later
                        # compare if it has changed
                        pre_ms = file_fingerprint(target_file_path)

                        if use_xslt_processing:
                            generate_ms_file_with_xslt(row,
                                                       source_file_path,
                                                       target_file_path,
                                                       saxon_proc,
                                                       xslt_execs)
                        else:
                            generate_ms_file(source_file_path,
                                             target_file_path,
                                             row)
                    except Exception:
                        continue
                    else:
                        # check if ms file has changed
                        if changed_by_size_or_hash(pre_ms, target_file_path):
                            changes.add(target_file_path)

                # otherwise, check if this file needs generating
                else:
                    try:
                        target_mtime = os.path.getmtime(target_file_path)
                        source_mtime = os.path.getmtime(source_file_path)
                    except OSError:
                        # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                        # It is then easiest to just generate a new one
                        logger.warning("Error getting time_modified for target or source file for publication_manuscript {}".format(manuscript_id))
                        logger.info("Generating new file...")
                        try:
                            # calculate file fingerprint for existing ms file, so we can later
                            # compare if it has changed
                            pre_ms = file_fingerprint(target_file_path)

                            if use_xslt_processing:
                                generate_ms_file_with_xslt(row,
                                                           source_file_path,
                                                           target_file_path,
                                                           saxon_proc,
                                                           xslt_execs)
                            else:
                                generate_ms_file(source_file_path,
                                                 target_file_path,
                                                 row)
                        except Exception:
                            continue
                        else:
                            # check if ms file has changed
                            if changed_by_size_or_hash(pre_ms, target_file_path):
                                changes.add(target_file_path)
                    else:
                        if target_mtime >= source_mtime:
                            # If the target ms file is newer than the source, continue to the next publication_manuscript
                            continue
                        else:
                            logger.info("File {} is older than source file {}, generating new file...".format(target_file_path, source_file_path))
                            try:
                                # calculate file fingerprint for existing ms file, so we can later
                                # compare if it has changed
                                pre_ms = file_fingerprint(target_file_path)

                                if use_xslt_processing:
                                    generate_ms_file_with_xslt(row,
                                                               source_file_path,
                                                               target_file_path,
                                                               saxon_proc,
                                                               xslt_execs)
                                else:
                                    generate_ms_file(source_file_path,
                                                     target_file_path,
                                                     row)
                            except Exception:
                                continue
                            else:
                                # check if ms file has changed
                                if changed_by_size_or_hash(pre_ms, target_file_path):
                                    changes.add(target_file_path)

            logger.debug("Changes made in publication script run: {}".format([c for c in changes]))
            if len(changes) > 0 and not no_git:
                outputs = []
                # If there are changes, try to commit them to git
                try:
                    for change in changes:
                        # Each changed file should be added, as there may be other activity in the git repo we don't want to commit
                        outputs.append(run_git_command(project, ["add", change]))
                    outputs.append(run_git_command(project, ["commit", "--author", git_author, "-m", "Published new web files"]))
                    outputs.append(run_git_command(project, ["push"]))
                except CalledProcessError:
                    logger.exception("Exception during git sync of webfile changes.")
                    logger.debug("Git outputs: {}".format("\n".join(outputs)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publishing script to publish changes to EST/COM/VAR/MS files for GDE project")
    parser.add_argument("project", help="Which project to publish, either a project name from --list_projects or 'all' for all valid projects")
    parser.add_argument("-i", "--publication_ids", type=int, nargs="*",
                        help="Force re-publication of specific publications (tries to publish all files, est/com/var/ms)")
    parser.add_argument("--all_ids", action="store_true",
                        help="Force re-publication of all publications (tries to publish all files, est/com/var/ms)")
    parser.add_argument("-l", "--list_projects", action="store_true",
                        help="Print a listing of available projects with seemingly valid configuration and exit")
    parser.add_argument("--git_author", type=str, help="Author used for git commits (Default 'Publisher <is@sls.fi>')", default="Publisher <is@sls.fi>")
    parser.add_argument("--no_git", action="store_true", help="Don't run git commands as part of publishing.")
    parser.add_argument("--is_multilingual", action="store_true", help="The publication is multilingual and original_filename is found in translation_text")
    parser.add_argument("--use_xslt_processing", action="store_true", help="XML files related to the publication are processed using project specific XSLT.")

    args = parser.parse_args()

    if args.list_projects:
        logger.info(f"Projects with seemingly valid configuration: {', '.join(projects)}")
        sys.exit(0)
    else:
        if args.publication_ids is None:
            ids = None
        elif len(args.publication_ids) == 0:
            ids = None
        else:
            # use a tuple rather than a list, to make SQLAlchemy happier more easily
            ids = tuple(args.publication_ids)
        if str(args.project).lower() == "all":
            for p in projects:
                check_publication_mtimes_and_publish_files(p, ids, git_author=args.git_author,
                                                           no_git=args.no_git, force_publish=args.all_ids,
                                                           use_xslt_processing=args.use_xslt_processing)
        else:
            if args.project in projects:
                check_publication_mtimes_and_publish_files(args.project, ids, git_author=args.git_author,
                                                           no_git=args.no_git, force_publish=args.all_ids,
                                                           is_multilingual=args.is_multilingual,
                                                           use_xslt_processing=args.use_xslt_processing)
            else:
                logger.error(f"{args.project} is not in the API configuration or lacks 'comments_database' setting, aborting...")
                sys.exit(1)
