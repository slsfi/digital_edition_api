import argparse
import logging
import os
import sys
from bs4 import BeautifulSoup
from io import StringIO
from lxml import etree as ET
from saxonche import PySaxonProcessor, PyXslt30Processor, PyXsltExecutable
from sqlalchemy import and_, create_engine, select
from sqlalchemy.sql import text
from subprocess import CalledProcessError
from typing import Any, Dict, List, Optional, Union
from werkzeug.security import safe_join

from sls_api.endpoints.generics import config, db_engine, \
    changed_by_size_or_hash, \
    file_fingerprint, \
    get_project_id_from_name, \
    get_table, \
    int_or_none, \
    transform_xml, \
    PRERENDERED_HTML_PATH_IN_PROJECT_ROOT, \
    XSL_PATH_MAP_FOR_HTML_TRANSFORMATIONS
from sls_api.endpoints.tools.files import run_git_command, update_files_in_git_repo
from sls_api.scripts.CTeiDocument import CTeiDocument
from sls_api.scripts.saxon_xml_document import SaxonXMLDocument
from sls_api.logging_handlers import WarningErrorFlagHandler

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Add handler to logger for flipping flags if errors/warnings occur,
# so we can show a message at the end of the script run.
logger_flags = WarningErrorFlagHandler()
root_logger.addHandler(logger_flags)

logger = logging.getLogger("publisher")
logger.setLevel(logging.INFO)

# List of projects in this API (useful for if we want to process all projects)
projects = [project for project in config if isinstance(config[project], dict)]

# Initialize a cache for collection legacy ids for fast lookups
collection_legacy_id_cache: Dict[int, Optional[str]] = {}

LEGACY_COMMENTS_XSL_PATH_IN_FILE_ROOT = "xslt/comment_html_to_tei.xsl"
COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT = "templates/comment.xml"

# Map of paths to XSLT stylesheets for web XML transformations for
# different text types. The paths to the XSLT stylesheets are relative
# to the project root.
XSL_PATH_MAP_FOR_PUBLISHING = {
    "com": "xslt/publisher/generate-web-xml-com.xsl",
    "est": "xslt/publisher/generate-web-xml-est.xsl",
    "ms": "xslt/publisher/generate-web-xml-ms.xsl"
}


def enable_debug_logging():
    logging.getLogger().setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)


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
        logger.warning("Project %s lacks comments_database configuration.", project)
        return []


def get_letter_info_from_database(letter_id):
    logger.info("Getting correspondence info for letter: %s", letter_id)
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
        project_file_root: str,
        xslt_proc: Optional[PyXslt30Processor],
        xml_to_html_stylesheets: bool = False
) -> Dict[str, Optional[PyXsltExecutable]]:
    """
    Compiles the XSLT stylesheets in the project files to Saxon XSLT
    executables. If `xml_to_html_stylesheets` is True, it compiles the
    stylesheets that transform web XML files to HTML, otherwise it
    compiles the stylesheets that transform the original XML files to
    web XML files.

    Returns:
    - A dictionary where the text types (est, com, ms ...) are keys
    and the compiled stylesheets are values. If a stylesheet for a
    text type can't be compiled, it's value will be set to None.
    """
    xslt_execs: Dict[str, Optional[PyXsltExecutable]] = {}

    if xml_to_html_stylesheets:
        # Stylesheets for web XML to HTML transformation
        xsl_map = XSL_PATH_MAP_FOR_HTML_TRANSFORMATIONS
    else:
        # Stylesheets for web XML transformation
        xsl_map = XSL_PATH_MAP_FOR_PUBLISHING

    for type_key, xsl_path in xsl_map.items():
        xsl_full_path = safe_join(project_file_root, xsl_path)

        if os.path.isfile(xsl_full_path) and xslt_proc is not None:
            try:
                xslt_execs[type_key] = xslt_proc.compile_stylesheet(
                    stylesheet_file=xsl_full_path,
                    encoding="utf-8"
                )
            except Exception:
                logger.exception("Failed to compile XSLT executable for '%s' files. Make sure '%s' exists and is valid in project root.", type_key, xsl_path)
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
                               com_xsl_path: Optional[str] = None):
    """
    Given a project name, and paths to valid EST/COM masters and targets, regenerates target files based on source files
    """
    # Generate est file for this document
    est_document = CTeiDocument()
    try:
        est_document.Load(est_master_file_path, bRemoveDelSpans=True)
        est_document.PostProcessMainText()
    except Exception as ex:
        logger.exception("Failed to handle est master file: %s", est_master_file_path)
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
        com_master_file_path = safe_join(config[project]["file_root"],
                                         COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)

        if not os.path.exists(com_master_file_path):
            logger.info("Skipping com file generation: no comments file associated with publication and no template file exists at %s", COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)
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
            com_xsl_path = safe_join(config[project]["file_root"],
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
        logger.exception("Failed to handle com master file: %s", com_master_file_path)
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
        logger.warning("XSLT executable for 'est' is missing. '%s' is invalid or does not exist in project root.", XSL_PATH_MAP_FOR_PUBLISHING.get("est"))
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
            logger.exception("Failed to handle est master file: %s", est_source_file_path)
            raise

    if not publication_info["publication_comment_id"]:
        # No publication_comment linked to publication, skip
        # generation of comments web XML
        logger.info("Skipping generation of comment file, no comment linked to publication.")
        return

    if xslt_execs["com"] is None:
        logger.warning("XSLT executable for 'com' is missing. '%s' is invalid or does not exist in project root. Comment file not generated.", XSL_PATH_MAP_FOR_PUBLISHING.get("com"))
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
        com_source_file_path = safe_join(config[project]["file_root"],
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
        logger.exception("Failed to handle com master file: %s", com_source_file_path)
        raise


def process_var_documents_and_generate_files(main_var_doc, main_var_path, var_docs, var_paths, publication_info):
    """
    Process generated CTeiDocument objects - comparing each var_doc in var_docs to the main_var_doc and saving target files
    """
    # First, compare the main variant against all other variants
    main_var_doc.ProcessVariants(var_docs)
    if publication_info is not None:
        main_var_doc.SetMetadata(publication_info['original_publication_date'],
                                 publication_info['p_id'], publication_info['name'],
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
        logger.exception("Failed to handle manuscript file: %s", master_file_path)
        raise ex

    if publication_info is not None:
        ms_document.SetMetadata(publication_info['original_publication_date'],
                                publication_info['p_id'], publication_info['name'],
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
            logger.error("XSLT executable for 'ms' is missing. '%s' is invalid or does not exist in project root.", XSL_PATH_MAP_FOR_PUBLISHING.get("ms"))
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
        logger.exception("Failed to handle manuscript file: %s", source_file_path)
        raise


def xml_to_html_xslt_modified_after_xml(
        xml_filepath: str,
        text_type: str,
        file_root: str
) -> bool:
    """
    Returns True if the XSLT files associated with the given text type
    have been modified after the given XML file has been modified,
    otherwise False. Also returns False if the XML or XSLT file paths
    are invalid.

    The XML file path must be a safe-joined full path including the
    project root.
    """
    if not os.path.isfile(xml_filepath):
        return False

    if text_type == "ms":
        xsl_text_types = ["ms_changes", "ms_normalized"]
    elif text_type == "var":
        xsl_text_types = ["var_base", "var_other"]
    else:
        xsl_text_types = [text_type]

    try:
        xml_mtime = os.path.getmtime(xml_filepath)

        for xt in xsl_text_types:
            xsl_file = XSL_PATH_MAP_FOR_HTML_TRANSFORMATIONS.get(xt)
            xsl_file = (
                safe_join(file_root, xsl_file)
                if xsl_file is not None
                else None
            )

            if xsl_file is None:
                return False

            # Compare modification time of XML and XSLT files
            if os.path.getmtime(xsl_file) > xml_mtime:
                # XSLT file has been modified after the XML file
                return True
    except OSError:
        return False

    return False


def get_xml_chapter_ids(file_path: str) -> List[str]:
    """
    Parse the given XML file and extract possible chapter divisions from it.

    Returns a list of @id values of <div type="chapter"> elements in the XML
    or an empty list if no chapter divisions in the file.
    """
    try:
        with open(file_path, "r", encoding="utf-8-sig") as xml_file:
            tree = ET.parse(xml_file)
        root = tree.getroot()

        # Declare namespace
        ns = {'tei': 'http://www.tei-c.org/ns/1.0'}

        # Find all <div type="chapter"> with @id
        ch_elems = root.xpath("./tei:text/tei:body//tei:div[@type='chapter'][@id]", namespaces=ns)
        ch_ids = []

        if ch_elems:
            for ch in ch_elems:
                id = ch.get("id")
                if id and id not in ch_ids:
                    ch_ids.append(id)

        return ch_ids

    except ET.ParseError:
        logger.exception("Parse error trying to open %s", file_path)
        raise
    except OSError:
        logger.exception("Error when trying to open %s", file_path)
        raise
    except Exception:
        logger.exception("Unexpected error when opening/parsing %s", file_path)
        raise


def clear_collection_legacy_id_cache():
    collection_legacy_id_cache.clear()


def cached_get_collection_legacy_id(collection_id: str) -> Optional[str]:
    c_id = int_or_none(collection_id)
    if c_id is None or c_id < 1:
        logger.error("Unable to convert %s into an integer.", collection_id)
        return None

    # Check if the collection id already exists in the cache,
    # `collection_legacy_id_cache` is in the global scope
    if c_id in collection_legacy_id_cache:
        return collection_legacy_id_cache.get(c_id)

    collection_table = get_table("publication_collection")

    try:
        with db_engine.connect() as connection:
            statement = (
                select(collection_table.c.legacy_id)
                .where(collection_table.c.id == c_id)
            )
            legacy_id: Optional[str] = (
                connection.execute(statement).scalar_one_or_none()
            )

            if legacy_id == "":
                legacy_id = None

            # Add the legacy id to the cache
            collection_legacy_id_cache[c_id] = legacy_id

            return legacy_id
    except Exception:
        logger.exception("Failed to query 'publication_collection' table for 'legacy_id' of collection with 'id' %s", collection_id)
        return None


def get_variant_type(publication_id: str, variant_id: str) -> Optional[int]:
    """
    Return the `type` value for a publication variant.

    Looks up `publication_version` by (publication_id, id) with `deleted < 1`
    and returns the integer in column `type`. Returns None if the IDs are not
    positive integers, no row matches, `type` is NULL, or a database error occurs.
    """
    p_id = int_or_none(publication_id)
    v_id = int_or_none(variant_id)
    if p_id is None or v_id is None or p_id < 1 or v_id < 1:
        logger.error("Unable to convert %s or %s into an integer.", publication_id, variant_id)
        return None

    variant_table = get_table("publication_version")

    try:
        with db_engine.connect() as connection:
            statement = (
                select(variant_table.c["type"])
                .where(variant_table.c.id == v_id)
                .where(variant_table.c.publication_id == p_id)
                .where(variant_table.c.deleted < 1)
            )
            return connection.execute(statement).scalar_one_or_none()
    except Exception:
        logger.exception("Failed to query 'publication_version' table for 'id' %s and 'publication_id' %s", variant_id, publication_id)
        return None


def transform_and_save(
        text_type: str,
        output_filepath: str,
        xml_filepath: str,
        xsl_filepath: Optional[str],
        xslt_params: Optional[Dict[str, str]] = None,
        saxon_proc: Optional[PySaxonProcessor] = None,
        saxon_xslt_exec: Optional[PyXsltExecutable] = None,
        output_format: str = "html"
) -> Optional[str]:
    # Calculate file fingerprint so we can determine if the output
    # file has changed after generating a new file
    pre_sig = file_fingerprint(output_filepath)

    try:
        # Ensure the folder path for the output file exists
        output_dirpath = os.path.dirname(output_filepath)
        if output_dirpath:
            os.makedirs(output_dirpath, exist_ok=True)
    except Exception:
        logger.exception("Error making dirs for path %s", output_dirpath)
        return None

    use_saxon_xslt: bool = (saxon_proc is not None and
                            saxon_xslt_exec is not None)

    try:
        content = transform_xml(
            xsl_file_path=(None if use_saxon_xslt else xsl_filepath),
            xml_file_path=xml_filepath,
            params=xslt_params,
            use_saxon=use_saxon_xslt,
            saxon_proc=saxon_proc,
            xslt_exec=(saxon_xslt_exec if use_saxon_xslt else None)
        )
    except Exception as e:
        logger.exception("Failed to transform %s: %s", xml_filepath, e)
        return None

    if not use_saxon_xslt and output_format == "html":
        # The legacy XSLT stylesheets output @id where @data-id is
        # required by the frontend, so replace them for applicable
        # text types.
        # TODO: fix this in all projects’ XSLT and then remove from here
        # TODO: and from generics.py
        if text_type in ["est", "ms", "inl", "tit", "fore"]:
            content = content.replace(" id=", " data-id=")

    # Save the transformed content
    try:
        with open(output_filepath, "w", encoding="utf-8") as outfile:
            outfile.write(content)
    except (OSError, Exception):
        logger.exception("Unexpected rrror saving %s", output_filepath)
        return None

    # Check if the output file was modified. If it was, return the file
    # path of the file, otherwise return None.
    if changed_by_size_or_hash(pre_sig, output_filepath):
        return output_filepath

    return None


def prerender_xml_to_html(
        project_file_root: str,
        xml_filepath: str,
        saxon_proc: Optional[PySaxonProcessor],
        xslt_execs: Optional[Dict[str, Optional[PyXsltExecutable]]]
) -> List[str]:
    """
    Transforms the given XML file into HTML, saves the result file(s) and
    returns the file path(s) of the saved HTML file(s).

    If `saxon_proc` and `xslt_execs` are None, lxml performs the
    transformation, if `saxon_proc` is an initialized PySaxonProcessor
    and `xslt_execs` is a dictionary of compiled Saxon XSLT stylesheets
    with text types as keys, Saxon performs the transformation.

    `xml_filepath` must be the safe-joined file path to the XML file from
    the project root.
    """
    if not os.path.isfile(xml_filepath):
        logger.error("Failed to prerender %s: source file does not exist", xml_filepath)
        return []

    # Parse filename to get collection id, publication id, text type,
    # and text type id
    file = os.path.basename(xml_filepath)  # filename with extension
    filename = os.path.splitext(file)[0]   # filename without extension
    filename_parts = filename.split("_")

    if len(filename_parts) < 3:
        logger.error("Failed to prerender %s: file name has invalid format", xml_filepath)
        return []

    coll_id = filename_parts[0]
    pub_id = ""
    text_type = ""
    type_id = ""

    if any(t in filename_parts for t in ("com", "est", "ms", "var")):
        pub_id = filename_parts[1]
        text_type = filename_parts[2]

        if any(x in filename_parts for x in ("ms", "var")):
            if len(filename_parts) < 4:
                logger.error("Failed to prerender %s: text type id missing from file name", xml_filepath)
                return []
            type_id = filename_parts[3]
    else:
        text_type = filename_parts[1]

    # For com, est, ms and var texts we need to check if the text is
    # divided into chapters, so each chapter can be rendered into a
    # separate HTML file. Since we also need to render the full text
    # we are treating it as a "chapter" with id None so it's processed
    # in the loop further down.
    chapter_ids: List[Optional[str]] = [None]
    if text_type in ["com", "est", "ms", "var"]:
        # For com files the chapter ids need to be checked from the
        # corresponding est file
        find_ch_file = (
            xml_filepath.replace(file, file.replace("com", "est"))
            if text_type == "com"
            else xml_filepath
        )
        try:
            chapter_ids = chapter_ids + get_xml_chapter_ids(find_ch_file)
        except Exception:
            logger.error("Failed to prerender %s: exception getting chapter ids.", xml_filepath)
            return []

    # Keep a list of generated HTML files that have changed. Though the
    # source XML file is just one file, in some cases (like ms) several
    # different HTML files are generated from it.
    changed_files = []

    book_id = cached_get_collection_legacy_id(coll_id) or coll_id

    var_type = (
        get_variant_type(pub_id, type_id)
        if text_type == "var"
        else None
    )
    if text_type == "var" and var_type is None:
        logger.error("Failed to prerender %s: unable to get variant type from database", xml_filepath)
        return []

    # Build a list of dictionaries with necessary information about each
    # transformation that need to be carried out
    to_transform = []

    for ch_id in chapter_ids:
        ch_filename_suffix = f'_{ch_id}' if ch_id is not None else ''
        text_type_versions = (
            ["_changes", "_normalized"]
            if text_type == "ms"
            else [""]
        )

        for type_version in text_type_versions:
            type_filename = (
                filename.replace("_ms_", f"_ms{type_version}_")
                if text_type == "ms"
                else filename
            )
            html_filename = f"{type_filename}{ch_filename_suffix}.html"
            html_filepath = safe_join(project_file_root,
                                      PRERENDERED_HTML_PATH_IN_PROJECT_ROOT,
                                      text_type,
                                      html_filename)

            if html_filepath is None:
                logger.error("Failed to prerender %s: unable to form safe path for HTML-file", xml_filepath)
                return []

            xslt_params = {"bookId": book_id}

            if ch_id is not None:
                xslt_params["sectionId"] = ch_id

            if text_type == "com":
                est_xml_path = safe_join(project_file_root,
                                         "xml",
                                         "est",
                                         file.replace("_com.xml", "_est.xml"))
                xslt_params["estDocument"] = f"file://{est_xml_path}"

            text_type_key = (
                f"{text_type}{type_version}"
                if text_type != "var"
                else ("var_base" if var_type == 1 else "var_other")
            )
            xsl_filepath = XSL_PATH_MAP_FOR_HTML_TRANSFORMATIONS.get(text_type_key)
            xsl_filepath = (
                safe_join(project_file_root, xsl_filepath)
                if xsl_filepath is not None
                else None
            )
            saxon_xslt_exec = (xslt_execs or {}).get(text_type_key)

            if saxon_proc is not None and saxon_xslt_exec is None:
                logger.error("Failed to prerender %s: Saxon XSLT executable is None", xml_filepath)
                return []

            if saxon_proc is None and (
                xsl_filepath is None or (
                    xsl_filepath is not None and
                    not os.path.isfile(xsl_filepath)
                )
            ):
                logger.error("Failed to prerender %s: XSL file %s does not exist", xml_filepath, xsl_filepath)
                return []

            to_transform.append({
                "output_filepath": html_filepath,
                "xml_filepath": xml_filepath,
                "xsl_filepath": xsl_filepath,
                "xslt_params": xslt_params,
                "saxon_xslt_exec": saxon_xslt_exec
            })

    # Perform a transformation for each dict in the list
    for t_data in to_transform:
        changed_file = transform_and_save(text_type,
                                          t_data["output_filepath"],
                                          t_data["xml_filepath"],
                                          t_data["xsl_filepath"],
                                          t_data["xslt_params"],
                                          saxon_proc,
                                          t_data["saxon_xslt_exec"],
                                          "html")
        if changed_file is not None:
            changed_files.append(changed_file)

    return changed_files


def check_publication_mtimes_and_publish_files(
        project: str,
        publication_ids: Union[tuple, None],
        git_author: str,
        no_git=False,
        force_publish=False,
        is_multilingual=False,
        use_xslt_processing=False
):
    update_success, result_str = update_files_in_git_repo(project)
    if not update_success:
        logger.error("Git update failed, terminating script run. Reason: %s", result_str)
        return False

    project_id = get_project_id_from_name(project)
    project_config = config.get(project)

    if project_id is None or project_config is None:
        logger.error("Project id not specified or missing project config. Terminating script run.")
        return False

    file_root = project_config.get("file_root")

    if file_root is None:
        logger.error("`file_root` not set in project config. Terminating script run.")
        return False

    # If publication_ids is a tuple of ints, we're (re)publishing a
    # certain publication(s).Explicitly set force_publish in this
    # instance, so we force-generate files for publishing (this
    # overrides mtime checks).
    publish_certain_ids: bool = isinstance(publication_ids, tuple)
    if publish_certain_ids:
        force_publish = True

    # Flag for prerendering XML to HTML
    prerender_xml: bool = project_config.get("prerender_xml", False)

    # Flag for using the Saxon XSLT processor for prerender transformations
    use_saxon_for_prerender: bool = project_config.get("use_saxon_xslt", False)

    if prerender_xml:
        xslt_proc_name = "Saxon" if use_saxon_for_prerender else "lxml"
        logger.info("Prerendering enabled, using %s for transformations.",
                    xslt_proc_name)

    # Clear cache of collection legacy ids
    clear_collection_legacy_id_cache()

    # Get publication, comment and manuscript data from the database
    try:
        with db_engine.connect() as connection:
            p = get_table("publication")
            pcol = get_table("publication_collection")
            pc = get_table("publication_comment")
            pm = get_table("publication_manuscript")
            tr = get_table("translation_text")

            shared_filters = [
                pcol.c.project_id == project_id,
                p.c.deleted != 1,
                pcol.c.deleted != 1
            ]
            if force_publish and publish_certain_ids:
                # append publication id checks if this is a forced
                # (re)publication of certain publication(s)
                shared_filters.append(p.c.id.in_(publication_ids))

            # publication query
            if not is_multilingual:
                pub_select = (
                    select(
                        p.c.id.label("p_id"),
                        p.c.publication_collection_id.label("c_id"),
                        p.c.original_filename.label("original_filename"),
                        p.c.published.label("published"),
                        p.c.original_publication_date.label("original_publication_date"),
                        p.c.genre.label("genre"),
                        p.c.language.label("language"),
                        p.c.publication_group_id.label("publication_group_id"),
                        p.c.publication_comment_id.label("publication_comment_id"),
                        p.c.name.label("name")
                    )
                    .select_from(
                        p.join(pcol, p.c.publication_collection_id == pcol.c.id)
                    )
                )
            else:
                pub_select = (
                    select(
                        p.c.id.label("p_id"),
                        p.c.publication_collection_id.label("c_id"),
                        tr.c.text.label("original_filename"),
                        p.c.published.label("published"),
                        p.c.original_publication_date.label("original_publication_date"),
                        p.c.genre.label("genre"),
                        p.c.publication_group_id.label("publication_group_id"),
                        p.c.publication_comment_id.label("publication_comment_id"),
                        p.c.name.label("name"),
                        tr.c.language.label("language")
                    )
                    .select_from(
                        p.join(pcol, p.c.publication_collection_id == pcol.c.id)
                        .join(
                            tr,
                            and_(p.c.translation_id == tr.c.translation_id,
                                 tr.c.field_name == "original_filename")
                        )
                    )
                )
            pub_stmt = (
                pub_select
                .where(*shared_filters)
                .order_by(p.c.publication_collection_id, p.c.id)
            )

            # comment query
            comment_filters = [*shared_filters, pc.c.deleted != 1]

            com_stmt = (
                select(
                    p.c.id.label("p_id"),
                    p.c.publication_collection_id.label("c_id"),
                    pc.c.original_filename.label("original_filename"),
                    pc.c.published.label("published"),
                    p.c.original_publication_date.label("original_publication_date"),
                    p.c.genre.label("genre"),
                    p.c.publication_group_id.label("publication_group_id"),
                    p.c.publication_comment_id.label("publication_comment_id"),
                    p.c.name.label("name")
                )
                .select_from(
                    p.join(pcol, p.c.publication_collection_id == pcol.c.id)
                    .join(pc, p.c.publication_comment_id == pc.c.id)
                )
                .where(*comment_filters)
                .order_by(p.c.publication_collection_id, p.c.id)
            )

            # manuscript query
            manuscript_filters = [*shared_filters, pm.c.deleted != 1]

            ms_stmt = (
                select(
                    pm.c.id.label("m_id"),
                    p.c.id.label("p_id"),
                    p.c.publication_collection_id.label("c_id"),
                    pm.c.original_filename.label("original_filename"),
                    pm.c.published.label("published"),
                    p.c.original_publication_date.label("original_publication_date"),
                    p.c.genre.label("genre"),
                    p.c.publication_group_id.label("publication_group_id"),
                    p.c.publication_comment_id.label("publication_comment_id"),
                    p.c.name.label("name"),
                    pm.c.name.label("m_name"),
                    pm.c.language.label("language")
                )
                .select_from(
                    pm.join(p, pm.c.publication_id == p.c.id)
                    .join(pcol, p.c.publication_collection_id == pcol.c.id)
                )
                .where(*manuscript_filters)
                .order_by(p.c.publication_collection_id,
                          p.c.id, pm.c.sort_order, pm.c.id)
            )

            publication_rows = connection.execute(pub_stmt).mappings().all()
            manuscript_rows = connection.execute(ms_stmt).mappings().all()
            comment_rows = connection.execute(com_stmt).mappings().all()
    except Exception:
        logger.exception("Unexpected error getting publication, comment and manuscript data from the database. Terminating script run.")
        return False

    comment_filenames = {
        row["p_id"]: row["original_filename"] for row in comment_rows
    }

    # Initialize variables for Saxon XSLT transformations
    saxon_proc: Optional[PySaxonProcessor] = None
    xslt_proc: Optional[PyXslt30Processor] = None
    xml_xslt_execs: Optional[Dict[str, Optional[PyXsltExecutable]]] = None
    html_xslt_execs: Optional[Dict[str, Optional[PyXsltExecutable]]] = None

    if use_xslt_processing or (prerender_xml and use_saxon_for_prerender):
        # Initialise a Saxon processor and Saxon XSLT 3.0 processor
        # Documentation for SaxonC's Python API:
        # https://www.saxonica.com/saxon-c/doc12/html/saxonc.html
        saxon_proc: PySaxonProcessor = PySaxonProcessor(license=False)
        xslt_proc: PyXslt30Processor = saxon_proc.new_xslt30_processor()

    if use_xslt_processing:
        # Compile the XSLT stylesheets used to transform the original XML
        # to web XML.
        # The compiled Saxon stylesheets are stored in a dictionary where
        # the text types (est, com, ms) are keys and the compiled
        # stylesheets are values. If a stylesheet for a text type can't be
        # compiled, it's value will be set to None.
        xml_xslt_execs: Dict[str, Optional[PyXsltExecutable]] = (
            compile_xslt_stylesheets(file_root, xslt_proc)
        )

    if prerender_xml and use_saxon_for_prerender:
        # Compile the XSLT stylesheets used to transform the web XML to
        # HTML. and store
        # The compiled Saxon stylesheets are stored in a dictionary where
        # the text types (est, com, ms, etc.) are keys and the compiled
        # stylesheets are values. If a stylesheet for a text type can't be
        # compiled, it's value will be set to None.
        html_xslt_execs: Dict[str, Optional[PyXsltExecutable]] = (
            compile_xslt_stylesheets(file_root, xslt_proc)
        )

    # Keep a list of changed XML files for later git commit
    xml_changes = set()
    # Keep a list of changed HTML files for later git commit
    html_changes = set()

    logger.info("Publications to process: %s", len(publication_rows))
    logger.info("Manuscripts to process: %s", len(manuscript_rows))

    # For each publication belonging to this project, check the
    # modification timestamp of its master files and compare them
    # to the generated web XML files
    for row in publication_rows:
        p_row = dict(row)
        publication_id = p_row["p_id"]
        collection_id = p_row["c_id"]

        # ****** READING TEXT AND COMMENTS ******
        logger.info("Processing reading text, comments and variants for publication %s: %s",
                    publication_id, p_row["name"])

        if not p_row["original_filename"]:
            logger.warning("Publication `original_filename` not set, skipping to next publication!")
            continue
        est_target_filename = "{}_{}_est.xml".format(collection_id, publication_id)
        com_target_filename = est_target_filename.replace("_est.xml", "_com.xml")

        if is_multilingual:
            language = p_row["language"]
            est_target_filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)

        est_target_file_path = safe_join(file_root, "xml", "est", est_target_filename)
        com_target_file_path = safe_join(file_root, "xml", "com", com_target_filename)
        # original_filename should be relative to the project root
        est_source_file_path = safe_join(file_root, p_row["original_filename"])

        # Get comment filename if a comment is linked to the publication
        # in the database. Default to template comment file if no entry
        # in publication_comment pointing to a comments file for this
        # publication. If no comment linked to the publication, set
        # comment file to None, so we can skip the generation of a
        # comment web file.
        if p_row["publication_comment_id"]:
            comment_file = comment_filenames.get(publication_id, COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)
        else:
            comment_file = None

        # Add the comment filename to the row dict so it can be passed
        # to called functions
        p_row["com_original_filename"] = comment_file

        if os.path.isdir(est_source_file_path):
            logger.error("Source file %s for reading text is a directory, skipping to next publication!", est_source_file_path)
            continue
        if not os.path.exists(est_source_file_path):
            # TODO: if no est source file we skip generating variant files
            # for the publication, since they are processed in the same loop.
            # This is problematic because we could have projects that have
            # variants but no established texts. Currently we don’t, but in
            # the future we might.
            logger.warning("Source file %s for reading text does not exist, skipping to next publication!", est_source_file_path)
            continue

        # Check comment file existence only if a comment is linked to the
        # publication in the database. If no comment linked to the
        # publication, set comment source file path to empty string, so
        # we can skip the generation of a comment web file.
        if comment_file:
            com_source_file_path = safe_join(file_root, comment_file)

            if os.path.isdir(com_source_file_path):
                logger.error("Source file %s for comment is a directory, skipping to next publication!", com_source_file_path)
                continue
            if not os.path.exists(com_source_file_path):
                logger.error("Source file %s for comment does not exist, skipping to next publication!", com_source_file_path)
                continue
        else:
            com_source_file_path = ""

        if force_publish:
            # during force_publish, just generate
            logger.debug("Generating new est/com XML-files.")
            try:
                # calculate file fingerprints for existing files, so we can later
                # compare if they have changed
                pre_est = file_fingerprint(est_target_file_path)
                pre_com = file_fingerprint(com_target_file_path)

                if use_xslt_processing:
                    generate_est_and_com_files_with_xslt(p_row,
                                                         project,
                                                         est_source_file_path,
                                                         com_source_file_path,
                                                         est_target_file_path,
                                                         com_target_file_path,
                                                         saxon_proc,
                                                         xml_xslt_execs)
                else:
                    generate_est_and_com_files(p_row,
                                               project,
                                               est_source_file_path,
                                               com_source_file_path,
                                               est_target_file_path,
                                               com_target_file_path)
            except Exception:
                logger.exception("Failed to generate reading text and comments files, skipping to next publication!")
                continue
            else:
                # check if est and/or com files have changed
                if changed_by_size_or_hash(pre_est, est_target_file_path):
                    xml_changes.add(est_target_file_path)
                if changed_by_size_or_hash(pre_com, com_target_file_path):
                    xml_changes.add(com_target_file_path)

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
                logger.warning("Error getting time_modified for target or source files for reading text or comments, generating new est/com XML-files.")
                try:
                    # calculate file fingerprints for existing files, so we can later
                    # compare if they have changed
                    pre_est = file_fingerprint(est_target_file_path)
                    pre_com = file_fingerprint(com_target_file_path)

                    if use_xslt_processing:
                        generate_est_and_com_files_with_xslt(p_row,
                                                             project,
                                                             est_source_file_path,
                                                             com_source_file_path,
                                                             est_target_file_path,
                                                             com_target_file_path,
                                                             saxon_proc,
                                                             xml_xslt_execs)
                    else:
                        generate_est_and_com_files(p_row,
                                                   project,
                                                   est_source_file_path,
                                                   com_source_file_path,
                                                   est_target_file_path,
                                                   com_target_file_path)
                except Exception:
                    logger.exception("Unexpected error generating reading text and comments files, skipping to next publication!")
                    continue
                else:
                    # check if est and/or com files have changed
                    if changed_by_size_or_hash(pre_est, est_target_file_path):
                        xml_changes.add(est_target_file_path)
                    if changed_by_size_or_hash(pre_com, com_target_file_path):
                        xml_changes.add(com_target_file_path)
            else:
                if est_target_mtime >= est_source_mtime and com_target_mtime >= com_source_mtime:
                    # If both the est and com files are newer than the source files, just continue to the next publication
                    continue
                else:
                    # If one or either is outdated, generate new ones
                    logger.debug("XML-files are outdated, generating new reading text and comments files.")
                    try:
                        # calculate file fingerprints for existing files, so we can later
                        # compare if they have changed
                        pre_est = file_fingerprint(est_target_file_path)
                        pre_com = file_fingerprint(com_target_file_path)

                        if use_xslt_processing:
                            generate_est_and_com_files_with_xslt(p_row,
                                                                 project,
                                                                 est_source_file_path,
                                                                 com_source_file_path,
                                                                 est_target_file_path,
                                                                 com_target_file_path,
                                                                 saxon_proc,
                                                                 xml_xslt_execs)
                        else:
                            generate_est_and_com_files(p_row,
                                                       project,
                                                       est_source_file_path,
                                                       com_source_file_path,
                                                       est_target_file_path,
                                                       com_target_file_path)
                    except Exception:
                        logger.exception("Unexpected error generating reading text and comments files, skipping to next publication!")
                        continue
                    else:
                        # check if est and/or com files have changed
                        if changed_by_size_or_hash(pre_est, est_target_file_path):
                            xml_changes.add(est_target_file_path)
                        if changed_by_size_or_hash(pre_com, com_target_file_path):
                            xml_changes.add(com_target_file_path)

        if prerender_xml:
            # * Prerender XML to HTML for established texts and comments

            # If force_publish, always render an est HTML-file because the XSLT
            # might have changed since last time. Otherwise, render est HTML if
            # the est web XML file was changed or the XSLT is newer than the
            # web XML file.
            # * Note! Prerendering is currently not supported for multilingual
            # * established texts.
            if (
                not is_multilingual and (
                    force_publish or
                    est_target_file_path in xml_changes or
                    xml_to_html_xslt_modified_after_xml(
                        est_target_file_path, "est", file_root
                    )
                )
            ):
                # prerender est
                logger.debug("Prerendering HTML for reading text.")
                est_html_file = prerender_xml_to_html(file_root,
                                                      est_target_file_path,
                                                      saxon_proc,
                                                      html_xslt_execs)
                html_changes.update(est_html_file)

            # If force_publish and a comment source file exists, always render a
            # com HTML-file because the XSLT might have changed since last time
            # and there is no way of checking if. Otherwise, render com HTML if
            # the com web XML file was changed.
            if (
                (force_publish and comment_file) or
                com_target_file_path in xml_changes or
                (
                    comment_file and
                    xml_to_html_xslt_modified_after_xml(
                        com_target_file_path, "com", file_root
                    )
                )
            ):
                # prerender com
                logger.debug("Prerendering HTML for comments.")
                com_html_file = prerender_xml_to_html(file_root,
                                                      com_target_file_path,
                                                      saxon_proc,
                                                      html_xslt_execs)
                html_changes.update(com_html_file)

        # ****** VARIANTS ******
        # Process all variants belonging to this publication
        # publication_version with type=1 is the "main" variant, the others
        # should have type=2 and be versions of that main variant
        try:
            with db_engine.connect() as connection:
                var_table = get_table("publication_version")
                variants_stmt = (
                    select(
                        var_table.c.id,
                        var_table.c.original_filename,
                        var_table.c.type
                    )
                    .where(var_table.c.publication_id == publication_id)
                    .where(var_table.c.deleted != 1)
                    .where(var_table.c.type.in_([1, 2]))
                    .order_by(
                        var_table.c.type,
                        var_table.c.sort_order,
                        var_table.c.id
                    )
                )
                variants_info = connection.execute(variants_stmt).mappings().all()
        except Exception:
            logger.exception("Unexpected error getting publication variants data, skipping to next publication!")
            continue

        if not variants_info:
            logger.debug("No variants found for the publication, skipping to next publication!")
            continue

        variants_info = [dict(v) for v in variants_info]
        type1_variants = [v for v in variants_info if int(v.get("type", -1)) == 1]

        if not type1_variants:
            logger.error("No main variant found for the publication, skipping to next publication!")
            continue
        elif len(type1_variants) > 1:
            logger.error("Multiple main variants found for the publication (variant ids %s), skipping to next publication!", ", ".join(str(v.get("id")) for v in type1_variants))
            continue

        main_variant = type1_variants[0]
        other_variants = [v for v in variants_info if int(v.get("type", -1)) == 2]

        if main_variant["original_filename"] is None:
            logger.error("`original_filename` is not set for main variant %s, skipping to next publication!", main_variant["id"])
            continue

        main_variant_source = safe_join(file_root, main_variant["original_filename"])

        if not main_variant_source:
            logger.error("Untrusted source file path for main variant %s, skipping to next publication!", main_variant["id"])
            continue
        if os.path.isdir(main_variant_source):
            logger.error("Source file %s for main variant %s is a directory, skipping to next publication!", main_variant_source, main_variant["id"])
            continue
        if not os.path.exists(main_variant_source):
            logger.error("Source file %s for main variant %s does not exist, skipping to next publication!", main_variant_source, main_variant["id"])
            continue

        target_filename = f"{collection_id}_{publication_id}_var_{main_variant['id']}.xml"

        # If any variants have changed, we need a CTeiDocument for the
        # main variant to ProcessVariants() with
        main_variant_target = safe_join(file_root, "xml", "var", target_filename)

        main_variant_doc = CTeiDocument()
        main_variant_doc.Load(main_variant_source)

        # For each "other" variant, create a new CTeiDocument if needed,
        # but if main_variant_updated is True, just make a new for all
        variant_docs = []
        variant_paths = []
        # Build a list of all variants regardless of change status,
        # so they can be prerendered to HTML if necessary
        all_variant_paths = [main_variant_target]

        for variant in other_variants:
            variant_id = variant["id"]

            if not variant["original_filename"]:
                logger.error("`original_filename` is not set for variant %s, skipping to next variant!", variant_id)
                continue

            source_file_path = safe_join(file_root, variant["original_filename"])

            if not source_file_path:
                logger.error("Untrusted source file path for variant %s, skipping to next variant!", variant_id)
                continue
            if os.path.isdir(source_file_path):
                logger.error("Source file %s for variant %s is a directory, skipping to next variant!", source_file_path, variant_id)
                continue
            if not os.path.exists(source_file_path):
                logger.error("Source file %s for variant %s does not exist, skipping to next variant!", source_file_path, variant_id)
                continue

            target_filename = f"{collection_id}_{publication_id}_var_{variant_id}.xml"
            target_file_path = safe_join(file_root, "xml", "var", target_filename)

            all_variant_paths.append(target_file_path)

            # in a force_publish, just load all variants for generation/processing
            if force_publish:
                logger.debug("Generating new var XML-file for variant %s.", variant_id)
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
                    logger.warning("Error getting time_modified for target or source files for variant %s, generating new var XML-file.", variant_id)
                    variant_doc = CTeiDocument()
                    variant_doc.Load(source_file_path)
                    variant_docs.append(variant_doc)
                    variant_paths.append(target_file_path)
                else:
                    if target_mtime < source_mtime:
                        logger.debug("File %s is older than source file %s, generating new file.", target_file_path, source_file_path)
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
                                                 p_row)

        # check if main variant has changed
        if changed_by_size_or_hash(pre_main_variant, main_variant_target):
            xml_changes.add(main_variant_target)

        # check if each variant has changed
        for path, pre_fp in pre_variants.items():
            if changed_by_size_or_hash(pre_fp, path):
                xml_changes.add(path)

        if prerender_xml:
            # * Prerender XML to HTML for variants
            for xml_path in all_variant_paths:
                # If force_publish, always render var HTML-file
                # because the XSLT might have changed since last
                # time. Otherwise, render var HTML if the var web
                # XML file was changed or the XSLT is newer than the
                # web XML file.
                if (
                    force_publish or
                    xml_path in xml_changes or
                    xml_to_html_xslt_modified_after_xml(
                        xml_path, "var", file_root
                    )
                ):
                    # prerender var
                    logger.debug("Prerendering HTML for variant %s.", xml_path)
                    var_html_file = prerender_xml_to_html(file_root,
                                                          xml_path,
                                                          saxon_proc,
                                                          html_xslt_execs)
                    html_changes.update(var_html_file)

    # ****** MANUSCRIPTS ******
    # For each publication_manuscript belonging to this project, check
    # the modification timestamp of its master file and compare it to
    # the generated web XML file

    # Build a list of all manuscripts regardless of change status,
    # so they can be prerendered to HTML if necessary
    all_ms_target_paths = []

    for row in manuscript_rows:
        m_row = dict(row)
        collection_id = m_row["c_id"]
        publication_id = m_row["p_id"]
        manuscript_id = m_row["m_id"]

        if not m_row["original_filename"]:
            logger.error("`original_filename` is not set for manuscript %s, skipping to next manuscript!", manuscript_id)
            continue

        source_file_path = safe_join(file_root, m_row["original_filename"])

        if not source_file_path:
            logger.error("Untrusted source file path for manuscript %s, skipping to next manuscript!", manuscript_id)
            continue
        if os.path.isdir(source_file_path):
            logger.error("Source file %s for manuscript %s is a directory, skipping to next manuscript!", source_file_path, manuscript_id)
            continue
        if not os.path.exists(source_file_path):
            logger.error("Source file %s for manuscript %s does not exist, skipping to next manuscript!", source_file_path, manuscript_id)
            continue

        target_filename = f"{collection_id}_{publication_id}_ms_{manuscript_id}.xml"
        target_file_path = safe_join(file_root, "xml", "ms", target_filename)

        all_ms_target_paths.append(target_file_path)

        # in a force_publish, just generate all ms files
        if force_publish:
            logger.debug("Generating new ms XML-file for manuscript %s.", manuscript_id)
            try:
                # calculate file fingerprint for existing ms file, so we can later
                # compare if it has changed
                pre_ms = file_fingerprint(target_file_path)

                if use_xslt_processing:
                    generate_ms_file_with_xslt(m_row,
                                               source_file_path,
                                               target_file_path,
                                               saxon_proc,
                                               xml_xslt_execs)
                else:
                    generate_ms_file(source_file_path,
                                     target_file_path,
                                     m_row)
            except Exception:
                logger.exception("Unexpected error generating new ms XML-file for manuscript %s, skipping to next manuscript!", manuscript_id)
                continue
            else:
                # check if ms file has changed
                if changed_by_size_or_hash(pre_ms, target_file_path):
                    xml_changes.add(target_file_path)
        # otherwise, check if this file needs generating
        else:
            try:
                target_mtime = os.path.getmtime(target_file_path)
                source_mtime = os.path.getmtime(source_file_path)
            except OSError:
                # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                # It is then easiest to just generate a new one
                logger.warning("Error getting time_modified for target or source file for manuscript %s, generating new ms XML-file.", manuscript_id)
                try:
                    # calculate file fingerprint for existing ms file, so we can later
                    # compare if it has changed
                    pre_ms = file_fingerprint(target_file_path)

                    if use_xslt_processing:
                        generate_ms_file_with_xslt(m_row,
                                                   source_file_path,
                                                   target_file_path,
                                                   saxon_proc,
                                                   xml_xslt_execs)
                    else:
                        generate_ms_file(source_file_path,
                                         target_file_path,
                                         m_row)
                except Exception:
                    logger.exception("Unexpected error generating new ms XML-file for manuscript %s, skipping to next manuscript!", manuscript_id)
                    continue
                else:
                    # check if ms file has changed
                    if changed_by_size_or_hash(pre_ms, target_file_path):
                        xml_changes.add(target_file_path)
            else:
                if target_mtime < source_mtime:
                    logger.debug("File %s is older than source file %s, generating new file.", target_file_path, source_file_path)
                    try:
                        # calculate file fingerprint for existing ms file, so we can later
                        # compare if it has changed
                        pre_ms = file_fingerprint(target_file_path)

                        if use_xslt_processing:
                            generate_ms_file_with_xslt(m_row,
                                                       source_file_path,
                                                       target_file_path,
                                                       saxon_proc,
                                                       xml_xslt_execs)
                        else:
                            generate_ms_file(source_file_path,
                                             target_file_path,
                                             m_row)
                    except Exception:
                        logger.exception("Unexpected error generating new ms XML-file for manuscript %s, skipping to next manuscript!", manuscript_id)
                        continue
                    else:
                        # check if ms file has changed
                        if changed_by_size_or_hash(pre_ms, target_file_path):
                            xml_changes.add(target_file_path)
                else:
                    # If the target ms file is newer than the source,
                    # continue to the next manuscript
                    continue

    if prerender_xml:
        # * Prerender XML to HTML for manuscripts
        for xml_path in all_ms_target_paths:
            # If force_publish, always render ms HTML-file
            # because the XSLT might have changed since last
            # time. Otherwise, render ms HTML if the ms web
            # XML file was changed or the XSLT is newer than the
            # web XML file.
            if (
                force_publish or
                xml_path in xml_changes or
                xml_to_html_xslt_modified_after_xml(
                    xml_path, "ms", file_root
                )
            ):
                # prerender ms
                logger.debug("Prerendering HTML for manuscript %s.", xml_path)
                ms_html_file = prerender_xml_to_html(file_root,
                                                     xml_path,
                                                     saxon_proc,
                                                     html_xslt_execs)
                html_changes.update(ms_html_file)

        # * Prerender XML to HTML for front matter pages (title page,
        # * foreword and introduction).
        # Since the front matter pages are not recorded in the database,
        # we have to scan the folders of the front matter pages’ XML files.
        # The front matter pages are prerendered if:
        # a) this is a force publication of all publications in the project
        # b) the XSLT stylesheet of the front matter page type has been
        #    modified later than the XML file.
        frontmatter_types = ["tit", "fore", "inl"]
        for f_type in frontmatter_types:
            xml_folder = safe_join(file_root, "xml", f_type)
            # Get file paths of all files with xml-extension in the front
            # matter type folder
            xml_file_paths = [safe_join(xml_folder, e.name)
                              for e in os.scandir(xml_folder)
                              if e.is_file() and e.name.lower().endswith(".xml")]

            for xml_path in xml_file_paths:
                if (
                    (force_publish and not publish_certain_ids) or
                    xml_to_html_xslt_modified_after_xml(
                        xml_path, f_type, file_root
                    )
                ):
                    html_file = prerender_xml_to_html(file_root,
                                                      xml_path,
                                                      saxon_proc,
                                                      html_xslt_execs)
                    html_changes.update(html_file)

    # Log a summary of warnings and errors
    if logger_flags.had_warning and logger_flags.had_error:
        logger.info("!!! There were WARNINGS and ERRORS during publisher script run !!!")
    elif logger_flags.had_error:
        logger.info("!!! There were ERRORS during publisher script run !!!")
    elif logger_flags.had_warning:
        logger.info("!!! There were WARNINGS during publisher script run !!!")

    # Log a summary of changed XML-files.
    if xml_changes:
        sorted_xml_changes = sorted(xml_changes)
        logger.info("XML changes made in publisher script run (%d):\n%s", len(xml_changes), "\n".join(sorted_xml_changes))
    else:
        logger.info("No XML changes made in publisher script run.")

    if prerender_xml:
        # Log a summary of changed HTML-files.
        if html_changes:
            sorted_html_changes = sorted(html_changes)
            logger.info("HTML changes made in publisher script run (%d):\n%s", len(html_changes), "\n".join(sorted_html_changes))
        else:
            logger.info("No HTML changes made in publisher script run.")

    # Merge the sets containing XML and HTML changes
    all_changes = xml_changes.union(html_changes)

    if not no_git and len(all_changes) > 0:
        outputs = []
        # If there are changes, try to commit them to git
        try:
            for change in all_changes:
                # Each changed file should be added, as there may be other activity in the git repo we don't want to commit
                outputs.append(run_git_command(project, ["add", change]))
            outputs.append(run_git_command(project, ["commit", "--author", git_author, "-m", "Published new web files"]))
            outputs.append(run_git_command(project, ["push"]))
        except CalledProcessError:
            logger.exception("Exception during git sync of webfile changes.")
            logger.debug("Git outputs: %s", "\n".join(outputs))


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
    parser.add_argument("--use_xslt_processing", action="store_true", help="XML files related to the publication are processed using project specific XSLT when generating web XML files.")
    parser.add_argument("--debug_logging", action="store_true", help="Enable DEBUG logging (default is INFO).")

    args = parser.parse_args()

    if args.list_projects:
        logger.info("Projects with seemingly valid configuration: %s", ", ".join(projects))
        sys.exit(0)
    else:
        if args.debug_logging:
            enable_debug_logging()

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
                logger.error("%s is not in the API configuration or lacks 'comments_database' setting, aborting...", args.project)
                sys.exit(1)
