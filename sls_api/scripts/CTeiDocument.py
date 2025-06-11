# Imports
from lxml import etree as ET
import re
import io
from io import StringIO
from bs4 import BeautifulSoup

# Configuration
# ------------------
config = dict()
config['namespace_prefix'] = 'tei'
config['namespace_xmlns'] = b'xmlns="http://www.tei-c.org/ns/1.0"'
config['namespace_url'] = 'http://www.tei-c.org/ns/1.0'
# Text types
# ------------------
config_texttypes = dict()
config_texttypes['est'] = 'ce_readingtext'
config_texttypes['com'] = 'ce_annotations'
config_texttypes['ms'] = 'ce_manuscript'
config_texttypes['var'] = 'ce_version'
# Genres
# ------------------
# These can be any but should be linked to one of the standard cg_ genres.
# If you use the cg_ genres already in your xml files, then config_genres can
# be left as an empty dictionary
config_genres = dict()
config_genres['lyrik'] = 'cg_poem'
config_genres['drama'] = 'cg_drama'
config_genres['historia och geografi'] = 'cg_nonfiction'
config_genres['dagböcker'] = 'cg_diary'
config_genres['prosa'] = 'cg_prose'
config_genres['brev'] = 'cg_letter'
config_genres['barnlitteratur'] = 'cg_childrensliterature'
config_genres['kontrakt'] = 'cg_nonfiction'
# Common strings
# ------------------
config_strings = dict()
config_strings['footnote'] = 'Fotnot'
config_strings['header'] = 'Rubrik'
config_strings['date'] = 'Datering'
config_strings['header-poem'] = 'Titel'


# ------------------------------------------------
# CTeiDocument Class
# This class will process the internal working copies
# of xml files to be publication ready
class CTeiDocument:

    # ------------------------------------------------
    # Class constructor
    def __init__(self):
        self.csTitle = ""  # The generic title for the document if it is missing in the xml file
        self.xmlTree = None
        self.xmlRoot = None
        self.sNamespace = config['namespace_xmlns']
        self.sNamespaceUrl = config['namespace_url']
        self.sPrefix = config['namespace_prefix']
        self.sPrefixUrl = '{' + config['namespace_url'] + '}'
        self.dNamespaces = {'tei', self.sNamespaceUrl}
        self.textTypes = config_texttypes
        self.sGenres = config_genres
        self.strings = config_strings

    # ------------------------------------------------
    # Loads an xml document from a file
    def Load(self, sFileName, bRemoveDelSpans=False):
        try:
            with io.open(sFileName, mode="rb") as xml_file:
                xml_contents = xml_file.read()
                return self.LoadString(xml_contents, bRemoveDelSpans)
        except EnvironmentError:
            raise

    # ------------------------------------------------
    # Loads an xml document from a string
    def LoadString(self, sXml, bRemoveDelSpans=False):
        # Replace namespace if necessary
        sXml = re.sub(br"xmlns=\"[^\"\\r\\n]*\"", self.sNamespace, sXml)
        # Remove link to stylesheet if it exists
        sXml = re.sub(br"\<\?xml-stylesheet(.*)\?\>", b'', sXml)
        # Remove delspans
        if bRemoveDelSpans:
            sXml = CTeiDocument.RemoveDelSpans(sXml)
        # Move note end anchors (this has previously done as a last step but it should be ok to do it before other processing)
        sXml = self.__MoveEndAnchors(sXml)
        # Create an xml parser, read the xml from string
        self.xmlTree = ET.ElementTree(ET.fromstring(sXml))
        # Get root element
        self.xmlRoot = self.xmlTree.getroot()
        # Return success
        return True

    # ------------------------------------------------
    # Processes a "main" text, aka. reading text
    def PostProcessMainText(self):
        # Cleanup root element (remove attributes)
        self.__CleanupRootElement()
        # Insert attribute to preserve space (if not already exists)
        self.__InsertXmlPreserveSpaceAttribute()
        # Auto number elements
        self.__AutoNumberElements(True)
        # Insert class declaration
        self.__InsertClassDecl()
        # return success
        return True

    # ------------------------------------------------
    # Processes other texts (variants, manuscripts, etc.)
    def PostProcessOtherText(self):
        # Cleanup root element (remove attributes)
        self.__CleanupRootElement()
        # Insert attribute to preserve space (if not already exists)
        self.__InsertXmlPreserveSpaceAttribute()
        # Auto number elements
        self.__AutoNumberElements(True)
        # Insert class declaration
        self.__InsertClassDecl()
        # return success
        return True

    # ------------------------------------------------
    # Move comment end anchors to outside of a possible persName etc. element
    # End anchors can be placed "on the wrong side" by the commenting tool
    def __MoveEndAnchors(self, xml):
        replacedXml = re.sub(b"(<anchor[^\\/]+xml:id=\"end[^\\/]+\\/>)(<\\/(persName|placeName|title|reg|foreign|rs)>)",
                             b"\\g<2>\\g<1>", xml)
        return replacedXml

    # ------------------------------------------------
    # Removes all attributes from the root element
    # Named RemoveLinkToStyleSheet in old tool
    def __CleanupRootElement(self):
        # Link to stylesheet is automatically remove so no need for that
        # Remove all attributes of root element, we need to use list so the keys of the dictionary are copied
        for key in list(self.xmlRoot.attrib):
            self.xmlRoot.attrib.pop(key)
        return True

    # ------------------------------------------------
    # Inserts an xml:space="preserve" attribute for the first body element found
    def __InsertXmlPreserveSpaceAttribute(self):
        # Find body element
        elemBody = self.xmlRoot.find('.//' + self.sPrefixUrl + 'body')
        if elemBody is not None:
            # Set space preserve attribute (standard xml namespace)
            elemBody.set('{http://www.w3.org/XML/1998/namespace}space', 'preserve')
            return True
        else:
            return False

    # ------------------------------------------------
    # Numbers all block elements, poem lines etc.
    def __AutoNumberElements(self, bNumberLines):
        # Auto number all block elements
        # Find all paragraph nodes using xpath
        oNodes = self.xmlRoot.xpath(
            './/' + self.sPrefix + ':text/' + self.sPrefix + ':body//*[self::' + self.sPrefix + ':p or self::' + self.sPrefix + ':lg or self::' + self.sPrefix + ':list or self::' + self.sPrefix + ':sp or self::' + self.sPrefix + ':castList]',
            namespaces={self.sPrefix: self.sNamespaceUrl})
        # Iterate all nodes and number them
        iCounter = 1
        sDivId = ''
        sPreviousDivId = ''
        sNodePrefix = ''
        for oNode in oNodes:
            # Find possible chapter ancestor div
            oDivNodes = oNode.xpath('./ancestor::' + self.sPrefix + ':div[@type="chapter"]',
                                    namespaces={self.sPrefix: self.sNamespaceUrl})
            # If inside chapter, get the id of the chapter
            if len(oDivNodes) > 0:
                if 'id' in oDivNodes[0].attrib:
                    sDivId = oDivNodes[0].attrib['id']
                else:
                    sDivId = ''
            # Check if chapter has changed, then reset numbering
            if sDivId != sPreviousDivId:
                iCounter = 1
            # Get node prefix for id
            sNodePrefix = self.__GetNodePrefix(oNode)
            # Create the id attribute
            sValue = ''
            if len(sDivId) > 2:
                sValue = sNodePrefix + sDivId[2:] + '_' + str(iCounter)
            else:
                sValue = sNodePrefix + str(iCounter)
            oNode.attrib['{http://www.w3.org/XML/1998/namespace}id'] = sValue
            # Increase counter and set previous chapter div id to current
            iCounter += 1
            sPreviousDivId = sDivId
        # --------------------
        # Auto number all lines in poems
        if bNumberLines:
            # Find all div elements
            oNodes = self.xmlRoot.xpath('.//' + self.sPrefix + ':div', namespaces={self.sPrefix: self.sNamespaceUrl})
            # Iterate all div elements
            for oNode in oNodes:
                # Check if div is a poem
                if 'type' in oNode.attrib:
                    if oNode.attrib['type'] == 'poem':
                        # Reset line counter
                        iCounter = 1
                        # Find all lines in the poem
                        oLineNodes = oNode.xpath('.//' + self.sPrefix + ':l',
                                                 namespaces={self.sPrefix: self.sNamespaceUrl})
                        # Iterate all lines and add line numbers, except for lines that are divided into parts, which should be counted as just one line
                        for oLineNode in oLineNodes:
                            if 'part' in oLineNode.attrib:
                                if oLineNode.attrib['part'] == 'I':
                                    oLineNode.attrib['n'] = str(iCounter)
                                    iCounter += 1
                            else:
                                oLineNode.attrib['n'] = str(iCounter)
                                iCounter += 1
        # --------------------
        # Auto number all tables
        self.__AutoNumber('table', 'table')
        # --------------------
        # Auto number all headers
        self.__AutoNumber('head', 'h')
        # Return success
        return True

    # ------------------------------------------------
    # Common autonumber
    def __AutoNumber(self, elementName, prefix):
        # Find all table elements
        oNodes = self.xmlRoot.xpath('.//' + self.sPrefix + ':' + elementName,
                                    namespaces={self.sPrefix: self.sNamespaceUrl})
        # Iterate all table elements
        iCounter = 1
        for oNode in oNodes:
            # Remove possible id attribute (artefact from the Topelius project)
            if 'id' in oNode.attrib:
                oNode.attrib.pop('id')
            # Create the table number attribute
            sValue = prefix + str(iCounter)
            oNode.attrib['{http://www.w3.org/XML/1998/namespace}id'] = sValue
            # Increase the counter value
            iCounter += 1
        # Return success
        return True

    # ------------------------------------------------
    # Get the node type prefix for an element
    def __GetNodePrefix(self, node):
        sNodeName = ET.QName(node.tag).localname
        sPrefix = ''
        if sNodeName == 'castList':
            sPrefix = 'cl'
        else:
            sPrefix = sNodeName
        return sPrefix

    # ------------------------------------------------
    # Insert class declarations into the header
    def __InsertClassDecl(self):

        # Get the profileDesc element
        elemProfileDesc = self.xmlRoot.find('.//' + self.sPrefixUrl + 'profileDesc')
        if elemProfileDesc is not None:

            # Get or create the encodingDesc element
            elemEncodingDesc = self.xmlRoot.find('.//' + self.sPrefixUrl + 'encodingDesc')
            if elemEncodingDesc is None:
                elemTeiHeader = self.xmlRoot.find('.//' + self.sPrefixUrl + 'teiHeader')
                elemEncodingDesc = ET.SubElement(elemTeiHeader, 'encodingDesc')

            # Get or create the elemClassDecl element
            elemClassDecl = self.xmlRoot.find('.//' + self.sPrefixUrl + 'classDecl')
            if elemClassDecl is None:
                elemClassDecl = ET.SubElement(elemEncodingDesc, 'classDecl')

            # Remove taxonomy elements if they exist
            for elemRemove in elemClassDecl.xpath("//" + self.sPrefix + ":taxonomy[@xml:id='cat_genre']",
                                                  namespaces={self.sPrefix: self.sNamespaceUrl}):
                elemRemove.getparent().remove(elemRemove)
            for elemRemove in elemClassDecl.xpath("//" + self.sPrefix + ":taxonomy[@xml:id='cat_editorial']",
                                                  namespaces={self.sPrefix: self.sNamespaceUrl}):
                elemRemove.getparent().remove(elemRemove)

            # Create genre category element
            sAtts = ['cg_poem', 'cg_letter', 'cg_childrensliterature', 'cg_diary', 'cg_nonfiction', 'cg_prose',
                     'cg_drama']
            sDescs = ['Poem', 'Letter', 'Children\'s literature', 'Diary', 'Non-fiction', 'Prose', 'Drama']
            # Create the taxonomy element
            elemTaxonomy = ET.SubElement(elemClassDecl, 'taxonomy')
            elemTaxonomy.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'cat_genre'
            # Create sub elements for taxonomy
            for i, val in enumerate(sAtts):
                elemCategory = ET.SubElement(elemTaxonomy, 'category')
                elemCategory.attrib['{http://www.w3.org/XML/1998/namespace}id'] = val
                elemCatDesc = ET.SubElement(elemCategory, 'catDesc')
                elemCatDesc.text = sDescs[i]

            # Create editorial category element
            sAtts = ['ce_readingtext', 'ce_introduction', 'ce_titlepage', 'ce_annotations', 'ce_basetext', 'ce_version',
                     'ce_manuscript']
            sDescs = ['Reading text', 'Introduction', 'Title Page', 'Annotations', 'Base text', 'Version', 'Manuscript']
            # Create the taxonomy element
            elemTaxonomy = ET.SubElement(elemClassDecl, 'taxonomy')
            elemTaxonomy.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'cat_editorial'
            # Create sub elements for taxonomy
            for i, val in enumerate(sAtts):
                elemCategory = ET.SubElement(elemTaxonomy, 'category')
                elemCategory.attrib['{http://www.w3.org/XML/1998/namespace}id'] = val
                elemCatDesc = ET.SubElement(elemCategory, 'catDesc')
                elemCatDesc.text = sDescs[i]

        # Return success
        return True

    # ------------------------------------------------
    # ldComments is a list of dictionaries with comment data from a database
    def ProcessCommments(self, ldComments, cMainText, sXsltPath):

        # Declare variables
        sPosition = ''

        # Get or create the div node for storing comments
        oBodyNode = self.xmlRoot.find('.//' + self.sPrefixUrl + 'body')
        oDivNode = self.__GetOrCreate(oBodyNode, '//' + self.sPrefix + ':div[@type="notes"]', 'div')
        oDivNode.attrib['type'] = 'notes'

        # Iterate all comments
        for comment in ldComments:

            # Get the position for the note in the main text
            sPosition = self.__GetNotePosition(cMainText, comment['id'])
            sPosition = re.sub('cl[0-9]+_', '', sPosition)
            sPosition = re.sub('lg[0-9]+_', '', sPosition)
            sPosition = re.sub('list[0-9]+_', '', sPosition)
            # sPosition = sPosition.replace('l', '')
            sPosition = re.sub('l([0-9]+)', r'\1', sPosition)
            sPosition = re.sub('p[0-9]+_', '', sPosition)

            # Position will be None if the note was not found in the main text, then we don't add the note.
            if sPosition is not None:

                # Create a note element
                oNoteNode = ET.SubElement(oDivNode, 'note')
                oNoteNode.attrib['type'] = 'editor'
                oNoteNode.attrib['id'] = 'en' + str(comment['id'])
                oNoteNode.attrib['target'] = '#start' + str(comment['id'])

                # Create position and chapter nodes
                if len(sPosition) > 0:
                    # Create a position node
                    oNode = ET.SubElement(oNoteNode, 'seg')
                    oNode.attrib['type'] = 'notePosition'
                    oNode.text = sPosition
                    # Create possible chapter node
                    sNewDiv = self.__GetChapter(sPosition)
                    if len(sNewDiv) > 0:
                        oNode = ET.SubElement(oNoteNode, 'seg')
                        oNode.attrib['type'] = 'noteSection'
                        oNode.text = 'ch' + sNewDiv

                # Create the lemma node
                soup = BeautifulSoup(comment['shortenedSelection'].replace('[...]', '<seg type="lemmaBreak">[...]</seg>'), "html.parser")
                xml_content = ET.fromstring('<seg type="noteLemma">' + str(soup) + '</seg>')
                oNoteNode.append(xml_content)

                # Create the text node (<seg type="noteText"> is created in the xslt file)
                try:
                    xml_content = ET.fromstring(CTeiDocument.HtmlToTeiXml(comment['description'], sXsltPath))
                    oNoteNode.append(xml_content)
                except Exception as e:
                    print(e)

        return True

    # ------------------------------------------------
    # Extract the chapter part of a note position
    def __GetChapter(self, sNotePosition):
        if sNotePosition is not None:
            if sNotePosition.find('_') >= 0:
                sTmp = sNotePosition.split('_')
                return re.sub('[^0-9.]', '', sTmp[0])
            else:
                return ''
        else:
            return ''

    # ------------------------------------------------
    # Get the note position in the reading text for a specific note id
    def __GetNotePosition(self, cMainText, iNoteId):
        # Temporary variables
        sStart = ''
        sEnd = ''
        # Find the start anchor element for the note id
        oAnchorNode = cMainText.xmlRoot.xpath('//' + cMainText.sPrefix + ':anchor[@xml:id="start' + str(iNoteId) + '"]',
                                              namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
        if len(oAnchorNode) > 0:
            # Check if start anchor is inside a foot note
            oFootNoteNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':note[@place]',
                                                 namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
            if len(oFootNoteNode) > 0:
                sStart = self.strings['footnote']
            else:
                # Not inside footnote, check if inside p
                oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':p[@xml:id]',
                                                   namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                if len(oParentNode) > 0:
                    sStart = oParentNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']
                else:
                    # Not inside p, check if inside l
                    oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':l[@n]',
                                                       namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                    if len(oParentNode) > 0:
                        sStart = 'l' + oParentNode[0].attrib['n']
                    else:
                        # Not inside l, check if inside lg
                        oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':lg[@xml:id]',
                                                           namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                        if len(oParentNode) > 0:
                            sStart = oParentNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']
                        else:
                            # Not inside lg, check if inside list
                            oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':list[@xml:id]',
                                                               namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                            if len(oParentNode) > 0:
                                sStart = oParentNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']
                            else:
                                # Not inside list, check if inside head
                                oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':head',
                                                                   namespaces={
                                                                       cMainText.sPrefix: cMainText.sNamespaceUrl})
                                if len(oParentNode) > 0:
                                    # Check if poem
                                    poemParentNode = oParentNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':div[@type="poem"]', namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                                    if 'type' in oParentNode[0].attrib:
                                        if oParentNode[0].attrib['type'] == 'title' and len(poemParentNode) > 0:
                                            sStart = self.strings['header-poem']
                                        else:
                                            sStart = self.strings['header']
                                    else:
                                        sStart = self.strings['header']
                                else:
                                    # Not inside head, check if inside dateline
                                    oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':dateline',
                                                                       namespaces={
                                                                           cMainText.sPrefix: cMainText.sNamespaceUrl})
                                    if len(oParentNode) > 0:
                                        sStart = self.strings['date']

            # Find end anchor if not inside a footnote
            if sStart != self.strings['footnote']:
                # Find the end anchor element for the note id
                oAnchorNode = cMainText.xmlRoot.xpath(
                    '//' + cMainText.sPrefix + ':anchor[@xml:id="end' + str(iNoteId) + '"]',
                    namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                if len(oAnchorNode) > 0:
                    # Check if inside p
                    oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':p[@xml:id]',
                                                       namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                    if len(oParentNode) > 0:
                        sEnd = oParentNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']
                    else:
                        # Not inside p, check if inside l
                        oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':l[@n]',
                                                           namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                        if len(oParentNode) > 0:
                            sEnd = 'l' + oParentNode[0].attrib['n']
                        else:
                            # Not inside l, check if inside list
                            oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':list[@xml:id]',
                                                               namespaces={cMainText.sPrefix: cMainText.sNamespaceUrl})
                            if len(oParentNode) > 0:
                                sEnd = oParentNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']
                            else:
                                # Not inside list, check if inside lg
                                oParentNode = oAnchorNode[0].xpath('./ancestor::' + cMainText.sPrefix + ':lg[@xml:id]',
                                                                   namespaces={
                                                                       cMainText.sPrefix: cMainText.sNamespaceUrl})
                                if len(oParentNode) > 0:
                                    sEnd = oParentNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']

            # Build and return the note position text
            if len(sStart) > 0:
                if len(sEnd) > 0 and sStart != sEnd:
                    return sStart + '-' + sEnd
                else:
                    return sStart
            else:
                return ''

        else:
            return None

    # ------------------------------------------------
    # Get the first found note id in a main text
    # This will be used to find all notes for the document
    def GetFirstNoteId(self):
        oAnchorNode = self.xmlRoot.xpath('//' + self.sPrefix + ':anchor[starts-with(@xml:id,"start")]',
                                         namespaces={self.sPrefix: self.sNamespaceUrl})
        if len(oAnchorNode) > 0:
            sAtt = oAnchorNode[0].attrib['{http://www.w3.org/XML/1998/namespace}id']
            return sAtt[5:]
        else:
            return None

    def GetAllNoteIDs(self):
        oAnchorNode = self.xmlRoot.xpath('//' + self.sPrefix + ':anchor[starts-with(@xml:id,"start")]',
                                         namespaces={self.sPrefix: self.sNamespaceUrl})
        if len(oAnchorNode) > 0:
            note_ids = []
            for elem in oAnchorNode:
                id_text = elem.attrib['{http://www.w3.org/XML/1998/namespace}id']
                note_ids.append(id_text[5:])
            return note_ids
        else:
            return None

    # ------------------------------------------------
    # Process variants, this determines and sets the type attribute of the variants
    # lcTeiDocsToRead is a list of CTeiDocument instances (variants)
    def ProcessVariants(self, lcTeiDocsToRead):

        # Get all 'app' nodes
        oNodes = self.xmlRoot.xpath('.//' + self.sPrefix + ':app', namespaces={self.sPrefix: self.sNamespaceUrl})

        # Iterate all nodes
        for oNode in oNodes:
            # Remove type attribute if it exists
            if 'type' in oNode.attrib:
                oNode.attrib.pop('type')

            # Variables for comparing
            sCurrentType = ''
            sAppNodeType = ''

            # Check if id attribute exists
            if 'id' in oNode.attrib:
                # Iterate all version documents
                for cTeiDoc in lcTeiDocsToRead:
                    # Search for variant with specific id
                    oAppNodes = cTeiDoc.xmlRoot.xpath('//' + cTeiDoc.sPrefix + ':app[@id="' + oNode.attrib['id'] + '"]',
                                                      namespaces={cTeiDoc.sPrefix: cTeiDoc.sNamespaceUrl})
                    if len(oAppNodes) > 0:
                        oAppNode = oAppNodes[0]
                        # If type attribute exists, figure out type
                        if 'type' in oAppNode.attrib:
                            sAppNodeType = oAppNode.attrib['type']
                            if sAppNodeType.find('sub') >= 0:
                                sCurrentType = 'sub'
                            if sAppNodeType.find('ort') >= 0 and sCurrentType != 'sub':
                                sCurrentType = 'ort'
                            if sAppNodeType.find('int') >= 0 and sCurrentType != 'sub' and sCurrentType != 'ort':
                                sCurrentType = 'int'

                                # Create type attribute if not empty
            if len(sCurrentType) > 0:
                oNode.attrib['type'] = sCurrentType

        return True

    # ------------------------------------------------
    # Get the main title of the document
    def GetMainTitle(self):
        # Find title
        elem = self.xmlRoot.find('.//' + self.sPrefixUrl + 'titleStmt/' + self.sPrefixUrl + 'title')
        # If title is found, return the text of the element, otherwise, return generic title
        if elem is not None:
            return elem.text
        else:
            return self.csTitle

    # ------------------------------------------------
    # Get a descriptive title for the document if it exists
    def GetCustomTitle(self):
        # Find title
        elem = self.xmlRoot.find(
            './/' + self.sPrefixUrl + 'profileDesc/' + self.sPrefixUrl + 'creation/' + self.sPrefixUrl + 'title[@type="desc"]')
        # If title is found, return the text of the element
        if elem is not None:
            return elem.text
        else:
            return None

    # ------------------------------------------------
    # This is just a "symbolic link" to GetMainTitle
    def GetVersionTitle(self):
        return self.GetMainTitle()

    # ------------------------------------------------
    def GetLetterId(self):
        # Find document title (this element is used for letter ids (database) in the Topelius project)
        elem = self.xmlRoot.find('.//' + self.sPrefixUrl + 'titleStmt/' + self.sPrefixUrl + 'title')
        # If title is found, return the text of the element
        if elem is not None:
            return re.sub('br', '', str(elem.text), flags=re.IGNORECASE)
            # If you want to validate the id, uncomment and the following lines and edit the RegEx to your needs
            # if re.match(r"^[Bb]r[0-9]", elem.text) is not None:
            #  return elem.text
            # else:
            #  return None
        else:
            return None

    # Get or create an xml element
    def __GetOrCreate(self, parent, xpath, elemName):
        oNodes = parent.xpath(xpath, namespaces={self.sPrefix: self.sNamespaceUrl})
        if len(oNodes) > 0:
            return oNodes[0]
        else:
            return ET.SubElement(parent, elemName)

    # ------------------------------------------------
    # Insert meta data into the document
    def SetMetadata(self, sOrigDate, sItemId, sMainTitle, sGenre, sTextType, sCollectionId, sGroupId):

        # Get the profileDesc element
        elemProfileDesc = self.xmlRoot.find('.//' + self.sPrefixUrl + 'profileDesc')
        # If no such element, create it
        if elemProfileDesc is None:
            oNode = self.xmlRoot.find('.//' + self.sPrefixUrl + 'teiHeader')
            elemProfileDesc = ET.SubElement(oNode, 'profileDesc')

        # Get the creation element
        elemContainer = self.__GetOrCreate(elemProfileDesc, self.sPrefix + ':creation', 'creation')

        # Create origDate
        if sOrigDate is not None and len(sOrigDate) > 0:
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':origDate', 'origDate')
            elem.text = str(sOrigDate)

        # Create default idNo and for book id
        if sItemId > 0:
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':idNo[not(@type)]', 'idNo')
            elem.text = str(sItemId)
            # For book id
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':idNo[@type="bookid"]', 'idNo')
            elem.attrib['type'] = 'bookid'
            if str(sItemId).find('_') >= 0:
                elem.text = str(sItemId[:sItemId.find('_')])

        # Create idNo for collection
        if sCollectionId > 0:
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':idNo[@type="collection"]', 'idNo')
            elem.attrib['type'] = 'collection'
            elem.text = str(sCollectionId)

        # Create idNo for group
        if sGroupId is not None and sGroupId > 0:
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':idNo[@type="group"]', 'idNo')
            elem.attrib['type'] = 'group'
            elem.text = str(sGroupId)

        # Create title element
        if sMainTitle is not None and len(sMainTitle) > 0:
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':title[@type="main"]', 'title')
            elem.attrib['type'] = 'main'
            elem.text = sMainTitle

        # Check if we have a textClass element (for genre and text type) already, if not, create it
        elemContainer = self.__GetOrCreate(elemProfileDesc, self.sPrefix + ':textClass', 'textClass')

        # Create genre
        sGenre = sGenre.lower() if sGenre is not None else ''
        sGenreId = sGenre
        # Check if genre should be mapped according to dictionary
        if sGenre in self.sGenres:
            sGenreId = self.sGenres[sGenre]
        # If we now have a genre, continue
        if len(sGenre) > 0:
            # Check if element already exists, in that case, skip
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':catRef[@target="' + sGenreId + '"]', 'catRef')
            elem.attrib['target'] = sGenreId

        # Create text class
        sTextType = sTextType.lower()
        sTextClass = sTextType
        # Check if class should be mapped according to dictionary
        if sTextType.lower() in self.textTypes:
            sTextClass = self.textTypes[sTextType]
        # If we now have a class, continue
        if len(sTextClass) > 0:
            # Check if element already exists, in that case, skip
            elem = self.__GetOrCreate(elemContainer, self.sPrefix + ':catRef[@target="' + sTextClass + '"]', 'catRef')
            elem.attrib['target'] = sTextClass

        # Return success
        return True

    # ------------------------------------------------
    # Used by the Topelius project to insert metadata in letters, can be edited to your needs
    def SetLetterTitleAndStatusAndMeta(self, letterData):
        # Get the profileDesc element
        elemProfileDesc = self.xmlRoot.find('.//' + self.sPrefixUrl + 'profileDesc')
        # If no such element, create it
        if elemProfileDesc is None:
            oNode = self.xmlRoot.find('.//' + self.sPrefixUrl + 'teiHeader')
            elemProfileDesc = ET.SubElement(oNode, 'profileDesc')

        # Get the creation element
        elemContainer = elemProfileDesc.find(self.sPrefixUrl + 'creation')
        # If no such element, create it
        if elemContainer is None:
            elemContainer = ET.SubElement(elemProfileDesc, 'creation')

        # Create the title element
        elem = ET.SubElement(elemContainer, 'title')
        elem.attrib['type'] = 'readingtext'
        elem.attrib['id'] = str(letterData['title_id'])
        elem.text = letterData['title']

        # Create element for place sent
        if len(letterData['sender_location']) > 0:
            elem = ET.SubElement(elemContainer, 'placeName')
            elem.attrib['type'] = 'sender'
            elem.attrib['id'] = str(letterData['sender_location_id'])
            elem.text = letterData['sender_location']

        # Create element for place received
        if len(letterData['reciever_location']) > 0:
            elem = ET.SubElement(elemContainer, 'placeName')
            elem.attrib['type'] = 'adressee'
            elem.attrib['id'] = str(letterData['reciever_location_id'])
            elem.text = letterData['reciever_location']

        # Create element for sender
        if len(letterData['sender']) > 0:
            elem = ET.SubElement(elemContainer, 'persName')
            elem.attrib['type'] = 'sender'
            elem.attrib['id'] = str(letterData['sender_id'])
            elem.text = letterData['sender']

        # Create element for receiver
        if len(letterData['reciever']) > 0:
            elem = ET.SubElement(elemContainer, 'persName')
            elem.attrib['type'] = 'adressee'
            elem.attrib['id'] = str(letterData['reciever_id'])
            elem.text = letterData['reciever']

        # Return success
        return True

    # ------------------------------------------------
    # Saves the xml document to a file
    def Save(self, sFileName):
        ET.ElementTree(self.xmlRoot).write(sFileName, encoding="UTF-8", xml_declaration=True)
        return True

    # ------------------------------------------------
    # Converts a HTML fragment from the commenting tool to TEI XML
    # The result is returned as a unicode formatted string
    @staticmethod
    def HtmlToTeiXml(sHtml, sXslPath):
        # Declare variables
        transform = None
        result = ""
        # Load the stylesheet
        xslt_root = ET.parse(sXslPath)
        transform = ET.XSLT(xslt_root)
        # Strip empty space from HTML
        sHtml = sHtml.strip()
        # Do the transformation
        if len(sHtml) > 0 and transform is not None:
            try:
                soup = BeautifulSoup(sHtml, "html.parser")
                soup.contents[0].unwrap()
                dom = ET.parse(StringIO("<note>" + str(soup) + "</note>"))
                newdom = transform(dom)
                result = ET.tostring(newdom, encoding='unicode')
            except Exception as e:
                print(e)
        # Return as TEI xml
        return result

    # ------------------------------------------------
    # Removes delSpans and elements "inside" delSpan element
    @staticmethod
    def RemoveDelSpans(sXml):
        sXml = sXml.decode('utf-8')
        # Initialise temporary variables
        xmlOut = ''
        position = 0
        currentPosition = 0
        # Remove "children" of all delSpan elements
        position = sXml.find('<delSpan')
        while position >= 0:
            xmlOut += sXml[currentPosition:position]
            endPosition = sXml.find('id="del', position)
            currentPosition = sXml.find('>', endPosition) + 1
            position = sXml.find('<delSpan', currentPosition)
        position = len(sXml)
        xmlOut += sXml[currentPosition:position]
        # Return the result
        xmlOut = xmlOut.encode('utf-8')
        return xmlOut
