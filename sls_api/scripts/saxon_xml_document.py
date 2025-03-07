import io
import re
from typing import Any, Dict, List, Optional

from saxonche import PySaxonApiError, PySaxonProcessor, PyXdmNode, PyXdmValue, PyXsltExecutable


class SaxonXMLDocument:
    """
    A class for processing XML documents using SaxonC's Python extension.

    This class provides methods to load, parse, and manipulate XML documents,
    with support for configurable namespaces and XPath evaluations.

    Documentation for SaxonC's Python API:
    https://www.saxonica.com/saxon-c/doc12/html/saxonc.html

    Attributes:
    - xml_doc_tree (PyXdmNode): Parsed XML tree representation.
    - xml_doc_str (str): String representation of the loaded XML document.
    - saxon_proc (PySaxonProcessor): An instance of PySaxonProcessor for
      SaxonC operations.
    - namespaces (list): List of namespaces to declare for XPath evaluation.

    Methods:
    - load_xml_file(filepath): Loads an XML document from a file and parses it.
    - transform_to_string(xslt_exec, parameters, format_output): Transforms
      the XML document using an XSLT stylesheet and returns the result document
      as a string.
    - transform_and_save(xslt_exec, output_filepath, parameters): Transforms
      the XML document using an XSLT stylesheet and saves the result
      document to a file. The class instance is also updated with the result
      document, so subsequent XPath queries or further XSLT processing is
      performed on the transformed document.
    - add_namespace(ns_prefix, ns_uri): Adds a namespace to the `namespaces`
      attribute of the class instance.
    - get_all_comment_ids(): Extracts all comment note IDs in the document.
    - get_all_comment_positions(comment_ids): Extracts the positions of all
      comment notes in the document.
    - _save_to_file(output_filepath): Saves the XML document to a file.
    - _parse_from_string(xml_str): Parses an XML document from a string.
    - _evaluate_xpath(xpath_str): Evaluates an XPath expression using the
      configured namespaces.
    - _remove_blank_lines(input_string): Removes blank lines from the given
      string.
    - _format_xml_with_line_endings(xml_string): Formats an XML string to add
      line endings after the XML declaration and processing instructions.
    - _set_xslt_parameters_from_dict(xslt_exec, parameters): Sets parameters
      for an XSLT processor from a dictionary.
    - _convert_primitive_type_to_xdm(value): Converts a primitive Python value
      to an XDM-compatible value.

    Parameters (during initialization):
    - saxon_proc (PySaxonProcessor): Instance of PySaxonProcessor for handling
      XML parsing and XPath evaluation.
    - xml_filepath (str, optional): Path to an XML file to load upon
      initialization.
    - namespaces (list, optional): A list of dictionaries containing namespace
      mappings. Defaults to TEI and XML namespaces.
    """

    def __init__(
            self,
            saxon_proc: PySaxonProcessor,
            xml_filepath: str = "",
            namespaces: Optional[List[Dict[str, str]]] = None
    ):
        """
        Initializes a SaxonXMLDocument instance.

        Parameters:
        - saxon_proc (PySaxonProcessor): An instance of PySaxonProcessor for
          handling XML parsing and XPath evaluation.
        - xml_filepath (str, optional): The file path of an XML document to
          load upon initialization. If provided, the XML document will be
          immediately loaded.
        - namespaces (list, optional): A list of dictionaries containing
          namespace mappings. Defaults to:
            [
                {"prefix": "xml", "uri": "http://www.w3.org/XML/1998/namespace"},
                {"prefix": "tei", "uri": "http://www.tei-c.org/ns/1.0"}
            ]

        Raises:
        - FileNotFoundError: If the specified XML file is not found.
        - ValueError: If the file cannot be loaded due to invalid content.
        """
        self.xml_doc_tree: Optional[PyXdmNode] = None
        self.xml_doc_str: str = ""
        self.saxon_proc: PySaxonProcessor = saxon_proc
        self.namespaces: List[Dict[str, str]] = namespaces or [
            {"prefix": "xml", "uri": "http://www.w3.org/XML/1998/namespace"},
            {"prefix": "tei", "uri": "http://www.tei-c.org/ns/1.0"}
        ]

        if xml_filepath:
            try:
                self.load_xml_file(filepath=xml_filepath)
            except FileNotFoundError:
                raise FileNotFoundError(f"The file '{xml_filepath}' does not exist.")
            except Exception as e:
                raise ValueError(f"Failed to load XML file '{xml_filepath}': {e}")

    def load_xml_file(self, filepath: str) -> bool:
        """
        Loads an XML document from a file and parses it.

        Parameters:
        - filepath (str): The file path of the XML document to load.

        Raises:
        - FileNotFoundError: If the file does not exist.
        - ValueError: If the file cannot be read or parsed.
        """
        try:
            with io.open(filepath, mode="r", encoding="utf-8-sig") as xml_file:
                self.xml_doc_str = xml_file.read()
                self.xml_doc_tree = self._parse_from_string(xml_str=self.xml_doc_str)
        except FileNotFoundError:
            raise FileNotFoundError(f"The file '{filepath}' was not found.")
        except (EnvironmentError, PySaxonApiError) as e:
            raise ValueError(f"Error reading or parsing the file '{filepath}': {e}")

    def transform_to_string(
            self,
            xslt_exec: PyXsltExecutable,
            parameters: Optional[Dict] = None,
            format_output: bool = True
    ):
        """
        Transforms the XML document using an XSLT stylesheet and returns the
        result document as a string.

        Parameters:
        - xslt_exec (PyXsltExecutable): The XSLT execution object.
        - parameters (dict, optional): A dictionary with parameters for the XSLT
          executable. Defaults to None.
        - format_output (bool, optional): Whether to postprocess the formatting
          of the transformation result or not. Defaults to True, in which case
          blank lines are removed and line endings are added after the XML
          declaration and any XML processing instructions. This prettifies the
          output, at a slight expense of performance. Generally, this is safe
          to set to True when the output is XML or HTML, but when outputting
          plain text you want to set it to False not to remove intentional
          blank lines from the result.

        Returns:
        - String representation of the result document.
        """
        # Initialize parameters as an empty dictionary if None is passed
        parameters = parameters or {}

        # Clear any parameters previously set on the XSLT executable
        xslt_exec.clear_parameters()

        if parameters:
            self._set_xslt_parameters_from_dict(xslt_exec, parameters)

        result = xslt_exec.transform_to_string(xdm_node=self.xml_doc_tree)

        if format_output:
            result = self._remove_blank_lines(result)
            result = self._format_xml_with_line_endings(result)

        return result

    def transform_and_save(
            self,
            xslt_exec: PyXsltExecutable,
            output_filepath: str,
            parameters: Optional[Dict] = None,
            format_output: bool = True
    ):
        """
        Transforms the XML document using an XSLT executable and saves the result
        document to the output filepath. The class instance is also updated with
        the result document, so subsequent XPath queries or further XSLT
        processing is performed on the transformed document.

        Parameters:
        - xslt_exec (PyXsltExecutable): The XSLT execution object.
        - output_filepath (str): The file path where the transformed document will
          be saved.
        - parameters (dict, optional): A dictionary with parameters for the XSLT
          executable. Defaults to None.
        - format_output (bool, optional): Whether to postprocess the formatting
          of the transformation result or not. Defaults to True, in which case
          blank lines are removed and line endings are added after the XML
          declaration and any XML processing instructions. This prettifies the
          output, at a slight expense of performance. Generally, this is safe
          to set to True when the output is XML or HTML, but when outputting
          plain text you want to set it to False not to remove intentional
          blank lines from the result.
        """
        self.xml_doc_str = self.transform_to_string(xslt_exec,
                                                    parameters,
                                                    format_output)
        self._save_to_file(output_filepath=output_filepath)

    def add_namespace(self, ns_prefix: str, ns_uri: str):
        """
        Adds the provided namespace to the `namespaces` attribute of the
        class instance if neither prefix or URI of the new namespace exists.

        Parameters:
        - ns_prefix (str): The prefix of the namespace.
        - ns_uri (str): The URI of the namespace.

        Returns:
        - bool: True if the namespace was added, False if not.
        """
        for ns in self.namespaces:
            if ns["prefix"] == ns_prefix or ns["uri"] == ns_uri:
                return False

        self.namespaces.append(
            {
                "prefix": ns_prefix,
                "uri": ns_uri
            }
        )
        return True

    def get_all_comment_ids(self) -> List[int]:
        """
        Extracts all comment note IDs from the XML document as integers.

        The method evaluates an XPath query to find all `@xml:id` attributes
        of <tei:anchor> elements whose IDs start with "start". The "start"
        prefix is removed, and the remaining part of the ID is converted to
        an integer.

        Returns:
        - list of int: A list of extracted comment IDs as integers.

        Raises:
        - ValueError: If any extracted ID (after removing the "start" prefix) is
          not a valid integer.
        """
        result = self._evaluate_xpath('//tei:anchor[starts-with(@xml:id,"start")]/@xml:id')

        ids = []
        if not result:
            return ids

        for i in range(result.size):
            comment_id = result[i].get_string_value(encoding="utf-8")
            try:
                ids.append(int(comment_id[5:]))  # Convert to integer after slicing
            except ValueError:
                raise ValueError(f"Invalid ID: '{comment_id}' is not convertible to an integer.")

        return ids

    def get_all_comment_positions(self, comment_ids: List[int]) -> Dict[str, Any]:
        """
        Extracts the positions of all comment notes in the XML document, based
        on the given list of comment IDs. Both start and end tags of the comment
        notes are considered.

        The positions are set accordingly:
        - The value of the @n attribute of ancestor <p>, <lg> and <l> elements.
        - The ancestor element name if the ancestor is a <head>, <note> or
          <date> element. If the ancestor i a <head> and the text type is `poem`,
          the position is set to `title`.
        - If none of the above match, the position is set to `null`.

        Returns:
        - A dictionary of comment note IDs as keys and corresponding positions
          as values.
        """
        xml_doc = self._parse_from_string(self.xml_doc_str)
        id_prefixes = ["start", "end"]
        comment_positions = {}

        for comment_id in comment_ids:
            for prefix in id_prefixes:
                position = "null"
                xml_id = prefix + str(comment_id)
                xp_result = self._evaluate_xpath('//tei:anchor[@xml:id = "' + xml_id + '"]/ancestor::*[self::tei:p or self::tei:lg or self::tei:l][@n][1]/@n', xml_doc)

                if xp_result and xp_result.size > 0:
                    position = xp_result[0].get_string_value(encoding="utf-8")
                else:
                    # The comment anchor is in an unnumbered ancestor, check if in a
                    # <head>, <note> or <date> element
                    xp_result = self._evaluate_xpath('//tei:anchor[@xml:id = "' + xml_id + '"]/ancestor::*[self::tei:head or self::tei:note or self::tei:date][1]/name()', xml_doc)

                    if xp_result and xp_result.size > 0:
                        position = xp_result[0].get_string_value(encoding="utf-8")

                        if position == "head":
                            xp_result = self._evaluate_xpath('//tei:anchor[@xml:id = "' + xml_id + '"]/ancestor::*[self::tei:text or self::tei:div][@type][1]/@type', xml_doc)

                            if xp_result and xp_result.size > 0:
                                head_type = xp_result[0].get_string_value(encoding="utf-8")

                                if head_type == "poem":
                                    position = "title"

                comment_positions[xml_id] = position

        return comment_positions

    def _save_to_file(self, output_filepath: str):
        """
        Saves the XML document to the specified output file.

        Parameters:
        - output_filepath (str): The file path where the XML document will be
          saved.
        """
        with open(output_filepath, "w", encoding="utf-8") as file:
            file.write(self.xml_doc_str)

    def _parse_from_string(self, xml_str: str) -> PyXdmNode:
        """
        Parses the XML document provided as a string.

        Parameters:
        - xml_str (str): String representation of an XML document.

        Returns:
        - The XDM node representation of the XML document (PyXdmNode).
        """
        return self.saxon_proc.parse_xml(xml_text=xml_str, encoding="utf-8")

    def _evaluate_xpath(
            self,
            xpath_str: str,
            node: Optional[PyXdmNode] = None
    ) -> PyXdmValue:
        """
        Evaluates an XPath expression using the namespaces configures
        in the class instance.

        Parameters:
        - xpath_str (str): The XPath expression to evaluate.
        - node (PyXdmNode, optional): The context node for the XPath query.

        Returns:
        - Result of the XPath evaluation (PyXdmValue).
        """
        xp_proc = self.saxon_proc.new_xpath_processor()

        for ns in self.namespaces:
            xp_proc.declare_namespace(prefix=ns["prefix"], uri=ns["uri"])

        node = node or self._parse_from_string(self.xml_doc_str)
        xp_proc.set_context(xdm_item=node)

        return xp_proc.evaluate(xpath_str, encoding="utf-8")

    def _remove_blank_lines(self, input_string: str) -> str:
        """
        Removes blank lines (lines with only whitespace or no content) from
        the given string.

        Parameters:
        - input_string (str): The input string containing lines of text.

        Returns:
        - A string with blank lines removed.
        """
        return "".join(line for line in input_string.splitlines(keepends=True) if line.strip())

    def _format_xml_with_line_endings(self, xml_string: str) -> str:
        """
        Formats an XML string to add line endings after the XML declaration
        and processing instructions.

        Parameters:
        - xml_string (str): The XML string to format.

        Returns:
        - The formatted XML string with line endings added.
        """
        # Regular expression to match the XML declaration and processing instructions
        pattern = r"(<\?xml[^>]+\?>)"

        # Replace each match with itself followed by a newline
        formatted_xml = re.sub(pattern, r"\1\n", xml_string)

        return formatted_xml

    def _set_xslt_parameters_from_dict(
            self,
            xslt_exec: PyXsltExecutable,
            parameters: dict
    ):
        """
        Sets parameters for an XSLT processor from a dictionary.

        Parameters:
        - xslt_exec (PyXsltExecutable): The XSLT execution object where the parameters
          will be set.
        - parameters (dict): A dictionary containing parameter names and their
          corresponding values. The values will be converted to the appropriate XDM
          type before being set on the XSLT processor.
        """
        for param_name, param_value in parameters.items():
            xslt_exec.set_parameter(name=param_name,
                                    value=self._convert_primitive_type_to_xdm(param_value))

    def _convert_primitive_type_to_xdm(self, value: any) -> PyXdmValue:
        """
        Converts a primitive Python value to an XDM-compatible value.

        Parameters:
        - value (any): A primitive Python value (int, str, bool, float) to be
          converted to an XDM value for use with the Saxon processor.

        Returns:
        - PyXdmValue: The XDM representation of the input value. If the input value
          does not match a recognized type, an empty XDM sequence is returned.

        Behavior:
        - Converts Python types to their XDM equivalents:
            - `int` -> XDM Integer
            - `str` -> XDM String (UTF-8 encoded by default)
            - `bool` -> XDM Boolean
            - `float` -> XDM Float
        - For unsupported types, returns an empty XDM sequence.
        """
        if isinstance(value, int):
            return self.saxon_proc.make_integer_value(value)
        elif isinstance(value, str):
            return self.saxon_proc.make_string_value(value, encoding="utf-8")
        elif isinstance(value, bool):
            return self.saxon_proc.make_boolean_value(value)
        elif isinstance(value, float):
            return self.saxon_proc.make_float_value(value)
        else:
            return self.saxon_proc.empty_sequence()
