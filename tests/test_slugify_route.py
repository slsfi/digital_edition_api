"""Regression tests for static-page route slug generation.

Run from the repository root with:
    python -m unittest discover -s tests

The production module initializes app configuration and database metadata at
import time, so these tests load only the pure `slugify_route` function from
the source file.
"""

import ast
import pathlib
import re
import unittest
import unicodedata


def load_slugify_route():
    """Load `slugify_route` without importing Flask/database dependencies."""
    generics_path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "sls_api"
        / "endpoints"
        / "generics.py"
    )
    module_ast = ast.parse(generics_path.read_text(encoding="utf-8"))
    function_node = next(
        node
        for node in module_ast.body
        if isinstance(node, ast.FunctionDef) and node.name == "slugify_route"
    )
    namespace = {
        "re": re,
        "unicodedata": unicodedata,
    }
    function_ast = ast.Module(body=[function_node], type_ignores=[])
    exec(
        compile(ast.fix_missing_locations(function_ast), generics_path, "exec"),
        namespace,
    )
    return namespace["slugify_route"]


slugify_route = load_slugify_route()


class SlugifyRouteTest(unittest.TestCase):
    def test_transliterates_swedish_characters(self):
        self.assertEqual(slugify_route("02 - L\u00e4s.md"), "las")
        self.assertEqual(
            slugify_route("03 - Fr\u00e5n Kuddn\u00e4s till Ule\u00e5borg.md"),
            "fran-kuddnas-till-uleaborg",
        )
        self.assertEqual(
            slugify_route("04 - Ra\u0308ttelser och tilla\u0308gg.md"),
            "rattelser-och-tillagg",
        )

    def test_removes_sort_prefixes_per_path_segment(self):
        self.assertEqual(
            slugify_route(
                "03 - Om utg\u00e5van/01 - Om Zacharias Topelius/"
                "02 - Biografi/07 - Livet p\u00e5 Bj\u00f6rkudden.md"
            ),
            "om-utgavan/om-zacharias-topelius/biografi/livet-pa-bjorkudden",
        )
        self.assertEqual(
            slugify_route("01 Zacharias Topelius i korthet.md"),
            "zacharias-topelius-i-korthet",
        )

    def test_preserves_digits_that_are_part_of_titles(self):
        self.assertEqual(slugify_route("01 - Chapter 2.md"), "chapter-2")
        self.assertEqual(slugify_route("01 - 2024 Overview.md"), "2024-overview")
        self.assertEqual(
            slugify_route("08 - omslag/214 - finland_i_19de_seklet.md"),
            "omslag/finland-i-19de-seklet",
        )
        self.assertEqual(slugify_route("11 - bildbank/39.md"), "bildbank/39")

    def test_only_removes_final_markdown_extension(self):
        self.assertEqual(slugify_route("01 - somdething.md"), "somdething")
        self.assertEqual(slugify_route("01 - File.MD"), "file")

    def test_normalizes_windows_path_separators(self):
        self.assertEqual(
            slugify_route("03 - Om utg\u00e5van\\01 - Om Zacharias.md"),
            "om-utgavan/om-zacharias",
        )

    def test_omits_numeric_grouping_directories(self):
        self.assertEqual(
            slugify_route("12/01 - avancerad s\u00f6kning.md"),
            "avancerad-sokning",
        )


if __name__ == "__main__":
    unittest.main()
