"""Regression tests for the sample YAML merger."""

import datetime
import unittest

import merge_yaml


class MergeYAMLTests(unittest.TestCase):
    def test_empty_mapping_replaces_sample_section(self):
        merged, _ = merge_yaml.merge(
            ["global:\n", "  a: 1\n", "  b: 2\n"], {"global": {}}
        )
        self.assertEqual(merged, ["global: {}\n"])

    def test_indentationless_list_is_replaced(self):
        merged, _ = merge_yaml.merge(
            ["values:\n", "- old\n", "next: true\n"], {"values": ["new"]}
        )
        self.assertEqual("".join(merged), 'values:\n  - new\nnext: true\n')

    def test_replaced_key_keeps_sample_comment(self):
        merged, _ = merge_yaml.merge(["enabled: true # keep this note\n"], {"enabled": False})
        self.assertEqual(merged, ["enabled: false # keep this note\n"])

    def test_hash_inside_quoted_scalar_is_not_a_comment(self):
        merged, _ = merge_yaml.merge(
            ['value: "before # literal"\n'], {"value": "after"}
        )
        self.assertEqual(merged, ['value: "after"\n'])

    def test_date_scalar_is_serialized_as_string(self):
        self.assertEqual(
            merge_yaml.scalar(datetime.date(2026, 11, 15)), '"2026-11-15"'
        )


if __name__ == "__main__":
    unittest.main()
