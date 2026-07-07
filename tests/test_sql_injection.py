"""SQL injection prevention tests for RawDataIO.

Per amendment 5: hostile device_ids, table names, limits must be rejected.
ReadFromBigQuery does not support parameterized queries, so allowlist
validation is the mechanism.
"""

import unittest

from verily.raw_data_tools.io.raw_data_io import RawDataIO, _validate_identifier


class ValidateIdentifierTest(unittest.TestCase):

    def test_valid_identifiers(self):
        for ident in ['device1', 'my_table', 'project.dataset',
                      'a-b-c', 'ABC123', 'x']:
            _validate_identifier(ident, 'test')

    def test_empty_string(self):
        with self.assertRaises(ValueError):
            _validate_identifier('', 'test')

    def test_too_long(self):
        with self.assertRaises(ValueError):
            _validate_identifier('a' * 65, 'test')

    def test_sql_injection_semicolon(self):
        with self.assertRaises(ValueError):
            _validate_identifier("table; DROP TABLE users", 'table')

    def test_sql_injection_quotes(self):
        with self.assertRaises(ValueError):
            _validate_identifier("device' OR '1'='1", 'device_id')

    def test_sql_injection_double_quotes(self):
        with self.assertRaises(ValueError):
            _validate_identifier('table" OR 1=1--', 'table')

    def test_sql_injection_comment(self):
        with self.assertRaises(ValueError):
            _validate_identifier('table-- comment', 'table')

    def test_sql_injection_union(self):
        with self.assertRaises(ValueError):
            _validate_identifier('table UNION SELECT', 'table')

    def test_sql_injection_backtick(self):
        with self.assertRaises(ValueError):
            _validate_identifier('`malicious`', 'table')

    def test_sql_injection_newline(self):
        with self.assertRaises(ValueError):
            _validate_identifier('table\nDROP', 'table')

    def test_sql_injection_parens(self):
        with self.assertRaises(ValueError):
            _validate_identifier('table()', 'table')


class RawDataIOInjectionTest(unittest.TestCase):

    def test_hostile_table_name(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset='ds').read_datapoints(
                table="datapoint; DROP TABLE users--")

    def test_hostile_device_id(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset='ds').read_datapoints(
                device_ids=["dev' OR '1'='1"])

    def test_hostile_data_type(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset='ds').read_datapoints(
                data_types=["IMU' UNION SELECT * FROM secrets--"])

    def test_negative_limit(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset='ds').read_datapoints(limit=-1)

    def test_zero_limit(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset='ds').read_datapoints(limit=0)

    def test_float_limit(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset='ds').read_datapoints(limit=1.5)

    def test_hostile_project(self):
        with self.assertRaises(ValueError):
            RawDataIO(project="proj; DROP--", dataset='ds')

    def test_hostile_dataset(self):
        with self.assertRaises(ValueError):
            RawDataIO(project='proj', dataset="ds' OR 1=1")


if __name__ == '__main__':
    unittest.main()
