# Lint as: python3
"""Tests for ds_sdk.core.utils.data_specs."""

import unittest

import verily.ds_sdk.core.utils.data_specs as data_specs


class DataSpecsTest(unittest.TestCase):

    def test_to_echo_name(self):
        echo_name = data_specs.to_echo_name('com.verily.heart_rate')

        self.assertEqual(echo_name, 'com_verily_heart__rate')

    def test_to_sensor_store_name(self):
        ss_name = data_specs.to_sensor_store_name('com_verily_heart__rate')

        self.assertEqual(ss_name, 'com.verily.heart_rate')


if __name__ == '__main__':
    unittest.main()
