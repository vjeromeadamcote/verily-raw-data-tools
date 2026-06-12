"""Tests for get_study_info in __init__.py"""

import unittest

from verily.ds_sdk.core.studies import get_study_info
from verily.ds_sdk.core.studies.study_info import StudyInfo


class TestGetStudyInfo(unittest.TestCase):
    """ Tests for get_study_info in __init__.py"""

    def test_get_study_info_in_existing_map(self):
        expected_study_info = StudyInfo(
            gcp_project='echo-autopush-avery',
            internal_echo_dataset='internal_avery',
            external_echo_dataset='sensor_store_avery',
            cloud_service_account='projects/-/serviceAccounts/ds-sdk-readers@echo-autopush-avery.iam.gserviceaccount.com',  # pylint: disable=line-too-long
            registry_id='e3764ce8-a2f8-c6d4-6f84-ef1bf936f9db')
        result = get_study_info('Avery_Discovery_Trial', 'autopush')
        self.assertEqual(
            result.gcp_project,
            expected_study_info.gcp_project)
        self.assertEqual(
            result.internal_echo_dataset,
            expected_study_info.internal_echo_dataset)

if __name__ == '__main__':
    unittest.main()
