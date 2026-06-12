"""Tests for registry_service.py"""

import unittest
from unittest import mock
from unittest.mock import ANY

from verily.ds_sdk.core.grpc.registry_service import \
    find_study_info_for_registry
from verily.ds_sdk.core.grpc.registry_service import \
    format_registry_into_study_info
from verily.ds_sdk.core.grpc.registry_service import list_registries
from verily.ds_sdk.core.grpc.registry_service import \
    verify_registry_fields_for_study_info
from verily.ds_sdk.core.studies.study_info import StudyInfo
from verily.ds_sdk.protos import management_service_pb2


class RegistryServiceTest(unittest.TestCase):

    @mock.patch('verily.ds_sdk.core.grpc.registry_service.list_registries')
    def test_find_study_info_for_registry_valid(self, list_registries_mock):
        mock_registry_one = mock.Mock()
        mock_registry_one.display_name = 'Mock Registry'
        mock_registry_one.policy = mock.Mock()
        mock_registry_one.policy.data_export_configs = [
            mock.Mock(big_query_export_config=mock.Mock(
                google_cloud_project_id='mock_project_id',
                dataset_name='sensor_store_mock_dataset'
            ))
        ]
        list_registries_mock.return_value = [mock_registry_one]

        study_info = find_study_info_for_registry('Mock_Registry', 'mock_env')
        self.assertIsInstance(study_info, StudyInfo)
        self.assertEqual(
            study_info.gcp_project, 'mock_project_id')
        self.assertEqual(
            study_info.internal_echo_dataset, 'internal_mock_dataset')
        self.assertEqual(
            study_info.external_echo_dataset, 'sensor_store_mock_dataset')

    @mock.patch('verily.ds_sdk.core.grpc.registry_service.list_registries')
    def test_find_study_info_for_registry_not_found(
        self, list_registries_mock):
        list_registries_mock.return_value = []
        with self.assertRaises(ValueError):
            find_study_info_for_registry('no_registries', 'mock_env')

    @mock.patch('verily.ds_sdk.core.grpc.registry_service.list_registries')
    def test_find_study_info_for_registry_no_data_export_configs(
        self, list_registries_mock):
        mock_registry = mock.Mock()
        mock_registry.display_name = 'Mock Registry'
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = []
        list_registries_mock.return_value = [mock_registry]

        with self.assertRaises(ValueError):
            find_study_info_for_registry('no_data_export_registry', 'mock_env')

    def test_format_registry_into_study_info_valid(self):
        mock_registry = mock.Mock()
        mock_registry.name = 'registries/mock_registry'
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = [
            mock.Mock(big_query_export_config=mock.Mock(
                google_cloud_project_id='mock_project_id',
                dataset_name='sensor_store_mock_dataset'
            ))
        ]

        study_info = format_registry_into_study_info(
            mock_registry, 'Mock_Registry')
        self.assertIsInstance(study_info, StudyInfo)
        self.assertEqual(
            study_info.gcp_project, 'mock_project_id')
        self.assertEqual(
            study_info.internal_echo_dataset, 'internal_mock_dataset')
        self.assertEqual(
            study_info.external_echo_dataset, 'sensor_store_mock_dataset')

    def test_format_registry_into_study_info_missing_attributes(self):
        mock_registry = mock.Mock()
        mock_registry.name = 'registries/mock_registry'
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = [
            mock.Mock(big_query_export_config=None)
        ]

        with self.assertRaises(AttributeError):
            format_registry_into_study_info(
                mock_registry, 'registry_missing_attributes')

    @mock.patch('google.auth.default')
    @mock.patch('google.auth.transport.grpc.secure_authorized_channel')
    @mock.patch('verily.ds_sdk.protos.management_service_pb2_grpc.ManagementServiceStub') # pylint: disable=line-too-long
    def test_list_registries(self, mock_stub, mock_channel, mock_auth):
        mock_creds = mock.Mock()
        mock_auth.return_value = (mock_creds, None)
        mock_channel.return_value = mock.Mock()

        mock_stub_instance = mock.Mock()
        mock_stub.return_value = mock_stub_instance
        mock_response = mock.Mock()
        mock_response.registries = ['registry1', 'registry2']
        mock_stub_instance.ListRegistries.return_value = mock_response

        registries = list_registries('prod')
        self.assertEqual(registries, ['registry1', 'registry2'])
        mock_auth.assert_called_once_with(
            scopes=['https://www.googleapis.com/auth/lifescience.sensorsuite'])
        mock_channel.assert_called_once_with(
            mock_creds,
            ANY,
            'sensorsuite-pa.googleapis.com:443')
        mock_stub.assert_called_once_with(mock_channel.return_value)
        mock_stub_instance.ListRegistries.assert_called_once_with(
            management_service_pb2.ListRegistriesRequest())

    def test_verify_registry_fields_for_study_info_valid(self):
        mock_registry = mock.Mock()
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = [
            mock.Mock(big_query_export_config=mock.Mock(
                google_cloud_project_id='mock_project_id',
                dataset_name='mock_dataset'
            ))
        ]

        result = verify_registry_fields_for_study_info(mock_registry)
        self.assertTrue(result)

    def test_verify_registry_fields_for_study_info_missing_policy(self):
        mock_registry = mock.Mock()
        mock_registry.policy = None

        result = verify_registry_fields_for_study_info(mock_registry)
        self.assertFalse(result)

    def test_verify_registry_fields_for_study_info_missing_export_configs(self):
        mock_registry = mock.Mock()
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = None

        result = verify_registry_fields_for_study_info(mock_registry)
        self.assertFalse(result)

    def test_verify_registry_fields_for_study_info_missing_big_query_export_config(self): # pylint: disable=line-too-long
        mock_registry = mock.Mock()
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = [
            mock.Mock(big_query_export_config=None)
        ]

        result = verify_registry_fields_for_study_info(mock_registry)
        self.assertFalse(result)

    def test_verify_registry_fields_for_study_info_missing_gcp_project(self):
        mock_registry = mock.Mock()
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = [
            mock.Mock(big_query_export_config=mock.Mock(
                google_cloud_project_id=None,
                dataset_name='mock_dataset'
            ))
        ]

        result = verify_registry_fields_for_study_info(mock_registry)
        self.assertFalse(result)

    def test_verify_registry_fields_for_study_info_missing_dataset(self):
        mock_registry = mock.Mock()
        mock_registry.policy = mock.Mock()
        mock_registry.policy.data_export_configs = [
            mock.Mock(big_query_export_config=mock.Mock(
                google_cloud_project_id='mock_project_id',
                dataset_name=None
            ))
        ]

        result = verify_registry_fields_for_study_info(mock_registry)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
