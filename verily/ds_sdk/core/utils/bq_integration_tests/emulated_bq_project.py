"""Example usage of the Emulated BQ Project in an integration test.

Note: test files must not be suffixed with '_test', can't have
Pytest picking these up due to bqemulatormanager not being
supported in Python versions <3.8, >=3.12
"""

import socket
import sys

import bqemulatormanager as bqem
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from verily.ds_sdk.core.utils.emulated_bq_project import EmulatedBQProject


def find_free_port() -> int:
    '''Finds an unused port.

    Reference:
    https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python
    '''

    with socket.socket() as s:

        # bind to a free port provided by the host
        s.bind(('', 0))

        # get the free port number
        port = s.getsockname()[1]
        return port


@pytest.mark.skipif(sys.version_info < (3, 8),
                    reason='Requires Python 3.8 or higher.')
@pytest.mark.skipif(sys.version_info >= (3, 12),
                    reason='Requires Python 3.11 or lower.')
def run_test_integration_test():
    '''Sample integration test that brings up the emulator, creates a step count
    table, writes some data to it, queries the table, and verifies the results.

    Note: bqemulatormanager requires Python >=3.8, <3.12;
            https://pypi.org/project/bqemulatormanager/
    '''

    project_id = 'sensors-studies-devteam'

    # find a free port to host emulator and emulated project on
    port = find_free_port()

    # spin up the emulator server using the bqem manager
    manager = bqem.Manager(project=project_id, port=port)

    expected_data = pd.DataFrame([{
        'user_id': 'user1',
        'device_id': 'device1',
        'annotation_label': '',
        'start_timestamp_utc': pd.to_datetime('2022-01-03T12:00:00.000Z'),
        'end_timestamp_utc': pd.to_datetime('2022-01-04T13:00:00.000Z')
    }, {
        'user_id': 'user2',
        'device_id': 'device2',
        'annotation_label': '',
        'start_timestamp_utc': pd.to_datetime('2022-01-01T01:00:00.000Z'),
        'end_timestamp_utc': pd.to_datetime('2022-01-05T00:00:00.000Z')
    }, {
        'user_id': 'user1',
        'device_id': 'device1',
        'annotation_label': '',
        'start_timestamp_utc': pd.to_datetime('2022-01-01T06:00:00.000Z'),
        'end_timestamp_utc': pd.to_datetime('2022-01-01T18:00:00.000Z')
    }])

    with manager:

        # create the emulated project that communicates with the emulator
        emulated_bq = EmulatedBQProject(
            project_id=project_id,
            emulator_port=port,
            input_json_schema_directory=\
                'verily/ds_sdk/core/utils/table_schemas_json_test')

        # initialize the emulated project
        emulated_bq.initialize_project()

        emulated_client = emulated_bq.emulated_client

        # write to table
        table_id = 'datascience_sdk_temp.incremental-com_verily_step__count'
        step_count_table = emulated_client.get_table(table=table_id)
        emulated_client.insert_rows_from_dataframe(table=step_count_table,
                                                   dataframe=expected_data)

        # read from table and convert to pd df
        sql = (
            'SELECT * FROM `sensors-studies-devteam.datascience_sdk_temp.'
            'incremental-com_verily_step__count`')
        got_data = emulated_client.query(sql).result().to_dataframe(
            create_bqstorage_client=False)

    assert_frame_equal(expected_data, got_data)


def main():
    run_test_integration_test()


if __name__ == '__main__':
    main()
