"""Utils for working with beam runners"""

from typing import Union

import apache_beam as beam
from apache_beam.runners.interactive import interactive_runner


def is_dataflow_runner(runner: Union[str, beam.runners.PipelineRunner]) -> bool:
    return (
        "dataflow" in runner.lower()
        if isinstance(runner, str)
        else isinstance(runner, beam.runners.DataflowRunner)
    )


def is_interactive_runner(
    runner: Union[str, beam.runners.PipelineRunner],
) -> bool:
    return (
        "interactive" in runner.lower()
        if isinstance(runner, str)
        else isinstance(runner, interactive_runner.InteractiveRunner)
    )
