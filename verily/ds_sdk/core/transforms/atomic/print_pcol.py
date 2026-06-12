"""Util PTransform for printing PCollections."""

import logging
from typing import Any

import apache_beam as beam


def _print_all(message):
    for p_fn in [print, logging.info]:
        p_fn(message)


class _PrintPcolDoFn(beam.DoFn):
    """Prints PCollections with Timestamp, Window & Pane info attached."""

    def process(  # type: ignore[override]
            self,
            elem: Any,
            timestamp=beam.DoFn.TimestampParam,
            window=beam.DoFn.WindowParam,
            pane_info=beam.DoFn.PaneInfoParam) -> Any:
        timestamp_str = ''
        # TODO(tanke): figure out why the Beam demo included this try/except
        try:
            timestamp_str = timestamp.to_utc_datetime()
        except:  # pylint: disable=bare-except
            timestamp_str = timestamp

        window_str = ''
        if window == beam.transforms.window.GlobalWindow():
            window_str = 'The Global Window'
        else:
            start = window.start.to_utc_datetime()
            end = window.end.to_utc_datetime()
            window_str = f'start: {start} end: {end}'

        block = '-' * 50
        m1 = f'Timestamp: {timestamp_str}'
        m2 = f'Window: {window_str}'
        m3 = f'Pane Info: {pane_info}'
        m4 = elem
        message = f'{block}\n{m1}\n{m2}\n{m3}\n{m4}\n{block}'
        _print_all(message)

        yield elem


class PrintPcol(beam.PTransform):
    """Prints PCollections with Timestamp, Window & Pane info attached."""

    def expand(self, pcol):
        return pcol | beam.ParDo(_PrintPcolDoFn())
