"""Utils for working with dataflow."""

import regex as re

_LABEL_REPLACE_PATERN = re.compile(r'[^\p{Ll}\p{Lo}\p{N}_-]')


def escape_dataflow_job_labels(label: str) -> str:
    """Lowers cases and replaces any invalid characters with a `_`."""
    escaped = _LABEL_REPLACE_PATERN.sub('_', label.lower())
    return escaped[0:63]
