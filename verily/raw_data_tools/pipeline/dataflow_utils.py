"""Utils for working with Dataflow."""

import re

# Replaces the `regex` library's Unicode-aware pattern \p{Ll}\p{Lo}\p{N}
# with ASCII [a-z0-9]. Dataflow job labels are ASCII-only per the API spec,
# so this is equivalent for all valid inputs.
_LABEL_REPLACE_PATTERN = re.compile(r'[^a-z0-9_-]')


def escape_dataflow_job_labels(label: str) -> str:
    """Lower-cases and replaces any invalid characters with a `_`."""
    escaped = _LABEL_REPLACE_PATTERN.sub('_', label.lower())
    return escaped[0:63]


def get_dataflow_url(job_id: str, region: str, project: str) -> str:
    """Build a Cloud Console URL for a Dataflow job."""
    return (
        f'https://console.cloud.google.com/dataflow/jobs'
        f'/{region}/{job_id}?project={project}'
    )


def get_dataflow_metrics_url(job_id: str, region: str, project: str) -> str:
    """Build a Cloud Console URL for a Dataflow job's metrics page."""
    return (
        f'https://console.cloud.google.com/dataflow/jobs'
        f'/{region}/{job_id}/metrics?project={project}'
    )
