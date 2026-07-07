"""Root conftest: installs the ds_sdk import blocker for all tests.

A meta-path finder that raises ImportError on any `verily.ds_sdk` import.
This proves that the package works standalone — the blocker prevents imports
from the bundled ds_sdk directory (or any installed copy).
"""

import importlib.abc
import importlib.machinery
import sys


class _DsSdkImportBlocker(importlib.abc.MetaPathFinder):
    """Meta-path finder that blocks all verily.ds_sdk imports."""

    BLOCKED_PREFIX = 'verily.ds_sdk'
    MESSAGE = (
        'Import of {mod!r} is blocked by the ds_sdk import blocker. '
        'verily-raw-data-tools must not depend on verily.ds_sdk.')

    def find_spec(self, fullname, path, target=None):
        if fullname == self.BLOCKED_PREFIX or fullname.startswith(
                self.BLOCKED_PREFIX + '.'):
            raise ImportError(self.MESSAGE.format(mod=fullname))
        return None


_blocker = _DsSdkImportBlocker()
if _blocker not in sys.meta_path:
    sys.meta_path.insert(0, _blocker)
