"""Top level module so DS SDK package falls under the verily namespace."""

# NOTE: Do not update this so the DS SDK can work with other verily namespaces
# packages.
import pkgutil

__path__ = pkgutil.extend_path(__path__, __name__)
