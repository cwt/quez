# -*- coding: utf-8 -*-
from importlib.metadata import version, PackageNotFoundError

try:
    __version__: str = version("quez")
except PackageNotFoundError:
    # Handle case where package is not installed (e.g., in development)
    __version__ = "0.0.0-dev"
