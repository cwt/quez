"""
quez: Pluggable, compressed in-memory queues for sync and asyncio applications.
"""
from importlib.metadata import version, PackageNotFoundError
from typing import List

try:
    __version__: str = version("quez")
except PackageNotFoundError:
    # Handle case where package is not installed (e.g., in development)
    __version__ = "0.0.0-dev"

from .compressors import (
    Compressor,
    ZlibCompressor,
    Bz2Compressor,
    LzmaCompressor,
    NullCompressor,
)
from .queues import CompressedQueue, AsyncCompressedQueue

__all__ = [
    # Core Queue Classes
    "CompressedQueue",
    "AsyncCompressedQueue",
    # Compressor Strategies
    "Compressor",
    "ZlibCompressor",
    "Bz2Compressor",
    "LzmaCompressor",
    "NullCompressor",
]
