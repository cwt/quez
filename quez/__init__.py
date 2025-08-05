"""
quez: Pluggable, compressed in-memory queues for sync and asyncio applications.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("quez")
except PackageNotFoundError:
    # Handle case where package is not installed (e.g., in development)
    __version__ = "0.0.0-dev"

from .compressors import (
    Bz2Compressor,
    Compressor,
    LzmaCompressor,
    NullCompressor,
    ZlibCompressor,
)
from .queues import (
    AsyncCompressedQueue,
    CompressedQueue,
)
from .deques import (
    CompressedDeque,
    AsyncCompressedDeque,
)

__all__ = [
    # Core Queue Classes
    "CompressedQueue",
    "AsyncCompressedQueue",
    "CompressedDeque",
    "AsyncCompressedDeque",
    # Compressor Strategies
    "Compressor",
    "ZlibCompressor",
    "Bz2Compressor",
    "LzmaCompressor",
    "NullCompressor",
]

# Conditionally expose optional compressors if they are available
try:
    from .compressors import ZstdCompressor

    __all__.append("ZstdCompressor")
except ImportError:
    pass

try:
    from .compressors import LzoCompressor

    __all__.append("LzoCompressor")
except ImportError:
    pass
