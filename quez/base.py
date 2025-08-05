"""
Core implementation of the synchronous and asynchronous compressed queues.
"""

import asyncio
import collections
import queue
import threading
from dataclasses import dataclass
from typing import Any, Dict, Generic, TypeVar

from .compressors import (
    Compressor,
    PickleSerializer,
    Serializer,
    ZlibCompressor,
)

# --- Generic Type Variables ---
QItem = TypeVar("QItem")
QueueType = TypeVar(
    "QueueType", bound=queue.Queue | asyncio.Queue | collections.deque
)


# --- Internal data structure for queue items ---
@dataclass(frozen=True)
class _QueueElement:
    """
    A wrapper for data stored in the queue, tracking its byte size.

    Attributes:
        compressed_data (bytes): The compressed version of the data.
        raw_size (int): The original byte size of the data before compression.
    """

    compressed_data: bytes
    raw_size: int


# --- The Shared Base Class ---
class _BaseCompressedQueue(Generic[QItem, QueueType]):
    """
    A generic base class holding the common logic for compressed queues.
    This class is not meant to be instantiated directly.
    """

    def __init__(
        self,
        queue_instance: QueueType,
        compressor: Compressor | None = None,
        serializer: Serializer | None = None,
    ):
        """
        Initializes the base queue.

        Args:
            queue_instance: An instance of a sync or async queue.
            compressor: A pluggable compressor. Defaults to ZlibCompressor.
            serializer: A pluggable serializer. Defaults to PickleSerializer.
        """
        self._queue: QueueType = queue_instance
        self.compressor = (
            compressor if compressor is not None else ZlibCompressor()
        )
        self.serializer = (
            serializer if serializer is not None else PickleSerializer()
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stats_lock = threading.Lock()
        self._total_raw_size: int = 0
        self._total_compressed_size: int = 0

    def _get_running_loop(self) -> asyncio.AbstractEventLoop:
        """
        Retrieves the current running asyncio event loop.

        Returns:
            asyncio.AbstractEventLoop: The currently running event loop.

        Raises:
            RuntimeError: If the method is called outside of a running asyncio event loop.
        """
        try:
            return asyncio.get_running_loop()
        except RuntimeError as e:
            raise RuntimeError(
                "AsyncCompressedQueue must be initialized within a running asyncio event loop."
            ) from e

    @property
    def stats(self) -> Dict[str, Any]:
        """
        Returns a dictionary with statistics about the items in the queue.
        The compression ratio is None if the queue is empty.

        Returns:
            Dict[str, Any]: A dictionary containing the following keys:
                - 'count': The number of items in the queue.
                - 'raw_size_bytes': The total raw size of the items in bytes.
                - 'compressed_size_bytes': The total compressed size of the items in bytes.
                - 'compression_ratio_pct': The compression ratio as a percentage,
                                           or None if the queue is empty.
        """
        # Lock ensures thread-safe stats reads, protecting against concurrent
        # updates in multi-threaded contexts.
        with self._stats_lock:
            count = self.qsize()
            raw_size = self._total_raw_size
            compressed_size = self._total_compressed_size

            ratio = (
                (1 - (compressed_size / raw_size)) * 100
                if raw_size > 0
                else None
            )

            return {
                "count": count,
                "raw_size_bytes": raw_size,
                "compressed_size_bytes": compressed_size,
                "compression_ratio_pct": ratio,
            }

    def qsize(self) -> int:
        """
        Return the approximate size of the queue.

        This method returns the current size of the queue. If the queue is of type
        either `queue.Queue` or `asyncio.Queue`, it returns the size using the respective
        method from the queue. If the queue type is not supported, it raises a
        NotImplementedError with a descriptive message.

        Returns:
            int: The size of the queue.

        Raises:
            NotImplementedError: If the queue type is not supported.
        """
        if isinstance(self._queue, (queue.Queue, asyncio.Queue)):
            return self._queue.qsize()
        else:
            raise NotImplementedError(
                "qsize() is not implemented for this queue type."
            )

    def empty(self) -> bool:
        """
        Return True if the queue is empty, False otherwise.

        This method checks if the underlying queue is empty. If the queue is of type
        either `queue.Queue` or `asyncio.Queue`, it returns the result of the respective
        `empty()` method from the queue. If the queue type is not supported, it raises a
        NotImplementedError with a descriptive message.

        Returns:
            bool: True if the queue is empty, False otherwise.

        Raises:
            NotImplementedError: If the queue type is not supported.
        """
        if isinstance(self._queue, (queue.Queue, asyncio.Queue)):
            return self._queue.empty()
        else:
            raise NotImplementedError(
                "empty() is not implemented for this queue type."
            )

    def full(self) -> bool:
        """
        Return True if the queue is full, False otherwise.

        This method checks if the underlying queue is full. If the queue is of type
        either `queue.Queue` or `asyncio.Queue`, it returns the result of the respective
        `full()` method from the queue. If the queue type is not supported, it raises a
        NotImplementedError with a descriptive message.

        Returns:
            bool: True if the queue is full, False otherwise.

        Raises:
            NotImplementedError: If the queue type is not supported.
        """
        if isinstance(self._queue, (queue.Queue, asyncio.Queue)):
            return self._queue.full()
        else:
            raise NotImplementedError(
                "full() is not implemented for this queue type."
            )
