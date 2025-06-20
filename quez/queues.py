"""
Core implementation of the synchronous and asynchronous compressed queues.
"""

import asyncio
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
QueueType = TypeVar("QueueType", bound=queue.Queue | asyncio.Queue)


# --- Internal data structure for queue items ---
@dataclass(frozen=True)
class _QueueElement:
    """A wrapper for data stored in the queue, tracking its byte size."""

    compressed_data: bytes
    raw_size: int


# --- 1. The Shared Base Class ---
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
        """Shared helper to get the running event loop for the async version."""
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
        """Return the approximate size of the queue."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self._queue.empty()

    def full(self) -> bool:
        """Return True if the queue is full, False otherwise."""
        return self._queue.full()


# --- 2. The Asynchronous Implementation ---
class AsyncCompressedQueue(
    _BaseCompressedQueue[QItem, "asyncio.Queue[_QueueElement]"]
):
    """
    An asyncio-compatible queue that transparently compresses any picklable
    object using pluggable compression and serialization strategies.
    """

    def __init__(
        self,
        maxsize: int = 0,
        *,
        compressor: Compressor | None = None,
        serializer: Serializer | None = None,
    ):
        super().__init__(asyncio.Queue(maxsize), compressor, serializer)
        # Eagerly get the loop during initialization. This ensures that the
        # queue is associated with the loop it was created in.
        self._loop = self._get_running_loop()

    async def put(self, item: QItem) -> None:
        """Serialize, compress, and put an item onto the queue."""
        loop = self._loop
        assert loop is not None, "Event loop not available"

        # CPU-bound tasks are run in an executor to avoid blocking the event loop.
        raw_bytes = await loop.run_in_executor(
            None, self.serializer.dumps, item
        )
        compressed_bytes = await loop.run_in_executor(
            None, self.compressor.compress, raw_bytes
        )

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock is required for thread-safe stat updates in case someone uses
        # this async Queue in a multi-threaded context.
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        await self._queue.put(element)

    async def get(self) -> QItem:
        """Get an item, decompress, deserialize, and return it."""
        loop = self._loop
        assert loop is not None, "Event loop not available"

        element = await self._queue.get()

        # Lock is required for thread-safe stat updates in case someone uses
        # this async Queue in a multi-threaded context.
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        # Decompression and deserialization are also run in an executor.
        raw_bytes = await loop.run_in_executor(
            None, self.compressor.decompress, element.compressed_data
        )
        item = await loop.run_in_executor(
            None, self.serializer.loads, raw_bytes
        )

        return item

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete."""
        self._queue.task_done()

    async def join(self) -> None:
        """Block until all items in the queue have been gotten and processed."""
        await self._queue.join()


# --- 3. The Synchronous Implementation ---
class CompressedQueue(
    _BaseCompressedQueue[QItem, "queue.Queue[_QueueElement]"]
):
    """
    A thread-safe, synchronous queue that transparently compresses any
    picklable object using pluggable compression and serialization strategies.
    """

    def __init__(
        self,
        maxsize: int = 0,
        *,
        compressor: Compressor | None = None,
        serializer: Serializer | None = None,
    ):
        super().__init__(queue.Queue(maxsize), compressor, serializer)

    def put(
        self, item: QItem, block: bool = True, timeout: float | None = None
    ) -> None:
        """Serialize, compress, and put an item onto the queue."""
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock is required for thread-safe stat updates
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        self._queue.put(element, block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: float | None = None) -> QItem:
        """Get an item, decompress, deserialize, and return it."""
        element = self._queue.get(block=block, timeout=timeout)

        # Lock is required for thread-safe stat updates
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        raw_bytes = self.compressor.decompress(element.compressed_data)
        item = self.serializer.loads(raw_bytes)

        return item

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete."""
        self._queue.task_done()

    def join(self) -> None:
        """Block until all items in the queue have been gotten and processed."""
        self._queue.join()
