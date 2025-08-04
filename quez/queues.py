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
        """
        Initializes an instance of AsyncCompressedQueue.

        This constructor initializes the AsyncCompressedQueue with an asyncio.Queue,
        which can have a maximum size specified by `maxsize`. It also allows
        overriding the default compressor and serializer, otherwise using the ZlibCompressor
        and PickleSerializer.

        Args:
            maxsize (int, optional): The maximum size of the queue. Defaults to 0 (unbounded).
            compressor (Compressor, optional): The compressor to use. Defaults to None (ZlibCompressor).
            serializer (Serializer, optional): The serializer to use. Defaults to None (PickleSerializer).
        """
        super().__init__(asyncio.Queue(maxsize), compressor, serializer)
        # Eagerly get the loop during initialization. This ensures that the
        # queue is associated with the loop it was created in.
        self._loop = self._get_running_loop()

    async def put(self, item: QItem) -> None:
        """
        Serialize, compress, and put an item onto the queue.

        This coroutine method takes an item, serializes it into bytes, compresses the
        serialized bytes using the configured compressor, and then puts the resulting
        _queue_element (containing the compressed data and the raw size) into the underlying
        queue. The put operation is performed asynchronously using the event loop to run
        the serialization and compression tasks in an executor, ensuring that these tasks
        do not block the event loop.

        Args:
            item (QItem): The item to serialize, compress, and put into the queue.
        """
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
        """
        Get an item, decompress, deserialize, and return it.

        This coroutine method retrieves an item from the queue, decompresses the
        compressed data, deserializes the raw bytes into the original item, and
        returns it. The get operation is performed asynchronously using the event loop
        to handle the decompression and deserialization tasks in an executor, ensuring
        that these tasks do not block the event loop.

        The method also updates statistics for the queue, including the total raw
        size and total compressed size, using a thread-safe lock to protect against
        concurrent updates from multiple threads.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the queue.
        """
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
        """
        Indicate that a formerly enqueued task is complete.

        This method calls the `task_done` method of the underlying queue to indicate
        that a task has been completed and that the corresponding resources can be
        freed. This is particularly useful in conjunction with the `join` method to
        wait for all items in the queue to be processed.
        """
        self._queue.task_done()

    async def join(self) -> None:
        """
        Block until all items in the queue have been gotten and processed.

        This method waits until all items in the queue have been retrieved and
        processed. It calls the `join` method of the underlying asyncio.Queue to
        block until the queue is empty. This is useful for ensuring that all tasks
        have been completed before the program proceeds.
        """
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
        """
        Initializes the CompressedQueue instance with the specified parameters.

        This constructor initializes a new instance of CompressedQueue with the provided
        parameters, creating an underlying queue instance using `queue.Queue(maxsize)`.

        The compressor and serializer are initialized with the provided values or their
        default implementations (ZlibCompressor and PickleSerializer, respectively).

        Args:
            maxsize (int, optional): The maximum size of the queue. Defaults to 0 (unbounded).
            compressor (Compressor, optional): The compressor to use. Defaults to None (ZlibCompressor).
            serializer (Serializer, optional): The serializer to use. Defaults to None (PickleSerializer).
        """
        super().__init__(queue.Queue(maxsize), compressor, serializer)

    def put(
        self, item: QItem, block: bool = True, timeout: float | None = None
    ) -> None:
        """
        Serialize, compress, and put an item onto the queue.

        This method takes an item, serializes it into bytes, compresses the serialized bytes
        using the configured compressor, and then puts the resulting _QueueElement (containing
        the compressed data and the raw size) into the underlying queue. The put operation is
        performed synchronously.

        Args:
            item (QItem): The item to serialize, compress, and put into the queue.
            block (bool, optional): If True, the method will block until there is space in the
                                    queue or until the timeout occurs. Defaults to True.
            timeout (float, optional): If block is True, the maximum time (in seconds) to wait
                                       before timing out. Defaults to None (no timeout).
        """
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock is required for thread-safe stat updates.
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        self._queue.put(element, block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: float | None = None) -> QItem:
        """
        Get an item, decompress, deserialize, and return it.

        This method retrieves an item from the queue, decompresses the compressed data,
        deserializes the raw bytes into the original item, and returns it. The get operation
        is performed synchronously.

        Args:
            block (bool, optional): If True, the method will block until an item is available
                                    or until the timeout occurs. Defaults to True.
            timeout (float, optional): If block is True, the maximum time (in seconds) to wait
                                       before timing out. Defaults to None (no timeout).

        Returns:
            QItem: The deserialized and decompressed item retrieved from the queue.
        """
        element = self._queue.get(block=block, timeout=timeout)

        # Lock is required for thread-safe stat updates.
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        raw_bytes = self.compressor.decompress(element.compressed_data)
        item = self.serializer.loads(raw_bytes)

        return item

    def task_done(self) -> None:
        """
        Indicate that a formerly enqueued task is complete.

        This method calls the `task_done` method of the underlying queue to indicate
        that a task has been completed and that the corresponding resources can be
        freed. This is particularly useful in conjunction with the `join` method to
        wait for all items in the queue to be processed.
        """
        self._queue.task_done()

    def join(self) -> None:
        """
        Block until all items in the queue have been gotten and processed.

        This method blocks the calling thread until all items have been retrieved from the queue
        and processed. It calls the `join` method of the underlying queue, which waits until the
        queue is empty. This is useful for ensuring that all tasks have been completed before the
        program proceeds.
        """
        self._queue.join()


# --- 4. The Synchronous Deque Implementation ---
class CompressedDeque(
    _BaseCompressedQueue[QItem, "collections.deque[_QueueElement]"]
):
    """
    A thread-safe, synchronous double-ended queue (deque) that transparently
    compresses any picklable object using pluggable compression and serialization
    strategies. It supports operations from both ends and is suitable for
    scenarios requiring FIFO, LIFO, or mixed access patterns.
    """

    def __init__(
        self,
        maxsize: int | None = None,
        *,
        compressor: Compressor | None = None,
        serializer: Serializer | None = None,
    ):
        """
        Initializes a new instance of CompressedDeque with the specified parameters.

        This constructor initializes a new instance of CompressedDeque with the provided
        parameters, creating an underlying deque instance with the specified maxsize.
        It also allows overriding the default compressor and serializer, otherwise using
        the ZlibCompressor and PickleSerializer.

        Additionally, a lock is created to ensure thread-safe operations, as deque operations
        are atomic but the class may require additional synchronization for thread safety.

        Args:
            maxsize (int | None, optional): The maximum size of the deque. Defaults to None (unbounded).
            compressor (Compressor, optional): The compressor to use. Defaults to None (ZlibCompressor).
            serializer (Serializer, optional): The serializer to use. Defaults to None (PickleSerializer).
        """
        # Use collections.deque with maxlen for bounded size.
        super().__init__(
            collections.deque(maxlen=maxsize), compressor, serializer
        )
        # Additional lock for deque operations, as deque is atomic but we need
        # to ensure consistency with stats and multi-step ops.
        self._lock = threading.Lock()

    def qsize(self) -> int:
        """
        Return the current size of the deque.

        This method returns the current number of items in the deque. It ensures thread-safe
        access by acquiring a lock before returning the size of the underlying deque.

        Returns:
            int: The number of items in the deque.
        """
        with self._lock:
            return len(self._queue)

    def empty(self) -> bool:
        """
        Return True if the deque is empty, False otherwise.

        This method returns a boolean indicating whether the deque currently contains any
        items. It ensures thread-safe access by acquiring a lock before checking the
        length of the underlying deque.

        Returns:
            bool: True if the deque is empty, False otherwise.
        """
        with self._lock:
            return len(self._queue) == 0

    def full(self) -> bool:
        """Return True if the deque is full (reached maxlen), False otherwise."""
        with self._lock:
            return (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            )

    def append(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the right end of the deque.

        Args:
            item (QItem): The item to serialize, compress, and append to the deque.
        """
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock for stats update
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        # Lock for deque operation
        with self._lock:
            self._queue.append(element)

    def appendleft(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the left end of the deque.

        Args:
            item (QItem): The item to serialize, compress, and append to left end of the deque.
        """
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock for stats update.
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        # Lock for deque operation.
        with self._lock:
            self._queue.appendleft(element)

    def pop(self) -> QItem:
        """
        Pop an item from the right end of the deque, decompress, deserialize, and return it.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the right end of the deque.
        """
        # Lock for deque operation.
        with self._lock:
            element = self._queue.pop()

        # Lock for stats update.
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        raw_bytes = self.compressor.decompress(element.compressed_data)
        item = self.serializer.loads(raw_bytes)

        return item

    def popleft(self) -> QItem:
        """
        Pop an item from the left end of the deque, decompress, deserialize, and return it.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the left end of the deque.
        """
        # Lock for deque operation.
        with self._lock:
            element = self._queue.popleft()

        # Lock for stats update.
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        raw_bytes = self.compressor.decompress(element.compressed_data)
        item = self.serializer.loads(raw_bytes)

        return item
