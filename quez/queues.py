"""
Core implementation of the synchronous and asynchronous compressed queues.
"""

import asyncio
import queue

from .compressors import (
    Compressor,
    Serializer,
)
from .base import QItem, _QueueElement, _BaseCompressedQueue


# --- 1. The Synchronous Implementation ---
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

    def _serialize_and_compress(self, item: QItem) -> _QueueElement:
        """Synchronously serialize and compress an item."""
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)
        return _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

    def _decompress_and_deserialize(self, element: _QueueElement) -> QItem:
        """Synchronously decompress and deserialize an item."""
        raw_bytes = self.compressor.decompress(element.compressed_data)
        return self.serializer.loads(raw_bytes)

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

        # Offload the entire CPU-bound operation to an executor.
        element = await loop.run_in_executor(
            None, self._serialize_and_compress, item
        )

        # Lock is required for thread-safe stat updates in case someone uses
        # this async Queue in a multi-threaded context.
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        await self._queue.put(element)

    def put_nowait(self, item: QItem) -> None:
        """
        Serialize, compress, and put an item onto the queue without blocking.

        This method performs the serialization and compression synchronously
        and then attempts to put the item into the queue. If the queue is full,
        it raises an `asyncio.QueueFull` exception.

        Args:
            item (QItem): The item to serialize, compress, and put into the queue.

        Raises:
            asyncio.QueueFull: If the queue is full.
        """
        # Note: Unlike the async `put`, this is a synchronous operation.
        # The expectation for a `nowait` method is immediate action without
        # yielding to the event loop.
        element = self._serialize_and_compress(item)

        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        self._queue.put_nowait(element)

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

        # Offload the entire CPU-bound operation to an executor.
        item = await loop.run_in_executor(
            None, self._decompress_and_deserialize, element
        )

        return item

    def get_nowait(self) -> QItem:
        """
        Get an item, decompress, and deserialize it without blocking.

        This method attempts to retrieve an item from the queue immediately.
        If the queue is empty, it raises an `asyncio.QueueEmpty` exception.
        The decompression and deserialization are performed synchronously.

        Returns:
            QItem: The deserialized and decompressed item.

        Raises:
            asyncio.QueueEmpty: If the queue is empty.
        """
        element = self._queue.get_nowait()

        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        # Note: Synchronous operation consistent with `nowait` behavior.
        item = self._decompress_and_deserialize(element)

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
