"""
Tests for the asynchronous AsyncCompressedQueue.
"""

import asyncio
import pytest
import pytest_asyncio
import threading

from quez import AsyncCompressedQueue
from quez.compressors import (
    Bz2Compressor,
    Compressor,
    LzmaCompressor,
    NullCompressor,
    ZlibCompressor,
)

# --- Test Fixtures and Parameters ---
COMPRESSOR_FIXTURES = [
    ZlibCompressor(),
    Bz2Compressor(),
    LzmaCompressor(),
    NullCompressor(),
]

TEST_ITEMS = [
    "a_string",
    b"some_bytes",
    {"a_dict": 123},
    ["a", "list", 1],
]


@pytest_asyncio.fixture(params=COMPRESSOR_FIXTURES)
def compressor(request):
    """Provides each compressor implementation for parametrization."""
    return request.param


# --- Core Functionality Tests ---
@pytest.mark.asyncio
async def test_put_get_roundtrip(compressor: Compressor):
    """Test basic put and get operations for a single item."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(compressor=compressor)
    item = {"data": "test_data"}
    await q.put(item)
    retrieved_item = await q.get()
    assert retrieved_item == item


@pytest.mark.asyncio
async def test_fifo_ordering(compressor: Compressor):
    """Test that items are retrieved in First-In, First-Out order."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(compressor=compressor)
    items = [f"item-{i}" for i in range(5)]
    for item in items:
        await q.put(item)

    retrieved_items = [await q.get() for _ in items]
    assert retrieved_items == items


@pytest.mark.asyncio
@pytest.mark.parametrize("item", TEST_ITEMS)
async def test_various_data_types(compressor: Compressor, item):
    """Test that the queue correctly handles various data types."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(compressor=compressor)
    await q.put(item)
    assert await q.get() == item


# --- Queue Property Tests ---
@pytest.mark.asyncio
async def test_qsize_empty_full(compressor: Compressor):
    """Test qsize(), empty(), and full() methods."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(
        maxsize=2, compressor=compressor
    )
    assert q.qsize() == 0
    assert q.empty()
    assert not q.full()

    await q.put("item1")
    assert q.qsize() == 1
    assert not q.empty()
    assert not q.full()

    await q.put("item2")
    assert q.qsize() == 2
    assert not q.empty()
    assert q.full()

    await q.get()
    assert q.qsize() == 1
    assert not q.full()


@pytest.mark.asyncio
async def test_maxsize_blocking(compressor: Compressor):
    """Test that put() awaits when the queue is full."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(
        maxsize=1, compressor=compressor
    )
    await q.put("full")

    put_task = asyncio.create_task(q.put("one more"))
    await asyncio.sleep(0.01)  # Give the task time to run and block
    assert not put_task.done()  # It should be waiting

    await q.get()  # Make space
    await asyncio.sleep(0.01)  # Give the task time to complete
    assert put_task.done()
    assert q.full()


@pytest.mark.asyncio
async def test_get_nowait(compressor: Compressor):
    """Test that get_nowait() raises an exception when the queue is empty."""
    # Note: AsyncCompressedQueue doesn't have `get_nowait`, this tests `get` on an empty queue
    # The underlying asyncio.Queue's get() would just hang.
    # A test for this scenario is more complex involving timeouts.
    q: AsyncCompressedQueue = AsyncCompressedQueue(compressor=compressor)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.get(), timeout=0.01)


# --- Stats Tests ---
@pytest.mark.asyncio
async def test_stats_calculation(compressor: Compressor):
    """Test that statistics are calculated correctly."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(compressor=compressor)
    item = "test_item_for_stats" * 10

    stats = q.stats
    assert stats["count"] == 0

    await q.put(item)
    stats = q.stats
    assert stats["count"] == 1
    assert stats["raw_size_bytes"] > 0

    if not isinstance(compressor, NullCompressor):
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
    else:
        assert stats["raw_size_bytes"] == stats["compressed_size_bytes"]

    await q.get()
    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0


@pytest.mark.asyncio
async def test_concurrent_stats_access(compressor: Compressor):
    q: AsyncCompressedQueue = AsyncCompressedQueue(compressor=compressor)

    async def put_items():
        for _ in range(100):
            await q.put("test_item")

    async def read_stats():
        for _ in range(100):
            stats = q.stats
            assert stats["count"] >= 0
            assert stats["raw_size_bytes"] >= 0
            assert stats["compressed_size_bytes"] >= 0

    await asyncio.gather(put_items(), read_stats())


# --- Concurrency Tests ---
@pytest.mark.asyncio
async def test_join_functionality(compressor: Compressor):
    """Test that join() waits for all tasks to be done."""
    q: AsyncCompressedQueue = AsyncCompressedQueue(
        maxsize=10, compressor=compressor
    )

    async def consumer():
        while True:
            await q.get()
            q.task_done()

    for i in range(10):
        await q.put(f"item_{i}")

    # Start a few consumers
    consumer_tasks = [asyncio.create_task(consumer()) for _ in range(3)]

    await q.join()  # This should block until the queue is empty

    # Clean up consumer tasks
    for task in consumer_tasks:
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(*consumer_tasks)

    assert q.empty()


def test_init_outside_of_loop():
    """Test that instantiating raises RuntimeError if no loop is running."""
    with pytest.raises(
        RuntimeError,
        match="must be initialized within a running asyncio event loop",
    ):
        # This needs to be run in a context where there is no event loop.
        # Pytest-asyncio provides one, so we need to be creative.
        # The easiest way is to try to get a loop inside the constructor call.
        q = AsyncCompressedQueue()

        # The error is raised on first async operation
        async def run():
            await q.put(1)

        # Try to run in a new event loop, but the queue was created outside
        with pytest.raises(RuntimeError):
            asyncio.run(run())
