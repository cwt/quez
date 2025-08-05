"""
Tests for the asynchronous, asyncio-compatible AsyncCompressedDeque.
"""

import random
import string
import pytest
import pytest_asyncio

from quez import AsyncCompressedDeque
from quez.compressors import (
    Bz2Compressor,
    Compressor,
    LzmaCompressor,
    NullCompressor,
    ZlibCompressor,
)

# --- Fixtures ---
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
    return request.param


# --- Core Functionality ---
@pytest.mark.asyncio
async def test_append_pop_roundtrip(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)
    item = {"data": "test_data"}
    await q.append(item)
    retrieved = await q.pop()
    assert retrieved == item


@pytest.mark.asyncio
async def test_appendleft_popleft_roundtrip(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)
    item = {"data": "test_data"}
    await q.appendleft(item)
    retrieved = await q.popleft()
    assert retrieved == item


@pytest.mark.asyncio
async def test_fifo_ordering(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)
    items = [f"item-{i}" for i in range(5)]
    for item in items:
        await q.append(item)

    retrieved = [await q.popleft() for _ in items]
    assert retrieved == items


@pytest.mark.asyncio
async def test_lifo_ordering(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)
    items = [f"item-{i}" for i in range(5)]
    for item in items:
        await q.append(item)

    retrieved = [await q.pop() for _ in items]
    assert retrieved == items[::-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("item", TEST_ITEMS)
async def test_various_data_types(compressor: Compressor, item):
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)
    await q.append(item)
    assert await q.pop() == item


# --- Queue Properties ---
@pytest.mark.asyncio
async def test_qsize_empty_full(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(
        maxsize=2, compressor=compressor
    )
    assert q.qsize() == 0
    assert q.empty()
    assert not q.full()

    await q.append("item1")
    assert q.qsize() == 1
    assert not q.empty()
    assert not q.full()

    await q.append("item2")
    assert q.qsize() == 2
    assert not q.empty()
    assert q.full()

    await q.pop()
    assert q.qsize() == 1
    assert not q.full()


# --- Stats ---
@pytest.mark.asyncio
async def test_stats_calculation(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)
    item = "test_item_for_stats" * 10

    stats = q.stats
    assert stats["count"] == 0

    await q.append(item)
    stats = q.stats
    assert stats["count"] == 1
    assert stats["raw_size_bytes"] > 0

    if not isinstance(compressor, NullCompressor):
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
    else:
        assert stats["raw_size_bytes"] == stats["compressed_size_bytes"]

    await q.pop()
    stats = q.stats
    assert stats["count"] == 0


# --- Eviction Logic ---
@pytest.mark.asyncio
async def test_eviction_stats(compressor: Compressor):
    q: AsyncCompressedDeque = AsyncCompressedDeque(
        maxsize=2, compressor=compressor
    )

    item1 = "".join(random.choices(string.ascii_lowercase, k=1000))
    item2 = "".join(random.choices(string.ascii_lowercase, k=1500))
    item3 = "".join(random.choices(string.ascii_lowercase, k=2000))

    from quez.compressors import PickleSerializer

    serializer = PickleSerializer()

    raw1 = len(serializer.dumps(item1))
    compressed1 = len(compressor.compress(serializer.dumps(item1)))
    raw2 = len(serializer.dumps(item2))
    compressed2 = len(compressor.compress(serializer.dumps(item2)))
    raw3 = len(serializer.dumps(item3))
    compressed3 = len(compressor.compress(serializer.dumps(item3)))

    if not isinstance(compressor, NullCompressor):
        assert len({compressed1, compressed2, compressed3}) == 3

    await q.append(item1)
    stats = q.stats
    assert stats["count"] == 1
    assert stats["raw_size_bytes"] == raw1
    assert stats["compressed_size_bytes"] == compressed1

    await q.append(item2)
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw1 + raw2

    await q.append(item3)  # Evicts item1
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw2 + raw3

    assert await q.popleft() == item2
    assert await q.popleft() == item3
    assert q.empty()


@pytest.mark.asyncio
async def test_appendleft_eviction_stats_async(compressor: Compressor):
    """Test eviction when appendleft evicts the rightmost item in async deque."""
    q: AsyncCompressedDeque = AsyncCompressedDeque(
        maxsize=2, compressor=compressor
    )

    item1 = "".join(random.choices(string.ascii_lowercase, k=1000))
    item2 = "".join(random.choices(string.ascii_lowercase, k=1500))
    item3 = "".join(random.choices(string.ascii_lowercase, k=2000))

    from quez.compressors import PickleSerializer

    serializer = PickleSerializer()

    raw1 = len(serializer.dumps(item1))
    raw2 = len(serializer.dumps(item2))
    raw3 = len(serializer.dumps(item3))
    compressed1 = len(compressor.compress(serializer.dumps(item1)))
    compressed2 = len(compressor.compress(serializer.dumps(item2)))
    compressed3 = len(compressor.compress(serializer.dumps(item3)))

    if not isinstance(compressor, NullCompressor):
        assert len({compressed1, compressed2, compressed3}) == 3

    await q.append(item1)
    await q.append(item2)
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw1 + raw2

    await q.appendleft(item3)  # Evicts item2
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw1 + raw3

    assert await q.pop() == item1
    assert await q.pop() == item3
    assert q.empty()


@pytest.mark.asyncio
async def test_pop_popleft_empty_async(compressor: Compressor):
    """Ensure popping from an empty AsyncCompressedDeque raises IndexError."""
    q: AsyncCompressedDeque = AsyncCompressedDeque(compressor=compressor)

    with pytest.raises(IndexError, match="pop from an empty deque"):
        await q.pop()

    with pytest.raises(IndexError, match="pop from an empty deque"):
        await q.popleft()
