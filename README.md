# Quez

**Quez** is a high-performance, memory-efficient library providing pluggable, compressed queues and deques for buffering data in both synchronous and asynchronous Python applications.

This library excels at managing large volumes of in-memory data, making it perfect for streaming data pipelines, logging systems, or high-throughput servers. It transparently compresses objects as they enter the data structure and decompresses them upon retrieval, slashing the memory footprint of in-flight data while maintaining a simple, familiar interface.

### **Key Features**

* **Flexible Data Structures**: Provides both FIFO (Queue) and Deque (double-ended queue) implementations to support a variety of access patterns.
* **Dual Sync and Async Interfaces**: Offers thread-safe quez.CompressedQueue and quez.CompressedDeque for multi-threaded applications, alongside quez.AsyncCompressedQueue and quez.AsyncCompressedDeque for asyncio.
* **Pluggable Compression Strategies**: Includes built-in support for zlib (default), bz2, and lzma, with optional zstd and lzo. The flexible architecture lets you plug in custom compression, serialization, or encryption algorithms.
* **Real-Time Observability**: Track performance with the .stats property, which reports item count, raw and compressed data sizes, and live compression ratio.
* **Optimized for Performance**: In the asyncio versions, CPU-intensive compression and decompression tasks run in a background thread pool, keeping the event loop responsive.
* **Memory Efficiency**: Handles large, temporary data bursts without excessive memory usage, preventing swapping and performance degradation.

## Installation

You can install the core library from PyPI:

    pip install quez

To enable optional, high-performance compression backends, you can install them as extras. For example, to install with zstd support:

    pip install quez[zstd]

Or install with all optional compressors:

    pip install quez[all]

Available extras:

* zstd: Enables the ZstdCompressor.
* lzo: Enables the LzoCompressor.

## **Quick Start**

Here's a quick example of using CompressedQueue to compress and store a random string:

```pycon
>>> import random
>>> import string
>>> from quez import CompressedQueue
>>> data = ''.join(random.choices(string.ascii_letters + string.digits, k=100)) * 10
>>> len(data)
1000
>>> q = CompressedQueue()  # Initialize the Queue with default ZlibCompressor
>>> q.put(data)
>>> q.stats
{'count': 1, 'raw_size_bytes': 1018, 'compressed_size_bytes': 131, 'compression_ratio_pct': 87.13163064833006}
>>> data == q.get()
True
>>> q.stats
{'count': 0, 'raw_size_bytes': 0, 'compressed_size_bytes': 0, 'compression_ratio_pct': None}
```

## Usage

### Synchronous Queue

Use CompressedQueue in standard multi-threaded Python applications.

```python
from quez import CompressedQueue
from quez.compressors import LzmaCompressor

# Use a different compressor for higher compression
q = CompressedQueue(compressor=LzmaCompressor())

# The API is the same as the standard queue.Queue
q.put({"data": "some important data"})
item = q.get()
q.task_done()
q.join()
```

### Asynchronous Queue

Use AsyncCompressedQueue in asyncio applications. The API mirrors asyncio.Queue.

```python
import asyncio
from quez import AsyncCompressedQueue
from quez.compressors import ZstdCompressor # Requires `pip install quez[zstd]`

async def main():
    # Using the high-speed Zstd compressor
    q = AsyncCompressedQueue(compressor=ZstdCompressor())

    await q.put({"request_id": "abc-123", "payload": "..."})
    item = await q.get()
    q.task_done()
    await q.join()
    print(item)

asyncio.run(main())
```

### Synchronous & Asynchronous Deque

For use cases requiring efficient appends and pops from both ends (LIFO and FIFO), use CompressedDeque and AsyncCompressedDeque. Their interfaces are similar to collections.deque.

**Synchronous Deque (CompressedDeque)**

```python
from quez import CompressedDeque

# Deques support adding/removing from both ends
d = CompressedDeque(maxsize=5)

d.append("item-at-right")      # Add to the right
d.appendleft("item-at-left") # Add to the left

# Items are still compressed
print(d.stats)

# Retrieve from both ends
print(d.popleft()) # "item-at-left"
print(d.pop())     # "item-at-right"
```

**Asynchronous Deque (AsyncCompressedDeque)**

```python
import asyncio
from quez import AsyncCompressedDeque

async def main():
    d = AsyncCompressedDeque(maxsize=5)

    await d.append("item-at-right")
    await d.appendleft("item-at-left")

    print(d.stats)

    print(await d.popleft()) # "item-at-left"
    print(await d.pop())     # "item-at-right"

asyncio.run(main())
```

### Extensibility

You can easily provide your own custom serializers or compressors. Any object that conforms to the Serializer or Compressor protocol can be used.

**Example: Custom JSON Serializer**

```python
import json
from quez import CompressedQueue

class JsonSerializer:
    def dumps(self, obj):
        # Serialize to JSON and encode to bytes
        return json.dumps(obj).encode('utf-8')

    def loads(self, data):
        # Decode from bytes and parse JSON
        return json.loads(data.decode('utf-8'))

# Now, use it with the queue
json_queue = CompressedQueue(serializer=JsonSerializer())

json_queue.put({"message": "hello world"})
data = json_queue.get()
print(data) # {'message': 'hello world'}
```

## A Note on Performance & Overhead

**Compression Overhead:** Keep in mind that compression algorithms have overhead. For very small or highly random data payloads (e.g., under 100-200 bytes), the compressed output might occasionally be slightly larger than the original. The memory-saving benefits of quez are most significant when dealing with larger objects or data with repeating patterns.
