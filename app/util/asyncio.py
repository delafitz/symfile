"""Asyncio helpers that work in both sync and
async-host contexts (e.g., CLI vs. uvicorn).

`asyncio.run()` raises if a loop is already running.
`run_coro()` detects this and falls back to a worker
thread, which gets its own fresh event loop.
"""

import asyncio
import concurrent.futures
from typing import Any, Coroutine


def run_coro(coro: Coroutine[Any, Any, Any]) -> Any:
    """Run an awaitable to completion from sync code.

    No running loop → asyncio.run(coro).
    Inside a running loop (uvicorn lifespan, etc.)
    → run in a worker thread with its own loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=1
    ) as pool:
        return pool.submit(
            asyncio.run, coro
        ).result()
