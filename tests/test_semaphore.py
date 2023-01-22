# -*- coding:utf-8 -*-
from __future__ import absolute_import

import os

import pytest
from aioredis import StrictRedis
from hamcrest import *

from aioredis_semaphore import Semaphore


@pytest.fixture()
def anyio_backend():
    return "asyncio"


class TestSemaphore:
    @pytest.fixture
    async def available_semaphores(self, anyio_backend):
        redis_host = os.environ.get("TEST_REDIS_HOST") or "localhost"
        client = StrictRedis(host=redis_host)
        s_count = 2
        sem1 = Semaphore(client=client, count=s_count)
        await sem1.reset()
        sem2 = Semaphore(client=client, count=s_count)
        await sem2.reset()
        sem3 = Semaphore(client=client, count=s_count, blocking=False)
        await sem3.reset()

        return {"s_count": s_count, "sem1": sem1, "sem2": sem2, "sem3": sem3}

    async def test_lock(self, anyio_backend, available_semaphores):
        sem1 = available_semaphores["sem1"]
        sem2 = available_semaphores["sem2"]
        available_slots = available_semaphores["s_count"]
        assert_that(await sem1.available_count(), is_(available_slots))
        assert_that(await sem1.acquire(), is_not(none()))
        assert_that(await sem1.available_count(), is_(available_slots - 1))
        await sem1.release()
        assert_that(await sem1.available_count(), is_(available_slots))

        assert_that(await sem2.available_count(), is_(available_slots))
        assert_that(await sem1.acquire(), is_not(none()))
        assert_that(await sem2.available_count(), is_(available_slots - 1))
        await sem1.release()

    async def test_with(self, anyio_backend, available_semaphores):
        sem1 = available_semaphores["sem1"]
        available_slots = available_semaphores["s_count"]

        assert await sem1.available_count() == available_slots
        async with sem1 as sem:
            assert await sem.available_count() == (available_slots - 1)
            async with sem:
                assert await sem.available_count() == (available_slots - 2)
        assert await sem1.available_count() == available_slots

    async def test_create_with_existing(self, anyio_backend, available_semaphores):
        sem1 = available_semaphores["sem1"]
        sem2 = available_semaphores["sem2"]

        async with sem1:
            async with sem2:
                assert await sem1.available_count() == 0
                assert await sem2.available_count() == 0

    async def test_nonblocking(self, anyio_backend, available_semaphores):
        sem3 = available_semaphores["sem3"]
        available_slots = available_semaphores["s_count"]

        from aioredis_semaphore import NotAvailable

        for _ in range(available_slots):
            await sem3.acquire()
        assert_that(await sem3.available_count(), is_(0))

        captured_exception = None
        try:
            async with sem3:
                pass
        except Exception as e:
            captured_exception = e

        assert_that(captured_exception, is_(NotAvailable))

    async def test_acquire_with_timeout(self, anyio_backend, available_semaphores):
        sem1 = available_semaphores["sem1"]
        available_slots = available_semaphores["s_count"]

        from aioredis_semaphore import NotAvailable

        for _ in range(available_slots):
            await sem1.acquire()

        captured_exception = None
        try:
            await sem1.acquire(timeout=1)
        except Exception as e:
            captured_exception = e

        assert_that(captured_exception, is_(NotAvailable))


if __name__ == "__main__":
    from os.path import dirname, abspath
    import sys
    import unittest

    d = dirname
    current_path = d(d(abspath(__file__)))
    sys.path.append(current_path)
    unittest.main()
