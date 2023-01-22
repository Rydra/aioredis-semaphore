===============
aioredis-semaphore
===============


A distributed semaphore and mutex built on Redis.


Installation
------------
To install aioredis-semaphore, simply::

    pip install aioredis-semaphore


Examples
--------

::

    # -*- coding:utf-8 -*-
    import anyio
    from aioredis import Redis
    from anyio import create_task_group, run

    from aioredis_semaphore import Semaphore

    semaphore = Semaphore(Redis(), count=2, namespace="example")


    async def task(i: int) -> None:
        async with semaphore:
            print("id: {}".format(i))
            print("sleep...")
            await anyio.sleep(2)


    async def main() -> None:
        async with create_task_group() as tg:
            for i in range(5):
                tg.start_soon(task, i)


    if __name__ == "__main__":
        run(main)
