import asyncio, concurrent.futures

async def main():
    print('entering main')
    synchronous_property()
    print('exiting main')

pool = concurrent.futures.ThreadPoolExecutor()

def synchronous_property():
    print('entering synchronous_property')
    result = pool.submit(asyncio.run, asynchronous()).result()
    print('exiting synchronous_property', result)

async def asynchronous():
    print('entering asynchronous')
    await asyncio.sleep(10)
    print('exiting asynchronous')
    return 42

asyncio.run(main())