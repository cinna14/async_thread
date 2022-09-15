import requests
import asyncio
from typing import List, Dict
import time
import os



def fetch(url: str) -> Dict:
	""" Make a HTTP Get request """
	start = time.monotonic()
	res = requests.get(url)
	request_time = time.monotonic() - start

	return {"url": url, "status_code": res.status_code, "request_time": request_time}


async def worker(name: str, queue, results: List):
	""" process the work asynchorouly """
	loop = asyncio.get_running_loop()

	while True:
		url = await queue.get()
		if os.getenv('DEBUG', True):
			
			# loop.set_debug()
			print(f"{name} --> Fetching {url}")

		future_results = loop.run_in_executor(None, fetch, url)
		result = await future_results
		# 1 print('default thread pool', result)

		# 2. Run in a custom thread pool:
	    # with concurrent.futures.ThreadPoolExecutor() as pool:
	    #     result = await loop.run_in_executor(
	    #         pool, blocking_io)
	    #     print('custom thread pool', result)

	    # 3. Run in a custom process pool:
	    # with concurrent.futures.ProcessPoolExecutor() as pool:
	    #     result = await loop.run_in_executor(
	    #         pool, cpu_bound)
	    #     print('custom process pool', result)

		results.append(result)
		queue.task_done()


async def distribute_work(urls: List, results: List):
    queue = asyncio.Queue()

    # put item in queue
    for url in urls:
    	queue.put_nowait(url)

    # create a task for each item in queue
    tasks = []
    for i in range(len(urls)):
    	task = asyncio.create_task(worker(f"Worker-{i+1}", queue, results))
    	tasks.append(task)


    start_time = time.monotonic()
    # wait until all the items in queue get processed
    await queue.join()
    total_time = time.monotonic() - start_time


    # cancel all the tasks which are still running
    for task in tasks:
    	task.cancel()


    print("-----------------")
    print(f"Concurrency: {len(urls)} workers, Total tasks: {len(urls)}, Total Time: {total_time:.2f} secs")



def run(urls: List):
	""" Entry point """

	results = []
	asyncio.run(distribute_work(urls, results))

	print(results)




if __name__ == '__main__':
	run(["http://www.google.com", "http://twitter.com", "http://instagram.com", "http://facebook.com"])