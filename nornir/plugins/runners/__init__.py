import asyncio

from typing import List
from concurrent.futures import ThreadPoolExecutor

from nornir.core.task import AggregatedResult, Task, AsyncTask
from nornir.core.inventory import Host


class SerialRunner:
    """
    SerialRunner runs the task over each host one after the other without any parellelization
    """

    def __init__(self) -> None:
        pass

    def run(self, task: Task, hosts: List[Host]) -> AggregatedResult:
        result = AggregatedResult(task.name)
        for host in hosts:
            result[host.name] = task.copy().start(host)
        return result


class ThreadedRunner:
    """
    ThreadedRunner runs the task over each host using threads

    Arguments:
        num_workers: number of threads to use
    """

    def __init__(self, num_workers: int = 20) -> None:
        self.num_workers = num_workers

    def run(self, task: Task, hosts: List[Host]) -> AggregatedResult:
        result = AggregatedResult(task.name)
        futures = []
        with ThreadPoolExecutor(self.num_workers) as pool:
            for host in hosts:
                future = pool.submit(task.copy().start, host)
                futures.append(future)

        for future in futures:
            worker_result = future.result()
            result[worker_result.host.name] = worker_result
        return result


class CoroutineThreadedRunner:
    """
    CoroutineThreadedRunner runs the task over each host using threads or coroutine

    Arguments:
        num_workers: number of threads to use
    """

    def __init__(self, num_workers: int = 20) -> None:
        self.num_workers = num_workers

    async def run(self, task: Task, hosts: List[Host]) -> AggregatedResult:
        result = AggregatedResult(task.name)
        futures = []

        async def execute_task_in_pool(task, limit, *args, **kwargs):
            async with limit:
                return await task(*args, **kwargs)

        if isinstance(task, AsyncTask):
            tasks = []

            # Use a Semaphore to limit the number of coroutine running at the same time
            limit = asyncio.Semaphore(self.num_workers)

            for host in hosts:
                new_task = task.copy()
                tasks.append(
                    asyncio.create_task(
                        execute_task_in_pool(new_task.start, limit, host)
                    )
                )

            for completed_task in asyncio.as_completed(tasks):
                response = await completed_task
                result[response.host.name] = response

        else:
            with ThreadPoolExecutor(self.num_workers) as pool:
                for host in hosts:
                    future = pool.submit(task.copy().start, host)
                    futures.append(future)

            for future in futures:
                worker_result = future.result()
                result[worker_result.host.name] = worker_result

        return result
