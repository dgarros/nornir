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

        if isinstance(task, AsyncTask):
            tasks = []

            for host in hosts:
                new_task = task.copy()
                tasks.append(new_task.start(host))

            responses = await asyncio.gather(*tasks)

            for response in responses:
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
