
import argparse
import logging
import asyncio
import datetime
import logging
import json
from typing import Awaitable, Callable
from icmplib import async_ping
from datetime import datetime
from functools import wraps, partial


class ping_parameter:
    def __init__(self, address:str, rate:int, duration:int, retry_count:int, retry_interval:float):
        self.address = address
        self.rate = rate
        self.duration = duration
        self.retry_count = retry_count
        self.retry_interval = retry_interval

class Scheduler:
    def __init__(self, callback:Callable[[type], Awaitable[None]]):
        self.__queue = asyncio.Queue()
        self.__completed_queue = asyncio.Queue()
        self.__logger = logging.getLogger(__name__)
        self.__callback = callback
        self.__event = asyncio.Event()
        self.__loop = asyncio.get_running_loop()
        self.__lock = asyncio.Lock()
        self.__main_task = None
        
    def start(self) -> "Scheduler":
        if not self.__main_task or self.__main_task.done():
            self.__event.set()
            self.__main_task = self.__loop.create_task(self.__asynciohandle())
        return self

    async def stop(self):
        try:
            self.__event.clear()  
            async with self.__lock:
                if self.__queue.qsize() > 0:
                    tasks = [await self.__queue.get() for _ in range(self.__queue.qsize())]
                    await asyncio.gather(*tasks, return_exceptions=True) 
            if self.__main_task:
                await self.__main_task 
        except Exception as e:
            self.__logger.error("Scheduler stop function encountered an error: {0}".format(e))

    async def add_task(self, execution :Awaitable, *args, **kwargs):
         async with self.__lock:
            wrapped_execution = self.__wrap_execution(execution, *args, **kwargs)
            await self.__queue.put(wrapped_execution) 

    async def __asynciohandle(self):
        while self.__event.is_set():
            try:
                if not self.__queue.empty():
                    async with self.__lock:
                        while not self.__queue.empty():
                            execution = await self.__queue.get()
                            task = self.__loop.create_task(execution())
                            task.add_done_callback(lambda t: self.__completed_queue.put_nowait(t))

                if not self.__completed_queue.empty():
                    completed_task = await self.__completed_queue.get()
                    try:
                        result = await completed_task
                        await self.__callback(result)
                    except Exception as e:
                        self.__logger.error("Task execution failed: {0}".format(e))
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                self.__logger.error("Scheduler __asynciohandle function encountered an error: {0}".format(e))
        
                    
    def __wrap_execution(self, execution: Awaitable, *args, **kwargs): 
        async def wrapper():
            return await execution(*args, **kwargs)
        return wrapper
        
class TestHandler:
    def __init__(self, parameters:"list[ping_parameter]"):
        self.__parameters = parameters
        self.__result = {}
        self.__event = asyncio.Event()
        self.__event.set()
        self.__logger = logging.getLogger(__name__)
        self.__scheduler = Scheduler(self.__SchedulerCallBack).start()
        self.__loop = asyncio.get_running_loop()
        self.__loop.create_task(self.__create_test_task(parameters))
        self.__start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    async def __create_test_task(self, parameters:list[ping_parameter]):
        
        for parameter in parameters:
           await self.__scheduler.add_task(is_alive, parameter)

    def __handleformat(self, rate:str):
        output = {"start_time":self.__start_time, 
                  "end_time":self.__end_time,
                  "rate":str(rate) + " packets/sec",
                  "results":self.__result}
        self.__logger.info(json.dumps(output, indent = 2))
 
    async def __SchedulerCallBack(self, response:type):
        if isinstance(response, dict):
            parameter = next((p for p in self.__parameters if p.address == response["address"]), None)
            if not parameter:
                return
            if response["packet_loss"] > 0:
                if parameter.retry_count > 0:
                    parameter.retry_count -= 1
                    self.__loop.create_task(self.__create_test_task([parameter]))
                    return
            self.__logger.debug("{0} done.".format(response["address"]))
            self.__result[response["address"]] = {
                                                "RTT_samples":response["rtts"],
                                                "RTT_avg":str(response["avg_rtt"]) + "ms",
                                                "RTT_max":str(response["max_rtt"]) + "ms",
                                                "packet_loss":str(response["packet_loss"]) + "%"
                                                }
            self.__parameters.remove(parameter)
            if len(self.__parameters) == 0:
                self.__event.clear()
                self.__end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.__handleformat(parameter.rate)


    async def release(self):
        while self.__event.is_set():
            await asyncio.sleep(1)
        await self.__scheduler.stop()

async def is_alive(parameter:ping_parameter) -> dict:
    host = await async_ping(parameter.address, interval = 1 / parameter.rate, timeout = parameter.duration)
    return {
                "address":parameter.address, 
                "rtts":host.rtts,
                "avg_rtt":host.avg_rtt,
                "max_rtt":host.max_rtt,
                "packet_loss":host.packet_loss
            }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets",        type=str, nargs="+", required=True, help="IP")
    parser.add_argument("--duration",       type=int, required=False, help="duration")
    parser.add_argument("--rate",           type=int, required=False, help="rate")
    parser.add_argument("--retry-count",    type=int, required=False, help="retry-count")
    parser.add_argument("--retry-interval", type=int, required=False, help="retry-interval")
    parser.add_argument("-v", "--verbose",  action="store_true", required=False, help="verbose logging mode")
    args = parser.parse_args()

    targets = args.targets
    duration = args.duration if args.duration else 60
    rate = args.rate if args.rate else 10
    retry_count = args.retry_count if args.retry_count else 3
    retry_interval = args.retry_interval if args.retry_interval else 2
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s  [%(levelname)s]: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger(__name__)
    file_handler = logging.FileHandler("Log.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    parameters = []
    for target in targets:
        parameters.append(ping_parameter(target,rate,duration,retry_count,retry_interval))
    handler = TestHandler(parameters)
    await handler.release()
    
if __name__ == "__main__":
    asyncio.run(main())