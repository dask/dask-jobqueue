import asyncio
import sys
import os
import signal
from contextlib import suppress
from enum import Enum
from typing import Dict, Optional
import warnings
from tornado.ioloop import IOLoop

from distributed.core import CommClosedError, Status, rpc
from distributed.scheduler import Scheduler
from distributed.utils import LoopRunner, import_term, SyncMethodMixin
from distributed.worker import Worker


# Close gracefully when receiving a SIGINT
signal.signal(signal.SIGINT, lambda *_: sys.exit())


class Role(Enum):
    """
    This Enum contains the various roles processes can be.
    """

    worker = "worker"
    scheduler = "scheduler"
    client = "client"


class BaseRunner(SyncMethodMixin):
    """Superclass for runner objects.

    This class contains common functionality for Dask cluster runner classes.

    To implement this class, you must provide

    1.  A ``get_role`` method which returns a role from the ``Role`` enum.
    2.  Optionally, a ``set_scheduler_address`` method for the scheduler process to communicate its address.
    3.  A ``get_scheduler_address`` method for all other processed to recieve the scheduler address.
    4.  Optionally, a ``get_worker_name`` to provide a platform specific name to the workers.
    5.  Optionally, a ``before_scheduler_start`` to perform any actions before the scheduler is created.
    6.  Optionally, a ``before_worker_start`` to perform any actions before the worker is created.
    7.  Optionally, a ``before_client_start`` to perform any actions before the client code continues.
    8.  Optionally, a ``on_scheduler_start`` to perform anything on the scheduler once it has started.
    9.  Optionally, a ``on_worker_start`` to perform anything on the worker once it has started.

    For that, you should get the following:

    A context manager and object which can be used within a script that is run in parallel to decide which processes
    run the scheduler, workers and client code.

    """

    __loop: Optional[IOLoop] = None

    def __init__(
        self,
        scheduler: bool = True,
        scheduler_options: Dict = None,
        worker_class: str = None,
        worker_options: Dict = None,
        client: bool = True,
        asynchronous: bool = False,
        loop: asyncio.BaseEventLoop = None,
    ):
        self.status = Status.created
        self.scheduler = scheduler
        self.scheduler_address = None
        self.scheduler_comm = None
        self.client = client
        if self.client and not self.scheduler:
            raise RuntimeError("Cannot run client code without a scheduler.")
        self.scheduler_options = (
            scheduler_options if scheduler_options is not None else {}
        )
        self.worker_class = (
            Worker if worker_class is None else import_term(worker_class)
        )
        self.worker_options = worker_options if worker_options is not None else {}
        self.role = None
        self.__asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)

        if not self.__asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def get_role(self) -> str:
        raise NotImplementedError()

    async def set_scheduler_address(self, scheduler: Scheduler) -> None:
        raise None

    async def get_scheduler_address(self) -> str:
        raise NotImplementedError()

    async def get_worker_name(self) -> str:
        return None

    async def before_scheduler_start(self) -> None:
        return None

    async def before_worker_start(self) -> None:
        return None

    async def before_client_start(self) -> None:
        return None

    async def on_scheduler_start(self, scheduler: Scheduler) -> None:
        return None

    async def on_worker_start(self, worker: Worker) -> None:
        return None

    @property
    def loop(self) -> Optional[IOLoop]:
        loop = self.__loop
        if loop is None:
            # If the loop is not running when this is called, the LoopRunner.loop
            # property will raise a DeprecationWarning
            # However subsequent calls might occur - eg atexit, where a stopped
            # loop is still acceptable - so we cache access to the loop.
            self.__loop = loop = self._loop_runner.loop
        return loop

    @loop.setter
    def loop(self, value: IOLoop) -> None:
        warnings.warn(
            "setting the loop property is deprecated", DeprecationWarning, stacklevel=2
        )
        if value is None:
            raise ValueError("expected an IOLoop, got None")
        self.__loop = value

    def __await__(self):
        async def _await():
            if self.status != Status.running:
                await self._start()
            return self

        return _await().__await__()

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, *args):
        await self._close()

    def __enter__(self):
        return self.sync(self.__aenter__)

    def __exit__(self, typ, value, traceback):
        return self.sync(self.__aexit__)

    def __del__(self):
        with suppress(AttributeError, RuntimeError):  # during closing
            self.loop.add_callback(self.close)

    async def _start(self) -> None:
        self.role = await self.get_role()
        if self.role == Role.scheduler:
            await self.start_scheduler()
            os.kill(
                os.getpid(), signal.SIGINT
            )  # Shutdown with a signal to give the event loop time to close
        elif self.role == Role.worker:
            await self.start_worker()
            os.kill(
                os.getpid(), signal.SIGINT
            )  # Shutdown with a signal to give the event loop time to close
        elif self.role == Role.client:
            self.scheduler_address = await self.get_scheduler_address()
            if self.scheduler_address:
                self.scheduler_comm = rpc(self.scheduler_address)
            await self.before_client_start()
        self.status = Status.running

    async def start_scheduler(self) -> None:
        await self.before_scheduler_start()
        async with Scheduler(**self.scheduler_options) as scheduler:
            await self.set_scheduler_address(scheduler)
            await self.on_scheduler_start(scheduler)
            await scheduler.finished()

    async def start_worker(self) -> None:
        if (
            "scheduler_file" not in self.worker_options
            and "scheduler_ip" not in self.worker_options
        ):
            self.worker_options["scheduler_ip"] = await self.get_scheduler_address()
        worker_name = await self.get_worker_name()
        await self.before_worker_start()
        async with self.worker_class(name=worker_name, **self.worker_options) as worker:
            await self.on_worker_start(worker)
            await worker.finished()

    async def _close(self) -> None:
        if self.status == Status.running:
            if self.scheduler_comm:
                with suppress(CommClosedError):
                    await self.scheduler_comm.terminate()
            self.status = Status.closed

    def close(self) -> None:
        return self.sync(self._close)


class AsyncCommWorld:
    def __init__(self):
        self.roles = {"scheduler": None, "client": None}
        self.role_lock = asyncio.Lock()
        self.scheduler_address = None


class AsyncRunner(BaseRunner):
    def __init__(self, commworld: AsyncCommWorld, *args, **kwargs):
        self.commworld = commworld
        super().__init__(*args, **kwargs)

    async def get_role(self) -> str:
        async with self.commworld.role_lock:
            if self.commworld.roles["scheduler"] is None and self.scheduler:
                self.commworld.roles["scheduler"] = self
                return Role.scheduler
            elif self.commworld.roles["client"] is None and self.client:
                self.commworld.roles["client"] = self
                return Role.client
            else:
                return Role.worker

    async def set_scheduler_address(self, scheduler: Scheduler) -> None:
        self.commworld.scheduler_address = scheduler.address

    async def get_scheduler_address(self) -> str:
        while self.commworld.scheduler_address is None:
            await asyncio.sleep(0.1)
        return self.commworld.scheduler_address
